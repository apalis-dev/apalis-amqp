#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(
    clippy::await_holding_lock,
    clippy::cargo_common_metadata,
    clippy::dbg_macro,
    clippy::empty_enum,
    clippy::enum_glob_use,
    clippy::inefficient_to_string,
    clippy::mem_forget,
    clippy::mutex_integer,
    clippy::needless_continue,
    clippy::todo,
    clippy::unimplemented,
    clippy::wildcard_imports,
    future_incompatible,
    missing_docs,
    missing_debug_implementations,
    unreachable_pub
)]

mod ack;
mod sink;
use apalis_codec::json::JsonCodec;
use apalis_core::{
    backend::{codec::Codec, queue::Queue, Backend, BackendExt, TaskStream},
    task::{builder::TaskBuilder, task_id::TaskId, Task},
    worker::{context::WorkerContext, ext::ack::AcknowledgeLayer},
};
use deadpool_lapin::{Manager, Pool};
use futures::{
    stream::{self, BoxStream},
    Stream, StreamExt, TryStreamExt,
};
use lapin::{
    message::Delivery,
    options::{BasicConsumeOptions, QueueDeclareOptions},
    types::FieldTable,
    Channel, ConnectionProperties, Error, ErrorKind,
};
use pin_project::pin_project;
use std::{
    io::{self},
    marker::PhantomData,
    str::FromStr,
    sync::Arc,
};
use utils::{AmqpContext, Config, DeliveryTag};

/// Contains basic utilities for handling config and messages
pub mod utils;

/// Type alias for an AMQP task with context and u64 as the task ID type.
pub type AmqpTask<T> = Task<T, AmqpContext, u64>;

/// Type alias for an AMQP task ID with u64 as the ID type.
pub type AmqpTaskId = TaskId<u64>;

#[derive(Debug)]
/// A wrapper around a `lapin` AMQP channel that implements message queuing functionality.
#[pin_project]
pub struct AmqpBackend<M, Codec> {
    channel: Channel,
    queue: lapin::Queue,
    message_type: PhantomData<M>,
    config: Config,
    #[pin]
    sink: sink::AmqpSink<M, Codec>,
}

impl<M, C> Clone for AmqpBackend<M, C> {
    fn clone(&self) -> Self {
        Self {
            channel: self.channel.clone(),
            queue: self.queue.clone(),
            message_type: PhantomData,
            config: self.config.clone(),
            sink: self.sink.clone(),
        }
    }
}

impl<M: Send + 'static, C> Backend for AmqpBackend<M, C>
where
    C: Codec<M, Compact = Vec<u8>>,
    C::Error: std::error::Error + Send + Sync + 'static,
{
    type Args = M;
    type Error = Error;
    type Beat = BoxStream<'static, Result<(), Self::Error>>;
    type Layer = AcknowledgeLayer<Self>;
    type Stream = TaskStream<Task<M, AmqpContext, Self::IdType>, Self::Error>;
    type Context = AmqpContext;
    type IdType = u64;
    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        let channel = self.channel.clone();
        let worker = worker.clone();
        let config = self.config.clone();
        let stream = stream::unfold(
            (channel, worker, config),
            move |(channel, worker, config)| async move {
                apalis_core::timer::sleep(config.heartbeat_interval()).await;
                let is_connected = channel.status().connected();
                if !is_connected {
                    Some((
                        Err(ErrorKind::IOError(Arc::new(io::Error::new(
                            io::ErrorKind::NotConnected,
                            format!("Channel not connected for worker {}", worker.name()),
                        )))
                        .into()),
                        (channel, worker, config),
                    ))
                } else {
                    Some((Ok(()), (channel, worker, config)))
                }
            },
        );
        stream.boxed()
    }

    fn middleware(&self) -> Self::Layer {
        AcknowledgeLayer::new(self.clone())
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        self.poll_delivery(worker)
            .map(move |item| {
                let item = item?;
                let bytes = item.data;
                let tag = item.delivery_tag;

                let msg: M = C::decode(&bytes).map_err(|e| {
                    ErrorKind::IOError(Arc::new(io::Error::new(io::ErrorKind::InvalidData, e)))
                })?;

                let task = TaskBuilder::new(msg)
                    .with_task_id(TaskId::new(tag))
                    .with_ctx(AmqpContext::new(DeliveryTag::new(tag), item.properties))
                    .build();
                Ok(Some(task))
            })
            .boxed()
    }
}

impl<M, C: Send + 'static> BackendExt for AmqpBackend<M, C>
where
    Self: Backend<Args = M, IdType = u64, Context = AmqpContext, Error = lapin::Error>,
    C: Codec<M, Compact = Vec<u8>> + Send + 'static,
    C::Error: std::error::Error + Send + Sync + 'static,
    M: Send + 'static + Unpin,
{
    type Codec = C;
    type Compact = Vec<u8>;
    type CompactStream = TaskStream<AmqpTask<Self::Compact>, Error>;

    fn get_queue(&self) -> Queue {
        Queue::from_str(self.queue.name().as_str()).expect("Queue should be a string")
    }

    fn poll_compact(self, worker: &WorkerContext) -> Self::CompactStream {
        self.poll_delivery(worker)
            .map_ok(move |item| {
                let bytes = item.data;
                let tag = item.delivery_tag;

                let task = TaskBuilder::new(bytes)
                    .with_task_id(TaskId::new(tag))
                    .with_ctx(AmqpContext::new(DeliveryTag::new(tag), item.properties))
                    .build();
                Some(task)
            })
            .boxed()
    }
}

impl<M, C> AmqpBackend<M, C> {
    fn poll_delivery(
        self,
        worker: &WorkerContext,
    ) -> impl Stream<Item = Result<Delivery, Error>> + 'static {
        let channel = self.channel.clone();
        let worker = worker.clone();
        let config = self.config.clone();
        let worker_name = worker.name().to_string();

        let stream = stream::once(async move {
            let consumer = channel
                .basic_consume(
                    config.namespace().as_str(),
                    &worker_name,
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .await?;
            Ok::<_, Error>(consumer)
        })
        .try_flatten();
        stream
    }
}

impl<M: Send + 'static> AmqpBackend<M, ()> {
    /// Constructs a new instance of `AmqpBackend` from a `lapin` channel.
    pub fn new(channel: Channel, queue: lapin::Queue) -> AmqpBackend<M, JsonCodec<Vec<u8>>> {
        Self::new_with_config(channel, queue, Config::new(std::any::type_name::<M>()))
    }

    /// Constructs a new instance of `AmqpBackend` with a config
    pub fn new_with_config(
        channel: Channel,
        queue: lapin::Queue,
        config: Config,
    ) -> AmqpBackend<M, JsonCodec<Vec<u8>>> {
        AmqpBackend {
            sink: sink::AmqpSink::new(),
            channel,
            message_type: PhantomData,
            queue,
            config,
        }
    }

    /// Get a ref to the inner `Channel`
    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    /// Get a ref to the inner `Queue`
    pub fn queue(&self) -> &lapin::Queue {
        &self.queue
    }

    /// Get a ref to the inner `Config`
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Constructs a new instance of `AmqpBackend` from an address string.
    ///
    /// This function creates a `deadpool_lapin::Pool` and uses it to obtain a `lapin::Connection`.
    /// It then creates a channel from that connection.
    pub async fn new_from_addr<S: AsRef<str>>(
        addr: S,
    ) -> Result<AmqpBackend<M, JsonCodec<Vec<u8>>>, lapin::Error> {
        let manager = Manager::new(addr.as_ref(), ConnectionProperties::default());
        let pool: Pool = deadpool::managed::Pool::builder(manager)
            .max_size(10)
            .build()
            .map_err(|error| {
                lapin::ErrorKind::IOError(Arc::new(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    error,
                )))
            })?;
        let amqp_conn = pool.get().await.map_err(|error| {
            lapin::ErrorKind::IOError(Arc::new(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                error,
            )))
        })?;
        let channel = amqp_conn.create_channel().await?;
        let queue = channel
            .queue_declare(
                std::any::type_name::<M>(),
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await?;
        Ok(Self::new(channel, queue))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use apalis::prelude::{BoxDynError, EventListenerExt};
    use apalis_codec::json::JsonCodec;
    use apalis_core::{backend::TaskSink, worker::builder::WorkerBuilder};
    use apalis_workflow::{Workflow, WorkflowSink};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    struct TestMessage;

    async fn test_job(_job: TestMessage, ctx: WorkerContext) {
        ctx.stop().unwrap();
    }

    #[tokio::test]
    async fn basic_worker() {
        let env = std::env::var("AMQP_ADDR").unwrap();
        let mut backend: AmqpBackend<TestMessage, JsonCodec<Vec<u8>>> =
            AmqpBackend::new_from_addr(&env).await.unwrap();
        backend.push(TestMessage).await.unwrap();

        let worker = WorkerBuilder::new("rango-amigo")
            .backend(backend)
            .build(test_job);

        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn workflow() {
        let env = std::env::var("AMQP_ADDR").unwrap();
        let mut backend: AmqpBackend<Vec<u8>, JsonCodec<Vec<u8>>> =
            AmqpBackend::new_from_addr(&env).await.unwrap();

        let workflow = Workflow::new("odd-numbers-workflow")
            .and_then(|a: usize| async move { Ok::<_, BoxDynError>((0..a).collect::<Vec<_>>()) })
            // Cant do filter_map coz Amqp doesnt implement WaitForCompletion yet
            // .filter_map(|x| async move {
            //     if x % 2 != 0 {
            //         Some(x)
            //     } else {
            //         None
            //     }
            // })
            .and_then(|a: Vec<usize>, ctx: WorkerContext| async move {
                println!("Sum: {}", a.iter().sum::<usize>());
                ctx.stop().unwrap();
                Ok::<_, BoxDynError>(())
            });
        backend.push_start(10).await.unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|_ctx, ev| {
                println!("On Event = {:?}", ev);
            })
            .build(workflow);
        worker.run().await.unwrap();
    }
}
