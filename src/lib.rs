#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(
    clippy::await_holding_lock,
    clippy::cargo_common_metadata,
    clippy::dbg_macro,
    clippy::empty_enums,
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
    backend::{
        finalize::Durable, future::BoxSyncFuture, Backend, BackendConfig, WireFormatBackend,
    },
    task::{builder::TaskBuilder, task_id::TaskId, Task},
    timer::Delay,
    worker::{context::WorkerContext, ext::ack::AcknowledgeLayer},
};
use deadpool_lapin::Pool;
use futures::{FutureExt, StreamExt};
use lapin::{
    options::{ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions},
    types::{AMQPValue, FieldTable, ShortString},
    Channel, Consumer, ExchangeKind,
};
use pin_project::pin_project;
use std::{
    future::Future,
    pin::Pin,
    task::{ready, Context, Poll},
};

pub use crate::{config::Config, delivery_tag::DeliveryTag, error::Error};

mod config;
mod error;
mod metadata;

/// Contains basic utilities for handling config and messages
mod delivery_tag;

/// Type alias for an AMQP task with context and u64 as the task ID type.
pub type AmqpTask<Args = Vec<u8>> = Task<Args>;

/// Type alias for an AMQP task ID with u64 as the ID type.
pub type AmqpTaskId = TaskId;

/// Backend that implements message queuing functionality via `amqp://` protocol.
#[pin_project]
#[derive(Debug)]
pub struct AmqpBackend<M> {
    pool: Pool,
    #[pin]
    channel: Option<Channel>,
    consumer: Option<Consumer>,
    config: Config,
    #[pin]
    sink: sink::AmqpSink<M>,
    codec: JsonCodec,
    state: State,
    heartbeat_timer: Option<Delay>,
}

impl<M> Clone for AmqpBackend<M> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            channel: self.channel.clone(),
            consumer: self.consumer.clone(),
            config: self.config.clone(),
            sink: self.sink.clone(),
            codec: self.codec.clone(),
            state: State::Init,
            heartbeat_timer: None,
        }
    }
}

enum State {
    Init,
    DeclareQueue(BoxSyncFuture<Result<Channel, Error>>),
    DeclareConsumer(BoxSyncFuture<Result<Consumer, Error>>),
    HeartBeat(BoxSyncFuture<Result<(), Error>>),
    Ready,
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            State::Init => f.write_str("Init"),
            State::DeclareQueue(_) => f.write_str("DeclareQueue(..)"),
            State::DeclareConsumer(_) => f.write_str("DeclareConsumer(..)"),
            State::HeartBeat(_) => f.write_str("HeartBeat(..)"),
            State::Ready => f.write_str("Ready"),
        }
    }
}

impl<M: Send + 'static> AmqpBackend<M> {
    /// Builds the future that acquires a connection, declares the queue,
    /// sets QoS, and starts consuming.
    fn declare_queue(pool: Pool, config: Config) -> BoxSyncFuture<Result<Channel, Error>> {
        async move {
            let conn = pool.get().await?;
            let channel = conn.create_channel().await?;

            // Extract the base queue name
            let queue_name = config.queue.as_ref();

            if config.scheduling {
                // 1. Declare a DLX Exchange dedicated to this queue setup
                let dlx_exchange = format!("{}_dlx", queue_name);
                channel
                    .exchange_declare(
                        dlx_exchange.as_str().into(),
                        ExchangeKind::Direct,
                        ExchangeDeclareOptions::default(),
                        FieldTable::default(),
                    )
                    .await?;

                // 2. Declare the final consumer queue (Must match config options)
                let _main_queue = channel
                    .queue_declare(
                        queue_name.into(),
                        config.declare_options,
                        config.declare_arguments.clone(),
                    )
                    .await?;

                // 3. Bind the consumer queue to the DLX exchange using its own name as routing key
                channel
                    .queue_bind(
                        queue_name.into(),
                        dlx_exchange.as_str().into(),
                        queue_name.into(),
                        QueueBindOptions::default(),
                        FieldTable::default(),
                    )
                    .await?;

                // 4. Create arguments for the Intermediary Waiting Queue
                let mut waiting_args = FieldTable::default();
                waiting_args.insert(
                    ShortString::from("x-dead-letter-exchange"),
                    AMQPValue::LongString(dlx_exchange.as_str().into()),
                );
                waiting_args.insert(
                    ShortString::from("x-dead-letter-routing-key"),
                    AMQPValue::LongString(queue_name.to_string().into()),
                );

                // 5. Declare the hidden waiting queue where delayed messages sit
                let waiting_queue = format!("{}_waiting", queue_name);
                channel
                    .queue_declare(
                        waiting_queue.into(),
                        QueueDeclareOptions {
                            durable: config.declare_options.durable,
                            auto_delete: config.declare_options.auto_delete,
                            ..QueueDeclareOptions::default()
                        },
                        waiting_args,
                    )
                    .await?;
            } else {
                // Standard behavior: Simply declare the queue natively
                let _queue = channel
                    .queue_declare(
                        queue_name.into(),
                        config.declare_options,
                        config.declare_arguments.clone(),
                    )
                    .await?;
            }

            Ok(channel)
        }
        .boxed()
        .into()
    }

    fn start_consumer(
        channel: Channel,
        config: Config,
        worker_name: String,
    ) -> BoxSyncFuture<Result<Consumer, Error>> {
        async move {
            // Set QoS/prefetch before consuming to limit memory usage
            let qos = config.qos_options;
            if config.prefetch_count > 0 {
                channel.basic_qos(config.prefetch_count, qos).await?;
            }

            let consumer = channel
                .basic_consume(
                    config.queue.as_ref().into(),
                    worker_name.as_str().into(),
                    config.consume_options,
                    config.declare_arguments, // Ensure these don't clash with waiting arguments
                )
                .await?;

            Ok(consumer)
        }
        .boxed()
        .into()
    }

    fn heartbeat_future(channel: Channel) -> BoxSyncFuture<Result<(), Error>> {
        async move {
            if !channel.status().connected() {
                return Err(Error::NotConnected);
            }
            Ok(())
        }
        .boxed()
        .into()
    }
}

impl<M: Send + 'static> Backend for AmqpBackend<M> {
    type Task = AmqpTask;
    type Error = Error;

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        loop {
            match &mut self.state {
                State::Init => {
                    if self.channel.is_none() {
                        let fut = Self::declare_queue(self.pool.clone(), self.config.clone());
                        self.state = State::DeclareQueue(fut);
                        continue;
                    }
                    if self.consumer.is_none() {
                        let fut = Self::start_consumer(
                            self.channel.clone().expect("a channel exists"),
                            self.config.clone(),
                            worker.name().to_string(),
                        );
                        self.state = State::DeclareConsumer(fut);
                        continue;
                    }
                    self.heartbeat_timer = Some(Delay::new(self.config.heartbeat_interval));
                    self.state = State::Ready;
                }

                State::DeclareQueue(fut) => match fut.poll_unpin(cx) {
                    Poll::Ready(Ok(channel)) => {
                        self.channel = Some(channel);
                        self.state = State::Init;
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready(Err(e)) => {
                        return Poll::Ready(Err(e));
                    }
                    Poll::Pending => return Poll::Pending,
                },

                State::DeclareConsumer(fut) => match fut.poll_unpin(cx) {
                    Poll::Ready(Ok(consumer)) => {
                        self.consumer = Some(consumer);
                        self.heartbeat_timer = Some(Delay::new(self.config.heartbeat_interval));
                        self.state = State::Init;
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready(Err(e)) => {
                        return Poll::Ready(Err(e));
                    }
                    Poll::Pending => return Poll::Pending,
                },

                State::HeartBeat(fut) => match fut.poll_unpin(cx) {
                    Poll::Ready(Ok(())) => {
                        self.heartbeat_timer = Some(Delay::new(self.config.heartbeat_interval));
                        self.state = State::Ready;
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready(Err(e)) => {
                        self.channel = None;
                        self.consumer = None;
                        return Poll::Ready(Err(e));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                State::Ready => {
                    let heartbeat_due = Pin::new(self.heartbeat_timer.as_mut().unwrap())
                        .poll(cx)
                        .is_ready();
                    if heartbeat_due {
                        let fut = Self::heartbeat_future(self.channel.clone().unwrap());

                        self.state = State::HeartBeat(fut);
                    }
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
        let item = ready!(self.consumer.as_mut().unwrap().poll_next_unpin(cx));
        match item {
            Some(Ok(item)) => {
                let bytes = item.data;
                let tag = item.delivery_tag;
                let props = item.properties;

                let task = TaskBuilder::new(bytes)
                    .task_id(TaskId::Int(tag))
                    .data(DeliveryTag::new(tag))
                    .with_metadata(metadata::properties_to_metadata(&props))
                    .build();
                Poll::Ready(Some(Ok(task)))
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e.into()))),
            None => Poll::Ready(None),
        }
    }

    fn poll_close(
        &mut self,
        _: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        // TODO: Add a CleanUp state
        // self.channel.as_ref().unwrap().close(200, "OK".into());
        Poll::Ready(Ok(()))
    }
}

impl<M> BackendConfig for AmqpBackend<M> {
    type Args = M;

    type Id = u64;

    type Kind = Durable;

    type Config = Config;

    type Layer = AcknowledgeLayer<Self>;

    fn config(&self) -> &Self::Config {
        &self.config
    }

    fn middleware(&mut self, _worker: &mut WorkerContext) -> Self::Layer {
        AcknowledgeLayer::new(self.clone())
    }
}

impl<M> WireFormatBackend for AmqpBackend<M> {
    type Codec = JsonCodec<Self::Compact>;
    type Compact = Vec<u8>;

    fn codec(&self) -> &Self::Codec {
        &self.codec
    }
}

impl<M: Send + 'static> AmqpBackend<M> {
    /// Constructs a new instance of `AmqpBackend` from a `lapin` channel.
    pub fn new(pool: Pool) -> AmqpBackend<M> {
        AmqpBackend {
            pool,
            sink: sink::AmqpSink::new(),
            channel: None,
            consumer: None,
            config: Config::default().queue(std::any::type_name::<M>()),
            codec: JsonCodec::default(),
            heartbeat_timer: None,
            state: State::Init,
        }
    }

    /// Get a ref to the inner `Config`
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Constructs a new instance of `AmqpBackend` from an address string with custom config.
    ///
    /// This allows customizing QoS settings, queue declaration options, and other settings.
    pub fn new_from_addr<S: AsRef<str>>(addr: S) -> Result<AmqpBackend<M>, Error> {
        let config = deadpool_lapin::Config {
            url: Some(addr.as_ref().to_string()),
            pool: None,
        };
        let pool = config
            .builder(Default::default, deadpool::Runtime::Tokio1)
            .map_err(|e| Error::Config(e.into()))?
            .build()
            .map_err(|e| Error::Config(e.into()))?;

        Ok(Self::new(pool))
    }

    /// Provide a custom config
    pub fn with_config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use apalis::prelude::{BoxDynError, EventListenerExt};
    use apalis_core::{backend::TaskSink, worker::builder::WorkerBuilder};
    use apalis_workflow::{SteppedFlow, WorkflowSink};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    struct TestMessage;

    async fn test_job(_job: TestMessage, ctx: WorkerContext) {
        ctx.stop().unwrap();
    }

    #[tokio::test]
    async fn basic_worker() {
        let env = std::env::var("AMQP_ADDR").unwrap();
        let mut backend = AmqpBackend::new_from_addr(&env).unwrap();
        backend.push(TestMessage).await.unwrap();

        let worker = WorkerBuilder::new("rango-amigo")
            .backend(backend)
            .build(test_job);

        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn workflow() {
        let env = std::env::var("AMQP_ADDR").unwrap();
        let mut backend = AmqpBackend::new_from_addr(&env).unwrap();

        let workflow = SteppedFlow::new("odd-numbers-workflow")
            .and_then(|a: usize| async move { Ok::<_, BoxDynError>((0..a).collect::<Vec<_>>()) })
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
