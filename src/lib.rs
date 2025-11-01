//! # apalis-amqp
//!
//! Message queuing utilities for Rust using apalis and AMQP.

//! ## Overview

//! `apalis-amqp` is a Rust crate that provides utilities for integrating `apalis` with AMQP message queuing systems.
//!  It includes an `AmqpBackend` implementation for use with the pushing and popping jobs.

//! ## Features

//! - Integration between apalis and AMQP message queuing systems.
//! - Easy creation of AMQP-backed job queues.
//! - Simple consumption of AMQP messages as apalis jobs.
//! - Supports message acknowledgement and rejection via `tower` layers.
//! - Supports all apalis middleware such as rate-limiting, timeouts, filtering, sentry, prometheus etc.

//! ## Getting started

//! Add apalis-amqp to your Cargo.toml file:

//! ````toml
//! [dependencies]
//! apalis = { version = "1.0.0-alpha.8" }
//! apalis-amqp = "1.0.0-alpha.1"
//! serde = "1"
//! ````

//! Then add to your main.rs

//! ````rust,no_run
//! # use apalis_core::worker::builder::WorkerBuilder;
//! # use apalis_core::backend::TaskSink;
//! use apalis_amqp::AmqpBackend;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Serialize, Deserialize)]
//! struct TestJob(usize);
//!
//! async fn test_job(job: TestJob) {
//!     dbg!(job);
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let env = std::env::var("AMQP_ADDR").unwrap();
//!     let mut mq = AmqpBackend::new_from_addr(&env).await.unwrap();
//!     mq.push(TestJob(42)).await.unwrap();
//!     WorkerBuilder::new("rango-amigo")
//!         .backend(mq)
//!         .build(test_job)
//!         .run()
//!         .await
//!         .unwrap();
//! }
//! ````
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
use apalis_core::{
    backend::{codec::json::JsonCodec, Backend, TaskStream},
    task::{builder::TaskBuilder, task_id::TaskId, Task},
    worker::{context::WorkerContext, ext::ack::AcknowledgeLayer},
};
use deadpool_lapin::{Manager, Pool};
use futures::{
    stream::{self, BoxStream},
    StreamExt, TryStreamExt,
};
use lapin::{
    options::{BasicConsumeOptions, QueueDeclareOptions},
    types::FieldTable,
    Channel, ConnectionProperties, Error, ErrorKind, Queue,
};
use pin_project::pin_project;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    io::{self},
    marker::PhantomData,
    sync::Arc,
};
use utils::{AmqpContext, Config, DeliveryTag};

/// Contains basic utilities for handling config and messages
pub mod utils;

type AmqpTask<T> = Task<T, AmqpContext, u64>;

#[derive(Debug)]
/// A wrapper around a `lapin` AMQP channel that implements message queuing functionality.
#[pin_project]
pub struct AmqpBackend<M, Codec = JsonCodec<Vec<u8>>> {
    channel: Channel,
    queue: Queue,
    message_type: PhantomData<M>,
    config: Config,
    #[pin]
    sink: sink::AmqpSink<M, Codec>,
}

impl<M> Clone for AmqpBackend<M> {
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

// impl<M: Serialize + DeserializeOwned + Send + Sync + 'static> MessageQueue<M> for AmqpBackend<M> {
//     type Error = Error;
//     /// Publishes a new job to the queue.
//     ///
//     /// This function serializes the provided job data to a JSON string and publishes it to the
//     /// queue with the namespace configured.
//     async fn enqueue(&mut self, message: M) -> Result<(), Self::Error> {
//         let _confirmation = self
//             .channel
//             .basic_publish(
//                 "",
//                 self.config.namespace().as_str(),
//                 BasicPublishOptions::default(),
//                 &serde_json::to_vec(&AmqpMessage {
//                     inner: message,
//                     task_id: Default::default(),
//                     attempt: Default::default(),
//                 })
//                 .map_err(|e| Error::IOError(Arc::new(io::Error::new(ErrorKind::InvalidData, e))))?,
//                 BasicProperties::default(),
//             )
//             .await?
//             .await?;
//         Ok(())
//     }

//     async fn size(&mut self) -> Result<usize, Self::Error> {
//         Ok(self.queue.message_count() as usize)
//     }

//     async fn dequeue(&mut self) -> Result<Option<M>, Self::Error> {
//         Ok(None)
//     }
// }

impl<M: Serialize + DeserializeOwned + Send + 'static> Backend for AmqpBackend<M> {
    type Args = M;
    type Compact = Vec<u8>;
    type Error = Error;
    type Beat = BoxStream<'static, Result<(), Self::Error>>;
    type Codec = JsonCodec<Vec<u8>>;
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
        .try_flatten()
        .map_ok(move |item| {
            let bytes = item.data;
            let tag = item.delivery_tag;
            let msg: M = serde_json::from_slice(&bytes).unwrap();

            let task = TaskBuilder::new(msg)
                .with_task_id(TaskId::new(tag))
                .with_ctx(AmqpContext::new(DeliveryTag::new(tag), item.properties))
                .build();
            Some(task)
        });
        stream.boxed()
    }
}

impl<M: Serialize + DeserializeOwned + Send + 'static> AmqpBackend<M> {
    /// Constructs a new instance of `AmqpBackend` from a `lapin` channel.
    pub fn new(channel: Channel, queue: Queue) -> Self {
        Self::new_with_config(channel, queue, Config::new(std::any::type_name::<M>()))
    }

    /// Constructs a new instance of `AmqpBackend` with a config
    pub fn new_with_config(channel: Channel, queue: Queue, config: Config) -> Self {
        Self {
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
    pub fn queue(&self) -> &Queue {
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
    pub async fn new_from_addr<S: AsRef<str>>(addr: S) -> Result<Self, lapin::Error> {
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

    use apalis_core::{backend::TaskSink, worker::builder::WorkerBuilder};
    use serde::Deserialize;

    #[derive(Debug, Serialize, Deserialize)]
    struct TestMessage;

    async fn test_job(_job: TestMessage, ctx: WorkerContext) {
        ctx.stop().unwrap();
    }

    #[tokio::test]
    async fn it_works() {
        let env = std::env::var("AMQP_ADDR").unwrap();
        let mut backend: AmqpBackend<TestMessage> = AmqpBackend::new_from_addr(&env).await.unwrap();
        backend.push(TestMessage).await.unwrap();

        let worker = WorkerBuilder::new("rango-amigo")
            .backend(backend)
            .build(test_job);

        worker.run().await.unwrap();
    }
}
