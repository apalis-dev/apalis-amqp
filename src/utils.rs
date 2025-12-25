use apalis_codec::json::JsonCodec;
use apalis_core::backend::codec::Codec;
use apalis_core::task::metadata::MetadataExt;
use apalis_core::task_fn::FromRequest;
use lapin::{BasicProperties, types::{ByteArray, FieldTable}};
use serde::{de::DeserializeOwned, Serialize};
use std::{convert::Infallible, time::Duration};

use crate::AmqpTask;

/// Config for the backend
#[derive(Clone, Debug)]
pub struct Config {
    max_retries: usize,
    namespace: String,
    heartbeat_interval: Duration,
}

impl Config {
    /// Creates a new `Config` instance with the given namespace.
    pub fn new(namespace: &str) -> Self {
        Config {
            max_retries: 25,
            namespace: namespace.to_owned(),
            heartbeat_interval: Duration::from_secs(60),
        }
    }

    /// Gets the maximum number of retries.
    pub fn max_retries(&self) -> usize {
        self.max_retries
    }

    /// Sets the maximum number of retries.
    pub fn set_max_retries(&mut self, max_retries: usize) {
        self.max_retries = max_retries;
    }

    /// Gets the namespace.
    pub fn namespace(&self) -> &String {
        &self.namespace
    }

    /// Sets the namespace.
    pub fn set_namespace(&mut self, namespace: String) {
        self.namespace = namespace;
    }

    /// Gets the heartbeat interval.
    pub fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }

    /// Sets the heartbeat interval.
    pub fn set_heartbeat_interval(&mut self, interval: Duration) {
        self.heartbeat_interval = interval;
    }
}

/// The context of a message
#[derive(Clone, Debug, Default)]
pub struct AmqpContext {
    tag: DeliveryTag,
    properties: BasicProperties,
}

impl AmqpContext {
    /// Creates a new `Context` instance with the given parameters.
    pub fn new(tag: DeliveryTag, properties: BasicProperties) -> Self {
        AmqpContext { tag, properties }
    }

    /// Gets the delivery tag.
    pub fn tag(&self) -> &DeliveryTag {
        &self.tag
    }

    /// Sets the delivery tag.
    pub fn set_tag(&mut self, tag: DeliveryTag) {
        self.tag = tag;
    }

    /// Gets the message properties.
    pub fn properties(&self) -> &BasicProperties {
        &self.properties
    }

    /// Sets the message properties.
    pub fn set_properties(&mut self, properties: BasicProperties) {
        self.properties = properties;
    }
}

impl<Args: Sync> FromRequest<AmqpTask<Args>> for AmqpContext {
    type Error = Infallible;
    async fn from_request(req: &AmqpTask<Args>) -> Result<Self, Self::Error> {
        Ok(req.parts.ctx.clone())
    }
}

impl<T: DeserializeOwned + Serialize> MetadataExt<T> for AmqpContext {
    type Error = lapin::Error;
    fn extract(&self) -> Result<T, lapin::Error> {
        self.properties
            .headers()
            .as_ref()
            .unwrap_or(&FieldTable::default())
            .inner()
            .get(std::any::type_name::<T>())
            .ok_or(
                std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid metadata type")
                    .into(),
            )
            .and_then(|v| match v {
                lapin::types::AMQPValue::ByteArray(bytes) => {
                    JsonCodec::<Vec<u8>>::decode(&bytes.as_slice().to_vec())
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e).into())
                }
                _ => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid metadata format",
                )
                .into()),
            })
    }
    fn inject(&mut self, value: T) -> Result<(), lapin::Error> {
        let mut cur = self.properties.headers().as_ref().unwrap().clone();
        cur.insert(
            std::any::type_name::<T>().into(),
            lapin::types::AMQPValue::ByteArray(ByteArray::from(
                JsonCodec::<Vec<u8>>::encode(&value).unwrap(),
            )),
        );
        self.properties = self.properties.clone().with_headers(cur);
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
/// A wrapper for the message to be acknowledged.
pub struct DeliveryTag(u64);

impl DeliveryTag {
    /// Creates a new `DeliveryTag` instance with the given value.
    pub fn new(value: u64) -> Self {
        DeliveryTag(value)
    }

    /// Gets the delivery tag value.
    pub fn value(&self) -> u64 {
        self.0
    }

    /// Sets the delivery tag value.
    pub fn set_value(&mut self, value: u64) {
        self.0 = value;
    }
}
