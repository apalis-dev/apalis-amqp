use apalis_core::task::attempt::Attempt;
use apalis_core::task::namespace::Namespace;
use apalis_core::task::task_id::TaskId;
use serde::{Deserialize, Serialize};

/// Config for the backend
#[derive(Clone, Debug)]
pub struct Config {
    max_retries: usize,
    namespace: Namespace,
}

impl Config {
    /// Creates a new `Config` instance with the given namespace.
    pub fn new(namespace: &str) -> Self {
        Config {
            max_retries: 25,
            namespace: namespace.to_owned().into(),
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
    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    /// Sets the namespace.
    pub fn set_namespace(&mut self, namespace: Namespace) {
        self.namespace = namespace;
    }
}

/// The context of a message
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct AmqpContext {
    tag: DeliveryTag,
}

impl AmqpContext {
    /// Creates a new `Context` instance with the given parameters.
    pub fn new(tag: DeliveryTag) -> Self {
        AmqpContext { tag }
    }

    /// Gets the delivery tag.
    pub fn tag(&self) -> &DeliveryTag {
        &self.tag
    }

    /// Sets the delivery tag.
    pub fn set_tag(&mut self, tag: DeliveryTag) {
        self.tag = tag;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
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


/// The representation that is sent in the underlying connection

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
// TODO: Use AMQPProperties and remove this
pub struct AmqpMessage<M> {
    /// The inner part of the message
    pub inner: M,
    /// The task id allocated to the message
    pub task_id: TaskId,
    /// The current attempt of the message
    pub attempt: Attempt,
}
