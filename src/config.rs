use std::time::Duration;

use apalis_core::backend::queue::Queue;
use lapin::{
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions,
        BasicQosOptions, BasicRejectOptions, QueueDeclareOptions,
    },
    types::{AMQPValue, FieldTable},
};

/// Configuration for a RabbitMQ-backed an apalis-amqp Backend.
///
/// `Config` controls queue declaration, message acknowledgement, quality of
/// service, publishing, retries, and connection heartbeat behavior.
///
/// # Example
///
/// ```
/// # use std::time::Duration;
/// # use apalis_amqp::Config;
///
/// let config = Config::default()
///     .queue("jobs")
///     .prefetch_count(50)
///     .heartbeat_interval(Duration::from_secs(15))
///     .max_retries(5)
///     .quorum(true);
/// ```
#[derive(Debug, Clone)]
pub struct Config {
    /// The queue to consume tasks from and publish tasks to.
    pub queue: Queue,

    /// The maximum number of unacknowledged messages to prefetch.
    ///
    /// A higher value can improve throughput by allowing the consumer to
    /// process more messages concurrently.
    ///
    /// A value of `0` means unlimited prefetch and is generally not
    /// recommended because it can result in unbounded messages being held
    /// by the consumer.
    ///
    /// Default: `10`.
    pub prefetch_count: u16,

    /// Additional options passed to RabbitMQ when configuring consumer QoS.
    ///
    /// This corresponds to [`lapin::options::BasicQosOptions`].
    ///
    /// Default: [`BasicQosOptions::default()`].
    pub qos_options: BasicQosOptions,

    /// The exchange used for publishing messages.
    ///
    /// An empty string refers to RabbitMQ's default exchange.
    ///
    /// Default: an empty string.
    pub exchange: String,

    /// Options used when consuming messages.
    ///
    /// Default: [`BasicConsumeOptions::default()`].
    pub consume_options: BasicConsumeOptions,

    /// Options used when acknowledging successfully processed messages.
    ///
    /// Default: [`BasicAckOptions::default()`].
    pub ack_options: BasicAckOptions,

    /// Options used when negatively acknowledging messages.
    ///
    /// Default: [`BasicNackOptions::default()`].
    pub nack_options: BasicNackOptions,

    /// Options used when rejecting messages.
    ///
    /// Default: [`BasicRejectOptions::default()`].
    pub reject_options: BasicRejectOptions,

    /// Interval at which the RabbitMQ connection heartbeat is maintained.
    ///
    /// This helps detect broken connections and keep the connection alive.
    ///
    /// Default: 30 seconds.
    pub heartbeat_interval: Duration,

    /// Options used when declaring the queue.
    ///
    /// Default: [`QueueDeclareOptions::default()`].
    pub declare_options: QueueDeclareOptions,

    /// Additional arguments passed when declaring the queue.
    ///
    /// This can be used to configure RabbitMQ-specific queue properties such
    /// as quorum queues, message TTLs, dead-letter exchanges, and other
    /// queue arguments.
    ///
    /// Default: an empty [`FieldTable`].
    pub declare_arguments: FieldTable,

    /// Options used when publishing messages to RabbitMQ.
    ///
    /// Default: [`BasicPublishOptions::default()`].
    pub publish_options: BasicPublishOptions,

    /// The maximum number of times a task may be retried.
    ///
    /// Default: `25`.
    pub max_retries: usize,

    /// Conditionally enable delayed messaging natively via the TTL + DLX pattern
    pub scheduling: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            queue: Queue::from("default"),
            prefetch_count: 10,
            qos_options: BasicQosOptions::default(),
            consume_options: BasicConsumeOptions::default(),
            exchange: String::new(),
            ack_options: BasicAckOptions::default(),
            nack_options: BasicNackOptions::default(),
            reject_options: BasicRejectOptions::default(),
            heartbeat_interval: Duration::from_secs(30),
            declare_options: QueueDeclareOptions {
                durable: true,
                exclusive: false,
                ..Default::default()
            },
            declare_arguments: FieldTable::default(),
            publish_options: BasicPublishOptions::default(),
            max_retries: 25,
            scheduling: false,
        }
    }
}

impl Config {
    /// Sets the queue used by the worker.
    pub fn queue(mut self, queue: impl Into<Queue>) -> Self {
        self.queue = queue.into();
        self
    }

    /// Sets the maximum number of unacknowledged messages to prefetch.
    ///
    /// A value of `0` disables the prefetch limit.
    pub fn prefetch_count(mut self, prefetch_count: u16) -> Self {
        self.prefetch_count = prefetch_count;
        self
    }

    /// Sets the RabbitMQ QoS options.
    pub fn qos_options(mut self, options: BasicQosOptions) -> Self {
        self.qos_options = options;
        self
    }

    /// Sets the exchange used for publishing messages.
    pub fn exchange(mut self, exchange: impl Into<String>) -> Self {
        self.exchange = exchange.into();
        self
    }

    /// Sets the options used when consuming messages.
    pub fn consume_options(mut self, options: BasicConsumeOptions) -> Self {
        self.consume_options = options;
        self
    }

    /// Sets the options used when acknowledging messages.
    pub fn ack_options(mut self, options: BasicAckOptions) -> Self {
        self.ack_options = options;
        self
    }

    /// Sets the options used when negatively acknowledging messages.
    pub fn nack_options(mut self, options: BasicNackOptions) -> Self {
        self.nack_options = options;
        self
    }

    /// Sets the options used when rejecting messages.
    pub fn reject_options(mut self, options: BasicRejectOptions) -> Self {
        self.reject_options = options;
        self
    }

    /// Sets the RabbitMQ connection heartbeat interval.
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// Sets the queue declaration options.
    pub fn declare_options(mut self, options: QueueDeclareOptions) -> Self {
        self.declare_options = options;
        self
    }

    /// Sets additional arguments used when declaring the queue.
    pub fn declare_arguments(mut self, arguments: FieldTable) -> Self {
        self.declare_arguments = arguments;
        self
    }

    /// Sets the RabbitMQ publishing options.
    pub fn publish_options(mut self, options: BasicPublishOptions) -> Self {
        self.publish_options = options;
        self
    }

    /// Sets the maximum number of task retries.
    pub fn max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Toggles conditional delayed messaging natively via the TTL + DLX pattern.
    pub fn scheduling(mut self, scheduling: bool) -> Self {
        self.scheduling = scheduling;
        self
    }

    /// Configures whether the queue is a RabbitMQ quorum queue.
    ///
    /// When enabled, this sets the `x-queue-type` declaration argument to
    /// `quorum`.
    ///
    /// When disabled, the `x-queue-type` argument is removed from the queue
    /// declaration arguments.
    ///
    /// # Example
    ///
    /// ```
    /// # use apalis_amqp::Config;
    ///
    /// let config = Config::default().quorum(true);
    /// ```
    pub fn quorum(mut self, quorum: bool) -> Self {
        if quorum {
            self.declare_arguments.insert(
                "x-queue-type".into(),
                AMQPValue::LongString("quorum".into()),
            );
        }

        self
    }
}
