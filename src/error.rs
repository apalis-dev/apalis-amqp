use apalis_core::error::BoxDynError;
use deadpool::managed::PoolError;

/// Errors encountered in `apalis-amqp` crate
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An error originating from the underlying `lapin` AMQP client.
    #[error("LapinError: {0}")]
    Lapin(#[from] lapin::Error),

    /// The connection or channel configuration was invalid or failed to build.
    #[error("ConfigError: {0}")]
    Config(BoxDynError),

    /// An error occurred while acquiring or managing a connection from the pool.
    #[error("LapinError: {0}")]
    Pool(#[from] PoolError<lapin::Error>),

    /// An operation was attempted before a connection/channel was established.
    #[error("NotConnected")]
    NotConnected,

    /// A confirmation was received without an associated delivery tag.
    #[error("MissingDeliveryTag")]
    MissingDeliveryTag,

    /// The broker returned the message as unroutable (e.g. no matching queue/exchange binding).
    #[error("Unroutable")]
    Unroutable,

    /// The broker explicitly rejected (nacked) a published message.
    #[error("PublishNacked")]
    PublishNacked,
}
