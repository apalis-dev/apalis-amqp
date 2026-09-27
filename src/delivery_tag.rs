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
