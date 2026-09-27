use apalis_core::{
    error::BoxDynError,
    task::{status::Status, ExecutionContext},
    worker::ext::ack::Acknowledge,
};
use futures::{future::BoxFuture, FutureExt};

use crate::{delivery_tag::DeliveryTag, error::Error, AmqpBackend};
impl<M: Send + 'static, Res: Send + Sync + 'static> Acknowledge<Res> for AmqpBackend<M> {
    type Error = Error;
    type Future = BoxFuture<'static, Result<(), Error>>;

    fn ack(&mut self, _res: &Result<Res, BoxDynError>, ctx: &ExecutionContext) -> Self::Future {
        let channel = self.channel.clone();
        let config = self.config.clone();
        let tag = ctx.data().get::<DeliveryTag>().cloned();
        let status = ctx.status();

        async move {
            let channel = channel.ok_or(Error::NotConnected)?;
            let tag = tag.ok_or(Error::MissingDeliveryTag)?.value();

            match status {
                Status::Pending | Status::Queued | Status::Running | Status::Failed => {
                    tracing::warn!(delivery_tag = tag, "task failed; nacking for redelivery");
                    channel.basic_nack(tag, config.nack_options).await?;
                }
                Status::Done => {
                    channel.basic_ack(tag, config.ack_options).await?;
                }
                Status::Killed => {
                    tracing::error!(delivery_tag = tag, "max retries exceeded");
                    let mut opts = config.reject_options;
                    opts.requeue = false; // drop or DLX, per queue args
                    channel.basic_reject(tag, opts).await?;
                }
                _ => {}
            }

            Ok(())
        }
        .boxed()
    }
}
