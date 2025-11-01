use apalis_core::{error::BoxDynError, task::Parts, worker::ext::ack::Acknowledge};
use futures::{future::BoxFuture, FutureExt};
use lapin::{options::BasicAckOptions, Error};

use crate::{utils::AmqpContext, AmqpBackend};

impl<M: Send + 'static, Res: Send + Sync + 'static> Acknowledge<Res, AmqpContext, u64>
    for AmqpBackend<M>
{
    type Error = Error;
    type Future = BoxFuture<'static, Result<(), Error>>;
    fn ack(
        &mut self,
        res: &Result<Res, BoxDynError>,
        parts: &Parts<AmqpContext, u64>,
    ) -> Self::Future {
        let channel = self.channel.clone();
        let tag = parts.ctx.tag().value();
        let is_ok = res.is_ok();
        async move {
            match is_ok {
                true => channel.basic_ack(tag, BasicAckOptions::default()).await,
                false => {
                    tracing::warn!("Task {} failed and will not be acknowledged.", tag);
                    channel.basic_reject(tag, Default::default()).await
                }
            }
        }
        .boxed()
    }
}
