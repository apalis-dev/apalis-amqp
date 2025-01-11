use apalis_core::{layers::Ack, response::Response};
use lapin::{options::BasicAckOptions, Error};

use crate::{utils::AmqpContext, AmqpBackend};

impl<M: Send + 'static, Res: Send + Sync + 'static> Ack<M, Res> for AmqpBackend<M> {
    type AckError = Error;
    type Context = AmqpContext;
    async fn ack(&mut self, ctx: &AmqpContext, res: &Response<Res>) -> Result<(), Error> {
        if res.is_success() {
            self.channel
                .basic_ack(ctx.tag().value(), BasicAckOptions::default())
                .await?;
        } else {
            tracing::warn!("Task {} failed and will not be acknowledged.", res.task_id);
        }
        Ok(())
    }
}
