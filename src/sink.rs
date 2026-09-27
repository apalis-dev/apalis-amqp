use std::{
    collections::VecDeque,
    fmt::Debug,
    pin::Pin,
    task::{Context, Poll},
    time::{SystemTime, UNIX_EPOCH},
};

use apalis_core::backend::future::BoxSyncFuture;
use futures::{FutureExt, Sink};
use lapin::{options::BasicPublishOptions, Confirmation};

use crate::{error::Error, metadata::metadata_to_properties, AmqpBackend, AmqpTask, State};
use pin_project::pin_project;

#[pin_project]
#[derive(Debug)]
pub(super) struct AmqpSink<T> {
    items: VecDeque<AmqpTask<Vec<u8>>>,
    pending_sends: VecDeque<BoxSyncFuture<Result<(), Error>>>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> Clone for AmqpSink<T> {
    fn clone(&self) -> Self {
        Self {
            items: VecDeque::new(),
            pending_sends: VecDeque::new(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T> AmqpSink<T> {
    pub(crate) fn new() -> Self {
        Self {
            items: VecDeque::new(),
            pending_sends: VecDeque::new(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T: Send + 'static> Sink<AmqpTask> for AmqpBackend<T> {
    type Error = Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        loop {
            match &mut self.state {
                State::Init => {
                    let pool = self.pool.clone();
                    let config = self.config.clone();

                    self.state = State::DeclareQueue(Self::declare_queue(pool, config));
                }

                State::DeclareQueue(fut) => match fut.poll_unpin(cx) {
                    Poll::Ready(Ok(channel)) => {
                        self.channel = Some(channel);
                        self.state = State::Init;
                        break;
                    }

                    Poll::Ready(Err(e)) => {
                        self.state = State::Init;
                        return Poll::Ready(Err(e));
                    }

                    Poll::Pending => {
                        return Poll::Pending;
                    }
                },

                // These states mean we have a channel
                State::Ready | State::DeclareConsumer(_) | State::HeartBeat(_) => {
                    break;
                }
            }
        }

        // First, try to flush any pending sends
        let sink = self.project().sink.project();

        // Poll pending sends
        while let Some(pending) = sink.pending_sends.front_mut() {
            match pending.poll_unpin(cx) {
                Poll::Ready(Ok(_)) => {
                    sink.pending_sends.pop_front();
                    tracing::debug!("Completed pending send to Amqp");
                }
                Poll::Ready(Err(e)) => {
                    sink.pending_sends.pop_front();
                    return Poll::Ready(Err(e));
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }

        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: AmqpTask<Vec<u8>>) -> Result<(), Self::Error> {
        let this = self.project().sink;
        let items = this.get_mut();
        items.items.push_back(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        let sink = this.sink.project();

        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // First, convert any queued items to pending sends
        while let Some(item) = sink.items.pop_front() {
            let delay_ms: Option<u64> = match item.run_at() {
                Some(run_at_secs) => {
                    if run_at_secs > now_secs {
                        Some((run_at_secs - now_secs) * 1000)
                    } else {
                        None
                    }
                }
                None => None,
            };
            let properties = metadata_to_properties(item.metadata());
            let bytes = item.args;
            let queue = this.config.queue.as_ref().to_owned();
            let exchange = this.config.exchange.clone();
            let channel = this.channel.clone();
            let future = async move {
                let (target_routing_key, final_properties) = match delay_ms {
                    Some(ms) => {
                        let waiting_queue = format!("{}_waiting", queue);
                        let properties_with_ttl = properties.with_expiration(ms.to_string().into());
                        (waiting_queue, properties_with_ttl)
                    }
                    None => (queue.to_string(), properties),
                };

                // 2. Publish using the dynamically computed variables
                let confirmation = channel
                    .unwrap()
                    .basic_publish(
                        exchange.into(),
                        target_routing_key.into(),
                        BasicPublishOptions::default(),
                        &bytes,
                        final_properties,
                    )
                    .await?
                    .await?;

                match confirmation {
                    Confirmation::Ack(returned) => {
                        if let Some(msg) = returned {
                            tracing::warn!(?msg, "message acked but returned as unroutable");
                            return Err(Error::Unroutable);
                        }
                        Ok(())
                    }
                    Confirmation::Nack(returned) => {
                        tracing::error!(?returned, "broker nacked publish");
                        Err(Error::PublishNacked)
                    }
                    Confirmation::NotRequested => {
                        tracing::debug!("publish sent without confirmation (confirms not enabled)");
                        Ok(())
                    }
                }
            }
            .boxed()
            .into();
            sink.pending_sends.push_back(future);
        }

        // Now poll all pending sends
        while let Some(pending) = sink.pending_sends.front_mut() {
            match pending.poll_unpin(cx) {
                Poll::Ready(Ok(_)) => {
                    sink.pending_sends.pop_front();
                }
                Poll::Ready(Err(e)) => {
                    sink.pending_sends.pop_front();
                    return Poll::Ready(Err(e));
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }

        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}
