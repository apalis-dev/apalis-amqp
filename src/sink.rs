use std::{
    collections::VecDeque,
    fmt::Debug,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use apalis_core::backend::codec::Codec;
use futures::{FutureExt, Sink};
use lapin::{options::BasicPublishOptions, publisher_confirm::Confirmation};

use crate::{AmqpBackend, AmqpTask};
use pin_project::pin_project;

#[pin_project]
#[derive(Debug)]
pub(super) struct AmqpSink<T, C> {
    items: VecDeque<AmqpTask<Vec<u8>>>,
    pending_sends: VecDeque<PendingSend>,
    _codec: std::marker::PhantomData<(T, C)>,
}

impl<T, C> Clone for AmqpSink<T, C> {
    fn clone(&self) -> Self {
        Self {
            items: VecDeque::new(),
            pending_sends: VecDeque::new(),
            _codec: std::marker::PhantomData,
        }
    }
}

impl<T, C> AmqpSink<T, C> {
    pub(crate) fn new() -> Self {
        Self {
            items: VecDeque::new(),
            pending_sends: VecDeque::new(),
            _codec: std::marker::PhantomData,
        }
    }
}

struct PendingSend {
    future: Pin<Box<dyn Future<Output = Result<Confirmation, lapin::Error>> + Send + 'static>>,
}

impl Debug for PendingSend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingSend").finish()
    }
}

impl<T, C> Sink<AmqpTask<Vec<u8>>> for AmqpBackend<T, C>
where
    C::Error: std::error::Error + Send,
    C: Codec<T, Compact = Vec<u8>>,
{
    type Error = lapin::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // First, try to flush any pending sends
        let sink = self.project().sink.project();

        // Poll pending sends
        while let Some(pending) = sink.pending_sends.front_mut() {
            match pending.future.as_mut().poll(cx) {
                Poll::Ready(Ok(_)) => {
                    sink.pending_sends.pop_front();
                    println!("Completed pending send to RSMQ");
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
        let namespace = self.config.namespace().to_owned();
        let channel = self.channel.clone();
        let sink = self.project().sink.project();

        // First, convert any queued items to pending sends
        while let Some(item) = sink.items.pop_front() {
            let bytes = item.args;
            let namespace = namespace.to_string();
            let channel = channel.clone();
            let properties = item.parts.ctx.properties().clone();
            // TODO: use item.parts.run_at to set a delay
            // Integrate with https://github.com/rabbitmq/rabbitmq-delayed-message-exchange
            let _delay = item.parts.run_at;
            // Create the future but don't poll it yet
            let future = async move {
                let confirmation = channel
                    .basic_publish(
                        "",
                        &namespace,
                        BasicPublishOptions::default(),
                        &bytes,
                        properties,
                    )
                    .await?
                    .await?;
                Ok(confirmation)
            }
            .boxed();
            sink.pending_sends.push_back(PendingSend { future });
        }

        // Now poll all pending sends
        while let Some(pending) = sink.pending_sends.front_mut() {
            match pending.future.as_mut().poll(cx) {
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
