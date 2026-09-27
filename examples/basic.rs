use std::str::FromStr;

use apalis::{layers::retry::RetryPolicy, prelude::*};
use apalis_amqp::AmqpBackend;
use serde::{Deserialize, Serialize};
use tracing::debug;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestMessage(usize);

async fn test_job(job: TestMessage, wrk: WorkerContext) {
    wrk.stop().unwrap();
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_str("debug").unwrap())
        .init();
    let env = std::env::var("AMQP_ADDR").unwrap();
    let mut mq = AmqpBackend::new_from_addr(&env).unwrap();
    // add some jobs
    mq.push(TestMessage(42)).await.unwrap();
    WorkerBuilder::new("rango-amigo")
        .backend(mq)
        .retry(RetryPolicy::retries(5))
        .enable_tracing()
        .on_event(|w, e| debug!("{}", e))
        .build(test_job)
        .run()
        .await
        .unwrap();
}
