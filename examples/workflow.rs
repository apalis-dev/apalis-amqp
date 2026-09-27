use std::time::Duration;

use apalis::prelude::*;
use apalis_amqp::{AmqpBackend, Config};
use apalis_workflow::SteppedFlow;

#[tokio::main]
async fn main() {
    let env = std::env::var("AMQP_ADDR").unwrap();

    let config = Config::default().queue("odd-numbers").scheduling(true); // Required for delay_for

    let mut backend = AmqpBackend::new_from_addr(&env)
        .unwrap()
        .with_config(config);

    let workflow = SteppedFlow::new("odd-numbers-workflow")
        .and_then(|a: usize| async move { Ok::<_, BoxDynError>((0..a).collect::<Vec<_>>()) })
        .delay_for(Duration::from_secs(3))
        .and_then(|a: Vec<usize>, wrk: WorkerContext| async move {
            println!("Sum: {}", a.iter().sum::<usize>());
            wrk.stop().unwrap();
            Ok::<_, BoxDynError>(())
        });
    backend.push(10).await.unwrap();

    let worker = WorkerBuilder::new("rango-tango")
        .backend(backend)
        .on_event(|_ctx, ev| {
            println!("On Event = {:?}", ev);
        })
        .build(workflow);
    worker.run().await.unwrap();
}
