use apalis::prelude::*;
use apalis_amqp::AmqpBackend;
use apalis_workflow::{Workflow, WorkflowSink};

#[tokio::main]
async fn main() {
    let env = std::env::var("AMQP_ADDR").unwrap();
    let mut backend = AmqpBackend::new_from_addr(&env).await.unwrap();

    let workflow = Workflow::new("odd-numbers-workflow")
        .and_then(|a: usize| async move { Ok::<_, BoxDynError>((0..a).collect::<Vec<_>>()) })
        .and_then(|a: Vec<usize>| async move {
            println!("Sum: {}", a.iter().sum::<usize>());
            Ok::<_, BoxDynError>(())
        });
    backend.push_start(10).await.unwrap();

    let worker = WorkerBuilder::new("rango-tango")
        .backend(backend)
        .on_event(|_ctx, ev| {
            println!("On Event = {:?}", ev);
        })
        .build(workflow);
    worker.run().await.unwrap();
}
