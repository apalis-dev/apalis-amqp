<h1 align="center">apalis-amqp</h1>
<div align="center">
 <strong>
   Background message processing for Rust using `apalis` and the `amqp` protocol.
 </strong>
</div>

<br />

<div align="center">
  <!-- Crates version -->
  <a href="https://crates.io/crates/apalis-amqp">
    <img src="https://img.shields.io/crates/v/apalis-amqp.svg?style=flat-square"
    alt="Crates.io version" />
  </a>
  <!-- Downloads -->
  <a href="https://crates.io/crates/apalis-amqp">
    <img src="https://img.shields.io/crates/d/apalis-amqp.svg?style=flat-square"
      alt="Download" />
  </a>
  <!-- docs.rs docs -->
  <a href="https://docs.rs/apalis-amqp">
    <img src="https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square"
      alt="docs.rs docs" />
  </a>
</div>
<br/>

## Overview

`apalis-amqp` provides utilities for integrating `apalis` with AMQP message queuing systems. It includes an `AmqpBackend` implementation for use with the pushing and popping messages.

## Features

- Integration between apalis and AMQP message queuing systems.
- Easy creation of AMQP-backed message queues.
- Simple consumption of AMQP messages as apalis messages.
- Supports message acknowledgement and rejection.
- Supports all apalis middleware such as rate-limiting, timeouts, filtering, sentry, prometheus etc.
- Supports persisting results to databases like `redis`, `postgres` and `sqlite` among others.
- Partial support for sequential workflows.

## Getting started

Before attempting to connect, you need a working amqp backend. We can easily setup using Docker:

### Setup RabbitMq

```sh
docker run -p 15672:15672 -p 5672:5672 -e RABBITMQ_DEFAULT_USER=apalis -e RABBITMQ_DEFAULT_PASS=apalis rabbitmq:4-management
```

### Basic example

Add apalis-amqp to your Cargo.toml

```toml
[dependencies]
apalis = "1.0.0-rc.10"
apalis-amqp = "1.0.0-rc.9"
```

Then add to your main.rs

```rust,no_run
 use apalis::prelude::*;
 use apalis_amqp::AmqpBackend;

 #[tokio::main]
 async fn main() {
    let env = std::env::var("AMQP_ADDR").unwrap();
    let mut backend = AmqpBackend::new_from_addr(&env).unwrap();

    backend.push(42u32).await.unwrap();

    async fn handle_message(task: u32) -> Result<(), BoxDynError> {
        Ok(())
    }

    WorkerBuilder::new("rango-amigo")
      .backend(backend)
      .build(handle_message)
      .run()
      .await
      .unwrap();
 }
```

### Workflow Example

```rust,no_run
use apalis::prelude::*;
use apalis_amqp::AmqpBackend;
use apalis_workflow::SteppedFlow;

#[tokio::main]
async fn main() {
    let env = std::env::var("AMQP_ADDR").unwrap();
    let mut backend = AmqpBackend::new_from_addr(&env).unwrap();

    let workflow = SteppedFlow::new("odd-numbers-workflow")
        .and_then(|a: usize| async move { Ok::<_, BoxDynError>((0..a).collect::<Vec<_>>()) })
        .and_then(|a: Vec<usize>| async move {
            println!("Sum: {}", a.iter().sum::<usize>());
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
```

## Observability

You can track your tasks using [apalis-board](https://github.com/apalis-dev/apalis-board).
![Task](https://github.com/apalis-dev/apalis-board/raw/main/screenshots/task.png)

## License

`apalis-amqp` is licensed under the Apache license. See the LICENSE file for details.
