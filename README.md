<h1 align="center">apalis-amqp</h1>
<div align="center">
 <strong>
   Message queuing for Rust using apalis and AMQP.
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

`apalis-amqp` is a Rust crate that provides utilities for integrating `apalis` with AMQP message queuing systems. It includes an `AmqpBackend` implementation for use with the pushing and popping messages, as well as a `MessageQueue<M>` implementation for consuming messages from an AMQP queue.

## Features

- Integration between apalis and AMQP message queuing systems.
- Easy creation of AMQP-backed message queues.
- Simple consumption of AMQP messages as apalis messages.
- Supports message acknowledgement and rejection via `tower` layers.
- Supports all apalis middleware such as rate-limiting, timeouts, filtering, sentry, prometheus etc.
- Supports ack messages and allows custom saving results to other backends

## Getting started

Before attempting to connect, you need a working amqp backend. We can easily setup using Docker:

### Setup RabbitMq

```
docker run -p 15672:15672 -p 5672:5672 -e RABBITMQ_DEFAULT_USER=apalis -e RABBITMQ_DEFAULT_PASS=apalis  rabbitmq:3.8.4-management
```

### Setup the rust code

Add apalis-amqp to your Cargo.toml

```toml
[dependencies]
apalis = "0.6"
apalis-amqp = "0.4"
serde = "1"
```

Then add to your main.rs

```rust
 use apalis::prelude::*;
 use apalis_amqp::AmqpBackend;
 use serde::{Deserialize, Serialize};

 #[derive(Debug, Serialize, Deserialize)]
 struct TestMessage(usize);

 async fn test_message(message: TestMessage) {
     dbg!(message);
 }

 #[tokio::main]
 async fn main() {
     let env = std::env::var("AMQP_ADDR").unwrap();
     let mq = AmqpBackend::<TestMessage>::new_from_addr(&env).await.unwrap();
     // This can be in another place in the program
     mq.enqueue(TestMessage(42)).await.unwrap();
     Monitor::new()
         .register(
             WorkerBuilder::new("rango-amigo")
                 .backend(mq)
                 .build_fn(test_message),
         )
         .run()
         .await
         .unwrap();
 }
```

Run your code and profit!

## License

apalis-amqp is licensed under the Apache license. See the LICENSE file for details.
