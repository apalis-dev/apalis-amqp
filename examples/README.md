## Example
How to run the examples

### Setup RabbitMq
```
docker run -p 15672:15672 -p 5672:5672 -e RABBITMQ_DEFAULT_USER=apalis -e RABBITMQ_DEFAULT_PASS=apalis  rabbitmq:3.8.4-management
```

### Run the example
```
AMQP_ADDR=amqp://apalis:apalis@127.0.0.1:5672/%2f cargo run --example basic
```
