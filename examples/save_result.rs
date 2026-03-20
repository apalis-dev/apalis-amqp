use apalis::{layers::retry::RetryPolicy, prelude::*};
use apalis_amqp::{utils::AmqpContext, AmqpBackend};
use futures::{future::BoxFuture, FutureExt};
use tokio_rusqlite::{params, Connection, Error};

#[derive(Debug, Clone)]
struct ResultPersist {
    conn: Connection,
}

impl Acknowledge<usize, AmqpContext, u64> for ResultPersist {
    type Future = BoxFuture<'static, Result<(), Self::Error>>;
    type Error = Error;

    fn ack(
        &mut self,
        res: &Result<usize, BoxDynError>,
        ctx: &Parts<AmqpContext, u64>,
    ) -> Self::Future {
        let task_id = *ctx.task_id.unwrap().inner();
        let output = *res.as_ref().unwrap_or(&0);

        assert_eq!(output, 84, "42 doubled successfully");
        let conn = self.conn.clone();
        let fut = async move {
            conn.call(move |conn| {
                conn.execute(
                    "INSERT INTO results (id, output) VALUES (?1, ?2)",
                    params![task_id, output],
                )?;
                Ok(())
            })
            .await
        };
        fut.boxed()
    }
}

async fn calculate_double(input: usize) -> Result<usize, BoxDynError> {
    Ok(input * 2)
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    let env = std::env::var("AMQP_ADDR").unwrap();

    let conn = Connection::open_in_memory().await?;

    conn.call(|conn| {
        conn.execute(
            "CREATE TABLE results (
                    id    INTEGER PRIMARY KEY,
                    output  INTEGER
                )",
            [],
        )?;
        Ok::<_, Error>(())
    })
    .await?;

    let mut mq = AmqpBackend::new_from_addr(&env).await.unwrap();
    // add some jobs
    mq.push(42).await.unwrap();
    WorkerBuilder::new("rango-amigo")
        .backend(mq)
        .retry(RetryPolicy::retries(5))
        .ack_with(ResultPersist { conn })
        .build(calculate_double)
        .run()
        .await?;

    Ok(())
}
