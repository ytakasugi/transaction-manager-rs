mod database;

use anyhow::Result;
use database::connection_pool::ConnectionPool;
use database::query_executor::QueryExecutor;
use sqlx::{Row, postgres::PgRow};

#[tokio::main]
async fn main() -> Result<()> {
    let connection_pool = ConnectionPool::shared().await?;
    let query_executor = QueryExecutor::from_shared_pool(&connection_pool);

    let worker_executor = query_executor.clone();
    let batch_executor = query_executor.clone();
    let ui_executor = query_executor.clone();

    let worker = tokio::spawn(async move { resident_feature(worker_executor).await });
    let batch = tokio::spawn(async move { scheduled_batch_feature(batch_executor).await });
    let ui = tokio::spawn(async move { screen_feature(ui_executor).await });

    let (worker_result, batch_result, ui_result) = tokio::try_join!(worker, batch, ui)?;
    worker_result?;
    batch_result?;
    ui_result?;

    println!("Hello, world!");

    Ok(())
}

async fn resident_feature(query_executor: QueryExecutor) -> Result<()> {
    health_check(&query_executor).await?;
    let _health_checks: Vec<i64> = query_executor
        .fetch_all(
            sqlx::query("SELECT 1::bigint as value UNION ALL SELECT 1::bigint as value")
                .try_map(|row: PgRow| row.try_get("value")),
        )
        .await?;
    Ok(())
}

async fn scheduled_batch_feature(query_executor: QueryExecutor) -> Result<()> {
    query_executor
        .execute_queries(vec![sqlx::query("SELECT 1"), sqlx::query("SELECT 1")])
        .await?;
    Ok(())
}

async fn screen_feature(query_executor: QueryExecutor) -> Result<()> {
    query_executor
        .execute_query(sqlx::query("SELECT 1"))
        .await?;
    Ok(())
}

async fn health_check(query_executor: &QueryExecutor) -> Result<()> {
    let _health_check: Option<i64> = query_executor
        .fetch_one(
            sqlx::query("SELECT 1::bigint as value").try_map(|row: PgRow| row.try_get("value")),
        )
        .await?;
    Ok(())
}
