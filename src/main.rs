mod database;

use anyhow::Result;
use database::connection_pool::{ConnectionPool, SharedConnectionPool};
use database::transaction_executor::TransactionExecutor;
use sqlx::{Row, postgres::PgRow};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    let connection_pool = ConnectionPool::shared().await?;
    let transaction_executor = TransactionExecutor::new(connection_pool.get().clone());

    let worker_pool = Arc::clone(&connection_pool);
    let batch_executor = transaction_executor.clone();
    let ui_executor = transaction_executor.clone();

    let worker = tokio::spawn(async move { resident_feature(worker_pool).await });
    let batch = tokio::spawn(async move { scheduled_batch_feature(batch_executor).await });
    let ui = tokio::spawn(async move { screen_feature(ui_executor).await });

    let (worker_result, batch_result, ui_result) = tokio::try_join!(worker, batch, ui)?;
    worker_result?;
    batch_result?;
    ui_result?;

    println!("Hello, world!");

    Ok(())
}

async fn resident_feature(connection_pool: SharedConnectionPool) -> Result<()> {
    health_check(&connection_pool).await?;
    let _health_checks: Vec<i64> = connection_pool
        .fetch_all(
            sqlx::query("SELECT 1::bigint as value UNION ALL SELECT 1::bigint as value")
                .try_map(|row: PgRow| row.try_get("value")),
        )
        .await?;
    Ok(())
}

async fn scheduled_batch_feature(transaction_executor: TransactionExecutor) -> Result<()> {
    transaction_executor
        .execute_queries(vec![sqlx::query("SELECT 1"), sqlx::query("SELECT 1")])
        .await?;
    Ok(())
}

async fn screen_feature(transaction_executor: TransactionExecutor) -> Result<()> {
    transaction_executor
        .execute_query(sqlx::query("SELECT 1"))
        .await?;
    Ok(())
}

async fn health_check(connection_pool: &SharedConnectionPool) -> Result<()> {
    let _health_check: Option<i64> = connection_pool
        .fetch_one(
            sqlx::query("SELECT 1::bigint as value").try_map(|row: PgRow| row.try_get("value")),
        )
        .await?;
    Ok(())
}
