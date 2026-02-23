use anyhow::{Context, Result};
use sqlx::{
    PgPool, Postgres, Transaction,
    postgres::PgArguments,
    query::Query,
};

#[derive(Clone)]
pub struct TransactionExecutor {
    pool: PgPool,
}

impl TransactionExecutor {
    /// 指定した接続プールを使うトランザクション実行器を作成します。
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// 単一クエリをトランザクション内で実行します。
    ///
    /// 成功時はコミットし、失敗時はロールバックします。
    pub async fn execute_query<'a>(&self, query: Query<'a, Postgres, PgArguments>) -> Result<()> {
        self.execute_queries(std::iter::once(query)).await
    }

    /// 複数クエリを単一トランザクション内で実行します。
    ///
    /// いずれかのクエリが失敗した場合はトランザクションをロールバックし、エラーを返します。
    pub async fn execute_queries<'a, I>(&self, queries: I) -> Result<()>
    where
        I: IntoIterator<Item = Query<'a, Postgres, PgArguments>>,
    {
        let mut tx: Transaction<'_, Postgres> = self
            .pool
            .begin()
            .await
            .context("Failed to start database transaction")?;

        for (index, query) in queries.into_iter().enumerate() {
            if let Err(error) = query.execute(&mut *tx).await {
                tx.rollback()
                    .await
                    .context("Failed to rollback transaction")?;
                return Err(error).with_context(|| {
                    format!("Failed to execute query in transaction at index {index}")
                });
            }
        }

        tx.commit().await.context("Failed to commit transaction")?;
        Ok(())
    }
}
