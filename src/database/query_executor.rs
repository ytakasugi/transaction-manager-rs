use anyhow::{Context, Result};
use crate::database::connection_pool::SharedConnectionPool;
use sqlx::{
    PgPool, Postgres, Transaction,
    postgres::{PgArguments, PgRow},
    query::Map,
    query::Query,
};

#[derive(Clone)]
pub struct QueryExecutor {
    pool: PgPool,
}

impl QueryExecutor {
    /// 指定した接続プールを使うクエリ実行器を作成します。
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// 共有接続プールからクエリ実行器を作成します。
    pub fn from_shared_pool(connection_pool: &SharedConnectionPool) -> Self {
        Self::new(connection_pool.get().clone())
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

    /// マッピング済みクエリを実行し、最大 1 行を返します。
    ///
    /// クエリ結果が空の場合は `Ok(None)` を返します。
    pub async fn fetch_one<'a, U, F>(
        &self,
        query: Map<'a, Postgres, F, PgArguments>,
    ) -> Result<Option<U>>
    where
        U: Send + Unpin,
        F: FnMut(PgRow) -> std::result::Result<U, sqlx::Error> + Send + 'static,
    {
        let row = query
            .fetch_optional(&self.pool)
            .await
            .context("Failed to fetch optional row")?;
        Ok(row)
    }

    /// マッピング済みクエリを実行し、全行をベクタとして返します。
    pub async fn fetch_all<'a, U, F>(
        &self,
        query: Map<'a, Postgres, F, PgArguments>,
    ) -> Result<Vec<U>>
    where
        U: Send + Unpin,
        F: FnMut(PgRow) -> std::result::Result<U, sqlx::Error> + Send + 'static,
    {
        let rows = query
            .fetch_all(&self.pool)
            .await
            .context("Failed to fetch rows")?;
        Ok(rows)
    }
}
