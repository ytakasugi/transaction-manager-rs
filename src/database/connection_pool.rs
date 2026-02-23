use anyhow::{Context, Result, anyhow, ensure};
use dotenv::dotenv;
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::{sync::Arc, time::Duration};
use tokio::sync::OnceCell;

const ENV_DATABASE_URL: &str = "DATABASE_URL";
const ENV_CONNECTION_POOL: &str = "CONNECTION_POOL";
const DEFAULT_MAX_CONNECTIONS: u32 = 10;

pub type SharedConnectionPool = Arc<ConnectionPool>;

static SHARED_CONNECTION_POOL: OnceCell<SharedConnectionPool> = OnceCell::const_new();

#[derive(Clone)]
pub struct ConnectionPool {
    pool: PgPool,
}

impl ConnectionPool {
    /// 遅延初期化される共有接続プールインスタンスを返します。
    ///
    /// プールはプロセス内で一度だけ作成され、以後はすべての呼び出し元で再利用されます。
    pub async fn shared() -> Result<SharedConnectionPool> {
        let connection_pool = SHARED_CONNECTION_POOL
            .get_or_try_init(|| async {
                let connection_pool = Self::new().await?;
                Ok::<SharedConnectionPool, anyhow::Error>(Arc::new(connection_pool))
            })
            .await?;

        Ok(Arc::clone(connection_pool))
    }

    /// 環境変数の設定から PostgreSQL 接続プールを新規作成します。
    ///
    /// 必須の環境変数:
    /// - `DATABASE_URL`
    ///
    /// 任意の環境変数:
    /// - `CONNECTION_POOL`（未設定時は `DEFAULT_MAX_CONNECTIONS`）
    async fn new() -> Result<Self> {
        dotenv().ok();
        let database_url = std::env::var(ENV_DATABASE_URL)
            .with_context(|| format!("{ENV_DATABASE_URL} must be set"))?;
        let max_connections = read_u32_env(ENV_CONNECTION_POOL, DEFAULT_MAX_CONNECTIONS)?;
        ensure!(
            max_connections > 0,
            "{ENV_CONNECTION_POOL} must be greater than 0"
        );

        let pool = PgPoolOptions::new()
            .min_connections(1)
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(5))
            .idle_timeout(Some(Duration::from_secs(300)))
            .max_lifetime(Some(Duration::from_secs(1800)))
            .test_before_acquire(true)
            .connect(&database_url)
            .await
            .context("Failed to create database connection pool")?;

        Ok(Self { pool })
    }

    /// database モジュール内で利用する SQLx の PostgreSQL プール参照を返します。
    pub(super) fn get(&self) -> &PgPool {
        &self.pool
    }
}

/// 環境変数を `u32` として読み取ります。
///
/// 変数が未設定の場合は `default_value` を返します。
fn read_u32_env(key: &str, default_value: u32) -> Result<u32> {
    match std::env::var(key) {
        Ok(value) => value
            .parse::<u32>()
            .with_context(|| format!("{key} must be a valid u32")),
        Err(std::env::VarError::NotPresent) => Ok(default_value),
        Err(error) => Err(anyhow!("Failed to read {key}: {error}")),
    }
}
