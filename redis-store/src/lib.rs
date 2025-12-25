use async_trait::async_trait;
pub use redis;
use redis::{AsyncCommands, Client, ExistenceCheck, RedisError, SetExpiry, SetOptions};
use std::fmt::Debug;
use time::OffsetDateTime;
use tower_sessions_core::{
    session::{Id, Record},
    session_store, SessionStore,
};

#[derive(Debug, thiserror::Error)]
pub enum RedisStoreError {
    #[error(transparent)]
    Redis(#[from] RedisError),

    #[error(transparent)]
    Decode(#[from] rmp_serde::decode::Error),

    #[error(transparent)]
    Encode(#[from] rmp_serde::encode::Error),
}

impl From<RedisStoreError> for session_store::Error {
    fn from(err: RedisStoreError) -> Self {
        match err {
            RedisStoreError::Redis(inner) => session_store::Error::Backend(inner.to_string()),
            RedisStoreError::Decode(inner) => session_store::Error::Decode(inner.to_string()),
            RedisStoreError::Encode(inner) => session_store::Error::Encode(inner.to_string()),
        }
    }
}

/// A Redis session store.
#[derive(Debug, Clone)]
pub struct RedisStore {
    client: Client,
    prefix: Option<String>,
}

impl RedisStore {
    /// Create a new Redis store with the provided client.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use tower_sessions_redis_store::{fred::prelude::*, RedisStore};
    ///
    /// # tokio_test::block_on(async {
    /// let pool = Pool::new(Config::default(), None, None, None, 6).unwrap();
    ///
    /// let _ = pool.connect();
    /// pool.wait_for_connect().await.unwrap();
    ///
    /// let session_store = RedisStore::new(pool);
    /// })
    /// ```
    pub fn new(client: Client) -> Self {
        Self {
            client,
            prefix: None,
        }
    }

    /// Create a new Redis store with the provided client and prefix.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use tower_sessions_redis_store::{fred::prelude::*, RedisStore};
    ///
    /// # tokio_test::block_on(async {
    /// let pool = Pool::new(Config::default(), None, None, None, 6).unwrap();
    ///
    /// let _ = pool.connect();
    /// pool.wait_for_connect().await.unwrap();
    ///
    /// let session_store = RedisStore::with_prefix(pool, "session:".to_string());
    /// })
    /// ```
    pub fn with_prefix(client: Client, prefix: String) -> Self {
        Self {
            client,
            prefix: Some(prefix),
        }
    }

    fn get_key(&self, id: &Id) -> String {
        if let Some(prefix) = &self.prefix {
            format!("{}{}", prefix, id)
        } else {
            id.to_string()
        }
    }

    async fn save_with_options(
        &self,
        record: &Record,
        options: Option<SetOptions>,
    ) -> session_store::Result<bool> {
        let expire_timestamp = OffsetDateTime::unix_timestamp(record.expiry_date) as u64;
        let options = if let Some(options) = options {
            options.with_expiration(SetExpiry::EXAT(expire_timestamp))
        } else {
            SetOptions::default().with_expiration(SetExpiry::EXAT(expire_timestamp))
        };
        let result: bool = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(RedisStoreError::Redis)?
            .set_options(
                self.get_key(&record.id),
                rmp_serde::to_vec(&record).map_err(RedisStoreError::Encode)?,
                options,
            )
            .await
            .map_err(RedisStoreError::Redis)?;
        Ok(result)
    }
}

#[async_trait]
impl SessionStore for RedisStore {
    async fn create(&self, record: &mut Record) -> session_store::Result<()> {
        loop {
            if !self
                .save_with_options(
                    record,
                    Some(SetOptions::default().conditional_set(ExistenceCheck::NX)),
                )
                .await?
            {
                record.id = Id::default();
                continue;
            }
            break;
        }
        Ok(())
    }

    async fn save(&self, record: &Record) -> session_store::Result<()> {
        self.save_with_options(
            record,
            Some(SetOptions::default().conditional_set(ExistenceCheck::XX)),
        )
        .await?;
        Ok(())
    }

    async fn load(&self, session_id: &Id) -> session_store::Result<Option<Record>> {
        let data: Option<Vec<u8>> = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(RedisStoreError::Redis)?
            .get(self.get_key(session_id))
            .await
            .map_err(RedisStoreError::Redis)?;

        if let Some(data) = data {
            Ok(Some(
                rmp_serde::from_slice(&data).map_err(RedisStoreError::Decode)?,
            ))
        } else {
            Ok(None)
        }
    }

    async fn delete(&self, session_id: &Id) -> session_store::Result<()> {
        self.client
            .get_multiplexed_async_connection()
            .await
            .map_err(RedisStoreError::Redis)?
            .del::<_, isize>(self.get_key(session_id))
            .await
            .map_err(RedisStoreError::Redis)?;
        Ok(())
    }
}
