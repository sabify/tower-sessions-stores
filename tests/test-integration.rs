#[macro_use]
mod common;

#[cfg(test)]
mod moka_store_tests {
    use axum::Router;
    use tower_sessions::SessionManagerLayer;
    use tower_sessions_moka_store::MokaStore;

    use crate::common::build_app;

    async fn app(max_age: Option<Duration>) -> Router {
        let moka_store = MokaStore::new(None);
        let session_manager = SessionManagerLayer::new(moka_store).with_secure(true);
        build_app(session_manager, max_age)
    }

    route_tests!(app);
}

#[cfg(test)]
mod redis_store_tests {
    use axum::Router;
    use tower_sessions::SessionManagerLayer;
    use tower_sessions_redis_store::{fred::prelude::*, RedisStore};

    use crate::common::build_app;

    async fn app(max_age: Option<Duration>) -> Router {
        let database_url = std::option_env!("REDIS_URL").unwrap();

        let config = Config::from_url(database_url).unwrap();

        #[cfg(not(feature = "dynamic-pool"))]
        let pool = {
            let pool = Pool::new(config, None, None, None, 6).unwrap();
            pool.connect();
            pool.wait_for_connect().await.unwrap();
            pool
        };

        #[cfg(feature = "dynamic-pool")]
        let pool = {
            let pool = DynamicPool::new(config, None, None, None, Default::default()).unwrap();
            pool.init().await.unwrap();
            pool.start_scale_task(std::time::Duration::from_secs(10));
            pool
        };

        let session_store = RedisStore::new(pool);
        let session_manager = SessionManagerLayer::new(session_store).with_secure(true);

        build_app(session_manager, max_age)
    }

    route_tests!(app);
}

#[cfg(test)]
mod sqlite_store_tests {
    use axum::Router;
    use tower_sessions::SessionManagerLayer;
    use tower_sessions_sqlx_store::{sqlx::SqlitePool, SqliteStore};

    use crate::common::build_app;

    async fn app(max_age: Option<Duration>) -> Router {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        let session_store = SqliteStore::new(pool);
        session_store.migrate().await.unwrap();
        let session_manager = SessionManagerLayer::new(session_store).with_secure(true);

        build_app(session_manager, max_age)
    }

    route_tests!(app);
}

#[cfg(test)]
mod postgres_store_tests {
    use axum::Router;
    use tower_sessions::SessionManagerLayer;
    use tower_sessions_sqlx_store::{sqlx::PgPool, PostgresStore};

    use crate::common::build_app;

    async fn app(max_age: Option<Duration>) -> Router {
        let database_url = std::option_env!("POSTGRES_URL").unwrap();
        let pool = PgPool::connect(database_url).await.unwrap();
        let session_store = PostgresStore::new(pool);
        session_store.migrate().await.unwrap();
        let session_manager = SessionManagerLayer::new(session_store).with_secure(true);

        build_app(session_manager, max_age)
    }

    route_tests!(app);
}

#[cfg(test)]
mod mysql_store_tests {
    use axum::Router;
    use tower_sessions::SessionManagerLayer;
    use tower_sessions_sqlx_store::{sqlx::MySqlPool, MySqlStore};

    use crate::common::build_app;

    async fn app(max_age: Option<Duration>) -> Router {
        let database_url = std::option_env!("MYSQL_URL").unwrap();

        let pool = MySqlPool::connect(database_url).await.unwrap();
        let session_store = MySqlStore::new(pool);
        session_store.migrate().await.unwrap();
        let session_manager = SessionManagerLayer::new(session_store).with_secure(true);

        build_app(session_manager, max_age)
    }

    route_tests!(app);
}

#[cfg(test)]
mod mongodb_store_tests {
    use axum::Router;
    use tower_sessions::SessionManagerLayer;
    use tower_sessions_mongodb_store::{mongodb, MongoDBStore};

    use crate::common::build_app;

    async fn app(max_age: Option<Duration>) -> Router {
        let database_url = std::option_env!("MONGODB_URL").unwrap();
        let client = mongodb::Client::with_uri_str(database_url).await.unwrap();
        let session_store = MongoDBStore::new(client, "tower-sessions".to_string());
        let session_manager = SessionManagerLayer::new(session_store).with_secure(true);

        build_app(session_manager, max_age)
    }

    route_tests!(app);
}

#[cfg(test)]
mod caching_store_tests {
    use axum::Router;
    use tower_sessions::{CachingSessionStore, SessionManagerLayer};
    use tower_sessions_moka_store::MokaStore;
    use tower_sessions_sqlx_store::{sqlx::SqlitePool, SqliteStore};

    use crate::common::build_app;

    async fn app(max_age: Option<Duration>) -> Router {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        let sqlite_store = SqliteStore::new(pool);
        sqlite_store.migrate().await.unwrap();

        let moka_store = MokaStore::new(None);
        let caching_store = CachingSessionStore::new(moka_store, sqlite_store);

        let session_manager = SessionManagerLayer::new(caching_store).with_secure(true);

        build_app(session_manager, max_age)
    }

    route_tests!(app);
}
