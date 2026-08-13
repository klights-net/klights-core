use klights_node_store::{NodeIdentity, NodeIdentityError, NodeIdentityFuture};
use klights_supervisor::DbExecutor;
use rusqlite::OptionalExtension;

const NODE_META_GET: &str = "SELECT value FROM _node_meta WHERE key = ?1";
const NODE_META_SET: &str = "INSERT INTO _node_meta (key, value) VALUES (?1, ?2) \
     ON CONFLICT(key) DO UPDATE SET value = excluded.value";

#[derive(Clone)]
pub struct SqliteNodeIdentity {
    executor: DbExecutor,
}

impl SqliteNodeIdentity {
    pub fn new(executor: DbExecutor) -> Self {
        Self { executor }
    }

    async fn call<T, F>(
        &self,
        query_name: &'static str,
        call: F,
    ) -> klights_supervisor::DbCallResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut rusqlite::Connection) -> klights_supervisor::DbClosureResult<T>
            + Send
            + 'static,
    {
        self.executor.call_raw(query_name, call).await
    }
}

impl NodeIdentity for SqliteNodeIdentity {
    fn backend_name(&self) -> &'static str {
        "sqlite"
    }

    fn ensure_node_identity<'a>(
        &'a self,
        cluster_id: &'a str,
        node_uid: &'a str,
    ) -> NodeIdentityFuture<'a, ()> {
        let cluster_id = cluster_id.to_string();
        let node_uid = node_uid.to_string();
        Box::pin(async move {
            self.call("node_local:ensure_identity", move |conn| {
                ensure_meta_matches_or_insert(conn, "cluster_id", &cluster_id)?;
                ensure_meta_matches_or_insert(conn, "node_uid", &node_uid)?;
                Ok(())
            })
            .await
            .map_err(|error| {
                NodeIdentityError::persistence_failed(
                    "ensure_node_identity",
                    format!("node.db identity check failed: {error}"),
                )
            })
        })
    }

    fn get_node_meta<'a>(&'a self, key: &'a str) -> NodeIdentityFuture<'a, Option<String>> {
        let key = key.to_string();
        Box::pin(async move {
            self.call("node_local:get_meta", move |conn| {
                conn.query_row(NODE_META_GET, [key], |row| row.get(0))
                    .optional()
                    .map_err(klights_supervisor::DbError::from)
            })
            .await
            .map_err(|error| {
                NodeIdentityError::persistence_failed(
                    "get_node_meta",
                    format!("node meta get failed: {error}"),
                )
            })
        })
    }

    fn set_node_meta<'a>(&'a self, key: &'a str, value: &'a str) -> NodeIdentityFuture<'a, ()> {
        let key = key.to_string();
        let value = value.to_string();
        Box::pin(async move {
            self.call("node_local:set_meta", move |conn| {
                conn.execute(NODE_META_SET, rusqlite::params![key, value])?;
                Ok(())
            })
            .await
            .map_err(|error| {
                NodeIdentityError::persistence_failed(
                    "set_node_meta",
                    format!("node meta set failed: {error}"),
                )
            })
        })
    }
}

fn ensure_meta_matches_or_insert(
    conn: &rusqlite::Connection,
    key: &str,
    expected: &str,
) -> rusqlite::Result<()> {
    let live: Option<String> = conn
        .query_row(NODE_META_GET, [key], |row| row.get(0))
        .optional()?;
    match live {
        None => {
            conn.execute(NODE_META_SET, rusqlite::params![key, expected])?;
            Ok(())
        }
        Some(actual) if actual == expected => Ok(()),
        Some(actual) => Err(rusqlite::Error::ToSqlConversionFailure(Box::new(
            std::io::Error::other(format!(
                "node.db identity mismatch for {key}: expected {expected}, found {actual}"
            )),
        ))),
    }
}
