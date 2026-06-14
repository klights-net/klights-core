//! Resource version advance helper.

use std::sync::Arc;

use ::redb::ReadableTable;
use anyhow::Result;

use crate::datastore::redb::accessor::RedbAccessor;
use crate::datastore::redb::tables;

pub struct RedbRvStore {
    accessor: Arc<RedbAccessor>,
}

impl RedbRvStore {
    pub fn new(accessor: Arc<RedbAccessor>) -> Self {
        Self { accessor }
    }

    pub async fn advance_rv(&self, min_rv: i64) -> Result<i64> {
        self.accessor
            .call("advance_rv_impl", move |db| {
                let w = db.begin_write()?;
                let current = {
                    let tbl = w.open_table(tables::META)?;
                    let g = tbl.get("rv")?;
                    g.map(|gv| {
                        std::str::from_utf8(gv.value())
                            .unwrap_or("0")
                            .parse::<i64>()
                            .unwrap_or(0)
                    })
                    .unwrap_or(0)
                };
                let next = current.saturating_add(1).max(min_rv.saturating_add(1));
                {
                    let mut tbl = w.open_table(tables::META)?;
                    tbl.insert("rv", next.to_string().as_bytes())?;
                }
                w.commit()?;
                Ok(next)
            })
            .await
    }
}
