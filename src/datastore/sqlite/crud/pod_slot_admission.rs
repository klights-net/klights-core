use super::super::queries;
use super::*;
use rusqlite::OptionalExtension;

#[derive(Debug, Clone)]
struct SlotRow {
    pod_uid: String,
    node_name: String,
    state: PodSlotAdmissionState,
    resource_version: i64,
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

fn row_to_slot(row: &rusqlite::Row<'_>) -> rusqlite::Result<SlotRow> {
    let state_text: String = row.get(2)?;
    let state = PodSlotAdmissionState::parse(&state_text).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(
            2,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::other(err.to_string())),
        )
    })?;
    Ok(SlotRow {
        pod_uid: row.get(0)?,
        node_name: row.get(1)?,
        state,
        resource_version: row.get(3)?,
    })
}

fn next_node_slot_resource_version(tx: &rusqlite::Transaction<'_>) -> rusqlite::Result<i64> {
    let current: i64 = tx
        .query_row(queries::NODE_META_POD_SLOT_RV_SELECT, [], |row| {
            row.get::<_, String>(0)
        })
        .optional()?
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);
    let next = current.saturating_add(1);
    tx.execute(queries::NODE_META_POD_SLOT_RV_UPSERT, [next.to_string()])?;
    Ok(next)
}

impl Datastore {
    pub async fn pod_slot_try_admit(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        node_name: &str,
    ) -> Result<PodSlotAdmissionResult> {
        let namespace = namespace.to_string();
        let pod_name = pod_name.to_string();
        let pod_uid = pod_uid.to_string();
        let node_name = node_name.to_string();
        let event_namespace = namespace.clone();
        let event_pod_name = pod_name.clone();
        let event_pod_uid = pod_uid.clone();

        let (result, event) = self
            .node_db_call("db_pod_slot_try_admit", move |conn| {
                let tx = conn.transaction()?;
                let existing = tx
                    .query_row(
                        queries::POD_SLOT_ADMISSION_SELECT,
                        rusqlite::params![namespace, pod_name],
                        row_to_slot,
                    )
                    .optional()?;

                match existing {
                    None => {
                        let rv = next_node_slot_resource_version(&tx)?;
                        tx.execute(
                            queries::POD_SLOT_ADMISSION_INSERT,
                            rusqlite::params![
                                namespace,
                                pod_name,
                                pod_uid,
                                node_name,
                                PodSlotAdmissionState::Admitted.as_str(),
                                rv,
                                now_ms(),
                            ],
                        )?;
                        tx.commit()?;
                        Ok((
                            PodSlotAdmissionResult::Admitted {
                                resource_version: rv,
                            },
                            Some(PodSlotAdmissionEvent::Changed {
                                namespace: event_namespace,
                                pod_name: event_pod_name,
                                pod_uid: event_pod_uid,
                                state: PodSlotAdmissionState::Admitted,
                                resource_version: rv,
                            }),
                        ))
                    }
                    Some(row) if row.pod_uid == pod_uid => {
                        if row.state == PodSlotAdmissionState::Admitted
                            && row.node_name == node_name
                        {
                            tx.commit()?;
                            return Ok((
                                PodSlotAdmissionResult::Admitted {
                                    resource_version: row.resource_version,
                                },
                                None,
                            ));
                        }
                        let rv = next_node_slot_resource_version(&tx)?;
                        tx.execute(
                            queries::POD_SLOT_ADMISSION_UPDATE,
                            rusqlite::params![
                                namespace,
                                pod_name,
                                pod_uid,
                                node_name,
                                PodSlotAdmissionState::Admitted.as_str(),
                                rv,
                                now_ms(),
                            ],
                        )?;
                        tx.commit()?;
                        Ok((
                            PodSlotAdmissionResult::Admitted {
                                resource_version: rv,
                            },
                            Some(PodSlotAdmissionEvent::Changed {
                                namespace: event_namespace,
                                pod_name: event_pod_name,
                                pod_uid: event_pod_uid,
                                state: PodSlotAdmissionState::Admitted,
                                resource_version: rv,
                            }),
                        ))
                    }
                    Some(row) => {
                        tx.commit()?;
                        Ok((
                            PodSlotAdmissionResult::Blocked {
                                blocking_uid: row.pod_uid,
                                blocking_node: row.node_name,
                                state: row.state,
                                resource_version: row.resource_version,
                            },
                            None,
                        ))
                    }
                }
            })
            .await
            .map_err(|err| anyhow!("pod_slot_try_admit failed: {err}"))?;

        if let Some(event) = event {
            let _ = self.pod_slot_admission_sender().send(event);
        }
        Ok(result)
    }

    pub async fn pod_slot_mark_terminating(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        node_name: &str,
    ) -> Result<()> {
        let namespace = namespace.to_string();
        let pod_name = pod_name.to_string();
        let pod_uid = pod_uid.to_string();
        let node_name = node_name.to_string();
        let event_namespace = namespace.clone();
        let event_pod_name = pod_name.clone();
        let event_pod_uid = pod_uid.clone();
        let event = self
            .node_db_call("db_pod_slot_mark_terminating", move |conn| {
                let tx = conn.transaction()?;
                let existing = tx
                    .query_row(
                        queries::POD_SLOT_ADMISSION_SELECT,
                        rusqlite::params![namespace, pod_name],
                        row_to_slot,
                    )
                    .optional()?;

                match existing {
                    Some(row) if row.pod_uid != pod_uid => Err(tokio_rusqlite::Error::Other(
                        Box::new(crate::datastore::errors::DatastoreError::conflict(
                            "pod slot admission UID precondition failed",
                        )),
                    )),
                    Some(row)
                        if row.state == PodSlotAdmissionState::Terminating
                            && row.node_name == node_name =>
                    {
                        tx.commit()?;
                        Ok(None)
                    }
                    Some(_) => {
                        let rv = next_node_slot_resource_version(&tx)?;
                        tx.execute(
                            queries::POD_SLOT_ADMISSION_UPDATE,
                            rusqlite::params![
                                namespace,
                                pod_name,
                                pod_uid,
                                node_name,
                                PodSlotAdmissionState::Terminating.as_str(),
                                rv,
                                now_ms(),
                            ],
                        )?;
                        tx.commit()?;
                        Ok(Some(PodSlotAdmissionEvent::Changed {
                            namespace: event_namespace,
                            pod_name: event_pod_name,
                            pod_uid: event_pod_uid,
                            state: PodSlotAdmissionState::Terminating,
                            resource_version: rv,
                        }))
                    }
                    None => {
                        let rv = next_node_slot_resource_version(&tx)?;
                        tx.execute(
                            queries::POD_SLOT_ADMISSION_INSERT,
                            rusqlite::params![
                                namespace,
                                pod_name,
                                pod_uid,
                                node_name,
                                PodSlotAdmissionState::Terminating.as_str(),
                                rv,
                                now_ms(),
                            ],
                        )?;
                        tx.commit()?;
                        Ok(Some(PodSlotAdmissionEvent::Changed {
                            namespace: event_namespace,
                            pod_name: event_pod_name,
                            pod_uid: event_pod_uid,
                            state: PodSlotAdmissionState::Terminating,
                            resource_version: rv,
                        }))
                    }
                }
            })
            .await
            .map_err(|err| anyhow!("pod_slot_mark_terminating failed: {err}"))?;

        if let Some(event) = event {
            let _ = self.pod_slot_admission_sender().send(event);
        }
        Ok(())
    }

    pub async fn pod_slot_clear_if_uid(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        _node_name: &str,
    ) -> Result<()> {
        let namespace = namespace.to_string();
        let pod_name = pod_name.to_string();
        let pod_uid = pod_uid.to_string();
        let event_namespace = namespace.clone();
        let event_pod_name = pod_name.clone();
        let event_pod_uid = pod_uid.clone();
        let event = self
            .node_db_call("db_pod_slot_clear_if_uid", move |conn| {
                let tx = conn.transaction()?;
                let existing = tx
                    .query_row(
                        queries::POD_SLOT_ADMISSION_SELECT,
                        rusqlite::params![namespace, pod_name],
                        row_to_slot,
                    )
                    .optional()?;
                let Some(row) = existing else {
                    tx.commit()?;
                    return Ok(None);
                };
                if row.pod_uid != pod_uid {
                    tx.commit()?;
                    return Ok(None);
                }
                let rv = next_node_slot_resource_version(&tx)?;
                tx.execute(
                    queries::POD_SLOT_ADMISSION_DELETE_IF_UID,
                    rusqlite::params![namespace, pod_name, pod_uid],
                )?;
                tx.commit()?;
                Ok(Some(PodSlotAdmissionEvent::Cleared {
                    namespace: event_namespace,
                    pod_name: event_pod_name,
                    pod_uid: event_pod_uid,
                    resource_version: rv,
                }))
            })
            .await
            .map_err(|err| anyhow!("pod_slot_clear_if_uid failed: {err}"))?;

        if let Some(event) = event {
            let _ = self.pod_slot_admission_sender().send(event);
        }
        Ok(())
    }
}
