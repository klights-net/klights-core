// Cluster-scoped (namespace=None) delete_collection handlers.
// Expands in the caller module where State/Query/AppState/AppError are in scope.
macro_rules! cluster_delete_collection_handler {
    ($fn_name:ident, $api_version:expr_2021, $kind:expr_2021) => {
        pub async fn $fn_name(
            State(state): State<Arc<AppState>>,
            Query(query): Query<DeleteCollectionQuery>,
            axum::Extension(identity): axum::Extension<crate::auth::AuthenticatedIdentity>,
        ) -> Result<Json<Value>, AppError> {
            crate::api::generated_handlers::inners::delete_collection_shared_inner(
                state,
                &identity,
                $api_version,
                $kind,
                None,
                query,
            )
            .await
        }
    };
}

// Create/update/patch wrappers that call base handlers and enqueue reconcile.
macro_rules! reconcile_handlers {
    ($resource:ident, $create_base:ident, $update_base:ident, $patch_base:ident) => {
        paste::paste! {
            async fn [<create_ $resource>](
                State(state): State<Arc<AppState>>,
                Path(namespace): Path<String>,
                Query(query): Query<CreateUpdateQuery>,
                axum::Extension(identity): axum::Extension<crate::auth::AuthenticatedIdentity>,
                LenientJson(body): LenientJson<Value>,
            ) -> Result<(StatusCode, Json<Value>), AppError> {
                let result = $create_base(
                    State(state.clone()),
                    Path(namespace.clone()),
                    Query(query),
                    axum::Extension(identity),
                    LenientJson(body),
                )
                .await?;

                let (status, json_response) = &result;
                if *status == StatusCode::CREATED {
                    state.controller_dispatcher.enqueue(&json_response.0).await;
                }

                Ok(result)
            }

            async fn [<update_ $resource>](
                State(state): State<Arc<AppState>>,
                Path((namespace, name)): Path<(String, String)>,
                Query(query): Query<CreateUpdateQuery>,
                axum::Extension(identity): axum::Extension<crate::auth::AuthenticatedIdentity>,
                LenientJson(body): LenientJson<Value>,
            ) -> Result<Json<Value>, AppError> {
                let result = $update_base(
                    State(state.clone()),
                    Path((namespace.clone(), name.clone())),
                    Query(query),
                    axum::Extension(identity),
                    LenientJson(body),
                )
                .await?;

                state.controller_dispatcher.enqueue(&result.0).await;

                Ok(result)
            }

            async fn [<patch_ $resource>](
                State(state): State<Arc<AppState>>,
                Path((namespace, name)): Path<(String, String)>,
                Query(query): Query<CreateUpdateQuery>,
                headers: HeaderMap,
                axum::Extension(identity): axum::Extension<crate::auth::AuthenticatedIdentity>,
                body: Bytes,
            ) -> Result<Json<Value>, AppError> {
                let result = $patch_base(
                    State(state.clone()),
                    Path((namespace.clone(), name.clone())),
                    Query(query),
                    headers,
                    axum::Extension(identity),
                    body,
                )
                .await?;

                state.controller_dispatcher.enqueue(&result.0).await;

                Ok(result)
            }
        }
    };
}

// Create-only wrapper for resources that only need post-create reconcile enqueue.
macro_rules! reconcile_create_handler {
    ($resource:ident, $create_base:ident) => {
        paste::paste! {
            async fn [<create_ $resource>](
                State(state): State<Arc<AppState>>,
                Path(namespace): Path<String>,
                Query(query): Query<CreateUpdateQuery>,
                axum::Extension(identity): axum::Extension<crate::auth::AuthenticatedIdentity>,
                LenientJson(body): LenientJson<Value>,
            ) -> Result<(StatusCode, Json<Value>), AppError> {
                let result = $create_base(
                    State(state.clone()),
                    Path(namespace.clone()),
                    Query(query),
                    axum::Extension(identity),
                    LenientJson(body),
                )
                .await?;

                let (status, json_response) = &result;
                if *status == StatusCode::CREATED {
                    state.controller_dispatcher.enqueue(&json_response.0).await;
                }

                Ok(result)
            }
        }
    };
}
