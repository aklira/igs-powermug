//! The target API v1 subset (SPECIFICATIONS §11.1, `schemas/target-api-v1.yaml` of
//! gridloop): `GET /capabilities`, `GET /state`, `GET /revisions/{id}` and
//! `POST /revisions/rollback`.
//!
//! The prepare and activate calls take a signed plan. They are not in this subset:
//! the signed plan's form for partition revisions is not decided yet.

use std::sync::{Arc, Mutex, PoisonError};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use powermug_revision::profile::Profile;
use powermug_store::{CriticalStore, GenerationError, Outcome};
use serde::Deserialize;
use serde_json::{json, Value};

/// The state the handlers share: the critical store and the target profile.
#[derive(Debug)]
pub struct Api {
    store: Mutex<CriticalStore>,
    profile: Profile,
}

impl Api {
    #[must_use]
    pub fn new(store: CriticalStore, profile: Profile) -> Self {
        Self {
            store: Mutex::new(store),
            profile,
        }
    }

    fn store(&self) -> std::sync::MutexGuard<'_, CriticalStore> {
        self.store.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The routes of the subset.
pub fn router(api: Arc<Api>) -> Router {
    Router::new()
        .route("/capabilities", get(capabilities))
        .route("/state", get(state))
        .route("/revisions/rollback", post(rollback))
        .route("/revisions/{id}", get(revision))
        .with_state(api)
}

/// An error answer: a status and a stable code, with a message in plain words.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }
}

impl From<GenerationError> for ApiError {
    fn from(error: GenerationError) -> Self {
        match error {
            GenerationError::NothingToRollBack(scope) => Self::new(
                StatusCode::CONFLICT,
                "nothing_to_roll_back",
                format!("nothing to roll back to in scope {scope}"),
            ),
            GenerationError::ParentMismatch { .. }
            | GenerationError::ConflictingRevision(_)
            | GenerationError::WrongScope { .. } => {
                Self::new(StatusCode::CONFLICT, "state_conflict", error.to_string())
            }
            GenerationError::UnknownRevision(_) => {
                Self::new(StatusCode::NOT_FOUND, "unknown_revision", error.to_string())
            }
            GenerationError::Database(_) => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "store_failure",
                "the critical store failed",
            ),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = json!({ "error": self.code, "message": self.message });
        (self.status, Json(body)).into_response()
    }
}

async fn capabilities(State(api): State<Arc<Api>>) -> Json<Value> {
    let profile = &api.profile;
    let limits = &profile.limits;
    Json(json!({
        "contract_version": profile.contract_version,
        "target_profile": profile.name,
        "product": { "name": "powermug", "version": env!("CARGO_PKG_VERSION") },
        "capabilities": profile.capabilities,
        "limits": {
            "max_partitions": limits.max_partitions,
            "max_rtu": limits.max_rtu,
            "max_points": limits.max_points,
            "max_events_per_second": limits.max_events_per_second,
            "max_sessions": limits.max_sessions,
            "max_processors": limits.max_processors,
        },
        // The sandbox is enforced at each adapter's start (PM-ADP-005); this subset does
        // not read that state, so it does not claim it.
        "sandbox": "unverified",
    }))
}

async fn state(State(api): State<Arc<Api>>) -> Result<Json<Value>, ApiError> {
    let scopes = api.store().scopes()?;
    let scopes: Vec<Value> = scopes
        .into_iter()
        .map(|scope| {
            json!({
                "scope": scope.scope,
                "active": scope.active,
                "previous": scope.previous,
            })
        })
        .collect();
    Ok(Json(json!({ "scopes": scopes, "health": "not_measured" })))
}

async fn revision(
    State(api): State<Arc<Api>>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let status = api.store().revision(&id)?;
    match status {
        Some(status) => Ok(Json(json!({
            "revision_id": status.id,
            "scope": status.scope,
            "state": status.state,
            "parent": status.parent,
        }))),
        None => Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "unknown_revision",
            format!("revision {id} is not known"),
        )),
    }
}

#[derive(Debug, Deserialize)]
struct RollbackRequest {
    site: String,
}

async fn rollback(
    State(api): State<Arc<Api>>,
    Json(request): Json<RollbackRequest>,
) -> Result<Json<Value>, ApiError> {
    let mut store = api.store();
    let Some(active) = store.active(&request.site)? else {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "nothing_active",
            format!("no revision is active in scope {}", request.site),
        ));
    };
    let outcome = store.rollback(&request.site, &active)?;
    let active = store.active(&request.site)?;
    Ok(Json(json!({
        "scope": request.site,
        "active": active,
        "changed": outcome == Outcome::Changed,
    })))
}
