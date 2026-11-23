//! The target API v1 subset (SPECIFICATIONS §11.1, `schemas/target-api-v1.yaml` of
//! gridloop): `GET /capabilities`, `GET /state`, `GET /revisions/{id}` and
//! `POST /revisions/rollback`.
//!
//! The prepare and activate calls take a signed plan. They are not in this subset:
//! the signed plan's form for partition revisions is not decided yet.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use ed25519_dalek::VerifyingKey;
use powermug_command::{CommandClient, CommandError, Mode};
use powermug_core::IndicatorSource;
use powermug_revision::canonical::canonical_bytes;
use powermug_revision::partition::{
    validate_stage1_partition, validate_stage2, validate_stage3, validate_stage4,
};
use powermug_revision::profile::{powermug_v1, Profile};
use powermug_revision::{verify_signed_plan, Finding};
use powermug_store::{CriticalStore, GenerationError, Outcome};
use serde::Deserialize;
use serde_json::{json, Value};

/// The state the handlers share: the critical store and the target profile.
pub struct Api {
    store: Mutex<CriticalStore>,
    profile: Profile,
    trusted_key: VerifyingKey,
    indicators: Arc<dyn IndicatorSource>,
    commands: CommandClient,
}

impl Api {
    /// The API over `store`, for the target `profile`. Signed plans are verified
    /// against `trusted_key` (§10.6).
    #[must_use]
    pub fn new(
        store: CriticalStore,
        profile: Profile,
        trusted_key: VerifyingKey,
        indicators: Arc<dyn IndicatorSource>,
        commands: CommandClient,
    ) -> Self {
        Self {
            store: Mutex::new(store),
            profile,
            trusted_key,
            indicators,
            commands,
        }
    }

    fn store(&self) -> std::sync::MutexGuard<'_, CriticalStore> {
        self.store.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl std::fmt::Debug for Api {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Api")
            .field("profile", &self.profile.name)
            .finish_non_exhaustive()
    }
}

/// The routes of the subset.
pub fn router(api: Arc<Api>) -> Router {
    Router::new()
        .route("/capabilities", get(capabilities))
        .route("/state", get(state))
        .route("/indicators", get(indicators))
        .route("/revisions/prepare", post(prepare))
        .route("/revisions/activate", post(activate))
        .route("/revisions/rollback", post(rollback))
        .route("/revisions/{id}", get(revision))
        .route("/mode", get(mode_get).post(mode_set))
        .with_state(api)
}

/// An error answer: a status and a stable code, with a message in plain words, and
/// the findings of a document that did not validate.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
    findings: Vec<Finding>,
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            findings: Vec::new(),
        }
    }

    fn invalid_document(findings: Vec<Finding>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_document",
            message: format!("{} finding(s) on the partition document", findings.len()),
            findings,
        }
    }
}

impl From<CommandError> for ApiError {
    fn from(error: CommandError) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "journal_error",
            format!("the command journal could not answer: {error}"),
        )
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
        let body = json!({
            "error": self.code,
            "message": self.message,
            "findings": self.findings,
        });
        (self.status, Json(body)).into_response()
    }
}

async fn capabilities(State(api): State<Arc<Api>>) -> Json<Value> {
    let profile = &api.profile;
    let limits = &profile.limits;
    // A planner assigns work from this list: only what this build does is claimed.
    let served: std::collections::BTreeMap<&String, &String> = profile
        .capabilities
        .iter()
        .filter(|(name, _)| IMPLEMENTED.contains(&name.as_str()))
        .collect();
    Json(json!({
        "contract_version": profile.contract_version,
        "target_profile": profile.name,
        "product": { "name": "powermug", "version": env!("CARGO_PKG_VERSION") },
        "capabilities": served,
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

/// The capabilities of `powermug@1` this build implements. The profile lists more (§11.2),
/// and stage 3 validates revisions against the whole profile; the cell claims only these.
/// `indicators.v1` and `scope.cell` are not claimed: only two of the indicators exist and
/// the cell has no scope model beyond itself.
pub const IMPLEMENTED: &[&str] = &["south.iec104", "target.state.rest", "target.revision.rest"];

/// The neutral indicators of §11.3, measured at the time of the request.
async fn indicators(State(api): State<Arc<Api>>) -> Json<Value> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let now_ns = i64::try_from(now.as_nanos()).unwrap_or(i64::MAX);
    let indicators: Vec<Value> = api
        .indicators
        .sample(now_ns)
        .into_iter()
        .map(|indicator| {
            json!({
                "name": indicator.name,
                "scope": indicator.scope,
                "session": indicator.session,
                "value": indicator.value,
                "unit": indicator.unit,
                "measured_at_ns": indicator.measured_at_ns,
            })
        })
        .collect();
    Json(json!({ "indicators": indicators }))
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

/// A signed plan and the revision asked for.
#[derive(Debug, Deserialize)]
struct PlanRequest {
    signed_plan: Value,
    revision_id: String,
}

/// The partition revision inside a verified plan: its content identifier (the id the
/// store keys it by), its site (the scope) and its parent.
struct Verified {
    id: String,
    scope: String,
    parent: Option<String>,
    document: Value,
}

/// Verifies the plan (§10.6), finds the revision and its Powermug document, and runs
/// stages 1 to 3 on the document (§10.4).
fn verified(api: &Api, request: &PlanRequest) -> Result<Verified, ApiError> {
    let plan = verify_signed_plan(&request.signed_plan, &api.trusted_key).map_err(|err| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_signed_plan",
            err.to_string(),
        )
    })?;
    let revision = plan.revision(&request.revision_id).map_err(|err| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "revision_not_in_plan",
            err.to_string(),
        )
    })?;
    let document = revision.get("powermug_document").cloned().ok_or_else(|| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "no_powermug_document",
            "the revision carries no powermug_document",
        )
    })?;
    let profile = powermug_v1();
    let mut findings = validate_stage1_partition(&document);
    findings.extend(validate_stage2(&document));
    findings.extend(validate_stage3(&document, &profile));
    if !findings.is_empty() {
        return Err(ApiError::invalid_document(findings));
    }
    let metadata = &document["metadata"];
    let id = metadata["id"].as_str().unwrap_or_default().to_owned();
    let scope = metadata["site"].as_str().unwrap_or_default().to_owned();
    let parent = metadata["parent"].as_str().map(str::to_owned);
    Ok(Verified {
        id,
        scope,
        parent,
        document,
    })
}

async fn prepare(
    State(api): State<Arc<Api>>,
    Json(request): Json<PlanRequest>,
) -> Result<Json<Value>, ApiError> {
    let verified = verified(&api, &request)?;
    let mut store = api.store();
    let active = store.active(&verified.scope)?;
    // Already active is an answer, not a conflict: it is checked before the parent.
    if active.as_deref() == Some(verified.id.as_str()) {
        return Ok(Json(json!({
            "status": "ALREADY_ACTIVE",
            "revision_id": verified.id,
            "scope": verified.scope,
        })));
    }
    let stage4 = validate_stage4(&verified.document, active.as_deref());
    if !stage4.is_empty() {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "parent_mismatch",
            stage4[0].message.clone(),
        ));
    }
    let body = canonical_bytes(&verified.document).map_err(|err| {
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "canonical_failure",
            err.to_string(),
        )
    })?;
    store.prepare(
        &verified.id,
        &verified.scope,
        verified.parent.as_deref(),
        &body,
    )?;
    Ok(Json(json!({
        "status": "PREPARED",
        "revision_id": verified.id,
        "scope": verified.scope,
    })))
}

async fn activate(
    State(api): State<Arc<Api>>,
    Json(request): Json<PlanRequest>,
) -> Result<Json<Value>, ApiError> {
    let verified = verified(&api, &request)?;
    let outcome = api
        .store()
        .activate(&verified.id, &verified.scope)
        .map_err(|err| match err {
            GenerationError::UnknownRevision(_) => ApiError::new(
                StatusCode::CONFLICT,
                "not_prepared",
                "the revision is not prepared",
            ),
            other => ApiError::from(other),
        })?;
    let status = match outcome {
        Outcome::Changed => "ACTIVE",
        Outcome::Unchanged => "ALREADY_ACTIVE",
    };
    Ok(Json(json!({
        "status": status,
        "revision_id": verified.id,
        "scope": verified.scope,
    })))
}

/// The operating mode of the cell (FUN-OPS-006): `normal`, `maintenance` or `shadow`.
async fn mode_get(State(api): State<Arc<Api>>) -> Result<Json<Value>, ApiError> {
    let mode = api.commands.mode()?;
    Ok(Json(json!({ "mode": mode.code() })))
}

/// The body of a mode change: the mode asked for, and the actor who asks.
#[derive(Debug, Deserialize)]
struct ModeRequest {
    mode: String,
    actor: String,
}

/// Sets the operating mode. The change is audited with its actor. Setting the mode the cell
/// already has changes nothing.
async fn mode_set(
    State(api): State<Arc<Api>>,
    Json(request): Json<ModeRequest>,
) -> Result<Json<Value>, ApiError> {
    let Some(mode) = Mode::from_code(&request.mode) else {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "unknown_mode",
            format!(
                "{} is not a mode: normal, maintenance or shadow",
                request.mode
            ),
        ));
    };
    if request.actor.trim().is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "actor_required",
            "a mode change names the actor who asks for it",
        ));
    }
    let at_ns = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos()),
    )
    .unwrap_or(i64::MAX);
    let changed = api.commands.set_mode(mode, &request.actor, at_ns)?;
    Ok(Json(json!({ "mode": mode.code(), "changed": changed })))
}
