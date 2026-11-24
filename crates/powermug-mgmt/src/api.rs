//! The target API v1 subset (SPECIFICATIONS §11.1, `schemas/target-api-v1.yaml` of
//! gridloop): `GET /capabilities`, `GET /state`, `GET /revisions/{id}` and
//! `POST /revisions/rollback`.
//!
//! The prepare and activate calls take a signed plan. They are not in this subset:
//! the signed plan's form for partition revisions is not decided yet.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::commands::{CommandGateway, CommandRequest, GatewayError};
use crate::tls::ClientIdentity;
use axum::extract::connect_info::IntoMakeServiceWithConnectInfo;
use axum::extract::{ConnectInfo, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use ed25519_dalek::VerifyingKey;
use powermug_command::{CommandClient, CommandError, Granularity, Mode};
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
    gateway: Option<Arc<dyn CommandGateway>>,
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
            gateway: None,
        }
    }

    /// Gives the API the cell's command gateway: without it, `POST /commands` is unavailable.
    #[must_use]
    pub fn with_commands(mut self, gateway: Arc<dyn CommandGateway>) -> Self {
        self.gateway = Some(gateway);
        self
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

/// The service to serve over mutual TLS: the routes, with the identity of each connection's
/// client available to the handlers (`ConnectInfo<ClientIdentity>`).
pub fn service(api: Arc<Api>) -> IntoMakeServiceWithConnectInfo<Router, ClientIdentity> {
    router(api).into_make_service_with_connect_info::<ClientIdentity>()
}

/// The routes of the subset. A handler that reads the client's identity needs the service of
/// [`service`], which serves the connection's identity with the routes.
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
        .route("/commands", post(command_submit))
        .route("/commands/{id}/execute", post(command_execute))
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
    drop(store);
    let rules = rules_of(&api, active.as_deref())?;
    if let Some(gateway) = api.gateway.as_ref() {
        gateway.active_rules_changed(&request.site, &rules);
        gateway.active_revision_changed(&request.site, active.as_deref());
    }
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
    // The class the revision declares must not be lower than the one its change computes to.
    let previous = match active.as_deref() {
        Some(id) => {
            let stored = store.body(id)?.ok_or_else(|| missing_body(id))?;
            Some(parse_stored(&stored)?)
        }
        None => None,
    };
    let next = powermug_revision::partition::parse(&verified.document)
        .map_err(|finding| ApiError::invalid_document(vec![finding]))?;
    let class_findings =
        powermug_revision::change::declared_class_findings(&next, previous.as_ref());
    if !class_findings.is_empty() {
        return Err(ApiError::invalid_document(class_findings));
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

/// A revision stored by an earlier call, read back as a partition revision.
fn stored_revision(
    api: &Api,
    id: &str,
) -> Result<powermug_revision::partition::PartitionRevision, ApiError> {
    let stored = api.store().body(id)?.ok_or_else(|| missing_body(id))?;
    parse_stored(&stored)
}

/// The error of an active revision whose stored body is missing.
fn missing_body(id: &str) -> ApiError {
    ApiError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        "missing_revision",
        format!("the active revision {id} has no stored body"),
    )
}

/// Reads the stored body of a revision: the canonical bytes of its partition document.
fn parse_stored(
    stored: &[u8],
) -> Result<powermug_revision::partition::PartitionRevision, ApiError> {
    let document: Value = serde_json::from_slice(stored).map_err(|err| {
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "stored_revision_unreadable",
            err.to_string(),
        )
    })?;
    powermug_revision::partition::parse(&document).map_err(|finding| {
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "stored_revision_unreadable",
            finding.message,
        )
    })
}

/// The command rules of the revision active in a scope: none when no revision is active.
fn rules_of(
    api: &Api,
    active: Option<&str>,
) -> Result<Vec<powermug_revision::partition::CommandRule>, ApiError> {
    match active {
        Some(id) => Ok(stored_revision(api, id)?.spec.commands),
        None => Ok(Vec::new()),
    }
}

/// Whether activating `verified` changes the commands of its partition in a way that blocks them
/// (`COMMAND_CRITICAL`, FUN-CFG-007): the class of the change against the revision now active in
/// its scope. Nothing active yet, or the same revision, is not a change.
fn command_critical_change(api: &Api, verified: &Verified) -> Result<bool, ApiError> {
    let Some(previous_id) = api.store().active(&verified.scope)? else {
        return Ok(false);
    };
    if previous_id == verified.id {
        return Ok(false);
    }
    let before = stored_revision(api, &previous_id)?;
    let after = powermug_revision::partition::parse(&verified.document).map_err(|finding| {
        ApiError::new(StatusCode::BAD_REQUEST, "invalid_document", finding.message)
    })?;
    Ok(powermug_revision::change::change_class(&before, &after)
        == powermug_revision::change::ChangeClass::CommandCritical)
}

async fn activate(
    State(api): State<Arc<Api>>,
    Json(request): Json<PlanRequest>,
) -> Result<Json<Value>, ApiError> {
    let verified = verified(&api, &request)?;
    let critical = command_critical_change(&api, &verified)?;
    let gateway = api.gateway.as_deref();
    if critical {
        if let Some(gateway) = gateway {
            gateway.begin_critical_change(&verified.scope);
        }
    }
    let outcome = match api.store().activate(&verified.id, &verified.scope) {
        Ok(outcome) => outcome,
        Err(err) => {
            if critical {
                if let Some(gateway) = gateway {
                    gateway.abort_critical_change(&verified.scope);
                }
            }
            return Err(match err {
                GenerationError::UnknownRevision(_) => ApiError::new(
                    StatusCode::CONFLICT,
                    "not_prepared",
                    "the revision is not prepared",
                ),
                other => ApiError::from(other),
            });
        }
    };
    let status = match outcome {
        Outcome::Changed => "ACTIVE",
        Outcome::Unchanged => "ALREADY_ACTIVE",
    };
    let active = match api.store().active(&verified.scope) {
        Ok(active) => active,
        Err(err) => {
            if critical {
                if let Some(gateway) = gateway {
                    gateway.abort_critical_change(&verified.scope);
                }
            }
            return Err(ApiError::from(err));
        }
    };
    let rules = match rules_of(&api, active.as_deref()) {
        Ok(rules) => rules,
        Err(err) => {
            if critical {
                if let Some(gateway) = gateway {
                    gateway.abort_critical_change(&verified.scope);
                }
            }
            return Err(err);
        }
    };
    if let Some(gateway) = gateway {
        gateway.active_rules_changed(&verified.scope, &rules);
        gateway.active_revision_changed(&verified.scope, active.as_deref());
    }
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

/// The body of a mode change: the mode asked for. An `actor` may be given, but it must be the
/// common name of the client certificate, which is the actor the change is audited with.
#[derive(Debug, Deserialize)]
struct ModeRequest {
    mode: String,
    actor: Option<String>,
}

/// Sets the operating mode. The change is audited with the common name of the client
/// certificate as its actor. Setting the mode the cell already has changes nothing.
async fn mode_set(
    State(api): State<Arc<Api>>,
    ConnectInfo(client): ConnectInfo<ClientIdentity>,
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
    let actor = issuer_of(&client)?;
    if request.actor.as_deref().is_some_and(|named| named != actor) {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "actor_mismatch",
            "a mode change is audited under the client certificate, not another actor",
        ));
    }
    let at_ns = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos()),
    )
    .unwrap_or(i64::MAX);
    let changed = api.commands.set_mode(mode, actor, at_ns)?;
    Ok(Json(json!({ "mode": mode.code(), "changed": changed })))
}

impl From<GatewayError> for ApiError {
    fn from(error: GatewayError) -> Self {
        match error {
            GatewayError::UnknownPoint(point) => Self::new(
                StatusCode::NOT_FOUND,
                "unknown_point",
                format!("{point} is not a commandable point of this cell"),
            ),
            GatewayError::Journal(message) => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "journal_error",
                format!("the command journal could not record the command: {message}"),
            ),
            GatewayError::UnknownCommand(command_id) => Self::new(
                StatusCode::NOT_FOUND,
                "unknown_command",
                format!("no command {command_id} is known to this cell"),
            ),
            GatewayError::Refused(reason) => Self::new(
                StatusCode::CONFLICT,
                "not_executable",
                format!("the command cannot be executed now: {reason}"),
            ),
        }
    }
}

/// The body of a command: its identity, its point and operation, its value and its expiry.
#[derive(Debug, Deserialize)]
struct CommandBody {
    command_id: String,
    point: String,
    operation: String,
    value: bool,
    expires_ns: Option<i64>,
    expected_revision: Option<String>,
}

/// How long a command lives when the request does not say (30 s, H).
const DEFAULT_LIFETIME_NS: i64 = 30_000_000_000;

/// Hands a command to the cell (SPECIFICATIONS 9, [GW] 11.1). The answer is the state the cell
/// records after its first steps: an authorized command is dispatched and its state moves on
/// with the adapter's answers.
async fn command_submit(
    State(api): State<Arc<Api>>,
    ConnectInfo(client): ConnectInfo<ClientIdentity>,
    Json(body): Json<CommandBody>,
) -> Result<Json<Value>, ApiError> {
    let Some(gateway) = api.gateway.as_ref() else {
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "no_command_path",
            "this cell serves no commands",
        ));
    };
    if body.command_id.trim().is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "command_id_required",
            "a command names its command_id, the idempotency key",
        ));
    }
    if !matches!(body.operation.as_str(), "direct_execute" | "select") {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "unknown_operation",
            format!(
                "{} is not an operation: direct_execute or select",
                body.operation
            ),
        ));
    }
    let identity = issuer_of(&client)?;
    let now = now_ns();
    // The issuer is the common name of the client certificate, at client granularity (GW 11.6).
    let request = CommandRequest {
        command_id: body.command_id.clone(),
        identity: identity.to_owned(),
        identity_level: Granularity::Client,
        point: body.point,
        operation: body.operation,
        on: body.value,
        expires_ns: body.expires_ns.unwrap_or(now + DEFAULT_LIFETIME_NS),
        expected_revision: body.expected_revision.unwrap_or_default(),
    };
    let state = gateway.submit(&request, now)?;
    Ok(Json(json!({
        "command_id": body.command_id,
        "state": state.code(),
    })))
}

/// The identity a command is issued under: the client's common name. A certificate without a
/// common name cannot issue a command.
fn issuer_of(client: &ClientIdentity) -> Result<&str, ApiError> {
    client.common_name().ok_or_else(|| {
        ApiError::new(
            StatusCode::UNAUTHORIZED,
            "unidentified_client",
            "the client certificate has no common name, so it cannot issue a command",
        )
    })
}

fn now_ns() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos()),
    )
    .unwrap_or(i64::MAX)
}

/// The body of an execution: the point the selected command is for, which routes it.
#[derive(Debug, Deserialize)]
struct ExecuteBody {
    point: String,
}

/// Executes a command that was selected and confirmed (SPECIFICATIONS 9.1, [GW] 11.3): the
/// execution is sent, and its state moves on with the adapter's answers.
async fn command_execute(
    State(api): State<Arc<Api>>,
    ConnectInfo(client): ConnectInfo<ClientIdentity>,
    Path(command_id): Path<String>,
    Json(body): Json<ExecuteBody>,
) -> Result<Json<Value>, ApiError> {
    let Some(gateway) = api.gateway.as_ref() else {
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "no_command_path",
            "this cell serves no commands",
        ));
    };
    let identity = issuer_of(&client)?;
    let state = gateway.execute(&command_id, &body.point, identity, now_ns())?;
    Ok(Json(json!({
        "command_id": command_id,
        "state": state.code(),
    })))
}
