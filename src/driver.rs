use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, OnceLock};
use std::time::SystemTime;

use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use tokio::runtime::Runtime;

use crate::abi::{self, IrodoriConnectorBuffer};
use crate::{ABI_VERSION, CONFIG_JSON, DRIVER_LINKED, ENGINE, MANIFEST_JSON};

static CONNECTIONS: OnceLock<Mutex<HashMap<String, SpannerConnection>>> = OnceLock::new();
static RUNTIME: OnceLock<Runtime> = OnceLock::new();

#[derive(Clone)]
struct SpannerConnection {
    client: Client,
    config: SpannerConfig,
    session: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SpannerConfig {
    database_path: String,
    access_token: String,
    redaction_values: Vec<String>,
}

#[derive(Default)]
struct ObjectMeta {
    columns: Vec<Value>,
}

#[derive(Deserialize)]
struct GcpServiceAccountKey {
    project_id: String,
    client_email: String,
    private_key: String,
}

type QueryRows = Vec<Vec<Value>>;
type QueryOutput = (Vec<String>, QueryRows, bool);

fn connections() -> &'static Mutex<HashMap<String, SpannerConnection>> {
    CONNECTIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn runtime() -> Result<&'static Runtime, String> {
    if let Some(runtime) = RUNTIME.get() {
        return Ok(runtime);
    }
    let runtime = Runtime::new().map_err(|err| format!("create tokio runtime failed: {err}"))?;
    let _ = RUNTIME.set(runtime);
    RUNTIME
        .get()
        .ok_or_else(|| "create tokio runtime failed.".to_string())
}

pub fn call_json(request: IrodoriConnectorBuffer) -> IrodoriConnectorBuffer {
    let request = match abi::parse_request(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let method = match abi::request_method(request.as_ref()) {
        Ok(method) => method,
        Err(response) => return response,
    };

    match method {
        "health" | "ping" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
        ])),
        "describe" | "capabilities" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
            (
                "manifest".to_string(),
                serde_json::from_str(MANIFEST_JSON).unwrap_or(Value::Null),
            ),
            (
                "config".to_string(),
                serde_json::from_str(CONFIG_JSON).unwrap_or(Value::Null),
            ),
        ])),
        "manifest" => abi::owned_buffer(MANIFEST_JSON.to_string()),
        "config" => abi::owned_buffer(CONFIG_JSON.to_string()),
        "connect" => connect(request.as_ref().expect("connect has request")),
        "query" => query(request.as_ref().expect("query has request")),
        "metadata" => metadata(request.as_ref().expect("metadata has request")),
        "close" => close(request.as_ref().expect("close has request")),
        other => abi::error(
            "connector.unknownMethod",
            format!("unknown connector method: {other}"),
        ),
    }
}

fn connect(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let config = match runtime()
        .and_then(|runtime| runtime.block_on(SpannerConfig::from_request(request)))
    {
        Ok(config) => config,
        Err(err) => return abi::error("connector.invalidRequest", err),
    };
    let client = Client::new();
    let session =
        match runtime().and_then(|runtime| runtime.block_on(create_session(&client, &config))) {
            Ok(session) => session,
            Err(err) => return abi::error("connector.connectFailed", config.redact(&err)),
        };
    let connection = SpannerConnection {
        client,
        config,
        session,
    };
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let response = Map::from_iter([
        ("engine".to_string(), Value::String(ENGINE.to_string())),
        (
            "connectionId".to_string(),
            Value::String(connection_id.clone()),
        ),
        ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
        (
            "database".to_string(),
            Value::String(connection.config.database_path.clone()),
        ),
        (
            "serverVersion".to_string(),
            Value::String("Google Cloud Spanner v1 API".to_string()),
        ),
    ]);
    guard.insert(connection_id, connection);
    abi::ok(response)
}

fn query(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let Some(sql) = abi::string_field(request, "sql")
        .or_else(|| abi::string_field(request, "query"))
        .or_else(|| abi::string_field(request, "statement"))
    else {
        return abi::error(
            "connector.invalidRequest",
            "query requires a string sql, query, or statement field.",
        );
    };
    let connection = match connection(&connection_id) {
        Ok(connection) => connection,
        Err(response) => return response,
    };
    match runtime()
        .and_then(|runtime| runtime.block_on(execute_sql(&connection, sql, abi::max_rows(request))))
    {
        Ok((columns, rows, truncated)) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            (
                "columns".to_string(),
                Value::Array(columns.into_iter().map(Value::String).collect()),
            ),
            (
                "rows".to_string(),
                Value::Array(rows.into_iter().map(Value::Array).collect()),
            ),
            ("truncated".to_string(), Value::Bool(truncated)),
        ])),
        Err(err) => abi::error("connector.queryFailed", connection.config.redact(&err)),
    }
}

fn metadata(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let connection = match connection(&connection_id) {
        Ok(connection) => connection,
        Err(response) => return response,
    };
    match runtime().and_then(|runtime| runtime.block_on(load_metadata(&connection))) {
        Ok(metadata) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            ("metadata".to_string(), metadata),
        ])),
        Err(err) => abi::error("connector.metadataFailed", connection.config.redact(&err)),
    }
}

fn close(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let connection = match connections().lock() {
        Ok(mut guard) => guard.remove(&connection_id),
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    if let Some(connection) = connection.as_ref() {
        let _ = runtime().and_then(|runtime| runtime.block_on(delete_session(connection)));
    }
    abi::ok(Map::from_iter([
        ("connectionId".to_string(), Value::String(connection_id)),
        ("closed".to_string(), Value::Bool(connection.is_some())),
    ]))
}

impl SpannerConfig {
    async fn from_request(request: &Value) -> Result<Self, String> {
        let service_json = option_string(
            request,
            &["serviceAccountJson", "credentialsJson", "serviceAccountKey"],
        )
        .or_else(|| {
            option_string(request, &["password", "privateKey"])
                .filter(|value| value.trim_start().starts_with('{'))
        });
        let access_token = if let Some(service_json) = service_json {
            let key: GcpServiceAccountKey = serde_json::from_str(&service_json)
                .map_err(|err| format!("invalid Google service account JSON: {err}"))?;
            fetch_oauth2_token(&Client::new(), &key.client_email, &key.private_key).await?
        } else {
            let explicit = option_string(
                request,
                &[
                    "token",
                    "accessToken",
                    "oauthAccessToken",
                    "bearerToken",
                    "password",
                ],
            )
            .or_else(|| std::env::var("GOOGLE_OAUTH_ACCESS_TOKEN").ok());
            // Nothing supplied: fall back to Application Default Credentials
            // rather than refusing. On a developer machine that means the
            // `gcloud` login already there, and on GCE/GKE/Cloud Run the
            // metadata server, which is why ADC works with nothing configured.
            match explicit {
                Some(token) => token,
                None => {
                    fetch_adc_token(
                        &Client::new(),
                        "https://www.googleapis.com/auth/cloud-platform",
                    )
                    .await?
                }
            }
        };
        // Borrow another service account's permissions without holding its key.
        let access_token = match option_string(
            request,
            &["impersonateServiceAccount", "serviceAccountImpersonation"],
        ) {
            Some(target) => {
                let delegates: Vec<String> = option_string(request, &["impersonationDelegates"])
                    .map(|value| {
                        value
                            .split(',')
                            .map(str::trim)
                            .filter(|part| !part.is_empty())
                            .map(ToOwned::to_owned)
                            .collect()
                    })
                    .unwrap_or_default();
                impersonate_service_account(
                    &Client::new(),
                    &access_token,
                    &target,
                    "https://www.googleapis.com/auth/cloud-platform",
                    &delegates,
                )
                .await?
            }
            None => access_token,
        };
        let project = option_string(request, &["projectId", "project"])
            .or_else(|| service_json_project(request))
            .ok_or_else(|| "Cloud Spanner requires projectId.".to_string())?;
        let instance = option_string(request, &["instanceId", "instance"])
            .ok_or_else(|| "Cloud Spanner requires instanceId.".to_string())?;
        let database = option_string(request, &["databaseId", "database", "db"])
            .ok_or_else(|| "Cloud Spanner requires databaseId.".to_string())?;
        let database_path = format!("projects/{project}/instances/{instance}/databases/{database}");
        let mut redaction_values = Vec::new();
        push_sensitive(&mut redaction_values, Some(&access_token));
        Ok(Self {
            database_path,
            access_token,
            redaction_values,
        })
    }

    fn redact(&self, message: &str) -> String {
        self.redaction_values
            .iter()
            .fold(message.to_string(), |message, secret| {
                if secret.is_empty() {
                    message
                } else {
                    message.replace(secret, "****")
                }
            })
    }
}

fn service_json_project(request: &Value) -> Option<String> {
    let service_json = option_string(
        request,
        &["serviceAccountJson", "credentialsJson", "serviceAccountKey"],
    )?;
    serde_json::from_str::<GcpServiceAccountKey>(&service_json)
        .ok()
        .map(|key| key.project_id)
}

async fn create_session(client: &Client, config: &SpannerConfig) -> Result<String, String> {
    let url = format!(
        "https://spanner.googleapis.com/v1/{}:sessions",
        config.database_path
    );
    let value = request_json(config, client.post(url).json(&json!({}))).await?;
    value
        .get("name")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| "Cloud Spanner create session response missing name.".to_string())
}

async fn delete_session(connection: &SpannerConnection) -> Result<(), String> {
    let url = format!("https://spanner.googleapis.com/v1/{}", connection.session);
    let _ = request_text(&connection.config, connection.client.delete(url)).await?;
    Ok(())
}

async fn execute_sql(
    connection: &SpannerConnection,
    sql: &str,
    cap: usize,
) -> Result<QueryOutput, String> {
    let url = format!(
        "https://spanner.googleapis.com/v1/{}:executeSql",
        connection.session
    );
    let value = request_json(
        &connection.config,
        connection.client.post(url).json(&json!({ "sql": sql })),
    )
    .await?;
    Ok(spanner_result_to_output(value, cap))
}

async fn load_metadata(connection: &SpannerConnection) -> Result<Value, String> {
    let sql = "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, SPANNER_TYPE, ORDINAL_POSITION, IS_NULLABLE \
               FROM INFORMATION_SCHEMA.COLUMNS \
               WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA') \
               ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION";
    let (columns, rows, _) = execute_sql(connection, sql, 100_000).await?;
    let mut schemas: BTreeMap<String, BTreeMap<String, ObjectMeta>> = BTreeMap::new();
    for row in rows {
        let schema = field(&columns, &row, "TABLE_SCHEMA").unwrap_or_default();
        let table = field(&columns, &row, "TABLE_NAME").unwrap_or_default();
        let column = field(&columns, &row, "COLUMN_NAME").unwrap_or_default();
        if table.is_empty() || column.is_empty() {
            continue;
        }
        let object = schemas.entry(schema).or_default().entry(table).or_default();
        object.columns.push(json!({
            "name": column,
            "dataType": field(&columns, &row, "SPANNER_TYPE").unwrap_or_default(),
            "nullable": field(&columns, &row, "IS_NULLABLE")
                .map(|value| value.eq_ignore_ascii_case("YES") || value.eq_ignore_ascii_case("true"))
                .unwrap_or(true),
            "ordinal": field(&columns, &row, "ORDINAL_POSITION")
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or((object.columns.len() + 1) as i64)
        }));
    }
    Ok(json!({
        "schemas": schemas
            .into_iter()
            .map(|(schema, objects)| json!({
                "name": schema,
                "objects": objects
                    .into_iter()
                    .map(|(name, object)| json!({
                        "schema": schema,
                        "name": name,
                        "kind": "table",
                        "columns": object.columns,
                        "indexes": [],
                        "primaryKey": [],
                        "foreignKeys": []
                    }))
                    .collect::<Vec<_>>()
            }))
            .collect::<Vec<_>>()
    }))
}

fn spanner_result_to_output(value: Value, cap: usize) -> QueryOutput {
    let columns = value
        .pointer("/metadata/rowType/fields")
        .and_then(Value::as_array)
        .map(|fields| {
            fields
                .iter()
                .filter_map(|field| field.get("name").and_then(Value::as_str))
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let row_values = value
        .get("rows")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let truncated = row_values.len() > cap;
    let rows = row_values
        .into_iter()
        .take(cap)
        .map(|row| row.as_array().cloned().unwrap_or_else(|| vec![row]))
        .collect();
    (columns, rows, truncated)
}

async fn request_json(
    config: &SpannerConfig,
    builder: reqwest::RequestBuilder,
) -> Result<Value, String> {
    let text = request_text(config, builder).await?;
    serde_json::from_str::<Value>(&text)
        .map_err(|err| format!("Cloud Spanner JSON response parse failed: {err}: {text}"))
}

async fn request_text(
    config: &SpannerConfig,
    builder: reqwest::RequestBuilder,
) -> Result<String, String> {
    let response = builder
        .bearer_auth(&config.access_token)
        .send()
        .await
        .map_err(|err| format!("Cloud Spanner request failed: {err}"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|err| format!("Cloud Spanner response read failed: {err}"))?;
    if !status.is_success() {
        return Err(format!("Cloud Spanner returned HTTP {status}: {text}"));
    }
    Ok(text)
}

/// A credential file as Application Default Credentials stores it.
///
/// ADC is not one thing. `gcloud auth application-default login` writes an
/// `authorized_user` file — a refresh token, not a key — while a service
/// account downloaded from the console writes a `service_account` file, and
/// workload identity federation writes `external_account`. They need three
/// different exchanges, and reading the `type` field is the only way to know
/// which one is in front of you.
#[derive(Debug, PartialEq, Eq)]
enum AdcKind {
    ServiceAccount,
    AuthorizedUser,
    ExternalAccount,
    Unknown(String),
}

fn adc_kind(document: &Value) -> AdcKind {
    match document.get("type").and_then(Value::as_str) {
        Some("service_account") => AdcKind::ServiceAccount,
        Some("authorized_user") => AdcKind::AuthorizedUser,
        Some("external_account") => AdcKind::ExternalAccount,
        Some(other) => AdcKind::Unknown(other.to_string()),
        None => AdcKind::Unknown(String::new()),
    }
}

/// Where Application Default Credentials looks for a credential file.
///
/// `GOOGLE_APPLICATION_CREDENTIALS` first, then the well-known path
/// `gcloud auth application-default login` writes to — the same order the
/// Google client libraries use, so a machine already set up for `gcloud` needs
/// no configuration here at all.
fn adc_paths() -> Vec<String> {
    adc_paths_from(
        std::env::var("GOOGLE_APPLICATION_CREDENTIALS")
            .ok()
            .as_deref(),
        std::env::var("CLOUDSDK_CONFIG").ok().as_deref(),
        std::env::var("HOME").ok().as_deref(),
    )
}

/// The search order itself, with the environment passed in.
///
/// Kept pure so it can be tested without `set_var`: the environment is
/// process-global, so env-mutating tests race each other under the default
/// parallel runner and fail in a way that looks like a logic bug.
fn adc_paths_from(
    explicit: Option<&str>,
    cloudsdk_config: Option<&str>,
    home: Option<&str>,
) -> Vec<String> {
    let mut paths = Vec::new();
    if let Some(explicit) = explicit.map(str::trim).filter(|value| !value.is_empty()) {
        paths.push(explicit.to_string());
    }
    let config_dir = cloudsdk_config
        .map(str::to_string)
        .or_else(|| home.map(|home| format!("{home}/.config/gcloud")));
    if let Some(config_dir) = config_dir {
        paths.push(format!("{config_dir}/application_default_credentials.json"));
    }
    paths
}

/// Exchange an `authorized_user` refresh token for an access token.
async fn fetch_refresh_token_grant(
    client: &Client,
    client_id: &str,
    client_secret: &str,
    refresh_token: &str,
) -> Result<String, String> {
    let body = format!(
        "grant_type=refresh_token&client_id={}&client_secret={}&refresh_token={}",
        form_encode(client_id),
        form_encode(client_secret),
        form_encode(refresh_token)
    );
    let response = client
        .post("https://oauth2.googleapis.com/token")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .map_err(|err| format!("Google token request failed: {err}"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|err| format!("Google token response read failed: {err}"))?;
    if !status.is_success() {
        return Err(format!(
            "Google returned HTTP {status} for the token request."
        ));
    }
    serde_json::from_str::<Value>(&text)
        .ok()
        .and_then(|value| {
            value
                .get("access_token")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .ok_or_else(|| "Google token response contained no access_token.".to_string())
}

/// Resolve an access token from Application Default Credentials.
async fn fetch_adc_token(client: &Client, scope: &str) -> Result<String, String> {
    let mut tried = Vec::new();
    for path in adc_paths() {
        let Ok(text) = std::fs::read_to_string(&path) else {
            tried.push(path);
            continue;
        };
        let document: Value = serde_json::from_str(&text)
            .map_err(|err| format!("credential file at {path} is not valid JSON: {err}"))?;
        return match adc_kind(&document) {
            AdcKind::ServiceAccount => {
                let key: GcpServiceAccountKey =
                    serde_json::from_value(document).map_err(|err| {
                        format!("service account file at {path} is missing fields: {err}")
                    })?;
                fetch_oauth2_token(client, &key.client_email, &key.private_key).await
            }
            AdcKind::AuthorizedUser => {
                let field = |name: &str| {
                    document
                        .get(name)
                        .and_then(Value::as_str)
                        .ok_or_else(|| format!("credential file at {path} is missing {name}."))
                };
                fetch_refresh_token_grant(
                    client,
                    field("client_id")?,
                    field("client_secret")?,
                    field("refresh_token")?,
                )
                .await
            }
            // Deliberately not guessed at: external_account is workload identity
            // federation, which needs a token exchange this connector does not
            // implement. Saying so beats a confusing failure three calls later.
            AdcKind::ExternalAccount => Err(format!(
                "the credential file at {path} is a workload identity (external_account) \
                 credential, which this connector does not support yet. Use a service \
                 account key or `gcloud auth application-default login`."
            )),
            AdcKind::Unknown(kind) => Err(format!(
                "the credential file at {path} has an unrecognised credential type {kind:?}."
            )),
        };
    }

    // No file anywhere: on GCE/GKE/Cloud Run the metadata server is the
    // credential source, and it is the reason ADC works with nothing configured.
    fetch_metadata_token(client, scope).await.map_err(|err| {
        if tried.is_empty() {
            err
        } else {
            format!("{err} (no credential file at: {})", tried.join(", "))
        }
    })
}

/// Ask the GCE metadata server for a token.
async fn fetch_metadata_token(client: &Client, scope: &str) -> Result<String, String> {
    let host = std::env::var("GCE_METADATA_HOST")
        .unwrap_or_else(|_| "metadata.google.internal".to_string());
    let url = format!(
        "http://{host}/computeMetadata/v1/instance/service-accounts/default/token?scopes={}",
        form_encode(scope)
    );
    let response = client
        .get(url)
        .header("Metadata-Flavor", "Google")
        .send()
        .await
        .map_err(|_| {
            "no Google credentials found: set GOOGLE_APPLICATION_CREDENTIALS, run \
             `gcloud auth application-default login`, or supply a service account key."
                .to_string()
        })?;
    let text = response
        .text()
        .await
        .map_err(|err| format!("metadata token response read failed: {err}"))?;
    serde_json::from_str::<Value>(&text)
        .ok()
        .and_then(|value| {
            value
                .get("access_token")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .ok_or_else(|| "the metadata server returned no access_token.".to_string())
}

/// Exchange a token for one belonging to another service account.
///
/// This is what `--impersonate-service-account` does: the caller keeps its own
/// identity and borrows the target's permissions, so nobody has to hold the
/// target's key.
async fn impersonate_service_account(
    client: &Client,
    source_token: &str,
    target: &str,
    scope: &str,
    delegates: &[String],
) -> Result<String, String> {
    let url = format!(
        "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{}:generateAccessToken",
        form_encode(target)
    );
    let body = serde_json::json!({
        "scope": [scope],
        "delegates": delegates
            .iter()
            .map(|d| format!("projects/-/serviceAccounts/{d}"))
            .collect::<Vec<_>>(),
    });
    let response = client
        .post(url)
        .bearer_auth(source_token)
        .json(&body)
        .send()
        .await
        .map_err(|err| format!("impersonation request failed: {err}"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|err| format!("impersonation response read failed: {err}"))?;
    if !status.is_success() {
        return Err(format!(
            "impersonating {target} failed with HTTP {status}. The caller needs \
             roles/iam.serviceAccountTokenCreator on that service account."
        ));
    }
    serde_json::from_str::<Value>(&text)
        .ok()
        .and_then(|value| {
            value
                .get("accessToken")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .ok_or_else(|| "impersonation response contained no accessToken.".to_string())
}

fn form_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

async fn fetch_oauth2_token(
    client: &Client,
    email: &str,
    private_key: &str,
) -> Result<String, String> {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let assertion = create_jwt_assertion(email, private_key, now)?;
    let body = format!(
        "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion={assertion}"
    );
    let response = client
        .post("https://oauth2.googleapis.com/token")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .map_err(|err| format!("GCP token request failed: {err}"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|err| format!("GCP token response read failed: {err}"))?;
    if !status.is_success() {
        return Err(format!("GCP token request returned HTTP {status}: {text}"));
    }
    let value = serde_json::from_str::<Value>(&text)
        .map_err(|err| format!("GCP token JSON parse failed: {err}: {text}"))?;
    value
        .get("access_token")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| "GCP token response missing access_token.".to_string())
}

fn create_jwt_assertion(email: &str, private_key: &str, now: u64) -> Result<String, String> {
    let exp = now + 3600;
    let header = r#"{"alg":"RS256","typ":"JWT"}"#;
    let claims = format!(
        r#"{{"iss":"{}","scope":"https://www.googleapis.com/auth/spanner.data https://www.googleapis.com/auth/cloud-platform","aud":"https://oauth2.googleapis.com/token","exp":{},"iat":{}}}"#,
        email, exp, now
    );
    let payload = format!(
        "{}.{}",
        base64_url_encode(header.as_bytes()),
        base64_url_encode(claims.as_bytes())
    );
    let signature = sign_rs256(private_key, payload.as_bytes())?;
    Ok(format!("{payload}.{}", base64_url_encode(&signature)))
}

fn sign_rs256(private_key: &str, message: &[u8]) -> Result<Vec<u8>, String> {
    use ring::rand::SystemRandom;
    use ring::signature::{RsaKeyPair, RSA_PKCS1_SHA256};

    let key = pem::parse(private_key)
        .map_err(|_| "invalid Google service account private key PEM.".to_string())?;
    if key.tag() != "PRIVATE KEY" {
        return Err("Google service account private key must use PKCS#8 PEM.".to_string());
    }
    let key_pair = RsaKeyPair::from_pkcs8(key.contents())
        .map_err(|_| "invalid Google service account PKCS#8 private key.".to_string())?;
    let mut signature = vec![0; key_pair.public().modulus_len()];
    key_pair
        .sign(
            &RSA_PKCS1_SHA256,
            &SystemRandom::new(),
            message,
            &mut signature,
        )
        .map_err(|_| "Google service account JWT signing failed.".to_string())?;
    Ok(signature)
}

fn base64_url_encode(input: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    let mut i = 0;
    while i < input.len() {
        let b0 = input[i] as usize;
        let b1 = if i + 1 < input.len() {
            input[i + 1] as usize
        } else {
            0
        };
        let b2 = if i + 2 < input.len() {
            input[i + 2] as usize
        } else {
            0
        };
        out.push(CHARS[b0 >> 2] as char);
        out.push(CHARS[((b0 & 3) << 4) | (b1 >> 4)] as char);
        if i + 1 < input.len() {
            out.push(CHARS[((b1 & 15) << 2) | (b2 >> 6)] as char);
        }
        if i + 2 < input.len() {
            out.push(CHARS[b2 & 63] as char);
        }
        i += 3;
    }
    out
}

fn connection(connection_id: &str) -> Result<SpannerConnection, IrodoriConnectorBuffer> {
    let guard = connections().lock().map_err(|_| {
        abi::error(
            "connector.statePoisoned",
            "Connector connection state is poisoned.",
        )
    })?;
    guard.get(connection_id).cloned().ok_or_else(|| {
        abi::error(
            "connector.connectionNotFound",
            format!("no open connection: {connection_id}"),
        )
    })
}

fn field(columns: &[String], row: &[Value], name: &str) -> Option<String> {
    columns
        .iter()
        .position(|column| column.eq_ignore_ascii_case(name))
        .and_then(|index| row.get(index))
        .and_then(|value| match value {
            Value::Null => None,
            Value::String(value) => Some(value.clone()),
            Value::Number(value) => Some(value.to_string()),
            Value::Bool(value) => Some(value.to_string()),
            _ => None,
        })
}

fn request_containers(request: &Value) -> Vec<&Value> {
    [
        Some(request),
        request.get("profile"),
        request.get("options"),
        request.get("auth"),
        request.get("secrets"),
        request
            .get("profile")
            .and_then(|profile| profile.get("options")),
        request
            .get("profile")
            .and_then(|profile| profile.get("auth")),
        request
            .get("profile")
            .and_then(|profile| profile.get("secrets")),
    ]
    .into_iter()
    .flatten()
    .collect()
}

fn option_string(request: &Value, fields: &[&str]) -> Option<String> {
    request_containers(request)
        .into_iter()
        .find_map(|container| {
            fields.iter().find_map(|field| {
                container
                    .get(*field)
                    .map(|value| match value {
                        Value::String(value) => value.clone(),
                        Value::Number(value) => value.to_string(),
                        Value::Bool(value) => value.to_string(),
                        _ => String::new(),
                    })
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
            })
        })
}

fn push_sensitive(values: &mut Vec<String>, value: Option<&str>) {
    if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
        if !values.iter().any(|existing| existing == value) {
            values.push(value.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_base64_url_without_padding() {
        assert_eq!(base64_url_encode(b"abc"), "YWJj");
        assert_eq!(base64_url_encode(b"ab"), "YWI");
    }

    #[test]
    fn rejects_invalid_service_account_private_keys_without_echoing_them() {
        let private_key = "not-a-private-key";
        let error = create_jwt_assertion(
            "service-account@example.invalid",
            private_key,
            1_700_000_000,
        )
        .expect_err("reject invalid JWT signing key");

        assert!(error.contains("private key"));
        assert!(!error.contains(private_key));
    }

    #[test]
    fn maps_spanner_rows() {
        let value = json!({
            "metadata": {"rowType": {"fields": [{"name": "id"}]}},
            "rows": [["1"]]
        });
        let (columns, rows, truncated) = spanner_result_to_output(value, 10);
        assert_eq!(columns, vec!["id"]);
        assert_eq!(rows[0], vec![json!("1")]);
        assert!(!truncated);
    }

    #[test]
    fn recognises_each_application_default_credential_shape() {
        // ADC is three different files needing three different exchanges, and
        // the `type` field is the only thing that says which.
        assert_eq!(
            adc_kind(&json!({ "type": "service_account", "client_email": "a@b" })),
            AdcKind::ServiceAccount
        );
        assert_eq!(
            adc_kind(&json!({ "type": "authorized_user", "refresh_token": "r" })),
            AdcKind::AuthorizedUser
        );
        assert_eq!(
            adc_kind(&json!({ "type": "external_account" })),
            AdcKind::ExternalAccount
        );
        assert_eq!(
            adc_kind(&json!({ "type": "something_new" })),
            AdcKind::Unknown("something_new".to_string())
        );
        assert_eq!(adc_kind(&json!({})), AdcKind::Unknown(String::new()));
    }

    #[test]
    fn looks_for_credentials_where_gcloud_puts_them() {
        // Matching the Google client libraries' search order means a machine
        // already set up for `gcloud` needs no configuration here.
        assert_eq!(
            adc_paths_from(
                Some("/keys/explicit.json"),
                Some("/cfg/gcloud"),
                Some("/home/u")
            ),
            vec![
                "/keys/explicit.json".to_string(),
                "/cfg/gcloud/application_default_credentials.json".to_string(),
            ]
        );
        // Without CLOUDSDK_CONFIG the well-known path under HOME is used.
        assert_eq!(
            adc_paths_from(None, None, Some("/home/u")),
            vec!["/home/u/.config/gcloud/application_default_credentials.json".to_string()]
        );
        // Nothing to go on: the caller falls through to the metadata server.
        assert!(adc_paths_from(None, None, None).is_empty());
    }

    #[test]
    fn an_empty_credentials_variable_is_not_a_path() {
        // An exported-but-empty variable is common in shell profiles and would
        // otherwise send the search to "".
        assert_eq!(
            adc_paths_from(Some("   "), Some("/cfg"), None),
            vec!["/cfg/application_default_credentials.json".to_string()]
        );
    }

    #[test]
    fn form_encoding_protects_the_grant_body() {
        // A refresh token or a service account email in a form body must not be
        // able to introduce another parameter.
        assert_eq!(form_encode("a&b=c"), "a%26b%3Dc");
        assert_eq!(
            form_encode("svc@project.iam.gserviceaccount.com"),
            "svc%40project.iam.gserviceaccount.com"
        );
        assert_eq!(form_encode("plain-Token_1.0~"), "plain-Token_1.0~");
    }
}
