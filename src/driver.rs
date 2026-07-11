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
            option_string(
                request,
                &[
                    "token",
                    "accessToken",
                    "oauthAccessToken",
                    "bearerToken",
                    "password",
                ],
            )
            .or_else(|| std::env::var("GOOGLE_OAUTH_ACCESS_TOKEN").ok())
            .ok_or_else(|| {
                "Cloud Spanner requires an OAuth access token or service account JSON.".to_string()
            })?
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
    use rsa::pkcs1v15::SigningKey;
    use rsa::pkcs8::DecodePrivateKey;
    use rsa::sha2::Sha256;
    use rsa::signature::{SignatureEncoding, Signer};
    use rsa::RsaPrivateKey;

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
    let pkey = RsaPrivateKey::from_pkcs8_pem(private_key)
        .map_err(|err| format!("invalid Google service account private key: {err}"))?;
    let signing_key = SigningKey::<Sha256>::new(pkey);
    let signature = signing_key.sign(payload.as_bytes()).to_vec();
    Ok(format!("{payload}.{}", base64_url_encode(&signature)))
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
    fn signs_service_account_jwt_with_pure_rust_rsa() {
        use rsa::pkcs8::{EncodePrivateKey, LineEnding};
        use rsa::rand_core::OsRng;
        use rsa::RsaPrivateKey;

        let private_key = RsaPrivateKey::new(&mut OsRng, 2048).expect("generate test RSA key");
        let private_key = private_key
            .to_pkcs8_pem(LineEnding::LF)
            .expect("encode test RSA key");
        let assertion = create_jwt_assertion(
            "service-account@example.invalid",
            private_key.as_str(),
            1_700_000_000,
        )
        .expect("sign JWT assertion");

        assert_eq!(assertion.split('.').count(), 3);
        assert!(!assertion.contains('='));
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
}
