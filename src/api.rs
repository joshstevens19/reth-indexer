use axum::{
    extract::{Query, State},
    http::{self, response::Builder, Response, Uri},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio_postgres::{
    types::{FromSql, ToSql, Type},
    Row,
};
use uuid::Uuid;

use crate::{
    postgres::{solidity_type_to_db_type, PostgresClient},
    types::IndexerContractMapping,
};

#[derive(Debug, serde::Serialize, Deserialize)]
struct PageInfo {
    next: Option<String>,
    previous: Option<String>,
}

#[derive(Debug, serde::Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HandlerResult {
    events: Value,
    paging_info: PageInfo,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Clone)]
struct AppState {
    connection_string: String,
    event_mappings: Vec<IndexerContractMapping>,
}

/// Configures the API routes.
///
/// # Arguments
///
/// * `state` - The application state.
///
/// # Returns
///
/// The configured router.
async fn routes(state: AppState) -> Router {
    Router::new()
        .route(
            "/api/*rest",
            get(
                |s: State<AppState>, uri: Uri, params: Query<HashMap<String, String>>| async move {
                    match handler(&uri, params, &s.connection_string, &s.event_mappings).await {
                        Ok(response) => Ok(response),
                        Err(err) => {
                            let response: Response<_> = Builder::new()
                                .status(http::StatusCode::BAD_REQUEST)
                                .body::<String>(err.error)
                                .unwrap();

                            Err(response)
                        }
                    }
                },
            ),
        )
        .with_state(state)
}

/// Starts the reth indexer API server.
///
/// # Arguments
///
/// * `event_mappings` - A vector of `IndexerContractMapping` representing the event mappings.
/// * `connection_string` - The connection string for the database.
pub async fn start_api(event_mappings: &[IndexerContractMapping], connection_string: &str) {
    let routes = routes(AppState {
        connection_string: connection_string.to_string(),
        event_mappings: event_mappings.to_vec(),
    })
    .await;

    let addr = SocketAddr::from(([127, 0, 0, 1], 3030));
    println!("reth indexer API started, listening on {addr}");
    axum::Server::bind(&addr)
        .serve(routes.into_make_service())
        .await
        .expect("reth indexer API failed");
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum DynamicValue {
    String(String),
    Integer(u64),
    Null(Value),
    Uuid(Uuid),
    Bool(bool),
}

impl<'a> FromSql<'a> for DynamicValue {
    fn from_sql(ty: &Type, raw: &[u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        match ty.name() {
            "numeric" | "int4" => {
                let number = raw
                    .iter()
                    .fold(0u64, |acc, &byte| (acc << 8) | u64::from(byte));
                Ok(DynamicValue::Integer(number))
            }
            "uuid" => {
                let uuid = Uuid::from_slice(raw);
                Ok(uuid
                    .map(DynamicValue::Uuid)
                    .unwrap_or(DynamicValue::Null(serde_json::Value::Null)))
            }
            "bool" => {
                let bool_value: Option<bool> = serde_json::from_slice(raw)?;
                Ok(bool_value
                    .map(DynamicValue::Bool)
                    .unwrap_or(DynamicValue::Null(serde_json::Value::Null)))
            }
            _ => {
                let value: Option<&str> = std::str::from_utf8(raw).ok();
                Ok(value
                    .map(|s| DynamicValue::String(s.to_owned()))
                    .unwrap_or(DynamicValue::Null(serde_json::Value::Null)))
            }
        }
    }

    fn accepts(_ty: &Type) -> bool {
        // Indicate that the DynamicValue type accepts any PostgreSQL type
        true
    }
}

/// Handles the API request.
///
/// # Arguments
///
/// * `uri` - The request URI.
/// * `connection_string` - The connection string for the Postgres database.
/// * `event_mappings` - The mappings for the indexed contract events.
///
/// # Returns
///
/// Returns the JSON response wrapped in a `Result` indicating either success or an error response.
///
/// # Errors
///
/// Returns a `Response` with a JSON error response if there is an error processing the request.
async fn handler(
    uri: &Uri,
    search_params: Query<HashMap<String, String>>,
    connection_string: &str,
    event_mappings: &[IndexerContractMapping],
) -> Result<Json<HandlerResult>, ErrorResponse> {
    let event_name = extract_event_name(uri, event_mappings)?;

    let offset = search_params
        .get("offset")
        .unwrap_or(&"0".to_string())
        .parse::<i64>()
        .map_err(|_| create_error_response("Invalid value for parameter offset".to_string()))?;
    let limit = search_params
        .get("limit")
        .unwrap_or(&"50".to_string())
        .parse::<i64>()
        .map_err(|_| create_error_response("Invalid value for parameter limit".to_string()))?;

    let mut sql_query_params: Vec<&(dyn ToSql + Sync)> = Vec::new();
    sql_query_params.push(&offset as &(dyn ToSql + Sync));
    sql_query_params.push(&limit as &(dyn ToSql + Sync));

    let mut filter_sql = String::new();

    for event_mapping in event_mappings {
        for abi_item in &event_mapping.decode_abi_items {
            if abi_item.name.to_lowercase() != event_name.to_lowercase() {
                continue;
            }
            for abi_input in &abi_item.inputs {
                if let Some(value) = search_params.get(&abi_input.name) {
                    if filter_sql.is_empty() {
                        filter_sql.push_str("WHERE ");
                    } else {
                        filter_sql.push_str(" AND ");
                    }

                    let db_type = solidity_type_to_db_type(&abi_input.type_);
                    // handle numeric types a bit differently
                    if db_type == "NUMERIC" {
                        let sql_filter: &String =
                            &format!("\"{}\" = CAST('{}' AS integer)", abi_input.name, value);

                        // add the casting
                        filter_sql.push_str(sql_filter);
                        continue;
                    }

                    sql_query_params.push(value as &(dyn ToSql + Sync));
                    filter_sql.push_str(&format!(
                        "\"{}\" = ${}",
                        abi_input.name,
                        sql_query_params.len()
                    ));
                }
            }

            for (key, value) in search_params.iter() {
                let snake_case_key = match key.as_str() {
                    "blockNumber" => "block_number",
                    "indexedId" => "indexed_id",
                    "contractAddress" => "contract_address",
                    "txHash" => "tx_hash",
                    "blockHash" => "block_hash",
                    _ => key.as_str(),
                };

                match snake_case_key {
                    "block_number" | "indexed_id" | "contract_address" | "tx_hash"
                    | "block_hash" => {
                        if filter_sql.is_empty() {
                            filter_sql.push_str("WHERE ");
                        } else {
                            filter_sql.push_str(" AND ");
                        }

                        if snake_case_key == "block_number" {
                            let sql_filter: &String =
                                &format!("\"{}\" = CAST('{}' AS integer)", snake_case_key, value);

                            // add the casting
                            filter_sql.push_str(sql_filter);
                            continue;
                        }

                        sql_query_params.push(value as &(dyn ToSql + Sync));
                        let sql_filter: &String =
                            &format!("\"{}\" = ${}", snake_case_key, sql_query_params.len());
                        filter_sql.push_str(sql_filter);
                    }
                    _ => {}
                }
            }
        }
    }

    let mut postgres = PostgresClient::new(connection_string).await.unwrap();

    let query = format!(
        "SELECT * FROM {} {} ORDER BY \"timestamp\" OFFSET $1 LIMIT $2",
        event_name, filter_sql
    );

    let result = postgres.read(&query, &sql_query_params).await.unwrap();

    let response = build_response(&event_name, offset, limit, &result, search_params);

    Ok(Json(response))
}

/// Creates a error response message.
///
/// # Arguments
///
/// * `message` - The error message.
///
/// # Returns
///
/// Returns a `Response` with a ErrorResponse
///
/// # Generic
///
/// * `T` - The type of the error response data.
fn create_error_response(message: String) -> ErrorResponse {
    ErrorResponse { error: message }
}

/// Extracts the event name from the URI path and validates it against the event mappings.
///
/// # Arguments
///
/// * `uri` - The URI.
/// * `event_mappings` - The event mappings.
///
/// # Returns
///
/// Returns the extracted event name if it is valid, or an error response if it is invalid.
///
/// # Errors
///
/// Returns a `Response` with a JSON error response and a `BadRequest` status code if the event
/// name is invalid.
fn extract_event_name(
    uri: &Uri,
    event_mappings: &[IndexerContractMapping],
) -> Result<String, ErrorResponse> {
    let event_name = uri
        .path()
        .split('/')
        .last()
        .ok_or_else(|| create_error_response("Invalid event name".to_string()))?;

    if !event_mappings.iter().any(|mapping| {
        mapping
            .decode_abi_items
            .iter()
            .any(|f| f.name.to_lowercase() == event_name.to_lowercase())
    }) {
        return Err(create_error_response("Invalid event name".to_string()));
    }

    Ok(event_name.to_string())
}

/// Builds the response for the handler based on the query results.
///
/// This function takes the event_name, offset, limit, and query results and constructs the response
/// containing the events and paging information.
///
/// # Arguments
///
/// * `event_name` - The event name
/// * `offset` - The offset used for pagination.
/// * `limit` - The limit used for pagination.
/// * `result` - The query results.
///
/// # Returns
///
/// Returns the constructed `HandlerResult` containing the events and paging information.
fn build_response(
    event_name: &str,
    offset: i64,
    limit: i64,
    result: &[Row],
    search_params: Query<HashMap<String, String>>,
) -> HandlerResult {
    let events = result
        .iter()
        .map(|row| {
            let mut json_object: Map<String, Value> = Map::new();
            for (i, column) in row.columns().iter().enumerate() {
                match row.try_get::<_, DynamicValue>(i) {
                    Ok(value) => {
                        json_object.insert(
                            column.name().to_string(),
                            serde_json::to_value(value).unwrap(),
                        );
                    }
                    Err(err) => println!("error: {:?}", err),
                }
            }
            Value::Object(json_object)
        })
        .collect();

    let existing_query_string: String = search_params
        .iter()
        .filter(|(key, _)| key != &"limit" && key != &"offset")
        .map(|(key, value)| format!("{}={}", key, value))
        .collect::<Vec<String>>()
        .join("&");

    let paging_info = PageInfo {
        next: Some(format!(
            "127.0.0.1:3030/{}?offset={}&limit={}{}",
            event_name,
            offset + limit,
            limit,
            if !existing_query_string.is_empty() {
                format!("&{}", existing_query_string)
            } else {
                String::new()
            }
        )),
        previous: if (offset - limit) <= 0 {
            None
        } else {
            Some(format!(
                "127.0.0.1:3030/{}?offset={:?}&limit={}{}",
                event_name,
                offset - limit,
                limit,
                if !existing_query_string.is_empty() {
                    format!("&{}", existing_query_string)
                } else {
                    String::new()
                }
            ))
        },
    };

    HandlerResult {
        events,
        paging_info,
    }
}
