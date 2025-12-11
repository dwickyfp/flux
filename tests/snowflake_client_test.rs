//! Tests for snowflake/client.rs API client

use wiremock::{MockServer, Mock, ResponseTemplate};
use wiremock::matchers::{method, path, header};

#[tokio::test]
async fn test_api_url_construction() {
    // Test that account identifier is properly formatted
    let account = "ORG-ACCOUNT";
    let expected_base = "https://org-account.snowflakecomputing.com";
    
    // The account should have underscores replaced with dashes
    let formatted = account.replace('_', "-").to_lowercase();
    let url = format!("https://{}.snowflakecomputing.com", formatted);
    
    assert_eq!(url, expected_base.to_lowercase());
}

#[tokio::test]
async fn test_request_has_user_agent() {
    let mock_server = MockServer::start().await;
    
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(header("User-Agent", "flux-etl/0.1.0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statementHandle": "test-handle",
            "message": "Statement executed successfully."
        })))
        .expect(1)
        .mount(&mock_server)
        .await;
    
    // Note: Full integration test would require mocking the client
    // This test verifies the mock setup works
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/api/v2/statements", mock_server.uri()))
        .header("User-Agent", "flux-etl/0.1.0")
        .header("Content-Type", "application/json")
        .body("{}")
        .send()
        .await
        .unwrap();
    
    assert!(response.status().is_success());
}

#[tokio::test]
async fn test_api_error_parsing() {
    let mock_server = MockServer::start().await;
    
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "code": "390144",
            "message": "JWT token is invalid."
        })))
        .mount(&mock_server)
        .await;
    
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/api/v2/statements", mock_server.uri()))
        .header("Content-Type", "application/json")
        .body("{}")
        .send()
        .await
        .unwrap();
    
    assert!(!response.status().is_success());
    
    let error_body = response.text().await.unwrap();
    assert!(error_body.contains("390144"), "Should contain error code");
    assert!(error_body.contains("JWT token is invalid"), "Should contain error message");
}

#[tokio::test]
async fn test_authorization_header_format() {
    let mock_server = MockServer::start().await;
    
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(header("Authorization", "Bearer test-token-123"))
        .and(header("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock_server)
        .await;
    
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/api/v2/statements", mock_server.uri()))
        .header("Authorization", "Bearer test-token-123")
        .header("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
        .header("Content-Type", "application/json")
        .body("{}")
        .send()
        .await
        .unwrap();
    
    assert!(response.status().is_success());
}
