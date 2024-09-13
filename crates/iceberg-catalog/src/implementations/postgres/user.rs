use super::dbutils::DBErrorHandler;
use crate::api::management::v1::CreateUserResponse;
use crate::service::{ErrorModel, Result};
use http::StatusCode;
use sqlx::PgConnection;
use uuid::Uuid;

pub(crate) async fn get_user(
    user_id: Uuid,
    connection: &mut PgConnection,
) -> Result<CreateUserResponse> {
    let row = sqlx::query!(
        r#"
        SELECT 
            id,
            name,
            email
        FROM users
        where deleted_at is null AND id = $1
        "#,
        user_id
    )
    .fetch_one(connection)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message(format!("User not found: {:?}", user_id))
            .r#type("UserNotFound".to_string())
            .build(),
        _ => e.into_error_model("Error fetching namespace".to_string()),
    })?;

    Ok(crate::api::management::v1::CreateUserResponse {
        id: row.id,
        name: row.name,
        email: row.email,
    })
}

pub(crate) async fn insert_user(
    user_id: Uuid,
    name: &str,
    email: &str,
    connection: &mut PgConnection,
) -> Result<CreateUserResponse> {
    let row = sqlx::query!(
        r#"
        INSERT INTO users (id, name, email)
        VALUES ($1, $2, $3)
        ON CONFLICT (id) -- we assume to have a globally unique id
        DO UPDATE SET name = $2, email = $3
        returning id, name, email
        "#,
        user_id,
        name,
        email
    )
    .fetch_one(connection)
    .await
    .map_err(|e| e.into_error_model("Error inserting user".to_string()))?;

    Ok(crate::api::management::v1::CreateUserResponse {
        id: row.id,
        name: row.name,
        email: row.email,
    })
}
