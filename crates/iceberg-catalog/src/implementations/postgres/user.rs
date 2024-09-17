use super::dbutils::DBErrorHandler;
use crate::api::management::v1::{User, UserOrigin};
use crate::service::{ErrorModel, Result};
use http::StatusCode;
use sqlx::PgConnection;
use uuid::Uuid;

#[expect(dead_code)]
pub(crate) async fn get_user(user_id: Uuid, connection: &mut PgConnection) -> Result<User> {
    let row = sqlx::query!(
        r#"
        SELECT 
            id,
            name,
            display_name,
            origin,
            email,
            created_at,
            updated_at
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
            .message(format!("User not found: '{user_id}'",))
            .r#type("UserNotFound".to_string())
            .build(),
        _ => e.into_error_model("Error fetching namespace".to_string()),
    })?;

    Ok(crate::api::management::v1::User {
        name: row.name,
        display_name: row.display_name,
        user_origin: match row.origin.as_str() {
            "explicit_registration" => UserOrigin::ExplicitRegistration,
            x => UserOrigin::ImplicitRegistration(x.to_string()),
        },
        email: row.email,
        id: row.id,

        created_at: row.created_at,
        updated_at: row.updated_at,
    })
}

pub(crate) async fn insert_user(
    user_id: Uuid,
    display_name: Option<&str>,
    name: Option<&str>,
    email: Option<&str>,
    origin: UserOrigin,
    connection: &mut PgConnection,
) -> Result<User> {
    let row = sqlx::query!(
        r#"
        INSERT INTO users (id, name, email, display_name, origin)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (id) -- we assume to have a globally unique id
        DO UPDATE SET name = $2, email = $3, display_name = $4, origin = $5
        returning id, name as "name?", email, display_name, created_at, updated_at, origin
        "#,
        user_id,
        name,
        email,
        display_name,
        match origin {
            UserOrigin::ExplicitRegistration => "explicit_registration".to_string(),
            UserOrigin::ImplicitRegistration(x) => x,
        },
    )
    .fetch_one(connection)
    .await
    .map_err(|e| e.into_error_model("Error inserting user".to_string()))?;

    Ok(User {
        name: row.name,
        display_name: row.display_name,
        user_origin: match row.origin.as_str() {
            "explicit_registration" => UserOrigin::ExplicitRegistration,
            x => UserOrigin::ImplicitRegistration(x.to_string()),
        },
        email: row.email,
        id: row.id,

        created_at: row.created_at,
        updated_at: row.updated_at,
    })
}
