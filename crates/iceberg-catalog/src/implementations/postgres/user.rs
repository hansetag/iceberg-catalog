use super::dbutils::DBErrorHandler;
use crate::api::management::v1::{User, UserOrigin};
use crate::service::token_verification::UserId;
use crate::service::{ErrorModel, Result};
use sqlx::PgConnection;

#[derive(sqlx::Type, Debug, Clone, Copy)]
#[sqlx(rename_all = "kebab-case")]
enum DbUserOrigin {
    ExplicitViaRegisterCall,
    ImplicitViaConfigCall,
}

#[expect(dead_code)]
pub(crate) async fn get_user(user_id: &str, connection: &mut PgConnection) -> Result<User> {
    let row = sqlx::query!(
        r#"
        SELECT 
            id,
            name,
            display_name,
            origin as "origin: DbUserOrigin",
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
        sqlx::Error::RowNotFound => ErrorModel::not_found(
            format!("User not found: '{user_id}'",),
            "UserNotFound",
            None,
        ),
        _ => e.into_error_model("Error fetching namespace".to_string()),
    })?;
    // let origin = row;
    Ok(crate::api::management::v1::User {
        name: row.name,
        display_name: row.display_name,
        user_origin: match row.origin {
            DbUserOrigin::ExplicitViaRegisterCall => UserOrigin::ExplicitViaRegisterCall,
            DbUserOrigin::ImplicitViaConfigCall => UserOrigin::ImplicitViaConfigCall,
        },
        email: row.email,
        id: row.id,
        created_at: row.created_at,
        updated_at: row.updated_at,
    })
}

pub(crate) async fn insert_user(
    id: &UserId,
    display_name: Option<&str>,
    name: &str,
    email: Option<&str>,
    origin: UserOrigin,
    connection: &mut PgConnection,
) -> Result<User> {
    // TODO: validate id again in here?

    let origin = match origin {
        UserOrigin::ExplicitViaRegisterCall => DbUserOrigin::ExplicitViaRegisterCall,
        UserOrigin::ImplicitViaConfigCall => DbUserOrigin::ImplicitViaConfigCall,
    };

    let row = sqlx::query!(
        r#"
        INSERT INTO users (id, name, email, display_name, origin)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (id) -- we assume to have a globally unique id
        DO UPDATE SET name = $2, email = $3, display_name = $4, origin = $5
        returning id, name as "name?", email, display_name, created_at, updated_at, origin as "origin: DbUserOrigin"
        "#,
        id.inner(),
        name,
        email,
        display_name,
        origin as _,
    )
    .fetch_one(connection)
    .await
    .map_err(|e| e.into_error_model("Error inserting user".to_string()))?;

    Ok(User {
        name: row.name,
        display_name: row.display_name,
        user_origin: match row.origin {
            DbUserOrigin::ExplicitViaRegisterCall => UserOrigin::ExplicitViaRegisterCall,
            DbUserOrigin::ImplicitViaConfigCall => UserOrigin::ImplicitViaConfigCall,
        },
        email: row.email,
        id: row.id,

        created_at: row.created_at,
        updated_at: row.updated_at,
    })
}
