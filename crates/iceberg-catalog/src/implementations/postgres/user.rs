use super::dbutils::DBErrorHandler;
use crate::api::management::v1::{
    CreateRoleRequest, ListRolesResponse, ListUsersResponse, Role, UpdateUserRequest, User,
    UserOrigin,
};
use crate::service::token_verification::UserId;
use crate::service::{ErrorModel, Result};
use sqlx::PgConnection;

#[derive(sqlx::Type, Debug, Clone, Copy)]
#[sqlx(rename_all = "kebab-case")]
enum DbUserOrigin {
    ExplicitViaRegisterCall,
    ImplicitViaConfigCall,
}
#[derive(sqlx::FromRow, Debug)]
pub struct UserRow {
    id: String,
    name: String,
    email: Option<String>,
    origin: DbUserOrigin,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<UserRow> for User {
    fn from(
        UserRow {
            id,
            name,
            email,
            origin,
            created_at,
            updated_at,
        }: UserRow,
    ) -> Self {
        User {
            id,
            name,
            email,
            user_origin: match origin {
                DbUserOrigin::ExplicitViaRegisterCall => UserOrigin::ExplicitViaRegisterCall,
                DbUserOrigin::ImplicitViaConfigCall => UserOrigin::ImplicitViaConfigCall,
            },
            created_at,
            updated_at,
        }
    }
}

pub(crate) async fn list_users<'e, 'c: 'e, E: sqlx::Executor<'c, Database = sqlx::Postgres>>(
    connection: E,
    include_deleted: bool,
    name_filter: Option<&str>,
) -> Result<ListUsersResponse> {
    // TODO: impl pagination
    let users = sqlx::query_as!(
        UserRow,
        r#"
        SELECT
            id,
            name,
            origin as "origin: DbUserOrigin",
            email,
            created_at,
            updated_at
        FROM users
        where (deleted_at is null OR $1) AND (name ILIKE $2)
        "#,
        include_deleted,
        name_filter.unwrap_or("%").to_string(),
    )
    .fetch_all(connection)
    .await
    .map_err(|e| e.into_error_model("Error fetching namespace".to_string()))?
    .into_iter()
    .map(User::from)
    .collect();

    Ok(ListUsersResponse {
        users,
        next_page_token: None,
    })
}

pub(crate) async fn delete_user<'c, 'e: 'c, E: sqlx::Executor<'c, Database = sqlx::Postgres>>(
    id: UserId,
    connection: E,
) -> Result<()> {
    // TODO: @christian soft delete or hard delete? I'm leaning towards soft delete to avoid
    //       dangling references if we ever decide to have entities reference users
    let row = sqlx::query!(
        r#"
        UPDATE users
        SET deleted_at = now()
        WHERE id = $1
        "#,
        id.inner(),
    )
    .execute(connection)
    .await
    .map_err(|e| e.into_error_model("Error deleting user".to_string()))?;

    if row.rows_affected() == 0 {
        return Err(ErrorModel::not_found(
            format!("User not found: '{id}'",),
            "UserNotFound",
            None,
        )
        .into());
    }

    Ok(())
}

pub(crate) async fn insert_user(
    id: &UserId,
    name: &str,
    email: Option<&str>,
    origin: UserOrigin,
    connection: &mut PgConnection,
) -> Result<User> {
    let origin = match origin {
        UserOrigin::ExplicitViaRegisterCall => DbUserOrigin::ExplicitViaRegisterCall,
        UserOrigin::ImplicitViaConfigCall => DbUserOrigin::ImplicitViaConfigCall,
    };

    Ok(sqlx::query_as!(
        UserRow,
        r#"
        INSERT INTO users (id, name, email, origin)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (id) -- we assume to have a globally unique id
        DO UPDATE SET name = $2, email = $3, origin = $4
        returning id, name, email, created_at, updated_at, origin as "origin: DbUserOrigin"
        "#,
        id.inner(),
        name,
        email,
        origin as _,
    )
    .fetch_one(connection)
    .await
    .map_err(|e| e.into_error_model("Error inserting user".to_string()))
    .map(User::from)?)
}

pub(crate) async fn update_user<'c, 'e: 'c, E>(
    id: UserId,
    UpdateUserRequest { name, email }: UpdateUserRequest,
    catalog_state: E,
) -> Result<User>
where
    E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let row = sqlx::query_as!(
        UserRow,
        r#"
        UPDATE users
        SET name = $2, email = $3, updated_at = now()
        WHERE id = $1
        returning id, name, email, created_at, updated_at, origin as "origin: DbUserOrigin"
        "#,
        id.inner(),
        name,
        email,
    )
    .fetch_optional(catalog_state)
    .await
    .map_err(|e| e.into_error_model("Error updating user".to_string()))?;

    if let Some(row) = row {
        Ok(User::from(row))
    } else {
        Err(ErrorModel::not_found(format!("User not found: '{id}'",), "UserNotFound", None).into())
    }
}

pub(crate) async fn insert_role<'c, 'e: 'c, E>(
    CreateRoleRequest {
        id,
        name,
        description,
    }: CreateRoleRequest,
    connection: E,
) -> Result<Role>
where
    E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let row = sqlx::query!(
        r#"
        INSERT INTO roles (id, name, description)
        VALUES ($1, $2, $3)
        ON CONFLICT (id) -- we assume to have a globally unique id
        DO UPDATE SET name = $2, description = $3
        returning id, name, description, created_at, updated_at
        "#,
        id,
        name,
        description,
    )
    .fetch_one(connection)
    .await
    .map_err(|e| e.into_error_model("Error inserting role".to_string()))?;

    Ok(Role {
        id: row.id,
        name: row.name,
        description: row.description,
        created_at: row.created_at,
        updated_at: row.updated_at,
    })
}

pub(crate) async fn list_roles<'c, 'e: 'c, E>(connection: E) -> Result<ListRolesResponse>
where
    E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    // TODO: impl pagination
    let roles = sqlx::query!(
        r#"
        SELECT id, name, description, created_at, updated_at
        FROM roles
        "#,
    )
    .fetch_all(connection)
    .await
    .map_err(|e| e.into_error_model("Error fetching roles".to_string()))?
    .into_iter()
    .map(|row| Role {
        id: row.id,
        name: row.name,
        description: row.description,
        created_at: row.created_at,
        updated_at: row.updated_at,
    })
    .collect();

    Ok(ListRolesResponse {
        roles,
        next_page_token: None,
    })
}

pub(crate) async fn delete_role<'c, 'e: 'c, E>(id: &str, connection: E) -> Result<()>
where
    E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let row = sqlx::query!(
        r#"
        DELETE FROM roles
        WHERE id = $1
        "#,
        id,
    )
    .execute(connection)
    .await
    .map_err(|e| e.into_error_model("Error deleting role".to_string()))?;

    if row.rows_affected() == 0 {
        return Err(ErrorModel::not_found(
            format!("Role not found: '{id}'",),
            "RoleNotFound",
            None,
        )
        .into());
    }

    Ok(())
}
