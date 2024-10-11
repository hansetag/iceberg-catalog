use super::dbutils::DBErrorHandler;
use crate::api::iceberg::v1::PaginationQuery;
use crate::api::management::v1::user::{
    ListUsersResponse, SearchUser, SearchUserResponse, User, UserLastUpdatedWith,
};
use crate::service::{CreateOrUpdateUserResponse, Result, UserId};
use itertools::Itertools;

#[derive(sqlx::Type, Debug, Clone, Copy)]
#[sqlx(rename_all = "kebab-case", type_name = "user_last_updated_with")]
enum DbUserLastUpdatedWith {
    UpdateFromToken,
    ConfigCallCreation,
    UpdateEndpoint,
}

#[derive(sqlx::FromRow, Debug)]
struct UserRow {
    id: String,
    name: String,
    email: Option<String>,
    last_updated_with: DbUserLastUpdatedWith,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<UserRow> for User {
    fn from(
        UserRow {
            id,
            name,
            email,
            last_updated_with,
            created_at,
            updated_at,
        }: UserRow,
    ) -> Self {
        User {
            id,
            name,
            email,
            last_updated_with: match last_updated_with {
                DbUserLastUpdatedWith::UpdateFromToken => UserLastUpdatedWith::UpdateFromToken,
                DbUserLastUpdatedWith::ConfigCallCreation => {
                    UserLastUpdatedWith::ConfigCallCreation
                }
                DbUserLastUpdatedWith::UpdateEndpoint => UserLastUpdatedWith::UpdateEndpoint,
            },
            created_at,
            updated_at,
        }
    }
}

pub(crate) async fn list_users<'e, 'c: 'e, E: sqlx::Executor<'c, Database = sqlx::Postgres>>(
    filter_user_id: Option<Vec<UserId>>,
    filter_name: Option<String>,
    _pagination: PaginationQuery,
    connection: E,
) -> Result<ListUsersResponse> {
    // TODO: impl pagination
    let filter_name = filter_name.unwrap_or_default();

    let users = sqlx::query_as!(
        UserRow,
        r#"
        SELECT
            id,
            name,
            last_updated_with as "last_updated_with: DbUserLastUpdatedWith",
            email,
            created_at,
            updated_at
        FROM users
        where (deleted_at is null)
            AND ($1 OR name ILIKE ('%' || $2 || '%'))
            AND ($3 OR id = any($4))
        "#,
        filter_name.is_empty(),
        filter_name.to_string(),
        filter_user_id.is_none(),
        filter_user_id
            .unwrap_or_default()
            .into_iter()
            .map_into()
            .collect::<Vec<String>>() as Vec<String>,
    )
    .fetch_all(connection)
    .await
    .map_err(|e| e.into_error_model("Error fetching users".to_string()))?
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
) -> Result<Option<()>> {
    let row = sqlx::query!(
        r#"
        UPDATE users
        SET deleted_at = now(),
            name = 'Deleted User',
            email = null
        WHERE id = $1
        "#,
        id.inner(),
    )
    .execute(connection)
    .await
    .map_err(|e| e.into_error_model("Error deleting user".to_string()))?;

    if row.rows_affected() == 0 {
        return Ok(None);
    }

    Ok(Some(()))
}

pub(crate) async fn create_or_update_user<
    'c,
    'e: 'c,
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
>(
    id: &UserId,
    name: &str,
    email: Option<&str>,
    last_updated_with: UserLastUpdatedWith,
    connection: E,
) -> Result<CreateOrUpdateUserResponse> {
    let db_last_updated_with = match last_updated_with {
        UserLastUpdatedWith::UpdateFromToken => DbUserLastUpdatedWith::UpdateFromToken,
        UserLastUpdatedWith::ConfigCallCreation => DbUserLastUpdatedWith::ConfigCallCreation,
        UserLastUpdatedWith::UpdateEndpoint => DbUserLastUpdatedWith::UpdateEndpoint,
    };

    // query_as doesn't respect FromRow: https://github.com/launchbadge/sqlx/issues/2584
    let user = sqlx::query!(
        r#"
        INSERT INTO users (id, name, email, last_updated_with)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (id)
        DO UPDATE SET name = $2, email = $3, last_updated_with = $4
        returning (xmax = 0) AS created, id, name, email, created_at, updated_at, last_updated_with as "last_updated_with: DbUserLastUpdatedWith"
        "#,
        id.inner(),
        name,
        email,
        db_last_updated_with as _,
    )
    .fetch_one(connection)
    .await
    .map_err(|e| e.into_error_model("Error creating or updating user".to_string()))?;
    let created = user.created.unwrap_or_default();
    let user = UserRow {
        id: user.id,
        name: user.name,
        email: user.email,
        last_updated_with: user.last_updated_with,
        created_at: user.created_at,
        updated_at: user.updated_at,
    };

    Ok(CreateOrUpdateUserResponse {
        created,
        user: User::from(user),
    })
}

pub(crate) async fn search_user<'e, 'c: 'e, E: sqlx::Executor<'c, Database = sqlx::Postgres>>(
    search_term: &str,
    connection: E,
) -> Result<SearchUserResponse> {
    let users = sqlx::query!(
        r#"
        SELECT id, name, name <-> $1 AS dist
        FROM users
        ORDER BY dist ASC
        LIMIT 10
        "#,
        search_term,
    )
    .fetch_all(connection)
    .await
    .map_err(|e| e.into_error_model("Error searching user".to_string()))?
    .into_iter()
    .map(|row| SearchUser {
        id: row.id,
        name: row.name,
    })
    .collect();

    Ok(SearchUserResponse { users })
}

#[cfg(test)]
mod test {
    use crate::api::iceberg::types::PageToken;
    use crate::implementations::postgres::CatalogState;

    use super::*;

    #[sqlx::test]
    async fn test_create_or_update_user(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let user_id = UserId::new("test_user_1").unwrap();
        let user_name = "Test User 1";

        create_or_update_user(
            &user_id,
            user_name,
            None,
            UserLastUpdatedWith::UpdateFromToken,
            &state.read_write.write_pool,
        )
        .await
        .unwrap();

        let users = list_users(
            None,
            None,
            PaginationQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(10),
            },
            &state.read_write.read_pool,
        )
        .await
        .unwrap();

        assert_eq!(users.users.len(), 1);
        assert_eq!(users.users[0].id, user_id.inner());
        assert_eq!(users.users[0].name, user_name);
        assert_eq!(users.users[0].email, None);

        // Update
        let user_name = "Test User 1 Updated";
        create_or_update_user(
            &user_id,
            user_name,
            None,
            UserLastUpdatedWith::UpdateFromToken,
            &state.read_write.write_pool,
        )
        .await
        .unwrap();

        let users = list_users(
            None,
            None,
            PaginationQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(10),
            },
            &state.read_write.read_pool,
        )
        .await
        .unwrap();

        assert_eq!(users.users.len(), 1);
        assert_eq!(users.users[0].id, user_id.inner());
        assert_eq!(users.users[0].name, user_name);
        assert_eq!(users.users[0].email, None);
    }

    #[sqlx::test]
    async fn test_search_user(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let user_id = UserId::new("test_user_1").unwrap();
        let user_name = "Test User 1";

        create_or_update_user(
            &user_id,
            user_name,
            None,
            UserLastUpdatedWith::UpdateFromToken,
            &state.read_write.write_pool,
        )
        .await
        .unwrap();

        let search_result = search_user("Test", &state.read_write.read_pool)
            .await
            .unwrap();
        assert_eq!(search_result.users.len(), 1);
        assert_eq!(search_result.users[0].id, user_id.inner());
        assert_eq!(search_result.users[0].name, user_name);
    }

    #[sqlx::test]
    async fn test_delete_user(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let user_id = UserId::new("test_user_1").unwrap();
        let user_name = "Test User 1";

        create_or_update_user(
            &user_id,
            user_name,
            None,
            UserLastUpdatedWith::UpdateFromToken,
            &state.read_write.write_pool,
        )
        .await
        .unwrap();

        delete_user(user_id, &state.read_write.write_pool)
            .await
            .unwrap();

        let users = list_users(
            None,
            None,
            PaginationQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(10),
            },
            &state.read_write.read_pool,
        )
        .await
        .unwrap();

        assert_eq!(users.users.len(), 0);

        // Delete non-existent user
        let user_id = UserId::new("test_user_2").unwrap();
        let result = delete_user(user_id, &state.read_write.write_pool)
            .await
            .unwrap();
        assert_eq!(result, None);
    }
}
