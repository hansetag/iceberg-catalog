use crate::api::iceberg::v1::{PageToken, PaginationQuery};
use crate::api::management::v1::ApiServer;
use crate::api::ApiContext;
use crate::request_metadata::RequestMetadata;
use crate::service::authz::{Authorizer, ServerAction, UserAction};
use crate::service::{
    AuthDetails, Catalog, CreateOrUpdateUserResponse, Result, SecretStore, State, Transaction,
    UserId,
};
use axum::response::IntoResponse;
use axum::Json;
use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};

/// How the user was last updated
#[derive(Debug, Serialize, utoipa::ToSchema, Clone)]
#[serde(rename_all = "kebab-case")]
pub enum UserLastUpdatedWith {
    /// The user was updated or created by the `/management/v1/user/update-from-token` - typically via the UI
    UpdateFromToken,
    /// The user was created by the `/catalog/v1/config` endpoint
    ConfigCallCreation,
    /// The user was updated by one of the dedicated update endpoints
    UpdateEndpoint,
}

/// User of the catalog
#[derive(Debug, Serialize, utoipa::ToSchema, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct User {
    /// Name of the user
    pub name: String,
    /// Email of the user
    #[serde(default)]
    pub email: Option<String>,
    /// The user's ID
    pub id: String,
    /// The endpoint that last updated the user
    pub last_updated_with: UserLastUpdatedWith,
    /// Timestamp when the user was created
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Timestamp when the user was last updated
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Serialize, utoipa::ToSchema, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct SearchUser {
    /// Name of the user
    pub name: String,
    /// ID of the user
    pub id: String,
}

/// Search result for users
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct SearchUserResponse {
    /// List of users matching the search criteria
    pub users: Vec<SearchUser>,
}

#[derive(Debug, Deserialize, utoipa::ToSchema, utoipa::IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct ListUsersQuery {
    /// Search for a specific username
    #[serde(default)]
    pub name: Option<String>,
    // Can we find a way to use PaginationQuery diectly with #[serde(flatten)]
    // without breaking OpenAPI?
    /// Next page token
    #[serde(skip_serializing_if = "PageToken::skip_serialize")]
    #[into_params(value_type=String)]
    pub page_token: PageToken,
    /// Signals an upper bound of the number of results that a client will receive.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub page_size: Option<i32>,
}

impl ListUsersQuery {
    #[must_use]
    pub fn pagination_query(&self) -> PaginationQuery {
        PaginationQuery {
            page_token: self.page_token.clone(),
            page_size: self.page_size,
        }
    }
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ListUsersResponse {
    pub users: Vec<User>,
    pub next_page_token: Option<String>,
}

impl IntoResponse for ListUsersResponse {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::OK, Json(self)).into_response()
    }
}

impl IntoResponse for SearchUserResponse {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::OK, Json(self)).into_response()
    }
}

#[derive(Debug, utoipa::ToSchema)]
pub struct SearchUserRequest {
    /// Search string for fuzzy search.
    /// Length is truncated to 64 characters.
    pub search: String,
}

impl<'de> Deserialize<'de> for SearchUserRequest {
    fn deserialize<D>(deserializer: D) -> std::result::Result<SearchUserRequest, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let search = String::deserialize(deserializer)?;
        let search = search.chars().take(64).collect();
        Ok(SearchUserRequest { search })
    }
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct UpdateUserRequest {
    pub name: String,
    #[serde(default)]
    pub email: Option<String>,
}

impl<C: Catalog, A: Authorizer + Clone, S: SecretStore> Service<C, A, S> for ApiServer<C, A, S> {}

#[async_trait::async_trait]
pub(super) trait Service<C: Catalog, A: Authorizer, S: SecretStore> {
    async fn create_or_update_user_from_token(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<CreateOrUpdateUserResponse> {
        let principal = match request_metadata.auth_details {
            AuthDetails::Principal(principal) => principal,
            AuthDetails::Unauthenticated => {
                return Err(ErrorModel::bad_request(
                    "Cannot register user without authentication",
                    "UnauthenticatedUserRegistration",
                    None,
                )
                .into())
            }
        };

        let name = principal.get_name()?;

        // No authz required, as the user is only updating themselves
        let mut t = C::Transaction::begin_write(context.v1_state.catalog).await?;
        let user = C::create_or_update_user(
            principal.user_id(),
            name,
            principal.email(),
            UserLastUpdatedWith::UpdateFromToken,
            t.transaction(),
        )
        .await?;
        t.commit().await?;

        Ok(user)
    }

    async fn search_user(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        request: SearchUserRequest,
    ) -> Result<SearchUserResponse> {
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;
        authorizer.require_search_users(&request_metadata).await?;

        // ------------------- Business Logic -------------------
        let SearchUserRequest { search } = request;
        C::search_user(&search, context.v1_state.catalog).await
    }

    async fn get_user(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        user_id: UserId,
    ) -> Result<User> {
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;
        authorizer
            .require_user_action(&request_metadata, &user_id, &UserAction::CanRead)
            .await?;

        // ------------------- Business Logic -------------------
        let filter_user_id = Some(vec![user_id.clone()]);
        let filter_name = None;
        let users = C::list_user(
            filter_user_id,
            filter_name,
            PaginationQuery {
                page_size: Some(1),
                page_token: PageToken::NotSpecified,
            },
            context.v1_state.catalog,
        )
        .await?;

        let user = users.users.into_iter().next().ok_or(ErrorModel::not_found(
            format!("User with id {user_id} not found."),
            "UserNotFound",
            None,
        ))?;

        Ok(user)
    }

    async fn list_user(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        query: ListUsersQuery,
    ) -> Result<ListUsersResponse> {
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;
        authorizer
            .require_server_action(&request_metadata, &ServerAction::CanListUsers)
            .await?;

        // ------------------- Business Logic -------------------
        let filter_user_id = None;
        let pagination_query = query.pagination_query();
        let users = C::list_user(
            filter_user_id,
            query.name,
            pagination_query,
            context.v1_state.catalog,
        )
        .await?;

        Ok(users)
    }

    async fn update_user(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        user_id: UserId,
        request: UpdateUserRequest,
    ) -> Result<()> {
        if request.name.is_empty() {
            return Err(ErrorModel::bad_request("Name cannot be empty", "EmptyName", None).into());
        }
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;
        authorizer
            .require_user_action(&request_metadata, &user_id, &UserAction::CanUpdate)
            .await?;

        // ------------------- Business Logic -------------------
        let email = request.email.as_deref().filter(|e| !e.is_empty());
        let mut t = C::Transaction::begin_write(context.v1_state.catalog).await?;
        let user = C::create_or_update_user(
            &user_id,
            &request.name,
            email,
            UserLastUpdatedWith::UpdateEndpoint,
            t.transaction(),
        )
        .await?;
        if user.created {
            t.rollback().await?;
            Err(ErrorModel::not_found("User does not exist", "UserNotFound", None).into())
        } else {
            t.commit().await
        }
    }

    async fn delete_user(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        user_id: UserId,
    ) -> Result<()> {
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;
        authorizer
            .require_user_action(&request_metadata, &user_id, &UserAction::CanDelete)
            .await?;

        // ------------------- Business Logic -------------------
        let mut t = C::Transaction::begin_write(context.v1_state.catalog).await?;
        let deleted = C::delete_user(user_id.clone(), t.transaction()).await?;
        if deleted.is_none() {
            return Err(ErrorModel::not_found(
                format!("User with id {user_id} not found."),
                "UserNotFound",
                None,
            )
            .into());
        }
        t.commit().await
    }
}
