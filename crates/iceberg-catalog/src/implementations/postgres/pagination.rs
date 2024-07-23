use chrono::Utc;
use iceberg_ext::catalog::rest::ErrorModel;
use std::fmt::Display;
use uuid::Uuid;

#[derive(Debug)]
pub(crate) enum PaginateToken {
    V1(V1PaginateToken),
}

#[derive(Debug)]
pub(crate) struct V1PaginateToken {
    pub(crate) created_at: chrono::DateTime<Utc>,
    pub(crate) id: Uuid,
}

impl Display for PaginateToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            PaginateToken::V1(V1PaginateToken { created_at, id }) => {
                format!("1&{}&{}", created_at.timestamp_micros(), id)
            }
        };
        write!(f, "{str}")
    }
}

impl TryFrom<&str> for PaginateToken {
    type Error = ErrorModel;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let parts = s.split('&').collect::<Vec<_>>();

        match *parts.first().ok_or(parse_error(None))? {
            "1" => match &parts[1..] {
                &[ts, id] => {
                    let created_at = chrono::DateTime::from_timestamp_micros(
                        ts.parse().map_err(|e| parse_error(Some(Box::new(e))))?,
                    )
                    .ok_or(parse_error(None))?;
                    let id = Uuid::parse_str(id).map_err(|e| parse_error(Some(Box::new(e))))?;
                    Ok(PaginateToken::V1(V1PaginateToken { created_at, id }))
                }
                _ => Err(parse_error(None)),
            },
            _ => Err(parse_error(None)),
        }
    }
}

fn parse_error(e: Option<Box<dyn std::error::Error + Send + Sync + 'static>>) -> ErrorModel {
    ErrorModel::bad_request(
        "Invalid paginate token".to_string(),
        "PaginateTokenParseError".to_string(),
        e,
    )
}
