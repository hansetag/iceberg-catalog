use validator::Validate;

#[derive(Clone, Hash, Eq, Debug, PartialEq, Serialize, Deserialize)]
pub struct Namespace(Vec<String>);

impl Validate for Namespace {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        Ok(())
    }
}
