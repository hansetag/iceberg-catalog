pub(crate) mod warehouse {
    use iceberg_ext::catalog::rest::ErrorModel;
    #[allow(clippy::enum_variant_names)]
    #[derive(thiserror::Error, Debug, strum::IntoStaticStr)]
    pub enum Error {
        #[error("Error fetching warehouse")]
        WarehouseFetchError,
        #[error("Provided warehouse id is not a valid UUID")]
        WarehouseIDIsNotUUID,
        #[error("Warehouse with this name ('{0}') already exists in the project.")]
        WarehouseNameAlreadyExists(String),
        #[error("Warehouse is not active")]
        WarehouseNotActive,
        #[error("Warehouse is not empty")]
        WarehouseNotEmpty,
        #[error("Warehouse not found")]
        WarehouseNotFound,
        #[error("Error creating Warehouse.")]
        WarehouseNotReturnedAfterCreation,
    }

    pub(crate) struct WarehouseError {
        table_location: String,
        table_id: String,
    }

    impl From<Error> for ErrorModel {
        fn from(value: Error) -> Self {
            let typ: &'static str = (&value).into();
            ErrorModel::internal(value.to_string(), typ, Some(Box::new(value)))
        }
    }
}
