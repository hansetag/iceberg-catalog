use crate::catalog::rest::ErrorModel;
use http::StatusCode;
use iceberg::spec::{
    NestedFieldRef, PartitionField, PartitionSpec, SchemaRef, UnboundPartitionField,
    UnboundPartitionSpec,
};
use std::cmp::max;
use std::collections::HashSet;

type Result<T> = std::result::Result<T, ErrorModel>;

#[derive(Debug)]
pub struct PartitionSpecBinder {
    spec_id: i32,
    schema: SchemaRef,
    partition_names: HashSet<String>,
    last_assigned_field_id: i32,
}

impl PartitionSpecBinder {
    const PARTITION_DATA_ID_START: i32 = 1000;

    pub fn new(schema: SchemaRef, spec_id: i32) -> Self {
        Self {
            spec_id,
            schema,
            last_assigned_field_id: Self::PARTITION_DATA_ID_START - 1,
            partition_names: HashSet::new(),
        }
    }

    pub fn bind(mut self, spec: &UnboundPartitionSpec) -> Result<PartitionSpec> {
        Ok(PartitionSpec {
            spec_id: self.spec_id,
            fields: spec
                .fields
                .iter()
                .map(|unbounded_field| self.bound_field(unbounded_field))
                .collect::<Result<Vec<PartitionField>>>()?,
        })
    }

    fn bound_field(&mut self, spec_field: &UnboundPartitionField) -> Result<PartitionField> {
        self.check_partition_names(&spec_field.name)?;
        self.check_schema_compatibility(&spec_field.name, spec_field.source_id)?;
        self.update_names(&spec_field.name);

        let partition_id = spec_field
            .partition_id
            .unwrap_or_else(|| self.increment_and_get_next_partition_id());

        let res = PartitionField::builder()
            .source_id(spec_field.source_id)
            .field_id(partition_id)
            .name(spec_field.name.clone())
            .transform(spec_field.transform)
            .build();

        self.last_assigned_field_id = max(self.last_assigned_field_id, partition_id);

        Ok(res)
    }

    fn err<T: Into<String>>(message: T) -> ErrorModel {
        ErrorModel {
            message: message.into(),
            r#type: "FailedToBuildPartitionSpec".to_owned(),
            code: StatusCode::CONFLICT.into(),
            stack: None,
        }
    }

    fn increment_and_get_next_partition_id(&mut self) -> i32 {
        self.last_assigned_field_id += 1;
        self.last_assigned_field_id
    }

    fn check_partition_names(&self, new_name: &str) -> Result<()> {
        if new_name.is_empty() {
            return Err(Self::err(format!(
                "Cannot use empty or null partition name: '{new_name}'."
            )));
        }

        if self.partition_names.contains(new_name) {
            return Err(Self::err(format!(
                "Cannot use partition name: '{new_name}' more than once."
            )));
        }

        Ok(())
    }

    fn check_schema_compatibility(&self, name: &str, source_id: i32) -> Result<()> {
        let schema_field = self
            .get_schema_field_by_name(name)
            .ok_or(Self::err("Field not found in schema."))?;

        if !self.is_schema_field_eq_source_id(schema_field, source_id) {
            return Err(Self::err(
                "Cannot create identity partition sourced from different field in schema.",
            ));
        }

        Ok(())

        // match self.schema.as_struct().field_by_name(name) {
        //     Some(schema_field) if schema_field.id != source_id => Err(Self::err(
        //         "Cannot create identity partition sourced from different field in schema.",
        //     )),
        //     Some(_) => Ok(()),
        //     None => Err(Self::err("Field not found in schema.")),
        // }
    }

    fn get_schema_field_by_name(&self, field_name: &str) -> Option<&NestedFieldRef> {
        self.schema.as_struct().field_by_name(field_name)
    }

    fn is_schema_field_eq_source_id(&self, schema_field: &NestedFieldRef, source_id: i32) -> bool {
        schema_field.id.eq(&source_id)
    }

    fn update_names(&mut self, new_name: &str) {
        self.partition_names.insert(new_name.to_owned());
    }
}

#[cfg(test)]
mod test {
    use crate::spec::spec_binder::PartitionSpecBinder;
    use iceberg::spec::Type::Primitive;
    use iceberg::spec::{
        NestedField, PrimitiveType, Schema, SchemaRef, Transform, UnboundPartitionField,
        UnboundPartitionSpec,
    };

    const MOCK_SPEC_ID: i32 = 0;

    fn mock_compatible_schema_and_spec() -> (SchemaRef, UnboundPartitionSpec) {
        let schema_fields = vec![
            NestedField::required(1, "id", Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "data", Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "category", Primitive(PrimitiveType::String)).into(),
        ];

        let spec_fields = vec![
            UnboundPartitionField::builder()
                .name("id".to_owned())
                .transform(Transform::Void)
                .partition_id(1)
                .source_id(1)
                .build(),
            UnboundPartitionField::builder()
                .name("data".to_owned())
                .transform(Transform::Unknown)
                .partition_id(2)
                .source_id(2)
                .build(),
            UnboundPartitionField::builder()
                .name("category".to_owned())
                .transform(Transform::Unknown)
                .partition_id(3)
                .source_id(3)
                .build(),
        ];

        (
            Schema::builder()
                .with_schema_id(1)
                .with_fields(schema_fields)
                .build()
                .expect("Cannot create schema mock!")
                .into(),
            UnboundPartitionSpec::builder()
                .with_spec_id(0)
                .with_fields(spec_fields)
                .build()
                .expect("Cannot create `unbounded_spec` mock."),
        )
    }

    fn mock_incompatible_schema_and_spec() -> (SchemaRef, UnboundPartitionSpec) {
        let schema_fields = vec![
            NestedField::required(1, "id", Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "data", Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "category", Primitive(PrimitiveType::String)).into(),
        ];

        let spec_fields = vec![
            UnboundPartitionField::builder()
                .name("id".to_owned())
                .transform(Transform::Void)
                .partition_id(1)
                .source_id(1)
                .build(),
            UnboundPartitionField::builder()
                .name("full_name".to_owned())
                .transform(Transform::Unknown)
                .partition_id(2)
                .source_id(2)
                .build(),
            UnboundPartitionField::builder()
                .name("email".to_owned())
                .transform(Transform::Unknown)
                .partition_id(4)
                .source_id(4)
                .build(),
        ];

        (
            Schema::builder()
                .with_schema_id(1)
                .with_fields(schema_fields)
                .build()
                .expect("Cannot create schema mock!")
                .into(),
            UnboundPartitionSpec::builder()
                .with_spec_id(0)
                .with_fields(spec_fields)
                .build()
                .expect("Cannot create `unbounded_spec` mock."),
        )
    }

    /// Ensure that the field names in the UnboundPartitionSpec are consistent with the names in the schema.
    #[test]
    fn names_consistent() {
        let (schema, spec) = mock_compatible_schema_and_spec();
        let binder = PartitionSpecBinder::new(schema, MOCK_SPEC_ID);

        assert!(spec
            .fields
            .iter()
            .map(|unbound| binder.check_schema_compatibility(&unbound.name, unbound.source_id))
            .collect::<Result<Vec<()>, _>>()
            .is_ok());
    }

    /// Ensure that if field names in the UnboundPartitionSpec is inconsistent return an error.
    #[test]
    fn names_inconsistent() {
        let (schema, spec) = mock_incompatible_schema_and_spec();
        let binder = PartitionSpecBinder::new(schema, MOCK_SPEC_ID);

        assert!(spec
            .fields
            .iter()
            .map(|unbound| binder.check_schema_compatibility(&unbound.name, unbound.source_id))
            .collect::<Result<Vec<()>, _>>()
            .is_err());
    }

    /// Ensure each field referenced in the UnboundPartitionSpec exists in the schema.
    #[test]
    fn names_exist() {
        let (schema, spec) = mock_compatible_schema_and_spec();
        let binder = PartitionSpecBinder::new(schema, MOCK_SPEC_ID);

        assert!(spec
            .fields
            .iter()
            .map(|unbound| binder.get_schema_field_by_name(&unbound.name))
            .all(|schema_field| schema_field.is_some()));
    }

    /// Ensure that if a field referenced in the UnboundPartitionSpec not exists an error is returned.
    #[test]
    fn names_not_exist() {
        let (schema, spec) = mock_incompatible_schema_and_spec();
        let binder = PartitionSpecBinder::new(schema, MOCK_SPEC_ID);

        assert!(spec
            .fields
            .iter()
            .map(|unbound| binder.get_schema_field_by_name(&unbound.name))
            .any(|schema_field| schema_field.is_none()));
    }

    // Ensure there are no duplicate fields in the PartitionSpec.
    #[test]
    fn no_duplicate() {
        let (schema, spec) = mock_compatible_schema_and_spec();
        let binder = PartitionSpecBinder::new(schema, MOCK_SPEC_ID);

        assert!(spec
            .fields
            .iter()
            .map(|unbound| binder.check_partition_names(&unbound.name))
            .all(|partition_name| partition_name.is_ok()));
    }

    // TODO:
    // Verify that the field IDs in the UnboundPartitionSpec match the field IDs in the schema.
    // Ensure the transforms applied to the fields in the UnboundPartitionSpec are valid and supported.
    // Ensure that no fields in the UnboundPartitionSpec are null.
    // Ensure the type of each field in the schema is compatible with the transform specified in the UnboundPartitionSpec.
    // Assign a unique spec ID to the new PartitionSpec.
    // Ensure that the field IDs in the PartitionSpec are sequential if required by the system version.
    // Log appropriate messages and handle errors gracefully if any validation fails.
}
