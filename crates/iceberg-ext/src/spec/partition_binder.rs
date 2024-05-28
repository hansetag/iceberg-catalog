use crate::catalog::rest::ErrorModel;
use crate::spec::partition_binder::bindable::Bindable;
use http::StatusCode;
use iceberg::spec::{
    NestedFieldRef, PartitionField, PartitionSpec, SchemaRef, Transform, Type, UnboundPartitionSpec,
};
use std::cmp::max;
use std::collections::HashSet;

mod bindable {
    use iceberg::spec::{PartitionField, Transform, UnboundPartitionField};

    pub(super) trait Bindable {
        fn get_spec_id(&self) -> Option<i32>;
        fn get_source_id(&self) -> i32;
        fn get_name(&self) -> String;
        fn get_transform(&self) -> Transform;
    }

    impl Bindable for PartitionField {
        fn get_spec_id(&self) -> Option<i32> {
            Some(self.field_id)
        }

        fn get_source_id(&self) -> i32 {
            self.source_id
        }

        fn get_name(&self) -> String {
            self.name.clone()
        }

        fn get_transform(&self) -> Transform {
            self.transform
        }
    }

    impl Bindable for UnboundPartitionField {
        fn get_spec_id(&self) -> Option<i32> {
            self.partition_id
        }

        fn get_source_id(&self) -> i32 {
            self.source_id
        }

        fn get_name(&self) -> String {
            self.name.clone()
        }

        fn get_transform(&self) -> Transform {
            self.transform
        }
    }
}

type Result<T> = std::result::Result<T, ErrorModel>;

#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub(crate) struct PartitionSpecBinder {
    spec_id: i32,
    schema: SchemaRef,
    partition_names: HashSet<String>,
    last_assigned_field_id: i32,
    dedup_fields: HashSet<(i32, String)>,
}

impl PartitionSpecBinder {
    const PARTITION_DATA_ID_START: i32 = 1000;

    pub(crate) fn new(schema: SchemaRef, spec_id: i32) -> Self {
        Self {
            spec_id,
            schema,
            last_assigned_field_id: Self::PARTITION_DATA_ID_START - 1,
            partition_names: HashSet::default(),
            dedup_fields: HashSet::default(),
        }
    }

    pub(crate) fn bind_spec_schema(mut self, spec: UnboundPartitionSpec) -> Result<PartitionSpec> {
        self.bind(self.spec_id, spec.fields)
    }

    pub(crate) fn update_spec_schema(mut self, other: &PartitionSpec) -> Result<PartitionSpec> {
        self.bind(self.spec_id, other.fields.clone())
    }

    fn bind<T: Bindable>(
        &mut self,
        spec_id: i32,
        fields: impl IntoIterator<Item = T>,
    ) -> Result<PartitionSpec> {
        Ok(PartitionSpec {
            spec_id,
            fields: fields
                .into_iter()
                .map(|bindable_field| self.bind_field(bindable_field))
                .collect::<Result<Vec<PartitionField>>>()?,
        })
    }

    fn bind_field<T: Bindable>(&mut self, spec_field: T) -> Result<PartitionField> {
        let spec_field_name = spec_field.get_name();
        self.check_partition_names(&spec_field_name)?;

        let schema_field = self.get_schema_field_by_name(&spec_field_name)?;

        Self::check_transform_compatibility(spec_field.get_transform(), &schema_field.field_type)?;
        Self::check_schema_field_eq_source_id(schema_field, spec_field.get_source_id())?;
        self.check_for_redundant_partitions(
            spec_field.get_source_id(),
            spec_field.get_transform(),
        )?;

        self.update_names(&spec_field_name);

        let field_id = spec_field
            .get_spec_id()
            .unwrap_or_else(|| self.increment_and_get_next_field_id());

        let res = PartitionField::builder()
            .source_id(spec_field.get_source_id())
            .field_id(field_id)
            .name(spec_field_name)
            .transform(spec_field.get_transform())
            .build();

        self.last_assigned_field_id = max(self.last_assigned_field_id, field_id);

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

    fn increment_and_get_next_field_id(&mut self) -> i32 {
        self.last_assigned_field_id += 1;
        self.last_assigned_field_id
    }

    fn check_partition_names(&self, new_name: &str) -> Result<()> {
        if new_name.is_empty() {
            return Err(Self::err("Cannot use empty partition name."));
        }

        if self.partition_names.contains(new_name) {
            return Err(Self::err(format!(
                "Cannot use partition name: '{new_name}' more than once."
            )));
        }

        Ok(())
    }

    fn get_schema_field_by_name(&self, field_name: &str) -> Result<&NestedFieldRef> {
        self.schema
            .as_struct()
            .field_by_name(field_name)
            .ok_or_else(|| Self::err(format!("Field '{field_name}' not found in schema.")))
    }

    fn check_schema_field_eq_source_id(
        schema_field: &NestedFieldRef,
        source_id: i32,
    ) -> Result<()> {
        if schema_field.id == source_id {
            return Ok(());
        }

        Err(Self::err(format!(
            "Cannot create identity partition sourced from different field in schema: '{}'.",
            schema_field.name
        )))
    }

    fn update_names(&mut self, new_name: &str) {
        self.partition_names.insert(new_name.to_owned());
    }

    fn check_transform_compatibility(transform: Transform, source_type: &Type) -> Result<()> {
        if transform != Transform::Void {
            if !source_type.is_primitive() {
                return Err(Self::err(format!(
                    "Cannot partition by non-primitive source field: '{source_type}'.",
                )));
            }

            if transform.result_type(source_type).is_err() {
                return Err(Self::err(format!(
                    "Invalid source type: '{source_type}' for transform: '{transform}'."
                )));
            }
        }

        Ok(())
    }

    fn check_for_redundant_partitions(
        &mut self,
        source_id: i32,
        transform: Transform,
    ) -> Result<()> {
        if !self
            .dedup_fields
            .contains(&(source_id, transform.dedup_name()))
        {
            return Ok(());
        }

        Err(Self::err(format!(
            "Cannot add redundant partition for: '{}' with transform: '{}' !",
            source_id, transform,
        )))
    }
}

/// Not covered and should be tested as part of `MetaDataBuilder`:
/// Assign a unique spec ID to the new `PartitionSpec`.
/// Ensure that the field IDs in the `PartitionSpec` are sequential if required by the system version.
#[cfg(test)]
mod test {
    use crate::spec::partition_binder::PartitionSpecBinder;
    use iceberg::spec::Type::{Primitive, Struct};
    use iceberg::spec::{
        NestedField, NestedFieldRef, PrimitiveType, Schema, SchemaRef, StructType, Transform, Type,
        UnboundPartitionField, UnboundPartitionSpec,
    };
    use std::collections::HashSet;
    use std::hash::Hash;

    const MOCK_SPEC_ID: i32 = 0;

    fn is_all_elements_uniq<T>(elements: T) -> bool
    where
        T: IntoIterator,
        T::Item: Eq + Hash,
    {
        let mut uniq = HashSet::new();
        elements.into_iter().all(move |x| uniq.insert(x))
    }

    fn create_schema_and_spec(
        schema_fields: Vec<NestedFieldRef>,
        spec_fields: Vec<UnboundPartitionField>,
    ) -> (SchemaRef, UnboundPartitionSpec) {
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

    fn mock_compatible_schema_and_spec() -> (SchemaRef, UnboundPartitionSpec) {
        let schema_fields = vec![
            NestedField::required(1, "id", Primitive(PrimitiveType::Uuid)).into(),
            NestedField::required(2, "data", Primitive(PrimitiveType::Date)).into(),
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
                .transform(Transform::Day)
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

        create_schema_and_spec(schema_fields, spec_fields)
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
                .transform(Transform::Identity)
                .partition_id(1)
                .source_id(1)
                .build(),
            UnboundPartitionField::builder()
                .name("full_name".to_owned())
                .transform(Transform::Identity)
                .partition_id(2)
                .source_id(2)
                .build(),
            UnboundPartitionField::builder()
                .name("email".to_owned())
                .transform(Transform::Identity)
                .partition_id(4)
                .source_id(4)
                .build(),
        ];

        create_schema_and_spec(schema_fields, spec_fields)
    }

    /// Ensure that the field names in the `UnboundPartitionSpec` are consistent with the names in the schema.
    #[test]
    fn names_consistent() {
        let (schema, spec) = mock_compatible_schema_and_spec();
        let binder = PartitionSpecBinder::new(schema, MOCK_SPEC_ID);

        assert!(spec
            .fields
            .iter()
            .map(|unbound| binder.get_schema_field_by_name(&unbound.name))
            .all(|field| field.is_ok()));
    }

    /// Ensure that if field names in the `UnboundPartitionSpec` is inconsistent return an error.
    #[test]
    fn names_inconsistent() {
        let (schema, spec) = mock_incompatible_schema_and_spec();
        let binder = PartitionSpecBinder::new(schema, MOCK_SPEC_ID);

        assert!(spec
            .fields
            .iter()
            .map(|unbound| binder.get_schema_field_by_name(&unbound.name))
            .any(|field| field.is_err()));
    }

    /// Ensure each field referenced in the `UnboundPartitionSpec` exists in the schema.
    #[test]
    fn names_exist() {
        let (schema, spec) = mock_compatible_schema_and_spec();
        let binder = PartitionSpecBinder::new(schema, MOCK_SPEC_ID);

        assert!(spec
            .fields
            .iter()
            .map(|unbound| binder.get_schema_field_by_name(&unbound.name))
            .all(|schema_field| schema_field.is_ok()));
    }

    /// Ensure that if a field referenced in the `UnboundPartitionSpec` not exists an error is returned.
    #[test]
    fn names_not_exist() {
        let (schema, spec) = mock_incompatible_schema_and_spec();
        let binder = PartitionSpecBinder::new(schema, MOCK_SPEC_ID);

        assert!(spec
            .fields
            .iter()
            .map(|unbound| binder.get_schema_field_by_name(&unbound.name))
            .any(|schema_field| schema_field.is_err()));
    }

    /// Ensure there are no duplicate fields in the `PartitionSpec`.
    #[test]
    fn no_duplicates() {
        let (schema, spec) = mock_compatible_schema_and_spec();
        let binder = PartitionSpecBinder::new(schema, MOCK_SPEC_ID);

        assert!(spec
            .fields
            .iter()
            .map(|unbound| binder.check_partition_names(&unbound.name))
            .all(|partition_name| partition_name.is_ok()));

        assert!(is_all_elements_uniq(
            binder
                .bind_spec_schema(spec)
                .expect("Cannot bind spec!")
                .fields
                .into_iter()
                .map(|field| field.name)
                .collect::<Vec<String>>()
        ));
    }

    /// Verify that the field IDs in the `UnboundPartitionSpec` match the field IDs in the schema.
    #[test]
    fn field_ids_match() {
        let (schema, spec) = mock_compatible_schema_and_spec();
        let binder = PartitionSpecBinder::new(schema, MOCK_SPEC_ID);

        assert!(spec
            .fields
            .iter()
            .map(
                |field| PartitionSpecBinder::check_schema_field_eq_source_id(
                    binder
                        .get_schema_field_by_name(&field.name)
                        .expect("Cannot get field."),
                    field.source_id
                )
            )
            .all(|res| res.is_ok()));
    }

    /// Verify that if the field IDs in the `UnboundPartitionSpec` doesn't match the field IDs in the schema an error returns.
    #[test]
    fn field_ids_not_match() {
        let (schema, spec) = {
            let schema_fields = vec![
                NestedField::required(1, "id", Primitive(PrimitiveType::Int)).into(),
                NestedField::required(3, "data", Primitive(PrimitiveType::String)).into(),
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
                    .partition_id(3)
                    .source_id(3)
                    .build(),
                UnboundPartitionField::builder()
                    .name("category".to_owned())
                    .transform(Transform::Unknown)
                    .partition_id(4)
                    .source_id(4)
                    .build(),
            ];

            create_schema_and_spec(schema_fields, spec_fields)
        };

        let binder = PartitionSpecBinder::new(schema, MOCK_SPEC_ID);

        assert!(spec
            .fields
            .iter()
            .map(
                |field| PartitionSpecBinder::check_schema_field_eq_source_id(
                    binder
                        .get_schema_field_by_name(&field.name)
                        .expect("Cannot get field."),
                    field.source_id
                )
            )
            .any(|res| res.is_err()));
    }

    /// Ensure the transforms applied to the fields in the `UnboundPartitionSpec` are valid and supported.
    /// Ensure the type of each field in the schema is compatible with the transform specified in the `UnboundPartitionSpec`.
    #[test]
    fn ensure_transform_compatibility() {
        fn get_transform_and_source<'a>(
            index: usize,
            spec: &UnboundPartitionSpec,
            binder: &'a PartitionSpecBinder,
        ) -> (Transform, &'a Type) {
            (
                spec.fields[index].transform,
                &*binder
                    .get_schema_field_by_name(&spec.fields[index].name)
                    .expect("Cannot get name!")
                    .field_type,
            )
        }

        let (schema, spec) = {
            let schema_fields = vec![
                NestedField::required(1, "id", Primitive(PrimitiveType::Int)).into(),
                NestedField::required(3, "data", Primitive(PrimitiveType::String)).into(),
                NestedField::required(3, "category", Struct(StructType::default())).into(),
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
                    .partition_id(3)
                    .source_id(3)
                    .build(),
                UnboundPartitionField::builder()
                    .name("category".to_owned())
                    .transform(Transform::Bucket(1))
                    .partition_id(4)
                    .source_id(4)
                    .build(),
            ];

            create_schema_and_spec(schema_fields, spec_fields)
        };

        let binder = PartitionSpecBinder::new(schema, MOCK_SPEC_ID);

        let (transform, source_type) = get_transform_and_source(0, &spec, &binder);
        assert!(PartitionSpecBinder::check_transform_compatibility(transform, source_type).is_ok());

        let (transform, source_type) = get_transform_and_source(1, &spec, &binder);
        assert!(PartitionSpecBinder::check_transform_compatibility(transform, source_type).is_ok());

        let (transform, source_type) = get_transform_and_source(2, &spec, &binder);
        assert!(
            PartitionSpecBinder::check_transform_compatibility(transform, source_type).is_err()
        );
    }

    /// Verify that each `PartitionField` only occurs once in a `PartitionSpec`.
    #[test]
    fn partition_field_occurs_once() {
        let (schema, spec) = {
            let schema_fields = vec![
                NestedField::required(1, "id", Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "data", Primitive(PrimitiveType::String)).into(),
            ];

            let spec_fields = vec![
                UnboundPartitionField::builder()
                    .name("id".to_owned())
                    .transform(Transform::Identity)
                    .partition_id(1)
                    .source_id(1)
                    .build(),
                UnboundPartitionField::builder()
                    .name("data".to_owned())
                    .transform(Transform::Identity)
                    .partition_id(2)
                    .source_id(2)
                    .build(),
            ];

            create_schema_and_spec(schema_fields, spec_fields)
        };

        let mut binder = PartitionSpecBinder::new(schema, MOCK_SPEC_ID);
        binder
            .bind_field(spec.fields[0].clone())
            .expect("Cannot bind field!");
        assert!(binder.bind_field(spec.fields[1].clone()).is_ok());
    }
}
