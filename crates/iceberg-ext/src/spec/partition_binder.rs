use crate::catalog::rest::ErrorModel;
use http::StatusCode;
use iceberg::spec::{
    NestedFieldRef, PartitionField, PartitionSpec, SchemaRef, Transform, Type,
    UnboundPartitionField, UnboundPartitionSpec,
};
use std::cmp::max;
use std::collections::HashSet;

type Result<T> = std::result::Result<T, ErrorModel>;

#[derive(Debug)]

pub(crate) struct PartitionSpecBinder {
    spec_id: i32,
    schema: SchemaRef,
    partition_names: HashSet<String>,
    last_assigned_field_id: i32,
    dedup_fields: HashSet<(i32, String)>,
}

impl PartitionSpecBinder {
    pub(crate) const PARTITION_DATA_ID_START: i32 = 1000;
    pub(crate) const UNPARTITIONED_LAST_ASSIGNED_ID: i32 = 999;

    pub(crate) fn new(
        schema: SchemaRef,
        spec_id: i32,
        last_assigned_field_id: Option<i32>,
    ) -> Self {
        Self {
            spec_id,
            schema,
            last_assigned_field_id: last_assigned_field_id
                .unwrap_or(Self::PARTITION_DATA_ID_START - 1),
            partition_names: HashSet::default(),
            dedup_fields: HashSet::default(),
        }
    }

    pub(crate) fn bind_spec(mut self, spec: UnboundPartitionSpec) -> Result<PartitionSpec> {
        Ok(PartitionSpec {
            spec_id: self.spec_id,
            fields: spec
                .fields
                .into_iter()
                .map(|bindable_field| self.bind_field(&bindable_field))
                .collect::<Result<Vec<PartitionField>>>()?,
        })
    }

    pub(crate) fn update_spec_schema(mut self, other: PartitionSpec) -> Result<PartitionSpec> {
        // build without validation because the schema may have changed in a way that makes this spec
        // invalid. the spec should still be preserved so that older metadata can be interpreted.

        // We perform some basic checks here similar to java implementation.
        // They should never fail if the spec was valid before the schema update.
        other
            .fields
            .iter()
            .map(|field| self.add_bound_field(field))
            .collect::<Result<Vec<_>>>()?;

        Ok(PartitionSpec {
            spec_id: self.spec_id,
            fields: other.fields,
        })
    }

    fn add_bound_field(&mut self, field: &PartitionField) -> Result<()> {
        self.get_schema_field_by_id(field.source_id)
            // Schema field is almost always going to be NULL, as the field is typically bound to a different schema
            .ok()
            .map(|schema_field| {
                self.check_name_does_not_collide_with_schema(
                    schema_field,
                    &field.name,
                    field.transform,
                )
            })
            .transpose()?;

        self.check_partition_name_set_and_unique(&field.name)?;

        self.last_assigned_field_id = max(self.last_assigned_field_id, field.field_id);
        self.update_names(&field.name);

        Ok(())
    }

    fn bind_field(&mut self, spec_field: &UnboundPartitionField) -> Result<PartitionField> {
        let spec_field_name = spec_field.name.as_str();
        let spec_field_id = spec_field.source_id;
        let transform = spec_field.transform;
        self.check_partition_name_set_and_unique(spec_field_name)?;

        let schema_field = self.get_schema_field_by_id(spec_field_id)?;
        Self::check_transform_compatibility(transform, &schema_field.field_type)?;

        self.check_name_does_not_collide_with_schema(schema_field, spec_field_name, transform)?;

        // Self::check_schema_field_eq_source_id(schema_field, spec_field.get_source_id())?;
        self.check_for_redundant_partitions(spec_field.source_id, spec_field.transform)?;

        self.update_names(spec_field_name);
        self.dedup_fields
            .insert((spec_field.source_id, transform.dedup_name()));

        let field_id = spec_field
            .partition_id
            .unwrap_or_else(|| self.increment_and_get_next_field_id());

        let res = PartitionField::builder()
            .source_id(spec_field.source_id)
            .field_id(field_id)
            .name(spec_field_name.to_owned())
            .transform(spec_field.transform)
            .build();

        self.last_assigned_field_id = max(self.last_assigned_field_id, field_id);

        Ok(res)
    }

    fn err<T: Into<String>>(message: T) -> ErrorModel {
        ErrorModel {
            message: message.into(),
            r#type: "FailedToBuildPartitionSpec".to_owned(),
            code: StatusCode::CONFLICT.into(),
            stack: vec![],
            source: None,
        }
    }

    fn increment_and_get_next_field_id(&mut self) -> i32 {
        self.last_assigned_field_id += 1;
        self.last_assigned_field_id
    }

    fn check_partition_name_set_and_unique(&self, new_name: &str) -> Result<()> {
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

    fn get_schema_field_by_id(&self, field_id: i32) -> Result<&NestedFieldRef> {
        self.schema
            .field_by_id(field_id)
            .ok_or_else(|| Self::err(format!("Field '{field_id}' not found in schema.")))
    }

    fn check_name_does_not_collide_with_schema(
        &self,
        schema_field: &NestedFieldRef,
        partition_name: &str,
        transform: Transform,
    ) -> Result<()> {
        match self.schema.field_by_name(partition_name) {
            Some(schema_collision) => {
                if transform == Transform::Identity {
                    if schema_collision.id == schema_field.id {
                        Ok(())
                    } else {
                        let err = Self::err(format!(
                            "Cannot create identity partition sourced from different field in schema: Schema Name '{}' != Partition Name '{partition_name}'.",
                            schema_field.name
                        ));

                        Err(err)
                    }
                } else {
                    let err = Self::err(format!(
                        "Cannot create partition with name: '{partition_name}' that conflicts with schema field and is not an identity transform.",
                    ));

                    Err(err)
                }
            }
            None => Ok(()),
        }
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
            "Cannot add redundant partition for: '{source_id}' with transform: '{transform}'",
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

    fn is_all_elements_unique<T>(elements: T) -> bool
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
                .add_partition_fields(spec_fields)
                .expect("Cannot create `unbounded_spec` mock.")
                .build(),
        )
    }

    fn mock_compatible_schema_and_spec() -> (SchemaRef, UnboundPartitionSpec) {
        let schema_fields = vec![
            NestedField::required(1, "id", Primitive(PrimitiveType::Uuid)).into(),
            NestedField::required(2, "data", Primitive(PrimitiveType::Date)).into(),
            NestedField::required(3, "category", Primitive(PrimitiveType::Int)).into(),
        ];

        let spec_fields = vec![
            UnboundPartitionField::builder()
                .name("id".to_owned())
                .transform(Transform::Identity)
                .partition_id(1)
                .source_id(1)
                .build(),
            UnboundPartitionField::builder()
                .name("data_day".to_owned())
                .transform(Transform::Day)
                .partition_id(2)
                .source_id(2)
                .build(),
            UnboundPartitionField::builder()
                .name("category_bucket[2]".to_owned())
                .transform(Transform::Bucket(2))
                .partition_id(3)
                .source_id(3)
                .build(),
        ];

        create_schema_and_spec(schema_fields, spec_fields)
    }

    /// Ensure each field referenced in the `UnboundPartitionSpec` exists in the schema.
    #[test]
    fn names_exist() {
        let (schema, spec) = mock_compatible_schema_and_spec();
        let binder = PartitionSpecBinder::new(schema, MOCK_SPEC_ID, None);

        assert!(spec
            .fields
            .iter()
            .map(|unbound| binder.get_schema_field_by_id(unbound.source_id))
            .all(|schema_field| schema_field.is_ok()));
    }

    /// Ensure that if a field referenced in the `UnboundPartitionSpec` not exists an error is returned.
    #[test]
    fn names_not_exist() {
        let schema_fields =
            vec![NestedField::required(1, "id", Primitive(PrimitiveType::Uuid)).into()];

        let spec_fields = vec![UnboundPartitionField::builder()
            .name("id".to_owned())
            .transform(Transform::Identity)
            .partition_id(2)
            .source_id(2)
            .build()];

        let (schema, spec) = create_schema_and_spec(schema_fields, spec_fields);
        let binder = PartitionSpecBinder::new(schema, MOCK_SPEC_ID, None);

        binder.bind_spec(spec).expect_err("Should fail binding!");
    }

    /// Ensure there are no duplicate fields in the `PartitionSpec`.
    #[test]
    fn no_duplicates() {
        let (schema, spec) = mock_compatible_schema_and_spec();
        let binder = PartitionSpecBinder::new(schema, MOCK_SPEC_ID, None);

        assert!(spec
            .fields
            .iter()
            .map(|unbound| binder.check_partition_name_set_and_unique(&unbound.name))
            .all(|partition_name| partition_name.is_ok()));

        assert!(is_all_elements_unique(
            binder
                .bind_spec(spec)
                .expect("Cannot bind spec!")
                .fields
                .into_iter()
                .map(|field| field.name)
                .collect::<Vec<String>>()
        ));
    }

    /// Verify we can add a duplicate partition for identity transform
    #[test]
    fn duplicate_identity_partition() {
        let (schema, spec) = {
            let schema_fields =
                vec![NestedField::required(1, "id", Primitive(PrimitiveType::Int)).into()];

            let spec_fields = vec![UnboundPartitionField::builder()
                .name("id".to_owned())
                .transform(Transform::Identity)
                .partition_id(1)
                .source_id(1)
                .build()];

            create_schema_and_spec(schema_fields, spec_fields)
        };

        let binder = PartitionSpecBinder::new(schema, MOCK_SPEC_ID, None);
        binder
            .bind_spec(spec)
            .expect("Can bind duplicate field name for identity transform.");
    }

    /// Validate that we cannot add a duplicate partition for non-identity transform
    #[test]
    fn duplicate_partition() {
        let (schema, spec) = {
            let schema_fields =
                vec![NestedField::required(1, "id", Primitive(PrimitiveType::Int)).into()];

            let spec_fields = vec![UnboundPartitionField::builder()
                .name("id".to_owned())
                .transform(Transform::Unknown)
                .partition_id(1)
                .source_id(1)
                .build()];

            create_schema_and_spec(schema_fields, spec_fields)
        };

        let binder = PartitionSpecBinder::new(schema, MOCK_SPEC_ID, None);
        binder
            .bind_spec(spec)
            .expect_err("Cannot bind duplicate field name for non-identity transform.");
    }

    /// Validate that for identity transforms, the partition name can also be different from the schema field name
    #[test]
    fn identity_partition_with_different_name() {
        let (schema, spec) = {
            let schema_fields =
                vec![NestedField::required(1, "id", Primitive(PrimitiveType::Int)).into()];

            let spec_fields = vec![UnboundPartitionField::builder()
                .name("id_partition".to_owned())
                .transform(Transform::Identity)
                .partition_id(2)
                .source_id(1)
                .build()];

            create_schema_and_spec(schema_fields, spec_fields)
        };

        let binder = PartitionSpecBinder::new(schema, MOCK_SPEC_ID, None);
        binder
            .bind_spec(spec)
            .expect("Can bind identity partition with different name.");
    }

    /// Validate that for identity transforms, the field id must match if the name is identical
    #[test]
    fn identity_partition_with_different_field_id() {
        let (schema, spec) = {
            let schema_fields =
                vec![NestedField::required(1, "id", Primitive(PrimitiveType::Int)).into()];

            let spec_fields = vec![UnboundPartitionField::builder()
                .name("id".to_owned())
                .transform(Transform::Identity)
                .partition_id(3)
                .source_id(2)
                .build()];

            create_schema_and_spec(schema_fields, spec_fields)
        };

        let binder = PartitionSpecBinder::new(schema, MOCK_SPEC_ID, None);
        binder
            .bind_spec(spec)
            .expect_err("Cannot bind identity partition with different field id.");
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
                    .get_schema_field_by_id(spec.fields[index].partition_id.unwrap())
                    .expect("Cannot get name!")
                    .field_type,
            )
        }

        let (schema, spec) = {
            let schema_fields = vec![
                NestedField::required(1, "id", Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "data", Primitive(PrimitiveType::String)).into(),
                NestedField::required(3, "category", Struct(StructType::default())).into(),
            ];

            let spec_fields = vec![
                UnboundPartitionField::builder()
                    .name("id_void".to_owned())
                    .transform(Transform::Void)
                    .partition_id(1)
                    .source_id(1)
                    .build(),
                UnboundPartitionField::builder()
                    .name("data_unknown".to_owned())
                    .transform(Transform::Unknown)
                    .partition_id(2)
                    .source_id(2)
                    .build(),
                UnboundPartitionField::builder()
                    .name("category_bucket_1".to_owned())
                    .transform(Transform::Bucket(1))
                    .partition_id(3)
                    .source_id(3)
                    .build(),
            ];

            create_schema_and_spec(schema_fields, spec_fields)
        };

        let binder = PartitionSpecBinder::new(schema, MOCK_SPEC_ID, None);

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

        let mut binder = PartitionSpecBinder::new(schema, MOCK_SPEC_ID, None);
        binder
            .bind_field(&spec.fields[0].clone())
            .expect("Cannot bind field!");
        assert!(binder.bind_field(&spec.fields[1].clone()).is_ok());
    }
}
