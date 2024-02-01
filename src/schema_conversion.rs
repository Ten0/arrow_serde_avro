use {
	arrow::datatypes::{DataType, Field},
	serde_avro_fast::schema::{LogicalType, RegularType, SchemaKey, SchemaMut, SchemaNode},
};

/// Convert from avro schema to arrow schema
pub(crate) fn schema_node_to_fields(schema: &SchemaMut, key: SchemaKey) -> Vec<Field> {
	match schema[key] {
		SchemaNode::RegularType(ref rt) => match rt {
			RegularType::Record(record) => record
				.fields
				.iter()
				.map(|record| {
					Field::new(
						record.name.as_str(),
						schema_node_to_data_type(schema, record.type_),
						false,
					)
				})
				.collect(),
			_ => todo!(),
		},
		SchemaNode::LogicalType { inner, .. } => schema_node_to_fields(schema, inner),
	}
}

fn schema_node_to_data_type(schema: &SchemaMut, key: SchemaKey) -> DataType {
	match schema[key] {
		SchemaNode::RegularType(ref rt) => match rt {
			serde_avro_fast::schema::RegularType::Null => DataType::Null,
			serde_avro_fast::schema::RegularType::Boolean => DataType::Boolean,
			serde_avro_fast::schema::RegularType::Int => DataType::Int32,
			serde_avro_fast::schema::RegularType::Long => DataType::Int64,
			serde_avro_fast::schema::RegularType::Float => DataType::Float32,
			serde_avro_fast::schema::RegularType::Double => DataType::Float64,
			serde_avro_fast::schema::RegularType::Bytes => DataType::Binary,
			serde_avro_fast::schema::RegularType::String => DataType::Utf8,
			serde_avro_fast::schema::RegularType::Array(_) => todo!(),
			serde_avro_fast::schema::RegularType::Map(_) => todo!(),
			serde_avro_fast::schema::RegularType::Union(_) => todo!(),
			serde_avro_fast::schema::RegularType::Record(_) => todo!(),
			serde_avro_fast::schema::RegularType::Enum(_) => todo!(),
			serde_avro_fast::schema::RegularType::Fixed(fixed) => DataType::FixedSizeBinary(
				fixed
					.size
					.try_into()
					.expect("fixed size too large for arrow FixedSizeBinary"),
			),
		},
		SchemaNode::LogicalType {
			inner,
			ref logical_type,
		} => {
			let inner_rt = match &schema[inner] {
				SchemaNode::RegularType(rt) => rt,
				SchemaNode::LogicalType { .. } => panic!("Nested logical types are not allowed"),
			};
			match (logical_type, inner_rt) {
				(LogicalType::TimestampMillis, RegularType::Long) => DataType::Timestamp(
					arrow::datatypes::TimeUnit::Millisecond,
					Some("+00:00".into()),
				),
				(LogicalType::TimestampMicros, RegularType::Long) => DataType::Timestamp(
					arrow::datatypes::TimeUnit::Microsecond,
					Some("+00:00".into()),
				),
				_ => schema_node_to_data_type(schema, inner),
			}
		}
	}
}
