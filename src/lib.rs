mod schema_conversion;

use {
	arrow::record_batch::RecordBatch,
	std::{io::BufRead, sync::Arc},
};

pub fn read_object_container_file_to_arrow(file: impl BufRead) -> anyhow::Result<RecordBatch> {
	let mut avro_reader =
		serde_avro_fast::object_container_file_encoding::Reader::from_reader(file)?;
	let schema: serde_avro_fast::schema::SchemaMut = avro_reader
		.schema()
		.json()
		.parse()
		.expect("should not fail to re-parse schema");
	let fields = schema_conversion::schema_node_to_fields(
		&schema,
		serde_avro_fast::schema::SchemaKey::root(),
	);

	struct ToArrowDeserializeSeed {
		arrow_builder: serde_arrow::ArrowBuilder,
	}
	impl<'de> serde::de::DeserializeSeed<'de> for &mut ToArrowDeserializeSeed {
		type Value = ();
		fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
		where
			D: serde::Deserializer<'de>,
		{
			self.arrow_builder
				.push(&serde_transcode::Transcoder::new(deserializer))
				.map_err(|ser_error| {
					<D::Error as serde::de::Error>::custom(format_args!(
						"Avro to Arrow transcoding serialization error: {ser_error}"
					))
				})
		}
	}
	let mut arrow_builder_de_seed = ToArrowDeserializeSeed {
		arrow_builder: serde_arrow::ArrowBuilder::new(&fields)?,
	};

	// Consume the entire input
	while let Some(()) = avro_reader.deserialize_seed_next(&mut arrow_builder_de_seed)? {}

	// serde_arrow boilerplate (https://github.com/chmp/serde_arrow/issues/119)
	let arrays = arrow_builder_de_seed.arrow_builder.build_arrays()?;
	let schema = arrow::datatypes::Schema::new(fields);
	let record_batch = RecordBatch::try_new(Arc::new(schema), arrays)?;

	Ok(record_batch)
}
