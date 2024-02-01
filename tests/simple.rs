use serde_derive::Serialize;

const AVRO_SCHEMA: &str = r#"
		{
			"type": "record",
			"name": "test",
			"fields": [
				{
					"name": "a",
					"type": "long",
					"default": 42
				},
				{
					"name": "b",
					"type": "string"
				}
			]
		}
		"#;

/// Used to generate the records
#[derive(Serialize, Debug, PartialEq, Eq)]
struct SchemaRecord<'a> {
	a: i64,
	#[serde(borrow)]
	b: &'a str,
}

#[test]
fn simple() {
	let input = &[
		SchemaRecord {
			a: 27,
			b: "foo".into(),
		},
		SchemaRecord {
			a: 42,
			b: "bar".into(),
		},
	];

	let schema: serde_avro_fast::Schema = AVRO_SCHEMA.parse().unwrap();

	// Serialize object container file encoding for next tests
	let mut serializer_config = serde_avro_fast::ser::SerializerConfig::new(&schema);
	let mut writer =
		serde_avro_fast::object_container_file_encoding::WriterBuilder::new(&mut serializer_config)
			.compression(
				serde_avro_fast::object_container_file_encoding::Compression::Deflate {
					level:
						serde_avro_fast::object_container_file_encoding::CompressionLevel::default(),
				},
			)
			.build(Vec::new())
			.unwrap();
	writer.serialize_all(input.iter()).unwrap();
	let serialized = writer.into_inner().unwrap();

	// Deserialize to arrow record batch
	let record_batch =
		arrow_serde_avro::read_object_container_file_to_arrow(serialized.as_slice()).unwrap();
	dbg!(&record_batch);
	assert_eq!(record_batch.num_rows(), 2);
}
