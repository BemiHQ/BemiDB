package main

type IcebergWriter struct {
	config  *Config
	storage StorageInterface
}

func NewIcebergWriter(config *Config) *IcebergWriter {
	storage := NewStorage(config)
	return &IcebergWriter{config: config, storage: storage}
}

func (writer *IcebergWriter) DeleteSchema(icebergSchema string) (err error) {
	return writer.storage.DeleteSchema(icebergSchema)
}

func (writer *IcebergWriter) DeleteSchemaTable(icebergSchemaTable IcebergSchemaTable) (err error) {
	return writer.storage.DeleteSchemaTable(icebergSchemaTable)
}

func (writer *IcebergWriter) WriteInternalStartSqlFile(queries []string) (err error) {
	return writer.storage.WriteInternalStartSqlFile(queries)
}
