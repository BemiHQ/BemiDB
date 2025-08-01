package common

import (
	goDuckdb "github.com/marcboeker/go-duckdb/v2"
)

type IcebergWriter struct {
	Config               *BaseConfig
	IcebergSchemaColumns []*IcebergSchemaColumn
	StorageS3            *StorageS3
}

func NewIcebergWriter(config *BaseConfig, storageS3 *StorageS3, icebergSchemaColumns []*IcebergSchemaColumn) *IcebergWriter {
	return &IcebergWriter{
		Config:               config,
		IcebergSchemaColumns: icebergSchemaColumns,
		StorageS3:            storageS3,
	}
}

func (writer *IcebergWriter) Write(s3TablePath string, loadRows func(appender *goDuckdb.Appender) (rowCount int, reachedEnd bool)) {
	s3DataPath := s3TablePath + "/data"
	s3MetadataPath := s3TablePath + "/metadata"

	var lastSequenceNumber int
	newManifestListItemsSortedDesc := []ManifestListItem{}
	finalManifestListFilesSortedAsc := []ManifestListFile{}

	var firstNewParquetFile ParquetFile
	var newParquetCount int

	for {
		newParquetFile, reachedEnd := writer.StorageS3.CreateParquet(s3DataPath, writer.IcebergSchemaColumns, loadRows)
		newParquetCount++

		if firstNewParquetFile.Key == "" {
			firstNewParquetFile = newParquetFile
		}

		newManifestFile, err := writer.StorageS3.CreateManifest(s3MetadataPath, newParquetFile)
		PanicIfError(writer.Config, err)

		lastSequenceNumber++
		newManifestListItem := ManifestListItem{SequenceNumber: lastSequenceNumber, ManifestFile: newManifestFile}
		newManifestListItemsSortedDesc = append([]ManifestListItem{newManifestListItem}, newManifestListItemsSortedDesc...)

		newManifestListFile, err := writer.StorageS3.CreateManifestList(s3MetadataPath, firstNewParquetFile.Uuid, newManifestListItemsSortedDesc)
		PanicIfError(writer.Config, err)

		finalManifestListFilesSortedAsc = append(finalManifestListFilesSortedAsc, newManifestListFile)
		_, err = writer.StorageS3.CreateMetadata(s3MetadataPath, writer.IcebergSchemaColumns, finalManifestListFilesSortedAsc)
		PanicIfError(writer.Config, err)

		LogInfo(writer.Config, "Written", newParquetFile.RecordCount, "records in Parquet file #"+IntToString(newParquetCount))

		if reachedEnd {
			return
		}
	}
}
