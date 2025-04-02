package main

type IcebergWriterTable struct {
	config                     *Config
	schemaTable                IcebergSchemaTable
	storage                    StorageInterface
	pgSchemaColumns            []PgSchemaColumn
	dynamicRowCountPerBatch    int
	maxParquetPayloadThreshold int
	continuedRefresh           bool
}

func NewIcebergWriterTable(
	config *Config,
	schemaTable IcebergSchemaTable,
	pgSchemaColumns []PgSchemaColumn,
	dynamicRowCountPerBatch int,
	maxParquetPayloadThreshold int,
	continuedRefresh bool,
) *IcebergWriterTable {
	return &IcebergWriterTable{
		config:                     config,
		schemaTable:                schemaTable,
		pgSchemaColumns:            pgSchemaColumns,
		dynamicRowCountPerBatch:    dynamicRowCountPerBatch,
		maxParquetPayloadThreshold: maxParquetPayloadThreshold,
		continuedRefresh:           continuedRefresh,
		storage:                    NewStorage(config),
	}
}

func (writer *IcebergWriterTable) Write(loadRows func() ([][]string, InternalTableMetadata)) {
	dataDirPath := writer.storage.CreateDataDir(writer.schemaTable)
	metadataDirPath := writer.storage.CreateMetadataDir(writer.schemaTable)

	var lastSequenceNumber int
	var firstNewParquetFile ParquetFile
	var newParquetCount int
	existingManifestListItemsSortedDesc := []ManifestListItem{}
	loadMoreRows := true

	finalManifestListItemsSortedAsc := []ManifestListItem{}
	finalManifestListFilesSortedAsc := []ManifestListFile{}

	if writer.continuedRefresh {
		existingManifestListFilesSortedAsc, err := writer.storage.ExistingManifestListFiles(metadataDirPath)
		PanicIfError(writer.config, err)

		existingManifestListItemsSortedDesc, err = writer.storage.ExistingManifestListItems(existingManifestListFilesSortedAsc[len(existingManifestListFilesSortedAsc)-1])
		PanicIfError(writer.config, err)

		finalManifestListFilesSortedAsc = existingManifestListFilesSortedAsc
		lastSequenceNumber = existingManifestListItemsSortedDesc[0].SequenceNumber
	}

	for loadMoreRows {
		newParquetFile, newInternalTableMetadata, loadedAllRows, err := writer.storage.CreateParquet(
			dataDirPath,
			writer.pgSchemaColumns,
			writer.maxParquetPayloadThreshold,
			loadRows,
		)
		PanicIfError(writer.config, err)

		// If Parquet is empty and we are continuing to refresh / process subsequent chunks, delete it and exit (no trailing Parquet files)
		if newParquetFile.RecordCount == 0 && (writer.continuedRefresh || newParquetCount > 0) {
			err = writer.storage.DeleteParquet(newParquetFile)
			PanicIfError(writer.config, err)
			return
		}

		newParquetCount++
		if firstNewParquetFile.Path == "" {
			firstNewParquetFile = newParquetFile
		}

		if writer.continuedRefresh {
			var overwrittenManifestListItemsSortedAsc []ManifestListItem
			var overwrittenManifestListFilesSortedAsc []ManifestListFile

			overwrittenManifestListItemsSortedAsc, overwrittenManifestListFilesSortedAsc, lastSequenceNumber = writer.overwriteExistingFiles(
				dataDirPath,
				metadataDirPath,
				Reverse(existingManifestListItemsSortedDesc),
				newParquetFile,
				firstNewParquetFile,
				lastSequenceNumber,
			)

			finalManifestListItemsSortedAsc = append(finalManifestListItemsSortedAsc, overwrittenManifestListItemsSortedAsc...)
			finalManifestListFilesSortedAsc = append(finalManifestListFilesSortedAsc, overwrittenManifestListFilesSortedAsc...)
		}

		newManifestFile, err := writer.storage.CreateManifest(metadataDirPath, newParquetFile)
		PanicIfError(writer.config, err)

		lastSequenceNumber++
		newManifestListItem := ManifestListItem{SequenceNumber: lastSequenceNumber, ManifestFile: newManifestFile}
		finalManifestListItemsSortedAsc = append(finalManifestListItemsSortedAsc, newManifestListItem)

		newManifestListFile, err := writer.storage.CreateManifestList(metadataDirPath, firstNewParquetFile.Uuid, Reverse(finalManifestListItemsSortedAsc))
		PanicIfError(writer.config, err)

		finalManifestListFilesSortedAsc = append(finalManifestListFilesSortedAsc, newManifestListFile)
		_, err = writer.storage.CreateMetadata(metadataDirPath, writer.pgSchemaColumns, finalManifestListFilesSortedAsc)
		PanicIfError(writer.config, err)

		loadMoreRows = !loadedAllRows

		if !loadMoreRows {
			newInternalTableMetadata.LastRefreshMode = REFRESH_MODE_FULL
		}
		err = writer.storage.WriteInternalTableMetadata(metadataDirPath, newInternalTableMetadata)
		PanicIfError(writer.config, err)

		LogDebug(writer.config, "Written", newParquetCount, "Parquet file(s). Load more rows:", loadMoreRows)
	}
}

func (writer *IcebergWriterTable) overwriteExistingFiles(
	dataDirPath string,
	metadataDirPath string,
	existingManifestListItemsSortedAsc []ManifestListItem,
	newParquetFile ParquetFile,
	firstNewParquetFile ParquetFile,
	lastSequenceNumber int,
) ([]ManifestListItem, []ManifestListFile, int) {
	overwrittenManifestListItemsSortedAsc := []ManifestListItem{}
	overwrittenManifestListFilesSortedAsc := []ManifestListFile{}

	for i, existingManifestListItem := range existingManifestListItemsSortedAsc {
		existingManifestFile := existingManifestListItem.ManifestFile
		existingParquetFilePath, err := writer.storage.ExistingParquetFilePath(existingManifestFile)
		PanicIfError(writer.config, err)

		overwrittenParquetFile, err := writer.storage.CreateOverwrittenParquet(dataDirPath, existingParquetFilePath, newParquetFile.Path, writer.pgSchemaColumns, writer.dynamicRowCountPerBatch)
		PanicIfError(writer.config, err)

		// Keep as is if no overlapping records found
		if overwrittenParquetFile.Path == "" {
			LogDebug(writer.config, "No overlapping records found")
			overwrittenManifestListItemsSortedAsc = append(overwrittenManifestListItemsSortedAsc, existingManifestListItem)
			continue
		}

		if overwrittenParquetFile.RecordCount == 0 {
			// DELETE
			LogDebug(writer.config, "Deleting", existingManifestFile.RecordCount, "record(s)...")

			deletedRecsManifestFile, err := writer.storage.CreateDeletedRecordsManifest(metadataDirPath, overwrittenParquetFile.Uuid, existingManifestFile)
			PanicIfError(writer.config, err)

			// Constructing a new manifest list without the previous manifest file and with the new "deleted" manifest file
			overwrittenManifestListItemsSortedAsc := []ManifestListItem{}
			for j, existingItem := range existingManifestListItemsSortedAsc {
				if i != j {
					overwrittenManifestListItemsSortedAsc = append(overwrittenManifestListItemsSortedAsc, existingItem)
				}
			}
			lastSequenceNumber++
			overwrittenManifestListItem := ManifestListItem{SequenceNumber: lastSequenceNumber, ManifestFile: deletedRecsManifestFile}
			overwrittenManifestListItemsSortedAsc = append(overwrittenManifestListItemsSortedAsc, overwrittenManifestListItem)

			overwrittenManifestList, err := writer.storage.CreateManifestList(metadataDirPath, firstNewParquetFile.Uuid, Reverse(overwrittenManifestListItemsSortedAsc))
			PanicIfError(writer.config, err)
			overwrittenManifestListFilesSortedAsc = append(overwrittenManifestListFilesSortedAsc, overwrittenManifestList)
			continue
		} else {
			// UPDATE (overwrite)
			LogDebug(writer.config, "Overwritting", existingManifestFile.RecordCount, "record(s) with", overwrittenParquetFile.RecordCount, "record(s)...")

			deletedRecsManifestFile, err := writer.storage.CreateDeletedRecordsManifest(metadataDirPath, overwrittenParquetFile.Uuid, existingManifestFile)
			PanicIfError(writer.config, err)

			overwrittenManifestFile, err := writer.storage.CreateManifest(metadataDirPath, overwrittenParquetFile)
			PanicIfError(writer.config, err)

			lastSequenceNumber++
			overwrittenManifestListItem := ManifestListItem{SequenceNumber: lastSequenceNumber, ManifestFile: overwrittenManifestFile}
			deletedRecsManifestListItem := ManifestListItem{SequenceNumber: lastSequenceNumber, ManifestFile: deletedRecsManifestFile}
			overwrittenManifestList, err := writer.storage.CreateManifestList(metadataDirPath, firstNewParquetFile.Uuid, []ManifestListItem{overwrittenManifestListItem, deletedRecsManifestListItem})
			PanicIfError(writer.config, err)

			overwrittenManifestListItemsSortedAsc = append(overwrittenManifestListItemsSortedAsc, overwrittenManifestListItem)
			overwrittenManifestListFilesSortedAsc = append(overwrittenManifestListFilesSortedAsc, overwrittenManifestList)
		}
	}

	return overwrittenManifestListItemsSortedAsc, overwrittenManifestListFilesSortedAsc, lastSequenceNumber
}
