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
	newManifestListItemsSortedDesc := []ManifestListItem{}
	existingManifestListItemsSortedDesc := []ManifestListItem{}
	finalManifestListFilesSortedAsc := []ManifestListFile{}

	if writer.continuedRefresh {
		existingManifestListFilesSortedAsc, err := writer.storage.ExistingManifestListFiles(metadataDirPath)
		PanicIfError(writer.config, err)

		existingManifestListItemsSortedDesc, err = writer.storage.ExistingManifestListItems(existingManifestListFilesSortedAsc[len(existingManifestListFilesSortedAsc)-1])
		PanicIfError(writer.config, err)

		lastSequenceNumber = existingManifestListItemsSortedDesc[0].SequenceNumber
		finalManifestListFilesSortedAsc = existingManifestListFilesSortedAsc
	}

	var firstNewParquetFile ParquetFile
	var newParquetCount int
	loadMoreRows := true

	for loadMoreRows {
		newParquetFile, newInternalTableMetadata, err := writer.storage.CreateParquet(
			dataDirPath,
			writer.pgSchemaColumns,
			writer.maxParquetPayloadThreshold,
			loadRows,
		)
		PanicIfError(writer.config, err)

		// If Parquet is empty and we are continuing to refresh / process subsequent chunks, delete it, mark the sync as completed and exit (no trailing Parquet files)
		if newParquetFile.RecordCount == 0 && (writer.continuedRefresh || newParquetCount > 0) {
			err = writer.storage.DeleteParquet(newParquetFile)
			PanicIfError(writer.config, err)

			err = writer.storage.WriteInternalTableMetadata(metadataDirPath, newInternalTableMetadata)
			PanicIfError(writer.config, err)

			return
		}

		newParquetCount++
		if firstNewParquetFile.Path == "" {
			firstNewParquetFile = newParquetFile
		}

		if writer.continuedRefresh && (newInternalTableMetadata.LastRefreshMode == RefreshModeIncremental || newInternalTableMetadata.LastRefreshMode == RefreshModeIncrementalInProgress) {
			var overwrittenManifestListFilesSortedAsc []ManifestListFile

			existingManifestListItemsSortedDesc, overwrittenManifestListFilesSortedAsc, lastSequenceNumber = writer.overwriteExistingFiles(
				dataDirPath,
				metadataDirPath,
				existingManifestListItemsSortedDesc,
				newParquetFile,
				firstNewParquetFile,
				lastSequenceNumber,
			)

			finalManifestListFilesSortedAsc = append(finalManifestListFilesSortedAsc, overwrittenManifestListFilesSortedAsc...)
		}

		newManifestFile, err := writer.storage.CreateManifest(metadataDirPath, newParquetFile)
		PanicIfError(writer.config, err)

		lastSequenceNumber++
		newManifestListItem := ManifestListItem{SequenceNumber: lastSequenceNumber, ManifestFile: newManifestFile}
		newManifestListItemsSortedDesc = append([]ManifestListItem{newManifestListItem}, newManifestListItemsSortedDesc...)

		finalManifestListItemsSortedDesc := append(newManifestListItemsSortedDesc, existingManifestListItemsSortedDesc...)
		newManifestListFile, err := writer.storage.CreateManifestList(metadataDirPath, firstNewParquetFile.Uuid, finalManifestListItemsSortedDesc)
		PanicIfError(writer.config, err)

		finalManifestListFilesSortedAsc = append(finalManifestListFilesSortedAsc, newManifestListFile)
		_, err = writer.storage.CreateMetadata(metadataDirPath, writer.pgSchemaColumns, finalManifestListFilesSortedAsc)
		PanicIfError(writer.config, err)

		err = writer.storage.WriteInternalTableMetadata(metadataDirPath, newInternalTableMetadata)
		PanicIfError(writer.config, err)

		loadMoreRows = newInternalTableMetadata.InProgress()
		LogDebug(writer.config, "Written", newParquetCount, "Parquet file(s). Load more rows:", loadMoreRows)
	}
}

func (writer *IcebergWriterTable) overwriteExistingFiles(
	dataDirPath string,
	metadataDirPath string,
	originalExistingManifestListItemsSortedDesc []ManifestListItem,
	newParquetFile ParquetFile,
	firstNewParquetFile ParquetFile,
	originalLastSequenceNumber int,
) (existingManifestListItemsSortedDesc []ManifestListItem, overwrittenManifestListFilesSortedAsc []ManifestListFile, lastSequenceNumber int) {
	originalExistingManifestListItemsSortedAsc := Reverse(originalExistingManifestListItemsSortedDesc)
	lastSequenceNumber = originalLastSequenceNumber

	for i, existingManifestListItem := range originalExistingManifestListItemsSortedAsc {
		existingManifestFile := existingManifestListItem.ManifestFile
		existingParquetFilePath, err := writer.storage.ExistingParquetFilePath(existingManifestFile)
		PanicIfError(writer.config, err)

		overwrittenParquetFile, err := writer.storage.CreateOverwrittenParquet(dataDirPath, existingParquetFilePath, newParquetFile.Path, writer.pgSchemaColumns, writer.dynamicRowCountPerBatch)
		PanicIfError(writer.config, err)

		// Keep as is if no overlapping records found
		if overwrittenParquetFile.Path == "" {
			LogDebug(writer.config, "No overlapping records found")
			existingManifestListItemsSortedDesc = append([]ManifestListItem{existingManifestListItem}, existingManifestListItemsSortedDesc...)
			continue
		}

		if overwrittenParquetFile.RecordCount == 0 {
			// DELETE
			LogDebug(writer.config, "Deleting", existingManifestFile.RecordCount, "record(s)...")

			deletedRecsManifestFile, err := writer.storage.CreateDeletedRecordsManifest(metadataDirPath, overwrittenParquetFile.Uuid, existingManifestFile)
			PanicIfError(writer.config, err)

			// Constructing a new manifest list without the previous manifest file and with the new "deleted" manifest file
			finalManifestListItemsSortedAsc := []ManifestListItem{}
			for j, existingItem := range originalExistingManifestListItemsSortedAsc {
				if i != j {
					finalManifestListItemsSortedAsc = append(finalManifestListItemsSortedAsc, existingItem)
				}
			}
			lastSequenceNumber++
			overwrittenManifestListItem := ManifestListItem{SequenceNumber: lastSequenceNumber, ManifestFile: deletedRecsManifestFile}
			finalManifestListItemsSortedAsc = append(finalManifestListItemsSortedAsc, overwrittenManifestListItem)

			overwrittenManifestList, err := writer.storage.CreateManifestList(metadataDirPath, firstNewParquetFile.Uuid, Reverse(finalManifestListItemsSortedAsc))
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

			existingManifestListItemsSortedDesc = append([]ManifestListItem{overwrittenManifestListItem}, existingManifestListItemsSortedDesc...)
			overwrittenManifestListFilesSortedAsc = append(overwrittenManifestListFilesSortedAsc, overwrittenManifestList)
		}
	}

	return existingManifestListItemsSortedDesc, overwrittenManifestListFilesSortedAsc, lastSequenceNumber
}
