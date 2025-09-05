package main

import (
	"github.com/BemiHQ/BemiDB/src/common"
)

const (
	COMPRESSION_FACTOR = 2 // 1 GB uncompressed data x 2 = ~90MB compressed data
)

type Syncer struct {
	Config       *Config
	Attio        *Attio
	StorageS3    *common.StorageS3
	DuckdbClient *common.DuckdbClient
}

func NewSyncer(config *Config) *Syncer {
	storageS3 := common.NewStorageS3(config.CommonConfig)
	duckdbClient := common.NewDuckdbClient(config.CommonConfig, common.SYNCER_DUCKDB_BOOT_QUERIES)

	return &Syncer{
		Config:       config,
		Attio:        NewAttio(config),
		StorageS3:    storageS3,
		DuckdbClient: duckdbClient,
	}
}

func (syncer *Syncer) Sync() {
	common.SendAnonymousAnalytics(syncer.Config.CommonConfig, "syncer-attio-start", syncer.name())

	for _, object := range []string{ATTIO_OBJECT_COMPANIES, ATTIO_OBJECT_DEALS, ATTIO_OBJECT_PEOPLE} {
		cappedBuffer := common.NewCappedBuffer(syncer.Config.CommonConfig, common.DEFAULT_CAPPED_BUFFER_SIZE)
		jsonQueueWriter := common.NewJsonQueueWriter(cappedBuffer)

		// Copy from Attio to cappedBuffer in a separate goroutine in parallel
		go func() {
			err := syncer.Attio.Load(object, jsonQueueWriter)
			common.PanicIfError(syncer.Config.CommonConfig, err)

			common.LogInfo(syncer.Config.CommonConfig, "Finished exporting data from Attio.")
			jsonQueueWriter.Close()
		}()

		icebergSchemaTable := common.IcebergSchemaTable{Schema: syncer.Config.DestinationSchemaName, Table: object}
		icebergTable := common.NewIcebergTable(syncer.Config.CommonConfig, syncer.StorageS3, syncer.DuckdbClient, icebergSchemaTable)
		icebergTable.ReplaceWith(func(syncingIcebergTable *common.IcebergTable) {
			var icebergSchemaColumns []*common.IcebergSchemaColumn

			switch object {
			case ATTIO_OBJECT_COMPANIES:
				icebergSchemaColumns = CompaniesIcebergSchemaColumns(syncer.Config.CommonConfig)
			case ATTIO_OBJECT_DEALS:
				icebergSchemaColumns = DealsIcebergSchemaColumns(syncer.Config.CommonConfig)
			case ATTIO_OBJECT_PEOPLE:
				icebergSchemaColumns = PeopleIcebergSchemaColumns(syncer.Config.CommonConfig)
			default:
				common.Panic(syncer.Config.CommonConfig, "Unknown object: "+object)
			}

			// Read from cappedBuffer and write to Iceberg
			icebergTableWriter := common.NewIcebergTableWriter(syncer.Config.CommonConfig, syncer.StorageS3, syncer.DuckdbClient, syncingIcebergTable, icebergSchemaColumns, 1)
			icebergTableWriter.InsertFromJsonCappedBuffer(cappedBuffer)
		})

		common.SendAnonymousAnalytics(syncer.Config.CommonConfig, "syncer-attio-finish", syncer.name())
	}
}

func (syncer *Syncer) name() string {
	return syncer.Config.ApiAccessToken[:5] + "..."
}
