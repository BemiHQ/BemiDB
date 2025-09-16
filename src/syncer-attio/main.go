package main

import (
	"github.com/BemiHQ/BemiDB/src/common"
	"github.com/BemiHQ/BemiDB/src/syncer-attio/lib"
)

func init() {
	attio.RegisterFlags()
}

func main() {
	config := attio.LoadConfig()
	defer common.HandleUnexpectedPanic(config.CommonConfig)

	storageS3 := common.NewStorageS3(config.CommonConfig)
	duckdbClient := common.NewDuckdbClient(config.CommonConfig, common.SYNCER_DUCKDB_BOOT_QUERIES)
	syncer := attio.NewSyncer(config, storageS3, duckdbClient)
	syncer.Sync()
}
