package main

import (
	"github.com/BemiHQ/BemiDB/src/common"
	"github.com/BemiHQ/BemiDB/src/syncer-postgres/lib"
)

func init() {
	postgres.RegisterFlags()
}

func main() {
	config := postgres.LoadConfig()
	defer common.HandleUnexpectedPanic(config.CommonConfig)

	syncer := postgres.NewSyncer(config)
	syncer.Sync()
}
