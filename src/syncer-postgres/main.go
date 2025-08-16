package main

import (
	"github.com/BemiHQ/BemiDB/src/common"
)

func main() {
	config := LoadConfig()
	defer common.HandleUnexpectedPanic(config.CommonConfig)

	syncer := NewSyncer(config)
	syncer.Sync()
}
