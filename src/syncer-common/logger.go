package common

import (
	"log"
)

type LogLevel string

const (
	LOG_LEVEL_TRACE = "TRACE"
	LOG_LEVEL_DEBUG = "DEBUG"
	LOG_LEVEL_WARN  = "WARN"
	LOG_LEVEL_INFO  = "INFO"
	LOG_LEVEL_ERROR = "ERROR"
)

var LOG_LEVELS = []string{
	LOG_LEVEL_TRACE,
	LOG_LEVEL_DEBUG,
	LOG_LEVEL_WARN,
	LOG_LEVEL_INFO,
	LOG_LEVEL_ERROR,
}

func LogError(config *BaseConfig, message ...interface{}) {
	log.Println(append([]interface{}{"[ERROR]"}, message...)...)
}

func LogWarn(config *BaseConfig, message ...interface{}) {
	if config.LogLevel != LOG_LEVEL_ERROR {
		log.Println(append([]interface{}{"[WARN]"}, message...)...)
	}
}

func LogInfo(config *BaseConfig, message ...interface{}) {
	if config.LogLevel != LOG_LEVEL_ERROR && config.LogLevel != LOG_LEVEL_WARN {
		log.Println(append([]interface{}{"[INFO]"}, message...)...)
	}
}

func LogDebug(config *BaseConfig, message ...interface{}) {
	if config.LogLevel == LOG_LEVEL_DEBUG || config.LogLevel == LOG_LEVEL_TRACE {
		log.Println(append([]interface{}{"[DEBUG]"}, message...)...)
	}
}

func LogTrace(config *BaseConfig, message ...interface{}) {
	if config.LogLevel == LOG_LEVEL_TRACE {
		log.Println(append([]interface{}{"[TRACE]"}, message...)...)
	}
}
