package postgres

import (
	log "github.com/sirupsen/logrus"
)

var (
	Log_Level = log.InfoLevel
)

func SetLogLevel(level log.Level) {
	// log.PanicLevel = 0
	// log.FatalLevel = 1
	// log.ErrorLevel = 2
	// log.WarnLevel = 3
	// log.InfoLevel = 4
	// log.DebugLevel = 5

	if level == 5 {
		Log_Level = log.DebugLevel
	}
}

// Create Context
func CreateLogger(fid string) *log.Entry {
	// FID = Function ID
	log := log.WithFields(log.Fields{"fid": fid})
	log.Logger.SetLevel(Log_Level)

	return log
}
