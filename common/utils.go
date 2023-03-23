package common

import (
	"time"
)

func FormatTimestamp(timestamp int64) string {
	tm := time.Unix(timestamp, 0)
	return tm.Format("2006-01-02 15:04:05")
}
