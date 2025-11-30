package helper

import (
	"fmt"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
)

var Log = logrus.New()

type StyleFormatter struct{}

func (f *StyleFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	timestamp := entry.Time.Format("2006-01-02 15:04:05")
	level := strings.ToUpper(entry.Level.String())
	function := "unknown"
	if entry.Caller != nil {
		function = entry.Caller.Function
	}
	msg := entry.Message
	return []byte(fmt.Sprintf("%s %-5s %s - %s\n", timestamp, level, function, msg)), nil
}

func init() {
	Log.SetFormatter(&StyleFormatter{})
	Log.SetOutput(os.Stdout)
	Log.SetReportCaller(true)
	Log.SetLevel(logrus.TraceLevel)
}
