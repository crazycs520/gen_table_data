package randsql

import (
	"bytes"
	"fmt"
	log "github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

const defaultLogTimeFormat = "2006/01/02 15:04:05.000"

type textFormatter struct{}

// Format implements logrus.Formatter
func (f *textFormatter) Format(entry *log.Entry) ([]byte, error) {
	var b *bytes.Buffer
	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}

	fmt.Fprintf(b, "%s ", entry.Time.Format(defaultLogTimeFormat))
	if file, ok := entry.Data["file"]; ok {
		fmt.Fprintf(b, "%s:%v:", file, entry.Data["line"])
	}
	fmt.Fprintf(b, " [%s] %s", entry.Level.String(), entry.Message)

	for k, v := range entry.Data {
		if k != "file" && k != "line" {
			fmt.Fprintf(b, " %v=%v", k, v)
		}
	}

	b.WriteByte('\n')
	return b.Bytes(), nil
}

func InitLogger() error {
	log.SetLevel(log.InfoLevel)

	formatter := &textFormatter{}
	log.SetFormatter(formatter)

	if err := initFileLog(nil); err != nil {
		return err
	}
	return nil
}

// initFileLog initializes file based logging options.
func initFileLog(logger *log.Logger) error {
	// use lumberjack to logrotate
	output := &lumberjack.Logger{
		Filename:  "gen_data.log",
		MaxSize:   1 * 1024 * 1024 * 1024,
		LocalTime: true,
	}

	if logger == nil {
		log.SetOutput(output)
	} else {
		logger.Out = output
	}
	return nil
}
