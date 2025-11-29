package croner

import (
	"log"
	"os"
)

type CronLogger interface {
	Infof(format string, args ...any)
	Errorf(format string, args ...any)
}

func newDefaultLogger() CronLogger {
	logger := log.New(os.Stdout, "[croner] ", log.LstdFlags|log.Lshortfile|log.Lmsgprefix)
	return &defaultLogger{
		logger: logger,
	}
}

type defaultLogger struct {
	logger *log.Logger
}

func (t *defaultLogger) Infof(format string, args ...any) {
	t.logger.Printf(format, args...)
}
func (t *defaultLogger) Errorf(format string, args ...any) {
	t.logger.Printf(format, args...)
}
