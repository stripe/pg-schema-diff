package log

import (
	"fmt"
	"log"
)

type (
	Logger interface {
		Errorf(msg string, args ...any)
	}

	simpleLogger struct{}
)

// SimpleLogger is a bare-bones implementation of the logging interface, e.g., used for testing
func SimpleLogger() Logger {
	return &simpleLogger{}
}

func (*simpleLogger) Errorf(msg string, args ...any) {
	formattedMessage := fmt.Sprintf(msg, args...)
	log.Println(fmt.Sprintf("[ERROR] %s", formattedMessage))
}
