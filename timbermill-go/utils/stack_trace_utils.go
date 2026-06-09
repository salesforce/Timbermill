package utils

import (
	es "github.com/go-errors/errors"
	"runtime/debug"
)

func StackTraceFromError(err error, skip int) string {
	return es.Wrap(err, skip).ErrorStack()
}

func StackTrace() string {
	return string(debug.Stack())
}
