package gravita

import (
	"context"
)

// Entry represents a single LambdaUDFHandler matching rule in Mux
type Entry struct {
	handler  LambdaUDFHandler
	matchers []matcher
}

// Handler registers a LambdaUDFHandler with Entry
func (e *Entry) Handler(handler LambdaUDFHandler) *Entry {
	e.handler = handler
	return e
}

// HandlerFunc registers a function that is the entity of LambdaUDF in Entry
func (e *Entry) HandlerFunc(f func(context.Context, [][]interface{}) ([]interface{}, error)) *Entry {
	e.handler = LambdaUDFHandlerFunc(f)
	return e
}

// GetHandler returns the Handler registered in the Entry
func (e *Entry) GetHandler() LambdaUDFHandler {
	return e.handler
}

// Match determines if the given event matches this Entry
func (e *Entry) Match(event *LambdaUDFEvent) bool {
	for _, m := range e.matchers {
		if !m.Match(event) {
			return false
		}
	}
	return true
}
