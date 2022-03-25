package gravita

import (
	"context"
	"encoding/json"
	"fmt"
)

type Mux struct {
	NotMatchHandler LambdaUDFHandler
	entries         []*Entry
}

func NewMux() *Mux {
	return &Mux{}
}

type lambdaUDFOutputData struct {
	Success    bool          `json:"success"`
	ErrorMsg   string        `json:"error_msg,omitempty"`
	NumRecords int           `json:"num_records,omitempty"`
	Results    []interface{} `json:"results,omitempty"`
}

func (mux *Mux) HandleLambdaEvent(ctx context.Context, event *LambdaUDFEvent) (jsonStr string, funcErr error) {
	defer func() {
		if panicValue := recover(); panicValue != nil {
			if err, ok := panicValue.(error); ok {
				funcErr = err
			} else {
				panic(panicValue)
			}
		}
	}()
	var handler LambdaUDFHandler
	for _, e := range mux.entries {
		if e.Match(event) {
			handler = e.GetHandler()
			if handler != nil {
				break
			}
		}
	}
	if handler == nil {
		if mux.NotMatchHandler != nil {
			handler = mux.NotMatchHandler
		} else {
			handler = LambdaUDFHandlerFunc(func(_ context.Context, _ [][]interface{}) ([]interface{}, error) {
				return nil, fmt.Errorf("external function `%s` not match", event.ExternalFunction)
			})
		}
	}

	var output lambdaUDFOutputData
	ctxWithMetadata := withMetadata(ctx, &event.LambdaUDFEventMetadata)
	results, err := handler.ExecuteUDF(ctxWithMetadata, event.Arguments)
	if err != nil {
		output.Success = false
		output.ErrorMsg = err.Error()
	} else {
		n := len(results)
		if n == event.NumRecords {
			output.NumRecords = n
			output.Results = results
		} else if n < event.NumRecords {
			output.NumRecords = event.NumRecords
			output.Results = make([]interface{}, event.NumRecords)
			copy(output.Results[:n], results[:n])
		} else {
			output.NumRecords = event.NumRecords
			output.Results = results[:event.NumRecords]
		}
		output.Success = true
	}
	var bs []byte
	bs, err = json.Marshal(output)
	jsonStr = string(bs)
	return
}

func (mux *Mux) NewEntry() *Entry {
	entry := &Entry{}
	mux.entries = append(mux.entries, entry)
	return entry
}

func (mux *Mux) Handle(exFunc string, handler LambdaUDFHandler) *Entry {
	return mux.NewEntry().ExternalFunction(exFunc).Handler(handler)
}

func (mux *Mux) HandleFunc(exFunc string, f func(context.Context, [][]interface{}) ([]interface{}, error)) *Entry {
	return mux.NewEntry().ExternalFunction(exFunc).HandlerFunc(f)
}

func (mux *Mux) HandleRow(exFunc string, handler LambdaUDFRowHandler) *Entry {
	return mux.NewEntry().ExternalFunction(exFunc).Handler(ParallelRowProcessHandler{
		RowHandler: handler,
	})
}

func (mux *Mux) HandleRowFunc(exFunc string, f func(context.Context, []interface{}) (interface{}, error)) *Entry {
	return mux.NewEntry().ExternalFunction(exFunc).Handler(ParallelRowProcessHandler{
		RowHandler: LambdaUDFRowHandlerFunc(f),
	})
}
