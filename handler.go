package gravita

import (
	"context"

	"golang.org/x/sync/errgroup"
)

//LambdaUDFHandler is an interface for handling the actual state of LambdaUDF
type LambdaUDFHandler interface {
	ExecuteUDF(context.Context, [][]interface{}) ([]interface{}, error)
}

// LambdaUDFHandlerFunc is a type of function that satisfies LambdaUDFHandler
type LambdaUDFHandlerFunc func(context.Context, [][]interface{}) ([]interface{}, error)

func (f LambdaUDFHandlerFunc) ExecuteUDF(ctx context.Context, args [][]interface{}) ([]interface{}, error) {
	return f(ctx, args)
}

//LambdaUDFRowHandler is an interface for handling the actual state of LambdaUDF Row
type LambdaUDFRowHandler interface {
	ExecuteUDFRow(context.Context, []interface{}) (interface{}, error)
}

// LambdaUDFRowHandlerFunc is a type of function that satisfies LambdaUDFRowHandler
type LambdaUDFRowHandlerFunc func(context.Context, []interface{}) (interface{}, error)

func (f LambdaUDFRowHandlerFunc) ExecuteUDFRow(ctx context.Context, args []interface{}) (interface{}, error) {
	return f(ctx, args)
}

// ParallelRowProcessHandler is a LambdaUDFHandler that can be used when each row is independent and processes rows in parallel
type ParallelRowProcessHandler struct {
	RowHandler LambdaUDFRowHandler
}

func (h ParallelRowProcessHandler) ExecuteUDF(ctx context.Context, args [][]interface{}) ([]interface{}, error) {
	n := len(args)
	results := make([]interface{}, len(args))
	if h.RowHandler == nil {
		return results, nil
	}

	var g errgroup.Group
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for i := 0; i < n; i++ {
		index := i
		rowArgs := args[i]
		g.Go(func() error {
			result, err := h.RowHandler.ExecuteUDFRow(ctx, rowArgs)
			if err != nil {
				return err
			}
			results[index] = result
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}
