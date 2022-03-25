package gravita

import (
	"context"
	"fmt"

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

type BatchProcessHandler struct {
	handler       LambdaUDFHandler
	distinct      bool
	batchSize     int
	maxBatchCount *int
}

func NewBatchProcessHandler(batchSize int, handler LambdaUDFHandler) *BatchProcessHandler {
	return &BatchProcessHandler{
		handler:   handler,
		distinct:  false,
		batchSize: batchSize,
	}
}

func (h *BatchProcessHandler) Distinct(enable bool) {
	h.distinct = enable
}

func (h *BatchProcessHandler) BatchSize(s int) {
	h.batchSize = s
}

func (h *BatchProcessHandler) MaxBatchCount(m int) {
	h.maxBatchCount = &m
}

func (h *BatchProcessHandler) ExecuteUDF(ctx context.Context, args [][]interface{}) ([]interface{}, error) {
	results := make([]interface{}, len(args))

	batchArgs := make([][]interface{}, 0, h.batchSize)
	batchKeys := make([]string, 0, h.batchSize)
	batchIndexes := make(map[string][]int, h.batchSize)

	for i, rowArgs := range args {
		var key string
		if h.distinct {
			key = fmt.Sprint(rowArgs)
		} else {
			key = fmt.Sprintf("%d", i)
		}
		indexes, ok := batchIndexes[key]
		if !ok {
			batchArgs = append(batchArgs, rowArgs)
			batchKeys = append(batchKeys, key)
		}
		batchIndexes[key] = append(indexes, i)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var g errgroup.Group
	batchCount := 0
	for i := h.batchSize; len(batchArgs) > 0; {
		if len(batchArgs) < h.batchSize {
			i = len(batchArgs)
		}
		targetArgs := batchArgs[:i]
		batchArgs = batchArgs[i:]
		targetKeys := batchKeys[:i]
		batchKeys = batchKeys[i:]
		g.Go(func() error {
			batchResults, err := h.handler.ExecuteUDF(ctx, targetArgs)
			if err != nil {
				return err
			}
			for j, result := range batchResults {
				key := targetKeys[j]

				indexes, ok := batchIndexes[key]
				if !ok {
					continue
				}
				for _, index := range indexes {
					results[index] = result
				}
			}
			return nil
		})
		batchCount++
		if h.maxBatchCount != nil && batchCount >= *h.maxBatchCount {
			break
		}
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}
