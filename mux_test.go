package gravita_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/mashiike/gravita"
	"github.com/stretchr/testify/require"
)

func testLambdaUDFEvent(exFunc string, args [][]interface{}) *gravita.LambdaUDFEvent {
	return &gravita.LambdaUDFEvent{
		LambdaUDFEventMetadata: gravita.LambdaUDFEventMetadata{
			RequestID:        "00000000-0000-0000-0000-000000000000",
			Cluster:          "dummy",
			User:             "test",
			Database:         "dev",
			ExternalFunction: exFunc,
			QueryID:          10,
			NumRecords:       len(args),
		},
		Arguments: args,
	}
}
func TestPanicHandler(t *testing.T) {
	mux := gravita.NewMux()
	mux.HandleFunc("*", func(_ context.Context, _ [][]interface{}) ([]interface{}, error) {
		panic(errors.New("hoge hoge panic"))
	})
	_, err := mux.HandleLambdaEvent(context.Background(), testLambdaUDFEvent("test_udf", [][]interface{}{
		{"hoge", 1},
		{"fuga", 2},
	}))
	require.EqualError(t, err, "hoge hoge panic")
}

func TestMuxHandler(t *testing.T) {
	cases := []struct {
		casename string
		callFunc string
		callArgs [][]interface{}
		prepare  func(mux *gravita.Mux)
		expected string
	}{
		{
			casename: "match *",
			prepare: func(mux *gravita.Mux) {
				mux.HandleFunc("*", func(ctx context.Context, args [][]interface{}) ([]interface{}, error) {
					ret := make([]interface{}, 0, len(args))
					for _, params := range args {
						ret = append(ret, fmt.Sprint(params[0], "=", params[1]))
					}
					return ret, nil
				}).Cluster("dummy").Database("dev").User("test")
			},
			expected: `{"results":[ "hoge=1", "fuga=2", "piyo=3"],"num_records":3, "success": true}`,
		},
		{
			casename: "long results",
			prepare: func(mux *gravita.Mux) {
				mux.HandleFunc("*", func(ctx context.Context, args [][]interface{}) ([]interface{}, error) {
					ret := make([]interface{}, 1, len(args)+1)
					for _, params := range args {
						ret = append(ret, fmt.Sprint(params[0], "=", params[1]))
					}
					return ret, nil
				}).Cluster("dummy").Database("dev").User("test")
			},
			expected: `{"results":[null, "hoge=1", "fuga=2"],"num_records":3, "success": true}`,
		},
		{
			casename: "short results",
			prepare: func(mux *gravita.Mux) {
				mux.HandleFunc("*", func(ctx context.Context, args [][]interface{}) ([]interface{}, error) {
					ret := make([]interface{}, 0, len(args)-1)
					for _, params := range args {
						ret = append(ret, fmt.Sprint(params[0], "=", params[1]))
						if len(ret) == len(args)-1 {
							break
						}
					}
					return ret, nil
				}).Cluster("dummy").Database("dev").User("test")
			},
			expected: `{"results":["hoge=1", "fuga=2", null],"num_records":3, "success": true}`,
		},
		{
			casename: "astr match",
			prepare: func(mux *gravita.Mux) {
				mux.HandleFunc("*", func(ctx context.Context, args [][]interface{}) ([]interface{}, error) {
					ret := make([]interface{}, 0, len(args))
					return ret, nil
				}).Cluster("*").Database("*").User("*")
			},
			expected: `{"results":[null, null, null],"num_records":3, "success": true}`,
		},
		{
			casename: "wildcard match",
			prepare: func(mux *gravita.Mux) {
				mux.HandleFunc("*_udf", func(ctx context.Context, args [][]interface{}) ([]interface{}, error) {
					ret := make([]interface{}, 0, len(args))
					return ret, nil
				}).Cluster("d*").Database("d*").User("t*")
			},
			expected: `{"results":[null, null, null],"num_records":3, "success": true}`,
		},
		{
			casename: "regexp match",
			prepare: func(mux *gravita.Mux) {
				mux.HandleFunc("*_udf", func(ctx context.Context, args [][]interface{}) ([]interface{}, error) {
					ret := make([]interface{}, 0, len(args))
					return ret, nil
				}).ClusterRegexp("d.*").Database("d.*").User("t.*")
			},
			expected: `{"results":[null, null, null],"num_records":3, "success": true}`,
		},
		{
			casename: "not match",
			prepare: func(mux *gravita.Mux) {
				mux.HandleFunc("*", func(ctx context.Context, i [][]interface{}) ([]interface{}, error) {
					panic(errors.New("matched"))
				}).Cluster("hoge")
			},
			expected: `{"error_msg":"external function ` + "`test_udf`" + ` not match", "success": false}`,
		},
		{
			casename: "match one void_udf",
			callFunc: "void_udf",
			prepare: func(mux *gravita.Mux) {
				mux.HandleFunc("test_udf", func(_ context.Context, _ [][]interface{}) ([]interface{}, error) {
					panic(errors.New("matched"))
				})
				mux.HandleFunc("void_udf", func(_ context.Context, args [][]interface{}) ([]interface{}, error) {
					ret := make([]interface{}, 0, len(args))
					return ret, nil
				})
			},
			expected: `{"results":[null, null, null],"num_records":3, "success": true}`,
		},
		{
			casename: "custom not match",
			callFunc: "void_udf",
			prepare: func(mux *gravita.Mux) {
				mux.NotMatchHandler = gravita.LambdaUDFHandlerFunc(func(ctx context.Context, i [][]interface{}) ([]interface{}, error) {
					return nil, errors.New("not match")
				})
			},
			expected: `{"error_msg":"not match", "success": false}`,
		},
		{
			casename: "row handler",
			callFunc: "concat",
			prepare: func(mux *gravita.Mux) {
				mux.HandleRowFunc("concat", func(_ context.Context, args []interface{}) (interface{}, error) {
					return fmt.Sprint(args...), nil
				})
			},
			expected: `{"results":["hoge1", "fuga2", "piyo3" ],"num_records":3, "success": true}`,
		},
		{
			casename: "row handler err",
			callFunc: "concat",
			prepare: func(mux *gravita.Mux) {
				mux.HandleRowFunc("concat", func(_ context.Context, args []interface{}) (interface{}, error) {
					return nil, errors.New("invalid")
				})
			},
			expected: `{"error_msg":"invalid", "success": false}`,
		},
		{
			casename: "batch handler",
			callArgs: [][]interface{}{
				{"hoge", 1},
				{"fuga", 2},
				{"hoge", 1},
				{"fuga", 2},
				{"hoge", 1},
				{"fuga", 2},
			},
			prepare: func(mux *gravita.Mux) {
				handler := gravita.NewBatchProcessHandler(2, gravita.LambdaUDFHandlerFunc(func(ctx context.Context, args [][]interface{}) ([]interface{}, error) {
					if len(args) > 2 {
						return nil, errors.New("too many args")
					}
					ret := make([]interface{}, 0, len(args))
					for _, params := range args {
						ret = append(ret, fmt.Sprint(params[0], "=", params[1]))
					}
					return ret, nil
				}))
				mux.Handle("*", handler)
			},
			expected: `{"results":["hoge=1", "fuga=2", "hoge=1", "fuga=2", "hoge=1", "fuga=2"],"num_records":6, "success": true}`,
		},
		{
			casename: "batch handler distinct",
			callArgs: [][]interface{}{
				{"hoge", 1},
				{"fuga", 2},
				{"hoge", 1},
				{"fuga", 2},
				{"hoge", 1},
				{"fuga", 2},
			},
			prepare: func(mux *gravita.Mux) {
				callCount := 0
				handler := gravita.NewBatchProcessHandler(2, gravita.LambdaUDFHandlerFunc(func(ctx context.Context, args [][]interface{}) ([]interface{}, error) {
					if len(args) > 2 {
						return nil, errors.New("too many args")
					}
					callCount++
					if callCount >= 2 {
						return nil, errors.New("too many call")
					}
					ret := make([]interface{}, 0, len(args))
					for _, params := range args {
						ret = append(ret, fmt.Sprint(params[0], "=", params[1]))
					}
					return ret, nil
				}))
				handler.Distinct(true)
				mux.Handle("*", handler)
			},
			expected: `{"results":["hoge=1", "fuga=2", "hoge=1", "fuga=2", "hoge=1", "fuga=2"],"num_records":6, "success": true}`,
		},
		{
			casename: "batch handler maxBatchCount",
			callArgs: [][]interface{}{
				{"hoge", 1},
				{"fuga", 2},
				{"hoge", 1},
				{"fuga", 2},
				{"hoge", 1},
				{"fuga", 2},
			},
			prepare: func(mux *gravita.Mux) {
				callCount := 0
				handler := gravita.NewBatchProcessHandler(2, gravita.LambdaUDFHandlerFunc(func(ctx context.Context, args [][]interface{}) ([]interface{}, error) {
					if len(args) > 2 {
						return nil, errors.New("too many args")
					}
					callCount++
					if callCount >= 3 {
						return nil, errors.New("too many call")
					}
					ret := make([]interface{}, 0, len(args))
					for _, params := range args {
						ret = append(ret, fmt.Sprint(params[0], "=", params[1]))
					}
					return ret, nil
				}))
				handler.MaxBatchCount(2)
				mux.Handle("*", handler)
			},
			expected: `{"results":["hoge=1", "fuga=2", "hoge=1", "fuga=2", null, null],"num_records":6, "success": true}`,
		},
	}

	for _, c := range cases {
		t.Run(c.casename, func(t *testing.T) {
			mux := gravita.NewMux()
			c.prepare(mux)
			callFunc := c.callFunc
			if callFunc == "" {
				callFunc = "test_udf"
			}
			callArgs := c.callArgs
			if callArgs == nil {
				callArgs = [][]interface{}{
					{"hoge", 1},
					{"fuga", 2},
					{"piyo", 3},
				}
			}
			actual, err := mux.HandleLambdaEvent(context.Background(), testLambdaUDFEvent(callFunc, callArgs))
			require.NoError(t, err)
			require.JSONEq(t, c.expected, actual)
		})
	}
}
