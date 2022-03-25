# gravita
A go package for Redshift Lambda UDF Multiplexer

[![GoDoc](https://godoc.org/github.com/mashiike/gravita?status.svg)](https://godoc.org/github.com/mashiike/gravita)
![Github Actions test](https://github.com/mashiike/gravita/workflows/Test/badge.svg?branch=main)
[![Go Report Card](https://goreportcard.com/badge/mashiike/gravita)](https://goreportcard.com/report/mashiike/gravita)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/mashiike/gravita/blob/master/LICENSE)


## Install

```sh
go get -u github.com/mashiike/gravita
```

## Examples

most simple case:
```go
func main() {
    mux := gravita.NewMux()
    mux.HandleFunc("*", func(ctx context.Context, args [][]interface{}) ([]interface{}, error) {
        ret := make([]interface{}, 0, len(args))
        // anything do
        return ret, nil
    })
    lambda.Start(mux.HandleLambdaEvent)
}
```

If each row is independent, can also do the following:
```go
func main() {
    mux := gravita.NewMux()
    mux.HandleRowFunc("*", func(_ context.Context, args []interface{}) (interface{}, error) {
        return fmt.Sprint(args...), nil
    })
    lambda.Start(mux.HandleLambdaEvent)
}
```

If you want to do batch processing, you can do the following:
```go
func main() {
    mux := gravita.NewMux()
    handler := gravita.NewBatchProcessHandler(
        100, //batchSize 
        gravita.LambdaUDFHandlerFunc(func(ctx context.Context, args [][]interface{}) ([]interface{}, error) {
            //Called in small batches, len(args) == batchSize
            ret := make([]interface{}, 0, len(args))
            // anything do
            return ret, nil
        }),
    )
    mux.Handle("*", handler)
    lambda.Start(mux.HandleLambdaEvent)
}
```

## LICENSE

MIT License

Copyright (c) 2022 IKEDA Masashi
