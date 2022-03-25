/*
Package gravita implements the LambdaUDF dispatcher.

a simple usecase:

```
	mux := gravita.NewMux()
	mux.HandleFunc("*", func(ctx context.Context, args [][]interface{}) ([]interface{}, error) {
		ret := make([]interface{}, 0, len(args))
		// anything do
		return ret, nil
	})
	lambda.Start(mux.HandleLambdaEvent)
````

more complex usecase
```
	mux := gravita.NewMux()
	mux.HandleFunc("*func1*", func(ctx context.Context, args [][]interface{}) ([]interface{}, error) {
		ret := make([]interface{}, 0, len(args))
		// anything do
		return ret, nil
	})
	mux.HandleFunc("*func2*", func(ctx context.Context, args [][]interface{}) ([]interface{}, error) {
		ret := make([]interface{}, 0, len(args))
		// anything do
		return ret, nil
	})
	lambda.Start(mux.HandleLambdaEvent)
````

*/
package gravita
