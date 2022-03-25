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

*/
package gravita
