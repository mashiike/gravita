package gravita

import "context"

//LambdaUDFEventMetadata represents the metadata of an Invoke in a LambdaUDFEvent
type LambdaUDFEventMetadata struct {
	RequestID        string `json:"request_id,omitempty"`
	Cluster          string `json:"cluster,omitempty"`
	User             string `json:"user,omitempty"`
	Database         string `json:"database,omitempty"`
	ExternalFunction string `json:"external_function,omitempty"`
	QueryID          int    `json:"query_id,omitempty"`
	NumRecords       int    `json:"num_records,omitempty"`
}

// LambdaUDFEvent represents the Event parameter when Redshift invokes as LambdaUDF
// see also: https://docs.aws.amazon.com/redshift/latest/dg/udf-creating-a-lambda-sql-udf.html
type LambdaUDFEvent struct {
	LambdaUDFEventMetadata `json:",inline"`

	Arguments [][]interface{} `json:"arguments,omitempty"`
}

type contextKey string

var metadataContextKey contextKey = "__lambda_udf_event_metadata"

// Metadata retrieves LambdaUDFEvent metadata from Context
func Metadata(ctx context.Context) *LambdaUDFEventMetadata {
	val := ctx.Value(metadataContextKey)
	if val == nil {
		return &LambdaUDFEventMetadata{}
	}
	if metadata, ok := val.(*LambdaUDFEventMetadata); ok {
		ret := *metadata
		return &ret
	}
	return &LambdaUDFEventMetadata{}
}

func withMetadata(ctx context.Context, metadata *LambdaUDFEventMetadata) context.Context {
	return context.WithValue(ctx, metadataContextKey, metadata)
}
