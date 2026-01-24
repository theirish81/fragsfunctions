package http

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/theirish81/frags"
	"github.com/theirish81/fragsfunctions"
)

func New() frags.ToolsCollection {
	client := http.Client{
		Timeout: time.Second * 30,
	}
	collection := fragsfunctions.NewBasicCollection("http", "http client functions")

	collection.AddFunction(frags.Function{
		Name:        "http_fetch",
		Description: "executes an HTTP request and returns the response body",
		Schema: &frags.Schema{
			Type:     frags.SchemaObject,
			Required: []string{"method", "url"},
			Properties: map[string]*frags.Schema{
				"method": {
					Type: frags.SchemaString,
					Enum: []string{"GET", "POST", "PUT", "PATCH", "DELETE"},
				},
				"url":     {Type: frags.SchemaString},
				"headers": {Type: frags.SchemaObject},
				"body":    {Type: frags.SchemaString},
			},
		},
		Func: func(ctx *frags.FragsContext, args map[string]any) (any, error) {
			method, err := fragsfunctions.GetArg[string](args, "method")
			if err != nil {
				return nil, err
			}
			url, err := fragsfunctions.GetArg[string](args, "url")
			if err != nil {
				return nil, err
			}
			headers, _ := fragsfunctions.GetArg[map[string]any](args, "headers")

			body, _ := fragsfunctions.GetArg[string](args, "body")
			var reader io.Reader
			if body != nil && len(*body) > 0 {
				reader = bytes.NewReader([]byte(*body))
			}

			req, err := http.NewRequest(*method, *url, reader)
			if headers != nil {
				for k, v := range *headers {
					req.Header.Set(k, v.(string))
				}
			}
			if err != nil {
				return nil, err
			}
			res, err := client.Do(req.WithContext(ctx))
			if err != nil {
				return nil, err
			}
			defer func() { _ = res.Body.Close() }()
			data, err := io.ReadAll(res.Body)
			if err != nil {
				return nil, err
			}
			if strings.Contains(res.Header.Get("Content-Type"), "json") {
				out := make(map[string]any)
				if err := json.Unmarshal(data, &out); err == nil {
					return out, nil
				}
				out2 := make([]any, 0)
				if err := json.Unmarshal(data, &out2); err == nil {
					return out2, nil
				}
			}
			return map[string]any{"data": string(data)}, nil
		},
	})
	return &collection
}
