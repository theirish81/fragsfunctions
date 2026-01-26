package fs

import (
	"os"

	"github.com/theirish81/frags"
	"github.com/theirish81/frags/schema"
	"github.com/theirish81/frags/util"
	"github.com/theirish81/fragsfunctions"
)

type FD struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

type Collection struct {
}

func New() frags.ToolsCollection {
	collection := fragsfunctions.NewBasicCollection("fs", "file system functions")

	collection.AddFunction(frags.ExternalFunction{
		Name:        "fs_ls",
		Description: "lists files in a provided directory",
		Schema: &schema.Schema{
			Type:     schema.Object,
			Required: []string{"path"},
			Properties: map[string]*schema.Schema{
				"path": {Type: schema.String},
			},
		},
		Func: func(ctx *util.FragsContext, args map[string]any) (any, error) {
			path, err := fragsfunctions.GetArg[string](args, "path")
			if err != nil {
				return nil, err
			}
			files, err := os.ReadDir(*path)
			if err != nil {
				return nil, err
			}
			fds := make([]FD, 0)
			for _, fd := range files {
				info, _ := fd.Info()
				fds = append(fds, FD{
					Name: fd.Name(),
					Size: info.Size(),
				})
			}
			return fds, nil
		},
	})
	collection.AddFunction(frags.ExternalFunction{
		Name:        "fs_read",
		Description: "reads a file and returns its contents",
		Schema: &schema.Schema{
			Type:     schema.Object,
			Required: []string{"path"},
			Properties: map[string]*schema.Schema{
				"path": {Type: schema.String},
			},
		},
		Func: func(ctx *util.FragsContext, args map[string]any) (any, error) {
			path, err := fragsfunctions.GetArg[string](args, "path")
			if err != nil {
				return nil, err
			}
			contents, err := os.ReadFile(*path)
			if err != nil {
				return nil, err
			}
			return map[string]any{"content": string(contents)}, nil
		},
	})
	collection.AddFunction(frags.ExternalFunction{
		Name:        "fs_write",
		Description: "writes a file and returns its contents",
		Schema: &schema.Schema{
			Type:     schema.Object,
			Required: []string{"path", "contents"},
			Properties: map[string]*schema.Schema{
				"path":     {Type: schema.String},
				"contents": {Type: schema.String},
			},
		},
		Func: func(ctx *util.FragsContext, args map[string]any) (any, error) {
			path, err := fragsfunctions.GetArg[string](args, "path")
			if err != nil {
				return nil, err
			}
			contents, err := fragsfunctions.GetArg[string](args, "contents")
			if err != nil {
				return nil, err
			}
			err = os.WriteFile(*path, []byte(*contents), 0644)
			if err != nil {
				return nil, err
			}
			return map[string]any{"success": true}, nil
		},
	})
	return &collection
}
