package data

import (
	"errors"

	"github.com/theirish81/frags"
	"github.com/theirish81/frags/schema"
	"github.com/theirish81/frags/scriptengines"
	"github.com/theirish81/frags/util"
	"github.com/theirish81/fragsfunctions"
	"github.com/theirish81/zealql"
)

func New(db *zealql.Database) frags.ToolsCollection {
	collection := fragsfunctions.NewBasicCollection("data", "data manipulation functions")
	collection.AddFunction(frags.ExternalFunction{
		Name:        "list_internal_db_tables",
		Description: `lists all the tables in the Internal Database, used for simplifying  dataset manipulation.`,
		Schema: &schema.Schema{
			Type: schema.Object,
		},
		Func: func(ctx *util.FragsContext, data map[string]any) (any, error) {
			return db.ListTables(), nil
		},
	})
	collection.AddFunction(frags.ExternalFunction{
		Name: "describe_internal_db_tables",
		Description: `describes multiple tables in the Internal Database, used for simplifying  dataset manipulation.
Provide no arguments to describe all the tables.`,
		Collection: "internal_db_functions",
		Schema: &schema.Schema{
			Type:        schema.Object,
			Description: "the tables to describe. Provide no arguments to describe all the tables.",
			Properties: map[string]*schema.Schema{
				"table_names": {
					Type: schema.Array,
					Items: &schema.Schema{
						Type: schema.String,
					},
				},
			},
		},
		Func: func(ctx *util.FragsContext, data map[string]any) (any, error) {
			descriptions := make([]string, 0)
			tableNames := make([]string, 0)
			if selectedTableNames, err := fragsfunctions.GetArg[[]any](data, "table_names"); err == nil && selectedTableNames != nil {
				for _, tableName := range *selectedTableNames {
					if castTableName, ok := tableName.(string); ok {
						tableNames = append(tableNames, castTableName)
					} else {
						return nil, errors.New("table names must be of type string")
					}
				}
			}
			if len(tableNames) == 0 {
				tableNames = db.ListTables()
			}
			for _, tn := range tableNames {
				if table, ok := db.GetTable(tn); ok {
					sql := table.ToSQL()
					descriptions = append(descriptions, sql)
				}
			}
			return descriptions, nil
		},
	})
	collection.AddFunction(frags.ExternalFunction{
		Name:        "query_internal_db_tables",
		Description: `queries the Internal Database, used for simplifying dataset manipulation.`,
		Schema: &schema.Schema{
			Type:        schema.Object,
			Required:    []string{"query"},
			Description: "the SQL-Lite compatible query. Only SELECT, no insert/update/alter operations allowed",
			Properties: map[string]*schema.Schema{
				"query": {
					Type: schema.String,
				},
			},
		},
		Func: func(ctx *util.FragsContext, data map[string]any) (any, error) {
			query, err := fragsfunctions.GetArg[string](data, "query")
			if err != nil {
				return nil, err
			}
			return db.Query(*query)
		},
	})
	collection.AddFunction(frags.ExternalFunction{
		Name: "insert_in_internal_db_table",
		Description: `insert data into the database, used for simplifying  dataset manipulation. If the table does not,
exist, it will be created upon insertion.`,
		Schema: &schema.Schema{
			Type:        schema.Object,
			Required:    []string{"table_name", "records"},
			Description: "the table to insert into",
			Properties: map[string]*schema.Schema{
				"table_name": {
					Type: schema.String,
				},
				"records": {
					Type: schema.Array,
					Items: &schema.Schema{
						Type: schema.Object,
					},
				},
			},
		},
		Func: func(ctx *util.FragsContext, data map[string]any) (any, error) {
			tableName, err := fragsfunctions.GetArg[string](data, "table_name")
			if err != nil {
				return nil, err
			}
			records, err := fragsfunctions.GetArg[[]any](data, "records")
			if err != nil {
				return nil, err
			}
			table, err := db.CreateTable(*tableName, *records)
			if err != nil {
				return nil, err
			}
			return table.ToSQL(), nil
		},
	})
	collection.AddFunction(frags.ExternalFunction{
		Name: "execute_javascript",
		Description: `execute JavaScript code (using completion-value notation) for the sole purpose of number crunching
and data reshaping. No NodeJS objects are allowed (console.log... etc). Use it to perform batch operations on data-sets,
or compute special values`,
		Schema: &schema.Schema{
			Type:     schema.Object,
			Required: []string{"code", "args"},
			Example: map[string]any{
				"code": "var t = args.raw.split(',').map(t => t.trim())\nt;",
				"args": map[string]any{"raw": "a, b, c"},
			},
			Properties: map[string]*schema.Schema{
				"code": {
					Type:        schema.String,
					Description: "the JavaScript code to execute, using completion-value notation",
					Example:     "var t = args.raw.split(',').map(t => t.trim())\nt;",
				},
				"args": {
					Type: schema.Object,
					Description: `the arguments to pass to the code. They will be exposed to the engine as the
object "args". Do not inline the arguments in the code`,
				},
			},
		},
		Func: func(ctx *util.FragsContext, data map[string]any) (any, error) {
			engine := scriptengines.NewJavascriptScriptingEngine()
			code, err := fragsfunctions.GetArg[string](data, "code")
			if err != nil {
				return nil, err
			}
			return engine.RunCode(ctx, *code, data["args"], nil)
		},
	})
	return &collection
}
