package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/theirish81/frags"
	"github.com/theirish81/fragsfunctions"
)

type DbCollection struct {
	fragsfunctions.BasicCollection
	conn *pgxpool.Pool
}

type TableSchema struct {
	Columns []ColumnSchema
}

type ColumnSchema struct {
	ColumnName string `json:"column_name"`
	DataType   string `json:"data_type"`
}

func New(ctx context.Context, connString string) (frags.ToolsCollection, error) {

	collection := DbCollection{
		BasicCollection: fragsfunctions.NewBasicCollection("postgres", "PostgreSQL database functions"),
	}

	conn, err := pgxpool.New(ctx, connString)
	if err != nil {
		return &collection, err
	}
	collection.conn = conn

	collection.AddFunction(frags.Function{
		Name:        "postgres_query",
		Description: "executes a SELECT query against a PostgreSQL database. Do not use to alter the database record or structure.",
		Schema: &frags.Schema{
			Type:     frags.SchemaObject,
			Required: []string{"query"},
			Properties: map[string]*frags.Schema{
				"query": {
					Type:        frags.SchemaString,
					Description: "a SQL SELECT query",
				},
			},
		},
		Func: func(args map[string]any) (any, error) {
			query, err := fragsfunctions.GetArg[string](args, "query")
			if err != nil {
				return nil, err
			}
			results := make([]map[string]any, 0)
			rows, err := conn.Query(context.Background(), *query)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			fields := rows.FieldDescriptions()
			for rows.Next() {
				values, err := rows.Values()
				if err != nil {
					return nil, err
				}

				rowMap := make(map[string]any)
				for i, field := range fields {
					value := values[i]

					// Convert UUID to string
					if uuid, ok := value.([16]byte); ok {
						value = fmt.Sprintf("%x-%x-%x-%x-%x",
							uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16])
					}
					rowMap[field.Name] = value
				}

				results = append(results, rowMap)
			}
			return results, nil
		},
	})
	collection.AddFunction(frags.Function{
		Name:        "postgres_statement",
		Description: "executes an UPDATE or INSERT statement against a PostgreSQL database table",
		Schema: &frags.Schema{
			Type:     frags.SchemaObject,
			Required: []string{"statement"},
			Properties: map[string]*frags.Schema{
				"statement": {Type: frags.SchemaString},
			},
		},
		Func: func(args map[string]any) (any, error) {
			statement, err := fragsfunctions.GetArg[string](args, "statement")
			if err != nil {
				return nil, err
			}
			_, err = conn.Exec(ctx, *statement)
			if err != nil {
				return nil, err
			}
			return map[string]any{"success": true}, nil
		},
	})

	collection.AddFunction(frags.Function{
		Name:        "postgres_table_schema",
		Description: "returns the schema of a PostgreSQL table",
		Schema: &frags.Schema{
			Type:     frags.SchemaObject,
			Required: []string{"table"},
			Properties: map[string]*frags.Schema{
				"table": {
					Type:        frags.SchemaString,
					Description: "the name of the table",
				},
			},
		},
		Func: func(args map[string]any) (any, error) {
			table, err := fragsfunctions.GetArg[string](args, "table")
			if err != nil {
				return nil, err
			}

			query := `
		SELECT column_name, data_type
		FROM information_schema.columns
		WHERE table_name = $1
		ORDER BY ordinal_position
	`
			rows, err := conn.Query(ctx, query, table)
			if err != nil {
				return nil, fmt.Errorf("query failed: %w", err)
			}
			defer rows.Close()

			schema := &TableSchema{
				Columns: []ColumnSchema{},
			}

			for rows.Next() {
				var col ColumnSchema
				err := rows.Scan(&col.ColumnName, &col.DataType)
				if err != nil {
					return nil, fmt.Errorf("scan failed: %w", err)
				}
				schema.Columns = append(schema.Columns, col)
			}

			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("rows iteration error: %w", err)
			}

			return schema, nil
		},
	})

	return &collection, nil

}
