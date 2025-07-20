package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	FALLBACK_SQL_QUERY  = "SELECT 1"
	INSPECT_SQL_COMMENT = " --INSPECT"
)

type QueryHandler struct {
	duckdb          *Duckdb
	icebergReader   *IcebergReader
	queryRemapper   *QueryRemapper
	config          *Config
	ResponseHandler *ResponseHandler
}

type PreparedStatement struct {
	// Parse
	Name          string
	OriginalQuery string
	Query         string
	Statement     *sql.Stmt
	ParameterOIDs []uint32

	// Bind
	Bound     bool
	Variables []interface{}
	Portal    string

	// Describe
	Described bool

	// Describe/Execute
	Rows *sql.Rows
}

func NewQueryHandler(config *Config, duckdb *Duckdb, icebergReader *IcebergReader) *QueryHandler {
	queryHandler := &QueryHandler{
		duckdb:          duckdb,
		icebergReader:   icebergReader,
		queryRemapper:   NewQueryRemapper(config, icebergReader, duckdb),
		config:          config,
		ResponseHandler: NewResponseHandler(config),
	}

	queryHandler.createSchemas()

	return queryHandler
}

func (queryHandler *QueryHandler) HandleSimpleQuery(originalQuery string) ([]pgproto3.Message, error) {
	queryStatements, originalQueryStatements, err := queryHandler.queryRemapper.ParseAndRemapQuery(originalQuery)
	if err != nil {
		return nil, err
	}
	if len(queryStatements) == 0 {
		return []pgproto3.Message{&pgproto3.EmptyQueryResponse{}}, nil
	}

	var queriesMessages []pgproto3.Message

	for i, queryStatement := range queryStatements {
		rows, err := queryHandler.duckdb.QueryContext(context.Background(), queryStatement)
		if err != nil {
			errorMessage := err.Error()
			if errorMessage == "Binder Error: UNNEST requires a single list as input" {
				// https://github.com/duckdb/duckdb/issues/11693
				LogWarn(queryHandler.config, "Couldn't handle query via DuckDB:", queryStatement+"\n"+err.Error())
				queriesMsgs, err := queryHandler.HandleSimpleQuery(FALLBACK_SQL_QUERY) // self-recursion
				if err != nil {
					return nil, err
				}
				queriesMessages = append(queriesMessages, queriesMsgs...)
				continue
			} else {
				return nil, err
			}
		}
		defer rows.Close()

		var queryMessages []pgproto3.Message
		descriptionMessages, err := queryHandler.rowsToDescriptionMessages(rows, originalQueryStatements[i])
		if err != nil {
			return nil, err
		}
		queryMessages = append(queryMessages, descriptionMessages...)
		dataMessages, err := queryHandler.rowsToDataMessages(rows, originalQueryStatements[i])
		if err != nil {
			return nil, err
		}
		queryMessages = append(queryMessages, dataMessages...)

		queriesMessages = append(queriesMessages, queryMessages...)
	}

	return queriesMessages, nil
}

func (queryHandler *QueryHandler) HandleParseQuery(message *pgproto3.Parse) ([]pgproto3.Message, *PreparedStatement, error) {
	ctx := context.Background()
	originalQuery := string(message.Query)
	queryStatements, _, err := queryHandler.queryRemapper.ParseAndRemapQuery(originalQuery)
	if err != nil {
		return nil, nil, err
	}
	if len(queryStatements) > 1 {
		return nil, nil, fmt.Errorf("multiple queries in a single parse message are not supported: %s", originalQuery)
	}

	preparedStatement := &PreparedStatement{
		Name:          message.Name,
		OriginalQuery: originalQuery,
		ParameterOIDs: message.ParameterOIDs,
	}
	if len(queryStatements) == 0 {
		return []pgproto3.Message{&pgproto3.ParseComplete{}}, preparedStatement, nil
	}

	query := queryStatements[0]
	preparedStatement.Query = query
	statement, err := queryHandler.duckdb.PrepareContext(ctx, query)
	preparedStatement.Statement = statement
	if err != nil {
		return nil, nil, err
	}

	return []pgproto3.Message{&pgproto3.ParseComplete{}}, preparedStatement, nil
}

func (queryHandler *QueryHandler) HandleBindQuery(message *pgproto3.Bind, preparedStatement *PreparedStatement) ([]pgproto3.Message, *PreparedStatement, error) {
	if message.PreparedStatement != preparedStatement.Name {
		return nil, nil, fmt.Errorf("prepared statement mismatch, %s instead of %s: %s", message.PreparedStatement, preparedStatement.Name, preparedStatement.OriginalQuery)
	}

	var variables []interface{}
	paramFormatCodes := message.ParameterFormatCodes

	for i, param := range message.Parameters {
		if param == nil {
			continue
		}

		textFormat := true
		if len(paramFormatCodes) == 1 {
			textFormat = paramFormatCodes[0] == 0
		} else if len(paramFormatCodes) > 1 {
			textFormat = paramFormatCodes[i] == 0
		}

		if textFormat {
			variables = append(variables, string(param))
		} else if len(param) == 4 {
			variables = append(variables, int32(binary.BigEndian.Uint32(param)))
		} else if len(param) == 8 {
			variables = append(variables, int64(binary.BigEndian.Uint64(param)))
		} else if len(param) == 16 {
			variables = append(variables, uuid.UUID(param).String())
		} else {
			return nil, nil, fmt.Errorf("unsupported parameter format: %v (length %d). Original query: %s", param, len(param), preparedStatement.OriginalQuery)
		}
	}

	LogDebug(queryHandler.config, "Bound variables:", variables)
	preparedStatement.Bound = true
	preparedStatement.Variables = variables
	preparedStatement.Portal = message.DestinationPortal

	messages := []pgproto3.Message{&pgproto3.BindComplete{}}

	return messages, preparedStatement, nil
}

func (queryHandler *QueryHandler) HandleDescribeQuery(message *pgproto3.Describe, preparedStatement *PreparedStatement) ([]pgproto3.Message, *PreparedStatement, error) {
	switch message.ObjectType {
	case 'S': // Statement
		if message.Name != preparedStatement.Name {
			return nil, nil, fmt.Errorf("statement mismatch, %s instead of %s: %s", message.Name, preparedStatement.Name, preparedStatement.OriginalQuery)
		}
	case 'P': // Portal
		if message.Name != preparedStatement.Portal {
			return nil, nil, fmt.Errorf("portal mismatch, %s instead of %s: %s", message.Name, preparedStatement.Portal, preparedStatement.OriginalQuery)
		}
	}

	preparedStatement.Described = true
	if preparedStatement.Query == "" || !preparedStatement.Bound { // Empty query or Parse->[No Bind]->Describe
		return []pgproto3.Message{&pgproto3.NoData{}}, preparedStatement, nil
	}

	rows, err := preparedStatement.Statement.QueryContext(context.Background(), preparedStatement.Variables...)
	if err != nil {
		return nil, nil, fmt.Errorf("couldn't execute statement: %w. Original query: %s", err, preparedStatement.OriginalQuery)
	}
	preparedStatement.Rows = rows

	messages, err := queryHandler.rowsToDescriptionMessages(preparedStatement.Rows, preparedStatement.OriginalQuery)
	if err != nil {
		return nil, nil, err
	}
	return messages, preparedStatement, nil
}

func (queryHandler *QueryHandler) HandleExecuteQuery(message *pgproto3.Execute, preparedStatement *PreparedStatement) ([]pgproto3.Message, error) {
	if message.Portal != preparedStatement.Portal {
		return nil, fmt.Errorf("portal mismatch, %s instead of %s: %s", message.Portal, preparedStatement.Portal, preparedStatement.OriginalQuery)
	}

	if preparedStatement.Query == "" {
		return []pgproto3.Message{&pgproto3.EmptyQueryResponse{}}, nil
	}

	if preparedStatement.Rows == nil { // Parse->[No Bind]->Describe->Execute or Parse->Bind->[No Describe]->Execute
		rows, err := preparedStatement.Statement.QueryContext(context.Background(), preparedStatement.Variables...)
		if err != nil {
			return nil, err
		}
		preparedStatement.Rows = rows
	}

	defer preparedStatement.Rows.Close()

	return queryHandler.rowsToDataMessages(preparedStatement.Rows, preparedStatement.OriginalQuery)
}

func (queryHandler *QueryHandler) createSchemas() {
	ctx := context.Background()
	schemas, err := queryHandler.icebergReader.Schemas()
	PanicIfError(queryHandler.config, err)

	for _, schema := range schemas {
		_, err := queryHandler.duckdb.ExecContext(
			ctx,
			"CREATE SCHEMA IF NOT EXISTS \"$schema\"",
			map[string]string{"schema": schema},
		)
		PanicIfError(queryHandler.config, err)
	}
}

func (queryHandler *QueryHandler) rowsToDescriptionMessages(rows *sql.Rows, originalQuery string) ([]pgproto3.Message, error) {
	cols, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("couldn't get column types: %w. Original query: %s", err, originalQuery)
	}

	var messages []pgproto3.Message

	rowDescription := queryHandler.generateRowDescription(cols)
	if rowDescription != nil {
		messages = append(messages, rowDescription)
	}

	return messages, nil
}

func (queryHandler *QueryHandler) rowsToDataMessages(rows *sql.Rows, originalQuery string) ([]pgproto3.Message, error) {
	cols, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("couldn't get column types: %w. Original query: %s", err, originalQuery)
	}

	var messages []pgproto3.Message
	for rows.Next() {
		dataRow, err := queryHandler.generateDataRow(rows, cols)
		if err != nil {
			return nil, fmt.Errorf("couldn't get data row: %w. Original query: %s", err, originalQuery)
		}
		messages = append(messages, dataRow)
	}

	commandTag := FALLBACK_SQL_QUERY
	upperOriginalQueryStatement := strings.ToUpper(originalQuery)
	switch {
	case strings.HasPrefix(upperOriginalQueryStatement, "SET "):
		commandTag = "SET"
	case strings.HasPrefix(upperOriginalQueryStatement, "SHOW "):
		commandTag = "SHOW"
	case strings.HasPrefix(upperOriginalQueryStatement, "DISCARD ALL"):
		commandTag = "DISCARD ALL"
	case strings.HasPrefix(upperOriginalQueryStatement, "BEGIN"):
		commandTag = "BEGIN"
	}

	messages = append(messages, &pgproto3.CommandComplete{CommandTag: []byte(commandTag)})
	return messages, nil
}

func (queryHandler *QueryHandler) generateRowDescription(cols []*sql.ColumnType) *pgproto3.RowDescription {
	description := pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{}}

	for _, col := range cols {
		typeIod := queryHandler.ResponseHandler.ColumnDescriptionTypeOid(col)

		if col.Name() == "Success" && typeIod == pgtype.BoolOID && len(cols) == 1 {
			// Skip the "Success" DuckDB column returned from SET ... commands
			return nil
		}

		description.Fields = append(description.Fields, pgproto3.FieldDescription{
			Name:                 []byte(col.Name()),
			TableOID:             0,
			TableAttributeNumber: 0,
			DataTypeOID:          typeIod,
			DataTypeSize:         -1,
			TypeModifier:         -1,
			Format:               0,
		})
	}
	return &description
}

func (queryHandler *QueryHandler) generateDataRow(rows *sql.Rows, cols []*sql.ColumnType) (*pgproto3.DataRow, error) {
	valuePointers := make([]interface{}, len(cols))
	for i, col := range cols {
		valuePointers[i] = queryHandler.ResponseHandler.RowValuePointer(col)
	}

	err := rows.Scan(valuePointers...)
	if err != nil {
		return nil, err
	}

	var values [][]byte
	for i, valuePointer := range valuePointers {
		value := queryHandler.ResponseHandler.RowValueBytes(valuePointer, cols[i])
		values = append(values, value)
	}
	dataRow := pgproto3.DataRow{Values: values}

	return &dataRow, nil
}
