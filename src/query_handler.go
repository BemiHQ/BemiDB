package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/csv"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	duckDb "github.com/marcboeker/go-duckdb"
	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

const (
	FALLBACK_SQL_QUERY  = "SELECT 1"
	INSPECT_SQL_COMMENT = " --INSPECT"
)

type QueryHandler struct {
	duckdb        *Duckdb
	icebergReader *IcebergReader
	queryRemapper *QueryRemapper
	config        *Config
}

////////////////////////////////////////////////////////////////////////////////////////////////////

type PreparedStatement struct {
	Name          string
	OriginalQuery string
	Query         string
	Statement     *sql.Stmt
	ParameterOIDs []uint32
	Variables     []interface{}
	Portal        string
	Rows          *sql.Rows
}

////////////////////////////////////////////////////////////////////////////////////////////////////

type NullDecimal struct {
	Present bool
	Value   duckDb.Decimal
}

func (nullDecimal *NullDecimal) Scan(value interface{}) error {
	if value == nil {
		nullDecimal.Present = false
		return nil
	}

	nullDecimal.Present = true
	nullDecimal.Value = value.(duckDb.Decimal)
	return nil
}

func (nullDecimal NullDecimal) String() string {
	if nullDecimal.Present {
		return fmt.Sprintf("%v", nullDecimal.Value.Float64())
	}
	return ""
}

////////////////////////////////////////////////////////////////////////////////////////////////////

type NullUint32 struct {
	Present bool
	Value   uint32
}

func (nullUint32 *NullUint32) Scan(value interface{}) error {
	if value == nil {
		nullUint32.Present = false
		return nil
	}

	nullUint32.Present = true
	nullUint32.Value = value.(uint32)
	return nil
}

func (nullUint32 NullUint32) String() string {
	if nullUint32.Present {
		return fmt.Sprintf("%v", nullUint32.Value)
	}
	return ""
}

////////////////////////////////////////////////////////////////////////////////////////////////////

type NullUint64 struct {
	Present bool
	Value   uint64
}

func (nullUint64 *NullUint64) Scan(value interface{}) error {
	if value == nil {
		nullUint64.Present = false
		return nil
	}

	nullUint64.Present = true
	nullUint64.Value = value.(uint64)
	return nil
}

func (nullUint64 NullUint64) String() string {
	if nullUint64.Present {
		return fmt.Sprintf("%v", nullUint64.Value)
	}
	return ""
}

////////////////////////////////////////////////////////////////////////////////////////////////////

type NullBigInt struct {
	Present bool
	Value   *big.Int
}

func (nullBigInt *NullBigInt) Scan(value interface{}) error {
	if value == nil {
		nullBigInt.Present = false
		return nil
	}

	nullBigInt.Present = true
	nullBigInt.Value = value.(*big.Int)
	return nil
}

func (nullBigInt NullBigInt) String() string {
	if nullBigInt.Present {
		return fmt.Sprintf("%v", nullBigInt.Value)
	}
	return ""
}

////////////////////////////////////////////////////////////////////////////////////////////////////

type NullArray struct {
	Present bool
	Value   []interface{}
}

func (nullArray *NullArray) Scan(value interface{}) error {
	if value == nil {
		nullArray.Present = false
		return nil
	}

	nullArray.Present = true
	nullArray.Value = value.([]interface{})
	return nil
}

func (nullArray NullArray) String() string {
	if nullArray.Present {
		var stringVals []string
		for _, v := range nullArray.Value {
			switch v.(type) {
			case []uint8:
				stringVals = append(stringVals, fmt.Sprintf("%s", v))
			default:
				stringVals = append(stringVals, fmt.Sprintf("%v", v))
			}
		}
		buffer := &bytes.Buffer{}
		csvWriter := csv.NewWriter(buffer)
		err := csvWriter.Write(stringVals)
		if err != nil {
			return ""
		}
		csvWriter.Flush()
		return "{" + strings.TrimRight(buffer.String(), "\n") + "}"
	}
	return ""
}

////////////////////////////////////////////////////////////////////////////////////////////////////

func NewQueryHandler(config *Config, duckdb *Duckdb, icebergReader *IcebergReader) *QueryHandler {
	queryHandler := &QueryHandler{
		duckdb:        duckdb,
		icebergReader: icebergReader,
		queryRemapper: NewQueryRemapper(config, icebergReader, duckdb),
		config:        config,
	}

	queryHandler.createSchemas()

	return queryHandler
}

func (queryHandler *QueryHandler) HandleQuery(originalQuery string) ([]pgproto3.Message, error) {
	queryStatements, originalQueryStatements, err := queryHandler.parseAndRemapQuery(originalQuery)
	if err != nil {
		LogError(queryHandler.config, "Couldn't map query:", originalQuery+"\n"+err.Error())
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
				queriesMsgs, err := queryHandler.HandleQuery(FALLBACK_SQL_QUERY) // self-recursion
				if err != nil {
					return nil, err
				}
				queriesMessages = append(queriesMessages, queriesMsgs...)
				continue
			} else {
				LogError(queryHandler.config, "Couldn't handle query via DuckDB:", queryStatement+"\n"+err.Error())
				return nil, err
			}
		}
		defer rows.Close()

		var queryMessages []pgproto3.Message
		descriptionMessages, err := queryHandler.rowsToDescriptionMessages(rows, queryStatement)
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
	queryStatements, _, err := queryHandler.parseAndRemapQuery(originalQuery)
	if err != nil {
		LogError(queryHandler.config, "Couldn't map query:", originalQuery+"\n"+err.Error())
		return nil, nil, err
	}
	if len(queryStatements) > 1 {
		return nil, nil, errors.New("multiple queries in a single parse message are not supported")
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
		LogError(queryHandler.config, "Couldn't prepare query via DuckDB:", query+"\n"+err.Error())
		return nil, nil, err
	}

	return []pgproto3.Message{&pgproto3.ParseComplete{}}, preparedStatement, nil
}

func (queryHandler *QueryHandler) HandleBindQuery(message *pgproto3.Bind, preparedStatement *PreparedStatement) ([]pgproto3.Message, *PreparedStatement, error) {
	if message.PreparedStatement != preparedStatement.Name {
		LogError(queryHandler.config, "Prepared statement mismatch:", message.PreparedStatement, "instead of", preparedStatement.Name)
		return nil, nil, errors.New("prepared statement mismatch")
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
		}
	}

	LogDebug(queryHandler.config, "Bound variables:", variables)
	preparedStatement.Variables = variables
	preparedStatement.Portal = message.DestinationPortal

	messages := []pgproto3.Message{&pgproto3.BindComplete{}}

	return messages, preparedStatement, nil
}

func (queryHandler *QueryHandler) HandleDescribeQuery(message *pgproto3.Describe, preparedStatement *PreparedStatement) ([]pgproto3.Message, *PreparedStatement, error) {
	switch message.ObjectType {
	case 'S': // Statement
		if message.Name != preparedStatement.Name {
			LogError(queryHandler.config, "Statement mismatch:", message.Name, "instead of", preparedStatement.Name)
			return nil, nil, errors.New("statement mismatch")
		}
	case 'P': // Portal
		if message.Name != preparedStatement.Portal {
			LogError(queryHandler.config, "Portal mismatch:", message.Name, "instead of", preparedStatement.Portal)
			return nil, nil, errors.New("portal mismatch")
		}
	}

	if preparedStatement.Query == "" {
		return []pgproto3.Message{&pgproto3.NoData{}}, preparedStatement, nil
	}

	if len(preparedStatement.ParameterOIDs) > 0 && len(preparedStatement.ParameterOIDs) != len(preparedStatement.Variables) { // Parse passed the parameters, but Bind didn't happen after
		return []pgproto3.Message{&pgproto3.NoData{}}, preparedStatement, nil
	}

	rows, err := preparedStatement.Statement.QueryContext(context.Background(), preparedStatement.Variables...)
	if err != nil {
		LogError(queryHandler.config, "Couldn't execute prepared statement via DuckDB:", preparedStatement.Query+"\n"+err.Error())
		return nil, nil, err
	}
	preparedStatement.Rows = rows

	messages, err := queryHandler.rowsToDescriptionMessages(preparedStatement.Rows, preparedStatement.Query)
	if err != nil {
		return nil, nil, err
	}
	return messages, preparedStatement, nil
}

func (queryHandler *QueryHandler) HandleExecuteQuery(message *pgproto3.Execute, preparedStatement *PreparedStatement) ([]pgproto3.Message, error) {
	if message.Portal != preparedStatement.Portal {
		LogError(queryHandler.config, "Portal mismatch:", message.Portal, "instead of", preparedStatement.Portal)
		return nil, errors.New("portal mismatch")
	}

	if preparedStatement.Query == "" {
		return []pgproto3.Message{&pgproto3.EmptyQueryResponse{}}, nil
	}

	if preparedStatement.Rows == nil { // If Describe step didn't have Bind step before
		rows, err := preparedStatement.Statement.QueryContext(context.Background(), preparedStatement.Variables...)
		if err != nil {
			LogError(queryHandler.config, "Couldn't execute prepared statement via DuckDB:", preparedStatement.Query+"\n"+err.Error())
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
	PanicIfError(err)

	for _, schema := range schemas {
		_, err := queryHandler.duckdb.ExecContext(
			ctx,
			"CREATE SCHEMA IF NOT EXISTS \"$schema\"",
			map[string]string{"schema": schema},
		)
		PanicIfError(err)
	}
}

func (queryHandler *QueryHandler) rowsToDescriptionMessages(rows *sql.Rows, query string) ([]pgproto3.Message, error) {
	cols, err := rows.ColumnTypes()
	if err != nil {
		LogError(queryHandler.config, "Couldn't get column types", query+"\n"+err.Error())
		return nil, err
	}

	var messages []pgproto3.Message

	rowDescription := queryHandler.generateRowDescription(cols)
	if rowDescription != nil {
		messages = append(messages, rowDescription)
	}

	return messages, nil
}

func (queryHandler *QueryHandler) rowsToDataMessages(rows *sql.Rows, originalQueryStatement string) ([]pgproto3.Message, error) {
	cols, err := rows.ColumnTypes()
	if err != nil {
		LogError(queryHandler.config, "Couldn't get column types", originalQueryStatement+"\n"+err.Error())
		return nil, err
	}

	var messages []pgproto3.Message
	for rows.Next() {
		dataRow, err := queryHandler.generateDataRow(rows, cols)
		if err != nil {
			LogError(queryHandler.config, "Couldn't get data row", originalQueryStatement+"\n"+err.Error())
			return nil, err
		}
		messages = append(messages, dataRow)
	}

	commandTag := FALLBACK_SQL_QUERY
	switch {
	case strings.HasPrefix(originalQueryStatement, "SET "):
		commandTag = "SET"
	case strings.HasPrefix(originalQueryStatement, "SHOW "):
		commandTag = "SHOW"
	case strings.HasPrefix(originalQueryStatement, "DISCARD ALL"):
		commandTag = "DISCARD ALL"
	}

	messages = append(messages, &pgproto3.CommandComplete{CommandTag: []byte(commandTag)})
	return messages, nil
}

func (queryHandler *QueryHandler) parseAndRemapQuery(query string) ([]string, []string, error) {
	queryTree, err := pgQuery.Parse(query)
	if err != nil {
		LogError(queryHandler.config, "Error parsing query:", query+"\n"+err.Error())
		return nil, nil, err
	}

	if strings.HasSuffix(query, INSPECT_SQL_COMMENT) {
		LogDebug(queryHandler.config, queryTree.Stmts)
	}

	var originalQueryStatements []string
	for _, stmt := range queryTree.Stmts {
		originalQueryStatement, err := pgQuery.Deparse(&pgQuery.ParseResult{Stmts: []*pgQuery.RawStmt{stmt}})
		if err != nil {
			return nil, nil, err
		}
		originalQueryStatements = append(originalQueryStatements, originalQueryStatement)
	}

	remappedStatements, err := queryHandler.queryRemapper.RemapStatements(queryTree.Stmts)
	if err != nil {
		return nil, nil, err
	}

	var queryStatements []string
	for _, remappedStatement := range remappedStatements {
		queryStatement, err := pgQuery.Deparse(&pgQuery.ParseResult{Stmts: []*pgQuery.RawStmt{remappedStatement}})
		if err != nil {
			return nil, nil, err
		}
		queryStatements = append(queryStatements, queryStatement)
	}

	return queryStatements, originalQueryStatements, nil
}

func (queryHandler *QueryHandler) generateRowDescription(cols []*sql.ColumnType) *pgproto3.RowDescription {
	description := pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{}}

	for _, col := range cols {
		typeIod := queryHandler.columnTypeOid(col)

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

// https://pkg.go.dev/github.com/jackc/pgx/v5/pgtype#pkg-constants
func (queryHandler *QueryHandler) columnTypeOid(col *sql.ColumnType) uint32 {
	switch col.DatabaseTypeName() {
	case "BOOLEAN":
		return pgtype.BoolOID
	case "BOOLEAN[]":
		return pgtype.BoolArrayOID
	case "SMALLINT":
		return pgtype.Int2OID
	case "SMALLINT[]":
		return pgtype.Int2ArrayOID
	case "INTEGER":
		return pgtype.Int4OID
	case "INTEGER[]":
		return pgtype.Int4ArrayOID
	case "UINTEGER":
		return pgtype.XIDOID
	case "UINTEGER[]":
		return pgtype.XIDArrayOID
	case "BIGINT":
		if isSystemTableOidColumn(col.Name()) {
			return pgtype.OIDOID
		}
		return pgtype.Int8OID
	case "BIGINT[]":
		return pgtype.Int8ArrayOID
	case "UBIGINT":
		return pgtype.XID8OID
	case "UBIGINT[]":
		return pgtype.XID8ArrayOID
	case "HUGEINT":
		return pgtype.NumericOID
	case "HUGEINT[]":
		return pgtype.NumericArrayOID
	case "FLOAT":
		return pgtype.Float4OID
	case "FLOAT[]":
		return pgtype.Float4ArrayOID
	case "DOUBLE":
		return pgtype.Float8OID
	case "DOUBLE[]":
		return pgtype.Float8ArrayOID
	case "VARCHAR":
		return pgtype.TextOID
	case "VARCHAR[]":
		return pgtype.TextArrayOID
	case "TIME":
		return pgtype.TimeOID
	case "TIME[]":
		return pgtype.TimeArrayOID
	case "DATE":
		return pgtype.DateOID
	case "DATE[]":
		return pgtype.DateArrayOID
	case "TIMESTAMP":
		return pgtype.TimestampOID
	case "TIMESTAMP[]":
		return pgtype.TimestampArrayOID
	case "BLOB":
		return pgtype.UUIDOID
	case "BLOB[]":
		return pgtype.UUIDArrayOID
	default:
		if strings.HasPrefix(col.DatabaseTypeName(), "DECIMAL") {
			if strings.HasSuffix(col.DatabaseTypeName(), "[]") {
				return pgtype.NumericArrayOID
			} else {
				return pgtype.NumericOID
			}
		}

		panic("Unsupported column type: " + col.DatabaseTypeName())
	}
}

func isSystemTableOidColumn(colName string) bool {
	oidColumns := map[string]bool{
		"oid":          true,
		"tableoid":     true,
		"relnamespace": true,
		"relowner":     true,
		"relfilenode":  true,
		"did":          true,
		"objoid":       true,
		"classoid":     true,
	}

	return oidColumns[colName]
}

func (queryHandler *QueryHandler) generateDataRow(rows *sql.Rows, cols []*sql.ColumnType) (*pgproto3.DataRow, error) {
	valuePtrs := make([]interface{}, len(cols))
	for i, col := range cols {
		switch col.ScanType().String() {
		case "int16":
			var value sql.NullInt16
			valuePtrs[i] = &value
		case "int32":
			var value sql.NullInt32
			valuePtrs[i] = &value
		case "int64":
			var value sql.NullInt64
			valuePtrs[i] = &value
		case "uint32": // xid
			var value NullUint32
			valuePtrs[i] = &value
		case "uint64": // xid8
			var value NullUint64
			valuePtrs[i] = &value
		case "float64", "float32":
			var value sql.NullFloat64
			valuePtrs[i] = &value
		case "string", "[]uint8": // []uint8 is for uuid
			var value sql.NullString
			valuePtrs[i] = &value
		case "bool":
			var value sql.NullBool
			valuePtrs[i] = &value
		case "time.Time":
			var value sql.NullTime
			valuePtrs[i] = &value
		case "*big.Int":
			var value NullBigInt
			valuePtrs[i] = &value
		case "duckdb.Decimal":
			var value NullDecimal
			valuePtrs[i] = &value
		case "[]interface {}":
			var value NullArray
			valuePtrs[i] = &value
		default:
			panic("Unsupported queried type: " + col.ScanType().String())
		}
	}

	err := rows.Scan(valuePtrs...)
	if err != nil {
		return nil, err
	}

	var values [][]byte
	for i, valuePtr := range valuePtrs {
		switch value := valuePtr.(type) {
		case *sql.NullInt16:
			if value.Valid {
				values = append(values, []byte(strconv.Itoa(int(value.Int16))))
			} else {
				values = append(values, nil)
			}
		case *sql.NullInt32:
			if value.Valid {
				values = append(values, []byte(strconv.Itoa(int(value.Int32))))
			} else {
				values = append(values, nil)
			}
		case *sql.NullInt64:
			if value.Valid {
				values = append(values, []byte(strconv.Itoa(int(value.Int64))))
			} else {
				values = append(values, nil)
			}
		case *NullUint32:
			if value.Present {
				values = append(values, []byte(value.String()))
			} else {
				values = append(values, nil)
			}
		case *NullUint64:
			if value.Present {
				values = append(values, []byte(value.String()))
			} else {
				values = append(values, nil)
			}
		case *sql.NullFloat64:
			if value.Valid {
				values = append(values, []byte(fmt.Sprintf("%v", value.Float64)))
			} else {
				values = append(values, nil)
			}
		case *sql.NullString:
			if value.Valid {
				values = append(values, []byte(value.String))
			} else {
				values = append(values, nil)
			}
		case *sql.NullBool:
			if value.Valid {
				values = append(values, []byte(fmt.Sprintf("%v", value.Bool)))
			} else {
				values = append(values, nil)
			}
		case *sql.NullTime:
			if value.Valid {
				switch cols[i].DatabaseTypeName() {
				case "DATE":
					values = append(values, []byte(value.Time.Format("2006-01-02")))
				case "TIME":
					values = append(values, []byte(value.Time.Format("15:04:05.999999")))
				case "TIMESTAMP":
					values = append(values, []byte(value.Time.Format("2006-01-02 15:04:05.999999")))
				default:
					panic("Unsupported type: " + cols[i].DatabaseTypeName())
				}
			} else {
				values = append(values, nil)
			}
		case *NullBigInt:
			if value.Present {
				values = append(values, []byte(value.String()))
			} else {
				values = append(values, nil)
			}
		case *NullDecimal:
			if value.Present {
				values = append(values, []byte(value.String()))
			} else {
				values = append(values, nil)
			}
		case *NullArray:
			if value.Present {
				values = append(values, []byte(value.String()))
			} else {
				values = append(values, nil)
			}
		case *string:
			values = append(values, []byte(*value))
		default:
			panic("Unsupported type: " + cols[i].ScanType().Name())
		}
	}
	dataRow := pgproto3.DataRow{Values: values}

	return &dataRow, nil
}
