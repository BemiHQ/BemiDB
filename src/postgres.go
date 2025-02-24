package main

import (
	"errors"
	"net"

	"github.com/jackc/pgx/v5/pgproto3"
)

const (
	PG_VERSION        = "17.0"
	PG_ENCODING       = "UTF8"
	PG_TX_STATUS_IDLE = 'I'

	SYSTEM_AUTH_USER = "bemidb"
)

type Postgres struct {
	backend *pgproto3.Backend
	conn    *net.Conn
	config  *Config
}

func NewPostgres(config *Config, conn *net.Conn) *Postgres {
	return &Postgres{
		conn:    conn,
		backend: pgproto3.NewBackend(*conn, *conn),
		config:  config,
	}
}

func NewTcpListener(config *Config) net.Listener {
	parsedIp := net.ParseIP(config.Host)
	if parsedIp == nil {
		panic("Invalid host: " + config.Host)
	}

	var network, host string
	if parsedIp.To4() == nil {
		network = "tcp6"
		host = "[" + config.Host + "]"
	} else {
		network = "tcp4"
		host = config.Host
	}

	tcpListener, err := net.Listen(network, host+":"+config.Port)
	PanicIfError(err)
	return tcpListener
}

func AcceptConnection(listener net.Listener) net.Conn {
	conn, err := listener.Accept()
	PanicIfError(err)
	return conn
}

func (postgres *Postgres) Run(queryHandler *QueryHandler) {
	err := postgres.handleStartup()
	if err != nil {
		LogError(postgres.config, "Error handling startup:", err)
		return // Terminate connection
	}

	for {
		message, err := postgres.backend.Receive()
		if err != nil {
			return // Terminate connection
		}

		switch message := message.(type) {
		case *pgproto3.Query:
			postgres.handleSimpleQuery(queryHandler, message)
		case *pgproto3.Parse:
			err = postgres.handleExtendedQuery(queryHandler, message)
			if err != nil {
				return // Terminate connection
			}
		case *pgproto3.Terminate:
			LogDebug(postgres.config, "Client terminated connection")
			return
		default:
			LogError(postgres.config, "Received message other than Query from client:", message)
			return // Terminate connection
		}
	}
}

func (postgres *Postgres) Close() error {
	return (*postgres.conn).Close()
}

func (postgres *Postgres) handleSimpleQuery(queryHandler *QueryHandler, queryMessage *pgproto3.Query) {
	LogDebug(postgres.config, "Received query:", queryMessage.String)
	messages, err := queryHandler.HandleSimpleQuery(queryMessage.String)
	if err != nil {
		postgres.writeError(err)
		return
	}
	messages = append(messages, &pgproto3.ReadyForQuery{TxStatus: PG_TX_STATUS_IDLE})
	postgres.writeMessages(messages...)
}

func (postgres *Postgres) handleExtendedQuery(queryHandler *QueryHandler, parseMessage *pgproto3.Parse) error {
	LogDebug(postgres.config, "Parsing query", parseMessage.Query)
	messages, preparedStatement, err := queryHandler.HandleParseQuery(parseMessage)
	if err != nil {
		postgres.writeError(err)
		return nil
	}
	postgres.writeMessages(messages...)

	var previousErr error
	for {
		message, err := postgres.backend.Receive()
		if err != nil {
			return err
		}

		switch message := message.(type) {
		case *pgproto3.Bind:
			if previousErr != nil { // Skip processing the next message if there was an error in the previous message
				continue
			}

			LogDebug(postgres.config, "Binding query", message.PreparedStatement)
			messages, preparedStatement, err = queryHandler.HandleBindQuery(message, preparedStatement)
			if err != nil {
				postgres.writeError(err)
				previousErr = err
			}
			postgres.writeMessages(messages...)
		case *pgproto3.Describe:
			if previousErr != nil { // Skip processing the next message if there was an error in the previous message
				continue
			}

			LogDebug(postgres.config, "Describing query", message.Name, "("+string(message.ObjectType)+")")
			var messages []pgproto3.Message
			messages, preparedStatement, err = queryHandler.HandleDescribeQuery(message, preparedStatement)
			if err != nil {
				postgres.writeError(err)
				previousErr = err
			}
			postgres.writeMessages(messages...)
		case *pgproto3.Execute:
			if previousErr != nil { // Skip processing the next message if there was an error in the previous message
				continue
			}

			LogDebug(postgres.config, "Executing query", message.Portal)
			messages, err := queryHandler.HandleExecuteQuery(message, preparedStatement)
			if err != nil {
				postgres.writeError(err)
				previousErr = err
			}
			postgres.writeMessages(messages...)
		case *pgproto3.Sync:
			LogDebug(postgres.config, "Syncing query")
			postgres.writeMessages(
				&pgproto3.ReadyForQuery{TxStatus: PG_TX_STATUS_IDLE},
			)

			// If there was an error or Parse->Bind->Sync (...) or Parse->Describe->Sync (e.g., Metabase)
			// it means that sync is the last message in the extended query protocol, we can exit handleExtendedQuery
			if previousErr != nil || preparedStatement.Bound || preparedStatement.Described {
				return nil
			}
			// Otherwise, wait for Bind/Describe/Execute/Sync.
			// For example, psycopg sends Parse->[extra Sync]->Bind->Describe->Execute->Sync
		}
	}
}

func (postgres *Postgres) writeMessages(messages ...pgproto3.Message) {
	var buf []byte
	var err error
	for _, message := range messages {
		buf, err = message.Encode(buf)
		PanicIfError(err, "Error encoding messages")
	}
	_, err = (*postgres.conn).Write(buf)
	PanicIfError(err, "Error writing messages")
}

func (postgres *Postgres) writeError(err error) {
	LogError(postgres.config, err.Error())

	postgres.writeMessages(
		&pgproto3.ErrorResponse{
			Severity: "ERROR",
			Message:  err.Error(),
		},
		&pgproto3.ReadyForQuery{TxStatus: PG_TX_STATUS_IDLE},
	)
}

func (postgres *Postgres) handleStartup() error {
	startupMessage, err := postgres.backend.ReceiveStartupMessage()
	if err != nil {
		return err
	}

	switch startupMessage := startupMessage.(type) {
	case *pgproto3.StartupMessage:
		params := startupMessage.Parameters
		LogDebug(postgres.config, "BemiDB: startup message", params)

		if params["database"] != postgres.config.Database {
			postgres.writeError(errors.New("database " + params["database"] + " does not exist"))
			return errors.New("database does not exist")
		}

		if postgres.config.User != "" && params["user"] != postgres.config.User && params["user"] != SYSTEM_AUTH_USER {
			postgres.writeError(errors.New("role \"" + params["user"] + "\" does not exist"))
			return errors.New("role does not exist")
		}

		postgres.writeMessages(
			&pgproto3.AuthenticationOk{},
			&pgproto3.ParameterStatus{Name: "client_encoding", Value: PG_ENCODING},
			&pgproto3.ParameterStatus{Name: "server_version", Value: PG_VERSION},
			&pgproto3.ReadyForQuery{TxStatus: PG_TX_STATUS_IDLE},
		)
		return nil
	case *pgproto3.SSLRequest:
		_, err = (*postgres.conn).Write([]byte("N"))
		if err != nil {
			return err
		}
		postgres.handleStartup()
		return nil
	default:
		return errors.New("unknown startup message")
	}
}
