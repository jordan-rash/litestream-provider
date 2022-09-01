package main

import (
	"context"
	"errors"
	"os"
	"strings"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/sirupsen/logrus"

	provider "github.com/jordan-rash/wasmcloud-provider"
	"github.com/jordan-rash/wasmcloud-provider/sqldb"
	wcsql "github.com/wasmcloud/interfaces/sqldb/tinygo"
)

var (
	serverCancels map[string]context.CancelFunc
	linkDefs      map[string]provider.LinkDefinition
	hostData      provider.HostData
)

var log = logrus.New()

func init() {
	os.Setenv("RUST_LOG", "debug")

	file, err := os.OpenFile("/tmp/litestream_wc.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err == nil {
		log.Out = file
	} else {
		log.Info("Failed to log to file, using default stderr")
	}

	log.SetFormatter(&logrus.JSONFormatter{})
	log.SetReportCaller(true)
}

type LitestreamConfig struct {
	URL             string
	AccessKeyID     string
	SecretAccessKey string
	DBLocation      string
	S3URL           string
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wasmcloudProvider, err := provider.Init(ctx)
	if err != nil {
		log.Error(err)
		cancel()
	}

	// Listen for Shutdown request
	go func() {
		<-wasmcloudProvider.Shutdown
		close(wasmcloudProvider.ProviderAction)
		cancel()
	}()

	// Wait for a valid link definiation
	log.Println("Ready for link definitions")
	actorData := <-wasmcloudProvider.Links

	// Configure and start Litestream
	lsConfig := validateConfig(actorData.ActorConfig)
	err = StartLitestream(lsConfig)
	if err != nil {
		log.Error(err)
		cancel()
	}

	// Start listening on topic for requests from actor
	wasmcloudProvider.ListenForActor(actorData.ActorID)

	//Wait for valid requests
	for actorRequest := range wasmcloudProvider.ProviderAction {
		go evaluateRequest(actorRequest, lsConfig)
	}
}

// Execute, Query, Ping
func evaluateRequest(actorRequest provider.ProviderAction, actorConfig LitestreamConfig) error {
	resp := provider.ProviderResponse{}
	switch actorRequest.Operation {
	case sqldb.EXECUTE_OPERATION.String():
		// Decode the request from actor
		state, err := sqldb.DecodeStatement(actorRequest.Msg)
		if err != nil {
			resp.Error = err.Error()
		}

		// Do the Execute
		er, err := SQLExecute(state.Sql, state.Database, actorConfig.DBLocation)
		if err != nil {
			resp.Error = err.Error()
		}

		// Encode response to actor
		buf := sqldb.EncodeExecuteResponse(er)

		resp.Msg = buf

		// Send response
		actorRequest.Respond <- resp
	case sqldb.QUERY_OPERATION.String():
		// Decode the request from actor
		state, err := sqldb.DecodeStatement(actorRequest.Msg)
		if err != nil {
			resp.Error = err.Error()
		}

		// Do the Query
		qr, err := SQLQuery(state.Sql, state.Database, actorConfig.DBLocation)
		if err != nil {
			resp.Error = err.Error()
		}

		// Encode response to actor
		buf := sqldb.EncodeQueryResponse(qr)
		resp.Msg = buf

		actorRequest.Respond <- resp
	case sqldb.PING_OPERATION.String():
		resp.Error = "PING_OPERATION"
	default:
		resp.Error = "Invalid SQLdb Operation"
		actorRequest.Respond <- resp
	}
	return nil
}

func SQLExecute(query, database, dbpath string) (*wcsql.ExecuteResult, error) {
	db, err := sqlx.Open("sqlite3", dbpath)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	defer db.Close()

	tx := db.MustBegin()
	res := tx.MustExec(strings.TrimSpace(query))
	tx.Commit()
	re, err := res.RowsAffected()
	if err != nil {
		log.Error(err)
		return nil, err
	}
	ret := wcsql.ExecuteResult{
		RowsAffected: uint64(re),
	}
	return &ret, nil
}

func SQLQuery(query, database, dbpath string) (*wcsql.QueryResult, error) {
	db, err := sqlx.Open("sqlite3", dbpath)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	defer db.Close()

	rows, err := db.Queryx(strings.TrimSpace(query))
	if err != nil {
		log.Error(err)
		return nil, err
	}
	defer rows.Close()

	if rows.Err() != nil {
		log.Error(err)
		return nil, err
	}

	cols := wcsql.Columns{}
	ct, err := rows.ColumnTypes()
	if err != nil {
		log.Error(err)
		return nil, err
	}

	for _, c := range ct {
		tC := wcsql.Column{}
		tC.DbType = strings.ToLower(c.DatabaseTypeName())
		tC.Name = c.Name()
		cols = append(cols, tC)
	}

	var rowCount uint64 = 0
	var allRows [][]interface{}

	for rows.Next() {
		val, err := rows.SliceScan()
		if err != nil {
			log.Error(err)
			return nil, err
		}
		allRows = append(allRows, val)
		rowCount++
	}

	rowsRaw, err := sqldb.EncodeRows(allRows, ct)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	ret := wcsql.QueryResult{
		Columns: cols,
		Rows:    rowsRaw,
		NumRows: rowCount,
	}

	log.Print(ret)
	return &ret, nil
}

func validateConfig(config map[string]string) LitestreamConfig {
	if config["URL"] == "" ||
		config["AccessKeyID"] == "" ||
		config["SecretAccessKey"] == "" ||
		config["S3URL"] == "" ||
		config["DBLocation"] == "" {
		log.
			WithField("URL", config["URL"]).
			WithField("AccessKeyID", config["AccessKeyID"]).
			WithField("SecretAccessKey", config["SecretAccessKey"]).
			WithField("DBLocation", config["DBLocation"]).
			WithField("S3URL", config["S3URL"]).
			WithError(errors.New("Invalid configuration provided")).
			Fatal()
	}

	return LitestreamConfig{
		URL:             config["URL"],
		AccessKeyID:     config["AccessKeyID"],
		SecretAccessKey: config["SecretAccessKey"],
		DBLocation:      config["DBLocation"],
		S3URL:           config["S3URL"],
	}
}
