package main

import (
	"errors"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/s3"
	_ "github.com/mattn/go-sqlite3"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Config struct {
	// Bind address for serving metrics.
	Addr string `yaml:"addr"`

	// List of databases to manage.
	DBs []*DBConfig `yaml:"dbs"`

	// Subcommand to execute during replication.
	// Litestream will shutdown when subcommand exits.
	Exec string `yaml:"exec"`

	// Global S3 settings
	AccessKeyID     string `yaml:"access-key-id"`
	SecretAccessKey string `yaml:"secret-access-key"`
}

type ReplicaConfig struct {
	Type                   string         `yaml:"type"` // "file", "s3"
	Name                   string         `yaml:"name"` // name of replica, optional.
	Path                   string         `yaml:"path"`
	URL                    string         `yaml:"url"`
	Retention              *time.Duration `yaml:"retention"`
	RetentionCheckInterval *time.Duration `yaml:"retention-check-interval"`
	SyncInterval           *time.Duration `yaml:"sync-interval"`
	SnapshotInterval       *time.Duration `yaml:"snapshot-interval"`
	ValidationInterval     *time.Duration `yaml:"validation-interval"`

	// S3 settings
	AccessKeyID     string `yaml:"access-key-id"`
	SecretAccessKey string `yaml:"secret-access-key"`
	Region          string `yaml:"region"`
	Bucket          string `yaml:"bucket"`
	Endpoint        string `yaml:"endpoint"`
	ForcePathStyle  *bool  `yaml:"force-path-style"`
	SkipVerify      bool   `yaml:"skip-verify"`

	// ABS settings
	AccountName string `yaml:"account-name"`
	AccountKey  string `yaml:"account-key"`

	// SFTP settings
	Host     string `yaml:"host"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	KeyPath  string `yaml:"key-path"`
}

func (c *ReplicaConfig) ReplicaType() string {
	scheme, _, _, _ := ParseReplicaURL(c.URL)
	if scheme != "" {
		return scheme
	} else if c.Type != "" {
		return c.Type
	}
	return "file"
}

type DBConfig struct {
	Path                 string         `yaml:"path"`
	MonitorInterval      *time.Duration `yaml:"monitor-interval"`
	MonitorDelayInterval *time.Duration `yaml:"monitor-delay-interval"`
	CheckpointInterval   *time.Duration `yaml:"checkpoint-interval"`
	MinCheckpointPageN   *int           `yaml:"min-checkpoint-page-count"`
	MaxCheckpointPageN   *int           `yaml:"max-checkpoint-page-count"`
	ShadowRetentionN     *int           `yaml:"shadow-retention-count"`

	Replicas []*ReplicaConfig `yaml:"replicas"`
}

var execCh chan error

func StartLitestream(config LitestreamConfig) error {
	log.Print("Starting main...")

	_, err := os.Stat(config.DBLocation)
	if err != nil {
		return err
	}

	repConf := ReplicaConfig{
		URL:             config.S3URL,
		AccessKeyID:     config.AccessKeyID,
		SecretAccessKey: config.SecretAccessKey,
	}

	dbConf := DBConfig{
		Path:     config.DBLocation,
		Replicas: []*ReplicaConfig{&repConf},
	}

	conf := Config{
		Addr: config.URL,
		DBs:  []*DBConfig{&dbConf},
	}
	if len(conf.DBs) == 0 {
		return errors.New("no databases specified in configuration")
	}

	DBs := []*litestream.DB{}
	for _, dbConfig := range conf.DBs {
		db, err := NewDBFromConfig(dbConfig)
		if err != nil {
			return err
		}

		// Open database & attach to program.
		if err := db.Open(); err != nil {
			return err
		}
		DBs = append(DBs, db)
	}

	for _, db := range DBs {
		log.Printf("initialized db: %s", db.Path())
		for _, r := range db.Replicas {
			switch client := r.Client.(type) {
			case *s3.ReplicaClient:
				log.Printf("replicating to: name=%q type=%q bucket=%q path=%q region=%q endpoint=%q sync-interval=%s", r.Name(), client.Type(), client.Bucket, client.Path, client.Region, client.Endpoint, r.SyncInterval)
			default:
				log.Printf("replicating to: name=%q type=%q", r.Name(), client.Type())
			}
		}
	}

	if conf.Addr != "" {
		hostport := conf.Addr
		if host, port, _ := net.SplitHostPort(conf.Addr); port == "" {
			log.Fatalf("must specify port for bind address: %q", conf.Addr)
		} else if host == "" {
			hostport = net.JoinHostPort("localhost", port)
		}

		log.Printf("serving metrics on http://%s/metrics", hostport)
		go func() {
			http.Handle("/metrics", promhttp.Handler())
			if err := http.ListenAndServe(conf.Addr, nil); err != nil {
				log.Printf("cannot start metrics server: %s", err)
			}
		}()
	}

	log.Printf("litestream initialization complete")

	return nil
}
