package main

import (
	"fmt"
	"net/url"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/s3"
)

func expand(s string) (string, error) {
	// Just expand to absolute path if there is no home directory prefix.
	prefix := "~" + string(os.PathSeparator)
	if s != "~" && !strings.HasPrefix(s, prefix) {
		return filepath.Abs(s)
	}

	// Look up home directory.
	u, err := user.Current()
	if err != nil {
		return "", err
	} else if u.HomeDir == "" {
		return "", fmt.Errorf("cannot expand path %s, no home directory available", s)
	}

	// Return path with tilde replaced by the home directory.
	if s == "~" {
		return u.HomeDir, nil
	}
	return filepath.Join(u.HomeDir, strings.TrimPrefix(s, prefix)), nil
}
func NewDBFromConfig(dbc *DBConfig) (*litestream.DB, error) {
	path, err := expand(dbc.Path)
	if err != nil {
		return nil, err
	}

	// Initialize database with given path.
	db := litestream.NewDB(path)

	// Override default database settings if specified in configuration.
	if dbc.MonitorInterval != nil {
		db.MonitorInterval = *dbc.MonitorInterval
	}
	if dbc.CheckpointInterval != nil {
		db.CheckpointInterval = *dbc.CheckpointInterval
	}
	if dbc.MinCheckpointPageN != nil {
		db.MinCheckpointPageN = *dbc.MinCheckpointPageN
	}
	if dbc.MaxCheckpointPageN != nil {
		db.MaxCheckpointPageN = *dbc.MaxCheckpointPageN
	}

	// Instantiate and attach replicas.
	for _, rc := range dbc.Replicas {
		r, err := NewReplicaFromConfig(rc, db)
		if err != nil {
			return nil, err
		}
		db.Replicas = append(db.Replicas, r)
	}

	return db, nil
}

//func NewDBFromConfigWithPath(dbc *DBConfig, path string) (*litestream.DB, error) {
//// Initialize database with given path.
//db := litestream.NewDB(path)

//// Override default database settings if specified in configuration.
//if dbc.MonitorDelayInterval != nil {
//db.MonitorDelayInterval = *dbc.MonitorDelayInterval
//}
//if dbc.CheckpointInterval != nil {
//db.CheckpointInterval = *dbc.CheckpointInterval
//}
//if dbc.MinCheckpointPageN != nil {
//db.MinCheckpointPageN = *dbc.MinCheckpointPageN
//}
//if dbc.MaxCheckpointPageN != nil {
//db.MaxCheckpointPageN = *dbc.MaxCheckpointPageN
//}
//if dbc.ShadowRetentionN != nil {
//db.ShadowRetentionN = *dbc.ShadowRetentionN
//}

//// Instantiate and attach replicas.
//for _, rc := range dbc.Replicas {
//r, err := NewReplicaFromConfig(rc, db)
//if err != nil {
//return nil, err
//}
//db.Replicas = append(db.Replicas, r)
//}

//return db, nil
//}

func isURL(s string) bool {
	return regexp.MustCompile(`^\w+:\/\/`).MatchString(s)
}
func NewReplicaFromConfig(c *ReplicaConfig, db *litestream.DB) (_ *litestream.Replica, err error) {
	// Ensure user did not specify URL in path.
	if isURL(c.Path) {
		return nil, fmt.Errorf("replica path cannot be a url, please use the 'url' field instead: %s", c.Path)
	}

	// Build replica.
	r := litestream.NewReplica(db, c.Name)
	if v := c.Retention; v != nil {
		r.Retention = *v
	}
	if v := c.RetentionCheckInterval; v != nil {
		r.RetentionCheckInterval = *v
	}
	if v := c.SyncInterval; v != nil {
		r.SyncInterval = *v
	}
	if v := c.SnapshotInterval; v != nil {
		r.SnapshotInterval = *v
	}
	if v := c.ValidationInterval; v != nil {
		r.ValidationInterval = *v
	}

	// Build and set client on replica.
	switch c.ReplicaType() {
	case "s3":
		if r.Client, err = newS3ReplicaClientFromConfig(c, r); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown replica type in config: %q", c.Type)
	}

	return r, nil
}

func newS3ReplicaClientFromConfig(c *ReplicaConfig, r *litestream.Replica) (_ *s3.ReplicaClient, err error) {
	// Ensure URL & constituent parts are not both specified.
	if c.URL != "" && c.Path != "" {
		return nil, fmt.Errorf("cannot specify url & path for s3 replica")
	} else if c.URL != "" && c.Bucket != "" {
		return nil, fmt.Errorf("cannot specify url & bucket for s3 replica")
	}

	bucket, path := c.Bucket, c.Path
	region, endpoint, skipVerify := c.Region, c.Endpoint, c.SkipVerify

	// Use path style if an endpoint is explicitly set. This works because the
	// only service to not use path style is AWS which does not use an endpoint.
	forcePathStyle := (endpoint != "")
	if v := c.ForcePathStyle; v != nil {
		forcePathStyle = *v
	}

	// Apply settings from URL, if specified.
	if c.URL != "" {
		_, host, upath, err := ParseReplicaURL(c.URL)
		if err != nil {
			return nil, err
		}
		ubucket, uregion, uendpoint, uforcePathStyle := s3.ParseHost(host)

		// Only apply URL parts to field that have not been overridden.
		if path == "" {
			path = upath
		}
		if bucket == "" {
			bucket = ubucket
		}
		if region == "" {
			region = uregion
		}
		if endpoint == "" {
			endpoint = uendpoint
		}
		if !forcePathStyle {
			forcePathStyle = uforcePathStyle
		}
	}

	// Ensure required settings are set.
	if bucket == "" {
		return nil, fmt.Errorf("bucket required for s3 replica")
	}

	// Build replica.
	client := s3.NewReplicaClient()
	client.AccessKeyID = c.AccessKeyID
	client.SecretAccessKey = c.SecretAccessKey
	client.Bucket = bucket
	client.Path = path
	client.Region = region
	client.Endpoint = endpoint
	client.ForcePathStyle = forcePathStyle
	client.SkipVerify = skipVerify
	return client, nil
}

func ParseReplicaURL(s string) (scheme, host, urlpath string, err error) {
	u, err := url.Parse(s)
	if err != nil {
		return "", "", "", err
	}

	switch u.Scheme {
	case "file":
		scheme, u.Scheme = u.Scheme, ""
		return scheme, "", path.Clean(u.String()), nil

	case "":
		return u.Scheme, u.Host, u.Path, fmt.Errorf("replica url scheme required: %s", s)

	default:
		return u.Scheme, u.Host, strings.TrimPrefix(path.Clean(u.Path), "/"), nil
	}
}
