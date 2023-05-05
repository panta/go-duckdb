package duckdb

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

var uncPathRe = regexp.MustCompile(`^/([a-zA-Z]:/.+)$`)

type ParsedDSN struct {
	DSN    string
	Path   string
	Params url.Values
}

func ParseDSN(dsn string) (*ParsedDSN, error) {
	initialDSN := dsn

	var params url.Values
	var dbPath string

	pos := strings.IndexRune(dsn, '?')
	if pos >= 1 {
		var err error
		params, err = url.ParseQuery(dsn[pos+1:])
		if err != nil {
			return nil, fmt.Errorf("bad DSN params: %w", err)
		}

		if !strings.HasPrefix(dsn, "file:") {
			dsn = dsn[:pos]
		}
	} else {
		dbPath = dsn
	}

	if strings.HasPrefix(dsn, "file:") {
		parsedDSN, err := url.Parse(dsn)
		if err != nil {
			return nil, fmt.Errorf("bad DSN: %w", err)
		}

		if parsedDSN.Path != "" && parsedDSN.Host != "" {
			if strings.HasPrefix(parsedDSN.Path, "/") {
				dbPath = parsedDSN.Host + parsedDSN.Path
			} else {
				dbPath = parsedDSN.Host + "/" + parsedDSN.Path
			}
		} else if parsedDSN.Path != "" {
			dbPath = parsedDSN.Path
		} else if parsedDSN.Opaque != "" {
			dbPath = parsedDSN.Opaque
		} else if parsedDSN.Host != "" {
			dbPath = parsedDSN.Host
		}

		m := uncPathRe.FindStringSubmatch(dbPath)
		if len(m) >= 1 {
			dbPath = m[1]
		}
	} else {
		dbPath = dsn
	}

	return &ParsedDSN{
		DSN:    initialDSN,
		Path:   dbPath,
		Params: params,
	}, nil
}
