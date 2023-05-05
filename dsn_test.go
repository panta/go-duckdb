package duckdb

import (
	"reflect"
	"testing"
)

func TestParseDSN(t *testing.T) {
	tests := []struct {
		name    string
		dsn     string
		want    *ParsedDSN
		wantErr bool
	}{
		{"simple", "file.db", &ParsedDSN{DSN: "file.db", Path: "file.db"}, false},
		{
			"simple-w-params",
			"file.db?access_mode=READ_ONLY",
			&ParsedDSN{
				DSN: "file.db?access_mode=READ_ONLY", Path: "file.db",
				Params: map[string][]string{
					"access_mode": []string{"READ_ONLY"},
				},
			},
			false,
		},
		{
			"simple-w-more-params",
			"file.db?access_mode=READ_ONLY&mode=memory",
			&ParsedDSN{
				DSN: "file.db?access_mode=READ_ONLY&mode=memory", Path: "file.db",
				Params: map[string][]string{
					"access_mode": []string{"READ_ONLY"},
					"mode":        []string{"memory"},
				},
			},
			false,
		},
		{
			"simple-win-w-params",
			"e:/aaa/file.db?access_mode=READ_ONLY&mode=memory",
			&ParsedDSN{
				DSN: "e:/aaa/file.db?access_mode=READ_ONLY&mode=memory", Path: "e:/aaa/file.db",
				Params: map[string][]string{
					"access_mode": []string{"READ_ONLY"},
					"mode":        []string{"memory"},
				},
			},
			false,
		},
		{
			"rel-path-w-params",
			"aaa/file.db?access_mode=READ_ONLY&mode=memory",
			&ParsedDSN{
				DSN: "aaa/file.db?access_mode=READ_ONLY&mode=memory", Path: "aaa/file.db",
				Params: map[string][]string{
					"access_mode": []string{"READ_ONLY"},
					"mode":        []string{"memory"},
				},
			},
			false,
		},
		{
			"abs-path-w-params",
			"/aaa/bbb/file.db?access_mode=READ_ONLY&mode=memory",
			&ParsedDSN{
				DSN: "/aaa/bbb/file.db?access_mode=READ_ONLY&mode=memory", Path: "/aaa/bbb/file.db",
				Params: map[string][]string{
					"access_mode": []string{"READ_ONLY"},
					"mode":        []string{"memory"},
				},
			},
			false,
		},
		{
			"file-no-slash-w-params",
			"file:test.db?cache=shared&mode=memory",
			&ParsedDSN{
				DSN: "file:test.db?cache=shared&mode=memory", Path: "test.db",
				Params: map[string][]string{
					"cache": []string{"shared"},
					"mode":  []string{"memory"},
				},
			},
			false,
		},
		{
			"file-double-slash-w-params",
			"file://test.db?cache=shared&mode=memory",
			&ParsedDSN{
				DSN: "file://test.db?cache=shared&mode=memory", Path: "test.db",
				Params: map[string][]string{
					"cache": []string{"shared"},
					"mode":  []string{"memory"},
				},
			},
			false,
		},
		{
			"file-abs-path-w-params",
			"file:///aaa/bbb/test.db?cache=shared&mode=memory",
			&ParsedDSN{
				DSN: "file:///aaa/bbb/test.db?cache=shared&mode=memory", Path: "/aaa/bbb/test.db",
				Params: map[string][]string{
					"cache": []string{"shared"},
					"mode":  []string{"memory"},
				},
			},
			false,
		},
		{
			"file-win-path-w-params",
			"file:///e:/aaa/test.db?cache=shared&mode=memory",
			&ParsedDSN{
				DSN: "file:///e:/aaa/test.db?cache=shared&mode=memory", Path: "e:/aaa/test.db",
				Params: map[string][]string{
					"cache": []string{"shared"},
					"mode":  []string{"memory"},
				},
			},
			false,
		},
		{
			"file-win-drive-path-two-slashes-w-params",
			"file://e:/aaa/test.db?cache=shared&mode=memory",
			&ParsedDSN{
				DSN: "file://e:/aaa/test.db?cache=shared&mode=memory", Path: "e:/aaa/test.db",
				Params: map[string][]string{
					"cache": []string{"shared"},
					"mode":  []string{"memory"},
				},
			},
			false,
		},
		{
			"memory",
			":memory:",
			&ParsedDSN{
				DSN: ":memory:", Path: ":memory:",
			},
			false,
		},
		{
			"file-memory",
			"file::memory:",
			&ParsedDSN{
				DSN: "file::memory:", Path: ":memory:",
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDSN(tt.dsn)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDSN() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseDSN() got = %v, want %v", got, tt.want)
			}
		})
	}
}
