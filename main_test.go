package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestQueryAggregate(t *testing.T) {
	svc := newTestService(t)

	insertTestAggregates(t, svc.db, []aggregatedEntry{
		{BucketStart: 0, BucketEnd: 60_000, SourceIP: "192.168.1.2", Host: "a.com", Process: "chrome", Outbound: "NodeA", Chains: `["NodeA"]`, Upload: 100, Download: 200, Count: 1},
		{BucketStart: 60_000, BucketEnd: 120_000, SourceIP: "192.168.1.2", Host: "b.com", Process: "chrome", Outbound: "NodeA", Chains: `["NodeA"]`, Upload: 50, Download: 20, Count: 1},
		{BucketStart: 60_000, BucketEnd: 120_000, SourceIP: "192.168.1.3", Host: "a.com", Process: "curl", Outbound: "DIRECT", Chains: `["DIRECT"]`, Upload: 10, Download: 30, Count: 1},
	})

	got, err := svc.queryAggregate("sourceIP", 1, 120_000)
	if err != nil {
		t.Fatalf("queryAggregate: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(got))
	}
	if got[0].Label != "192.168.1.2" || got[0].Upload != 150 || got[0].Download != 220 || got[0].Total != 370 {
		t.Fatalf("unexpected first row: %+v", got[0])
	}
}

func TestQueryTrendFillsEmptyBuckets(t *testing.T) {
	svc := newTestService(t)

	insertTestAggregates(t, svc.db, []aggregatedEntry{
		{BucketStart: 0, BucketEnd: 60_000, SourceIP: "192.168.1.2", Host: "a.com", Process: "chrome", Outbound: "NodeA", Chains: `["NodeA"]`, Upload: 100, Download: 200, Count: 1},
		{BucketStart: 120_000, BucketEnd: 180_000, SourceIP: "192.168.1.2", Host: "b.com", Process: "chrome", Outbound: "NodeA", Chains: `["NodeA"]`, Upload: 50, Download: 20, Count: 1},
	})

	got, err := svc.queryTrend(0, 180_000, 60_000)
	if err != nil {
		t.Fatalf("queryTrend: %v", err)
	}

	if len(got) != 4 {
		t.Fatalf("expected 4 buckets, got %d", len(got))
	}
	if got[1].Timestamp != 60_000 || got[1].Upload != 0 || got[1].Download != 0 {
		t.Fatalf("expected empty middle bucket, got %+v", got[1])
	}
	if got[2].Timestamp != 120_000 || got[2].Upload != 50 || got[2].Download != 20 {
		t.Fatalf("unexpected populated bucket: %+v", got[2])
	}
}

func TestOpenDatabaseAddsDetailColumns(t *testing.T) {
	svc := newTestService(t)

	rows, err := svc.db.Query(`PRAGMA table_info(traffic_logs)`)
	if err != nil {
		t.Fatalf("table_info: %v", err)
	}
	defer rows.Close()

	columns := map[string]bool{}
	for rows.Next() {
		var (
			cid        int
			name       string
			typeName   string
			notNull    int
			defaultV   any
			primaryKey int
		)
		if err := rows.Scan(&cid, &name, &typeName, &notNull, &defaultV, &primaryKey); err != nil {
			t.Fatalf("scan table_info: %v", err)
		}
		columns[name] = true
	}

	for _, name := range []string{"destination_ip", "chains"} {
		if !columns[name] {
			t.Fatalf("expected column %q to exist", name)
		}
	}
}

func TestLoadConfigDefaultsRetentionPolicy(t *testing.T) {
	for _, key := range []string{
		"TRAFFIC_MONITOR_LISTEN",
		"MIHOMO_URL",
		"MIHOMO_SECRET",
		"TRAFFIC_MONITOR_DB",
		"TRAFFIC_MONITOR_POLL_INTERVAL_MS",
		"TRAFFIC_MONITOR_RETENTION_DAYS",
		"TRAFFIC_MONITOR_AGG_RETENTION_DAYS",
		"TRAFFIC_MONITOR_ALLOWED_ORIGIN",
	} {
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("unsetenv %s: %v", key, err)
		}
	}

	if err := os.Setenv("CLASH_API", "http://10.0.0.2:9090"); err != nil {
		t.Fatalf("setenv CLASH_API: %v", err)
	}
	if err := os.Setenv("CLASH_SECRET", "legacy-secret"); err != nil {
		t.Fatalf("setenv CLASH_SECRET: %v", err)
	}
	if err := os.Setenv("TRAFFIC_MONITOR_DB", "/tmp/override.db"); err != nil {
		t.Fatalf("setenv TRAFFIC_MONITOR_DB: %v", err)
	}
	if err := os.Setenv("TRAFFIC_MONITOR_RETENTION_DAYS", "90"); err != nil {
		t.Fatalf("setenv TRAFFIC_MONITOR_RETENTION_DAYS: %v", err)
	}
	if err := os.Setenv("TRAFFIC_MONITOR_AGG_RETENTION_DAYS", "120"); err != nil {
		t.Fatalf("setenv TRAFFIC_MONITOR_AGG_RETENTION_DAYS: %v", err)
	}

	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}

	if cfg.ListenAddr != ":8080" {
		t.Fatalf("expected listen addr default to be :8080, got %q", cfg.ListenAddr)
	}
	if cfg.MihomoURL != "" {
		t.Fatalf("expected mihomo url default to be empty, got %q", cfg.MihomoURL)
	}
	if cfg.MihomoSecret != "" {
		t.Fatalf("expected mihomo secret default to be empty, got %q", cfg.MihomoSecret)
	}
	original := isContainerRuntime
	isContainerRuntime = func() bool { return false }
	t.Cleanup(func() {
		isContainerRuntime = original
	})
	if defaultDatabasePath() != "./data/traffic_monitor.db" {
		t.Fatalf("expected local default database path to be ./data/traffic_monitor.db, got %q", defaultDatabasePath())
	}
}

func TestResolveMihomoSettingsUsesEnvironmentAndPersists(t *testing.T) {
	db, err := openDatabase(filepath.Join(t.TempDir(), "traffic.db"))
	if err != nil {
		t.Fatalf("openDatabase: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	if err := saveMihomoSettings(db, mihomoSettings{
		URL:    "http://db.example:9090",
		Secret: "db-secret",
	}); err != nil {
		t.Fatalf("saveMihomoSettings: %v", err)
	}

	got, err := resolveMihomoSettings(db, "http://env.example:9090/", "env-secret")
	if err != nil {
		t.Fatalf("resolveMihomoSettings: %v", err)
	}

	if got.URL != "http://env.example:9090" {
		t.Fatalf("expected env url to win, got %q", got.URL)
	}
	if got.Secret != "env-secret" {
		t.Fatalf("expected env secret to win, got %q", got.Secret)
	}

	stored, err := loadMihomoSettings(db)
	if err != nil {
		t.Fatalf("loadMihomoSettings: %v", err)
	}
	if stored != got {
		t.Fatalf("expected stored settings to match resolved settings, got %+v want %+v", stored, got)
	}
}

func TestResolveMihomoSettingsFallsBackToStoredValues(t *testing.T) {
	db, err := openDatabase(filepath.Join(t.TempDir(), "traffic.db"))
	if err != nil {
		t.Fatalf("openDatabase: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	want := mihomoSettings{
		URL:    "http://stored.example:9090",
		Secret: "stored-secret",
	}
	if err := saveMihomoSettings(db, want); err != nil {
		t.Fatalf("saveMihomoSettings: %v", err)
	}

	got, err := resolveMihomoSettings(db, "", "")
	if err != nil {
		t.Fatalf("resolveMihomoSettings: %v", err)
	}

	if got != want {
		t.Fatalf("expected stored settings fallback, got %+v want %+v", got, want)
	}
}

func TestDefaultDatabasePathUsesContainerDataDir(t *testing.T) {
	original := isContainerRuntime
	isContainerRuntime = func() bool { return true }
	t.Cleanup(func() {
		isContainerRuntime = original
	})

	if defaultDatabasePath() != "/data/traffic_monitor.db" {
		t.Fatalf("expected container default database path to be /data/traffic_monitor.db, got %q", defaultDatabasePath())
	}
}

func TestOpenDatabaseCreatesParentDirectory(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "nested", "traffic_monitor.db")

	db, err := openDatabase(dbPath)
	if err != nil {
		t.Fatalf("openDatabase: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	if _, err := os.Stat(filepath.Dir(dbPath)); err != nil {
		t.Fatalf("expected parent dir to exist: %v", err)
	}
}

func TestCleanupOldLogsKeepsThirtyDaysOfAggregates(t *testing.T) {
	svc := newTestService(t)

	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.Local).UnixMilli()

	insertTestLogs(t, svc.db, []trafficLog{
		{Timestamp: now - int64(4*24*time.Hour/time.Millisecond), SourceIP: "old-raw", Host: "a.com", Process: "chrome", Outbound: "NodeA", Upload: 1, Download: 1},
		{Timestamp: now - int64(2*24*time.Hour/time.Millisecond), SourceIP: "keep-raw", Host: "a.com", Process: "chrome", Outbound: "NodeA", Upload: 2, Download: 2},
	})
	insertTestAggregates(t, svc.db, []aggregatedEntry{
		{BucketStart: now - int64(31*24*time.Hour/time.Millisecond), BucketEnd: now - int64(31*24*time.Hour/time.Millisecond) + 60000, SourceIP: "old-agg", Host: "a.com", Process: "chrome", Outbound: "NodeA", Chains: `["DIRECT"]`, Upload: 3, Download: 3, Count: 1},
		{BucketStart: now - int64(20*24*time.Hour/time.Millisecond), BucketEnd: now - int64(20*24*time.Hour/time.Millisecond) + 60000, SourceIP: "keep-agg", Host: "a.com", Process: "chrome", Outbound: "NodeA", Chains: `["DIRECT"]`, Upload: 4, Download: 4, Count: 1},
	})

	if err := svc.cleanupOldLogs(now); err != nil {
		t.Fatalf("cleanupOldLogs: %v", err)
	}

	var rawCount int
	if err := svc.db.QueryRow(`SELECT COUNT(*) FROM traffic_logs`).Scan(&rawCount); err != nil {
		t.Fatalf("count traffic_logs: %v", err)
	}
	if rawCount != 0 {
		t.Fatalf("expected cleanup to remove persisted raw logs, got %d rows", rawCount)
	}

	var aggregateCount int
	if err := svc.db.QueryRow(`SELECT COUNT(*) FROM traffic_aggregated`).Scan(&aggregateCount); err != nil {
		t.Fatalf("count traffic_aggregated: %v", err)
	}
	if aggregateCount != 1 {
		t.Fatalf("expected 1 aggregate row after cleanup, got %d", aggregateCount)
	}
}

func TestProcessConnectionsBuffersAggregatesWithoutPersistingRawLogs(t *testing.T) {
	svc := newTestService(t)

	payload := &connectionsResponse{
		Connections: []connection{
			{
				ID:       "conn-1",
				Upload:   128,
				Download: 256,
				Chains:   []string{"ProxyA", "RelayB"},
				Metadata: struct {
					SourceIP      string "json:\"sourceIP\""
					Host          string "json:\"host\""
					DestinationIP string "json:\"destinationIP\""
					Process       string "json:\"process\""
				}{
					SourceIP:      "192.168.1.8",
					Host:          "api.example.com",
					DestinationIP: "1.1.1.1",
					Process:       "curl",
				},
			},
		},
	}

	if err := svc.processConnections(payload); err != nil {
		t.Fatalf("processConnections: %v", err)
	}

	var rawCount int
	if err := svc.db.QueryRow(`SELECT COUNT(*) FROM traffic_logs`).Scan(&rawCount); err != nil {
		t.Fatalf("count traffic_logs: %v", err)
	}
	if rawCount != 0 {
		t.Fatalf("expected no raw logs to be persisted, got %d", rawCount)
	}

	if len(svc.aggregateBuffer) != 1 {
		t.Fatalf("expected 1 aggregate buffer entry, got %d", len(svc.aggregateBuffer))
	}
	for _, entry := range svc.aggregateBuffer {
		if entry.Host != "api.example.com" || entry.DestinationIP != "1.1.1.1" {
			t.Fatalf("unexpected aggregate entry: %+v", entry)
		}
		if entry.Outbound != "ProxyA" || entry.Process != "curl" {
			t.Fatalf("unexpected routing fields: %+v", entry)
		}
		if entry.Upload != 128 || entry.Download != 256 || entry.Count != 1 {
			t.Fatalf("unexpected aggregate totals: %+v", entry)
		}
	}
}

func TestCollectOnceSkipsPollingWhenMihomoURLIsUnset(t *testing.T) {
	svc := newTestService(t)
	svc.client = &http.Client{
		Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			t.Fatalf("unexpected outbound request when mihomo url is unset")
			return nil, nil
		}),
	}
	svc.setMihomoSettings(mihomoSettings{})

	svc.collectOnce(context.Background())
}

func TestHandleMihomoSettingsRoundTrip(t *testing.T) {
	svc := newTestService(t)

	req := httptest.NewRequest(http.MethodPut, "/api/settings/mihomo", bytes.NewBufferString(`{"url":"http://127.0.0.1:9090/","secret":"test-secret"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	svc.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected put status: %d body=%s", rec.Code, rec.Body.String())
	}

	var updated mihomoSettings
	if err := json.Unmarshal(rec.Body.Bytes(), &updated); err != nil {
		t.Fatalf("decode put response: %v", err)
	}
	if updated.URL != "http://127.0.0.1:9090" {
		t.Fatalf("expected trailing slash to be trimmed, got %q", updated.URL)
	}
	if updated.Secret != "test-secret" {
		t.Fatalf("expected updated secret in response, got %q", updated.Secret)
	}

	stored, err := loadMihomoSettings(svc.db)
	if err != nil {
		t.Fatalf("loadMihomoSettings: %v", err)
	}
	if stored != updated {
		t.Fatalf("expected persisted settings to match response, got %+v want %+v", stored, updated)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/settings/mihomo", nil)
	getRec := httptest.NewRecorder()
	svc.routes().ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("unexpected get status: %d body=%s", getRec.Code, getRec.Body.String())
	}

	var fetched mihomoSettings
	if err := json.Unmarshal(getRec.Body.Bytes(), &fetched); err != nil {
		t.Fatalf("decode get response: %v", err)
	}
	if fetched != updated {
		t.Fatalf("expected get response to match updated settings, got %+v want %+v", fetched, updated)
	}
}

func TestHandleMihomoSettingsRejectsEmptyURL(t *testing.T) {
	svc := newTestService(t)

	req := httptest.NewRequest(http.MethodPut, "/api/settings/mihomo", bytes.NewBufferString(`{"url":"   ","secret":""}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	svc.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected bad request for empty mihomo url, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestQueryAggregateIncludesBufferedData(t *testing.T) {
	svc := newTestService(t)

	insertTestAggregates(t, svc.db, []aggregatedEntry{
		{
			BucketStart: 60_000,
			BucketEnd:   120_000,
			SourceIP:    "192.168.1.2",
			Host:        "a.com",
			Process:     "chrome",
			Outbound:    "NodeA",
			Chains:      `["DIRECT"]`,
			Upload:      30,
			Download:    40,
			Count:       1,
		},
	})

	svc.aggregateBuffer["pending"] = &aggregatedEntry{
		BucketStart:   120_000,
		BucketEnd:     180_000,
		SourceIP:      "192.168.1.2",
		Host:          "a.com",
		DestinationIP: "1.1.1.1",
		Process:       "chrome",
		Outbound:      "NodeA",
		Chains:        `["DIRECT"]`,
		Upload:        50,
		Download:      60,
		Count:         1,
	}

	got, err := svc.queryAggregate("sourceIP", 60_000, 180_000)
	if err != nil {
		t.Fatalf("queryAggregate: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("expected 1 row, got %d", len(got))
	}
	if got[0].Upload != 80 || got[0].Download != 100 || got[0].Total != 180 || got[0].Count != 2 {
		t.Fatalf("unexpected aggregated result: %+v", got[0])
	}
}

func TestQueryTrendRebucketsAggregatedMinuteData(t *testing.T) {
	svc := newTestService(t)

	insertTestAggregates(t, svc.db, []aggregatedEntry{
		{BucketStart: 60_000, BucketEnd: 120_000, SourceIP: "192.168.1.2", Host: "a.com", Process: "chrome", Outbound: "NodeA", Chains: `["DIRECT"]`, Upload: 10, Download: 20, Count: 1},
		{BucketStart: 120_000, BucketEnd: 180_000, SourceIP: "192.168.1.2", Host: "a.com", Process: "chrome", Outbound: "NodeA", Chains: `["DIRECT"]`, Upload: 30, Download: 40, Count: 1},
		{BucketStart: 180_000, BucketEnd: 240_000, SourceIP: "192.168.1.2", Host: "a.com", Process: "chrome", Outbound: "NodeA", Chains: `["DIRECT"]`, Upload: 50, Download: 60, Count: 1},
		{BucketStart: 240_000, BucketEnd: 300_000, SourceIP: "192.168.1.2", Host: "a.com", Process: "chrome", Outbound: "NodeA", Chains: `["DIRECT"]`, Upload: 70, Download: 80, Count: 1},
	})

	got, err := svc.queryTrend(0, 600_000, 300_000)
	if err != nil {
		t.Fatalf("queryTrend: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("expected 3 buckets, got %d", len(got))
	}
	if got[0].Timestamp != 0 || got[0].Upload != 160 || got[0].Download != 200 {
		t.Fatalf("unexpected first bucket: %+v", got[0])
	}
	if got[1].Timestamp != 300_000 || got[1].Upload != 0 || got[1].Download != 0 {
		t.Fatalf("unexpected second bucket: %+v", got[1])
	}
}

func TestQueryTrendIncludesBufferedData(t *testing.T) {
	svc := newTestService(t)

	insertTestAggregates(t, svc.db, []aggregatedEntry{
		{BucketStart: 0, BucketEnd: 60_000, SourceIP: "192.168.1.2", Host: "a.com", Process: "chrome", Outbound: "NodeA", Chains: `["DIRECT"]`, Upload: 10, Download: 20, Count: 1},
	})

	svc.aggregateBuffer["pending"] = &aggregatedEntry{
		BucketStart:   60_000,
		BucketEnd:     120_000,
		SourceIP:      "192.168.1.2",
		Host:          "a.com",
		DestinationIP: "1.1.1.1",
		Process:       "chrome",
		Outbound:      "NodeA",
		Chains:        `["DIRECT"]`,
		Upload:        30,
		Download:      40,
		Count:         1,
	}

	got, err := svc.queryTrend(0, 120_000, 60_000)
	if err != nil {
		t.Fatalf("queryTrend: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("expected 3 buckets, got %d", len(got))
	}
	if got[0].Timestamp != 0 || got[0].Upload != 10 || got[0].Download != 20 {
		t.Fatalf("unexpected first bucket: %+v", got[0])
	}
	if got[1].Timestamp != 60_000 || got[1].Upload != 30 || got[1].Download != 40 {
		t.Fatalf("unexpected second bucket: %+v", got[1])
	}
}

func TestHandleLogsClearsAggregatesAndBuffer(t *testing.T) {
	svc := newTestService(t)

	insertTestLogs(t, svc.db, []trafficLog{
		{Timestamp: 1000, SourceIP: "192.168.1.2", Host: "a.com", Process: "chrome", Outbound: "NodeA", Upload: 100, Download: 200},
	})
	insertTestAggregates(t, svc.db, []aggregatedEntry{
		{BucketStart: 0, BucketEnd: 60_000, SourceIP: "192.168.1.2", Host: "a.com", Process: "chrome", Outbound: "NodeA", Chains: `["DIRECT"]`, Upload: 100, Download: 200, Count: 1},
	})
	svc.aggregateBuffer["pending"] = &aggregatedEntry{BucketStart: 60_000, BucketEnd: 120_000, SourceIP: "192.168.1.2"}

	req := httptest.NewRequest(http.MethodDelete, "/api/traffic/logs", nil)
	rec := httptest.NewRecorder()

	svc.handleLogs(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
	}

	var rawCount int
	if err := svc.db.QueryRow(`SELECT COUNT(*) FROM traffic_logs`).Scan(&rawCount); err != nil {
		t.Fatalf("count traffic_logs: %v", err)
	}
	if rawCount != 0 {
		t.Fatalf("expected traffic_logs to be empty, got %d rows", rawCount)
	}

	var aggregateCount int
	if err := svc.db.QueryRow(`SELECT COUNT(*) FROM traffic_aggregated`).Scan(&aggregateCount); err != nil {
		t.Fatalf("count traffic_aggregated: %v", err)
	}
	if aggregateCount != 0 {
		t.Fatalf("expected traffic_aggregated to be empty, got %d rows", aggregateCount)
	}

	if len(svc.aggregateBuffer) != 0 {
		t.Fatalf("expected aggregate buffer to be empty, got %d entries", len(svc.aggregateBuffer))
	}
}

func TestHandleConnectionDetailsReturnsGroupedDetails(t *testing.T) {
	svc := newTestService(t)

	insertTestAggregates(t, svc.db, []aggregatedEntry{
		{
			BucketStart:   0,
			BucketEnd:     60_000,
			SourceIP:      "192.168.1.2",
			Host:          "a.com",
			DestinationIP: "1.1.1.1",
			Process:       "chrome",
			Outbound:      "NodeA",
			Chains:        `["NodeA","RelayA"]`,
			Upload:        100,
			Download:      200,
			Count:         1,
		},
		{
			BucketStart:   60_000,
			BucketEnd:     120_000,
			SourceIP:      "192.168.1.2",
			Host:          "a.com",
			DestinationIP: "1.1.1.1",
			Process:       "chrome",
			Outbound:      "NodeA",
			Chains:        `["NodeA","RelayA"]`,
			Upload:        10,
			Download:      20,
			Count:         1,
		},
		{
			BucketStart:   0,
			BucketEnd:     60_000,
			SourceIP:      "192.168.1.3",
			Host:          "a.com",
			DestinationIP: "8.8.8.8",
			Process:       "curl",
			Outbound:      "NodeB",
			Chains:        `["NodeB"]`,
			Upload:        30,
			Download:      40,
			Count:         1,
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/traffic/details?dimension=host&primary=a.com&secondary=192.168.1.2&start=1&end=120000", nil)
	rec := httptest.NewRecorder()

	svc.handleConnectionDetails(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
	}

	var got []connectionDetail
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("expected 1 grouped detail, got %d", len(got))
	}
	if got[0].DestinationIP != "1.1.1.1" {
		t.Fatalf("unexpected destination ip: %+v", got[0])
	}
	if got[0].SourceIP != "192.168.1.2" {
		t.Fatalf("unexpected source ip: %+v", got[0])
	}
	if got[0].Process != "chrome" || got[0].Outbound != "NodeA" {
		t.Fatalf("unexpected routing info: %+v", got[0])
	}
	if got[0].Upload != 110 || got[0].Download != 220 || got[0].Count != 2 {
		t.Fatalf("unexpected aggregates: %+v", got[0])
	}
	if len(got[0].Chains) != 2 || got[0].Chains[0] != "NodeA" {
		t.Fatalf("unexpected chains: %+v", got[0])
	}
}

func TestHandleConnectionDetailsIncludesBufferedData(t *testing.T) {
	svc := newTestService(t)

	svc.aggregateBuffer["pending"] = &aggregatedEntry{
		BucketStart:   120_000,
		BucketEnd:     180_000,
		SourceIP:      "192.168.1.2",
		Host:          "a.com",
		DestinationIP: "1.1.1.1",
		Process:       "chrome",
		Outbound:      "NodeA",
		Chains:        `["NodeA","RelayA"]`,
		Upload:        15,
		Download:      25,
		Count:         1,
	}

	req := httptest.NewRequest(http.MethodGet, "/api/traffic/details?dimension=host&primary=a.com&secondary=192.168.1.2&start=1&end=180000", nil)
	rec := httptest.NewRecorder()

	svc.handleConnectionDetails(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
	}

	var got []connectionDetail
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("expected 1 grouped detail, got %d", len(got))
	}
	if got[0].Upload != 15 || got[0].Download != 25 || got[0].Count != 1 {
		t.Fatalf("unexpected buffered detail aggregates: %+v", got[0])
	}
}

func TestEmbeddedIndexDisablesPeriodicAutoRefresh(t *testing.T) {
	content, err := webAssets.ReadFile("web/index.html")
	if err != nil {
		t.Fatalf("read embedded index.html: %v", err)
	}

	html := string(content)
	if !strings.Contains(html, `href="/styles.css"`) {
		t.Fatalf("expected embedded index.html to load external stylesheet")
	}
	if !strings.Contains(html, `src="/app.js"`) {
		t.Fatalf("expected embedded index.html to load external app script")
	}
	if !strings.Contains(html, `id="statusBanner"`) || !strings.Contains(html, `id="dashboardShell"`) {
		t.Fatalf("expected embedded index.html to preserve dashboard shell regions")
	}

	scriptContent, err := webAssets.ReadFile("web/app.js")
	if err != nil {
		t.Fatalf("read embedded app.js: %v", err)
	}

	script := string(scriptContent)
	if !strings.Contains(script, `elements.refreshBtn.addEventListener("click", loadData)`) {
		t.Fatalf("expected manual refresh handler to remain available")
	}
	if !strings.Contains(script, `elements.range.addEventListener("change", () => {`) {
		t.Fatalf("expected range change handler to exist")
	}
	if !strings.Contains(script, "if (Number(elements.range.value) !== -1) updateCustomInputs()") {
		t.Fatalf("expected preset range change to keep custom inputs in sync")
	}
	if !strings.Contains(script, `elements.start.addEventListener("change", () => {`) {
		t.Fatalf("expected custom start time change handler to exist")
	}
	if !strings.Contains(script, `elements.end.addEventListener("change", () => {`) {
		t.Fatalf("expected custom end time change handler to exist")
	}
	if !strings.Contains(script, `elements.range.value = "-1"`) {
		t.Fatalf("expected manual datetime edits to switch range into custom mode")
	}
	if !strings.Contains(script, `elements.start.addEventListener("change", () => {`) || !strings.Contains(script, `loadData()`) {
		t.Fatalf("expected manual time edits to request fresh data")
	}
	if strings.Contains(script, "setInterval(loadData, 30000)") {
		t.Fatalf("expected periodic auto refresh to be removed")
	}
	if !strings.Contains(script, "await loadSettings()") {
		t.Fatalf("expected initial page boot to load saved mihomo settings before fetching data")
	}
	if !strings.Contains(script, "if (state.settingsRequired)") || !strings.Contains(script, "await loadData()") {
		t.Fatalf("expected initial page boot to gate data loading on saved mihomo settings")
	}
	if !strings.Contains(html, `id="settingsPanel"`) || !strings.Contains(html, `id="settingsBtn"`) {
		t.Fatalf("expected embedded index.html to include mihomo settings entry points")
	}
	if !strings.Contains(script, `sendJSON("/api/settings/mihomo", "PUT", payload)`) {
		t.Fatalf("expected embedded index.html to save mihomo settings through the settings API")
	}
	for _, label := range []string{"1 天", "7 天", "15 天", "30 天", "自定义"} {
		if !strings.Contains(html, label) {
			t.Fatalf("expected range option %q to exist", label)
		}
	}
	for _, label := range []string{"最近 1 小时", "最近 24 小时"} {
		if strings.Contains(html, label) {
			t.Fatalf("expected old range option %q to be removed", label)
		}
	}
}

func TestEmbeddedIndexIncludesGithubAndLicenseFooter(t *testing.T) {
	content, err := webAssets.ReadFile("web/index.html")
	if err != nil {
		t.Fatalf("read embedded index.html: %v", err)
	}

	html := string(content)
	for _, unwanted := range []string{
		`id="primaryHint"`,
		`id="secondaryHint"`,
		`id="detailHint"`,
		`class="panel-subtitle"`,
	} {
		if strings.Contains(html, unwanted) {
			t.Fatalf("expected embedded index.html to remove verbose helper text marker %q", unwanted)
		}
	}

	for _, want := range []string{
		`id="runtimeSummary"`,
		`id="selectionPath"`,
		`id="overviewSection"`,
		`id="drilldownSection"`,
		`class="panel card card-count"`,
		`class="panel card card-upload"`,
		`class="panel card card-download"`,
		`class="panel card card-total"`,
		`id="secondaryTitle"`,
		`id="detailTitle"`,
		`href="https://github.com/zhf883680/clash-traffic-monitor"`,
		`>GitHub<`,
		`href="/LICENSE"`,
		`>MIT License<`,
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("expected embedded index.html to contain %q", want)
		}
	}
}

func TestEmbeddedTrendChartIncludesAxisAndTooltip(t *testing.T) {
	indexContent, err := webAssets.ReadFile("web/index.html")
	if err != nil {
		t.Fatalf("read embedded index.html: %v", err)
	}

	html := string(indexContent)
	for _, unwanted := range []string{
		`class="trend-legend"`,
		`class="trend-legend-item total"`,
		`class="trend-legend-item upload"`,
		`class="trend-legend-item download"`,
	} {
		if strings.Contains(html, unwanted) {
			t.Fatalf("expected embedded index.html to remove %q", unwanted)
		}
	}

	for _, want := range []string{
		`id="trendTooltip"`,
		`id="trendAxis"`,
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("expected embedded index.html to contain %q", want)
		}
	}
	if strings.Contains(html, `class="trend-axis-caption"`) {
		t.Fatalf("expected embedded index.html to remove the trend axis caption")
	}

	scriptContent, err := webAssets.ReadFile("web/app.js")
	if err != nil {
		t.Fatalf("read embedded app.js: %v", err)
	}

	script := string(scriptContent)
	for _, want := range []string{
		`trendTooltip: document.getElementById("trendTooltip")`,
		`trendAxis: document.getElementById("trendAxis")`,
		"function formatTrendAxisTick(",
		"function getTrendTickIndexes(",
		"function updateTrendAxisLabels(points) {",
		"function showTrendTooltip(event) {",
		"function hideTrendTooltip() {",
		"elements.trendAxis.innerHTML =",
		"updateTrendAxisLabels(points)",
		`elements.trendCanvas.addEventListener("mousemove", showTrendTooltip)`,
		`elements.trendCanvas.addEventListener("mouseleave", hideTrendTooltip)`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("expected embedded app.js to contain %q", want)
		}
	}

	styleContent, err := webAssets.ReadFile("web/styles.css")
	if err != nil {
		t.Fatalf("read embedded styles.css: %v", err)
	}

	styles := string(styleContent)
	for _, want := range []string{
		".trend-chart-shell",
		"grid-template-rows: minmax(0, 1fr) auto;",
		".trend-tooltip",
		"position: fixed;",
		".trend-tooltip-time",
		".trend-tooltip-metric",
		".trend-axis",
		".trend-axis span",
	} {
		if !strings.Contains(styles, want) {
			t.Fatalf("expected embedded styles.css to contain %q", want)
		}
	}
}

func TestEmbeddedAppScriptIncludesContextualDashboardLabels(t *testing.T) {
	content, err := webAssets.ReadFile("web/app.js")
	if err != nil {
		t.Fatalf("read embedded app.js: %v", err)
	}

	script := string(content)
	for _, want := range []string{
		"function syncContextSummary()",
		`${primary} 访问的主机`,
		`${primary} 的访问设备`,
		`${primary} 命中的目标主机`,
		`selectionPath: document.getElementById("selectionPath")`,
		`secondaryTitle: document.getElementById("secondaryTitle")`,
		`detailTitle: document.getElementById("detailTitle")`,
		`elements.selectionPath.textContent =`,
		`elements.secondaryTitle.textContent =`,
		`elements.secondaryTitle.title =`,
		`elements.detailTitle.textContent =`,
		`elements.detailTitle.title =`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("expected embedded app.js to contain %q", want)
		}
	}
}

func TestEmbeddedStylesConstrainDashboardHeight(t *testing.T) {
	content, err := webAssets.ReadFile("web/styles.css")
	if err != nil {
		t.Fatalf("read embedded styles.css: %v", err)
	}

	styles := string(content)
	for _, want := range []string{
		"--overview-panel-height:",
		"clamp(228px, 29vh, 244px);",
		"height: var(--overview-panel-height);",
		"grid-template-areas:",
		`"count total"`,
		`"upload download"`,
		".card-upload",
		"grid-area: upload;",
		".card-download",
		"grid-area: download;",
		".dashboard .panel-title",
		".secondary-panel .panel-head",
		"grid-template-columns: minmax(0, 1fr) auto;",
		"overflow-wrap: anywhere;",
		"-webkit-line-clamp: 2;",
		"width: 180px;",
		".ranking-title .mono",
		"flex: 1 1 auto;",
		".ranking-total",
		"flex: 0 0 auto;",
		".ranking-list",
		".detail-table-wrap",
		".detail-cards-shell",
		"overflow: auto",
	} {
		if !strings.Contains(styles, want) {
			t.Fatalf("expected embedded styles.css to contain %q", want)
		}
	}
}

func TestRoutesServeLicense(t *testing.T) {
	svc := newTestService(t)

	req := httptest.NewRequest(http.MethodGet, "/LICENSE", nil)
	rec := httptest.NewRecorder()

	svc.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
	}
	if contentType := rec.Header().Get("Content-Type"); !strings.Contains(contentType, "text/plain") {
		t.Fatalf("expected text/plain content type, got %q", contentType)
	}
	for _, want := range []string{
		"MIT License",
		"Permission is hereby granted, free of charge",
	} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("expected license response to contain %q", want)
		}
	}
}

func TestRoutesServeEmbeddedStaticAssets(t *testing.T) {
	svc := newTestService(t)

	tests := []struct {
		path        string
		contentType string
		bodyNeedle  string
	}{
		{path: "/styles.css", contentType: "text/css", bodyNeedle: ":root"},
		{path: "/app.js", contentType: "javascript", bodyNeedle: "loadSettings"},
	}

	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			rec := httptest.NewRecorder()

			svc.routes().ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
			}
			if contentType := rec.Header().Get("Content-Type"); !strings.Contains(contentType, tc.contentType) {
				t.Fatalf("expected %s content type for %s, got %q", tc.contentType, tc.path, contentType)
			}
			if !strings.Contains(rec.Body.String(), tc.bodyNeedle) {
				t.Fatalf("expected %s response to contain %q", tc.path, tc.bodyNeedle)
			}
		})
	}
}

func newTestService(t *testing.T) *service {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "traffic.db")
	db, err := openDatabase(dbPath)
	if err != nil {
		t.Fatalf("openDatabase: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	return &service{
		db:              db,
		lastConnections: make(map[string]connection),
		aggregateBuffer: make(map[string]*aggregatedEntry),
	}
}

func insertTestLogs(t *testing.T, db *sql.DB, logs []trafficLog) {
	t.Helper()

	for _, entry := range logs {
		chainsRaw, err := json.Marshal(entry.Chains)
		if err != nil {
			t.Fatalf("marshal chains: %v", err)
		}

		_, err = db.Exec(
			`INSERT INTO traffic_logs (timestamp, source_ip, host, destination_ip, process, outbound, chains, upload, download)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			entry.Timestamp,
			entry.SourceIP,
			entry.Host,
			entry.DestinationIP,
			entry.Process,
			entry.Outbound,
			string(chainsRaw),
			entry.Upload,
			entry.Download,
		)
		if err != nil {
			t.Fatalf("insert log: %v", err)
		}
	}
}

func insertTestAggregates(t *testing.T, db *sql.DB, entries []aggregatedEntry) {
	t.Helper()

	for _, entry := range entries {
		_, err := db.Exec(
			`INSERT INTO traffic_aggregated
			 (bucket_start, bucket_end, source_ip, host, destination_ip, process, outbound, chains, upload, download, count)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			entry.BucketStart,
			entry.BucketEnd,
			entry.SourceIP,
			entry.Host,
			entry.DestinationIP,
			entry.Process,
			entry.Outbound,
			entry.Chains,
			entry.Upload,
			entry.Download,
			entry.Count,
		)
		if err != nil {
			t.Fatalf("insert aggregate: %v", err)
		}
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}
