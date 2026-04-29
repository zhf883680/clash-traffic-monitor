package main

import (
	"bytes"
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

//go:embed LICENSE web/*
var webAssets embed.FS

const (
	defaultListenAddr      = ":8080"
	defaultPollInterval    = 5 * time.Second
	aggregateFlushInterval = 10 * time.Minute
	aggregateRetention     = 30 * 24 * time.Hour
	defaultAllowedOrigin   = "*"
)

var isContainerRuntime = func() bool {
	_, err := os.Stat("/.dockerenv")
	return err == nil
}

type config struct {
	ListenAddr   string
	MihomoURL    string
	MihomoSecret string
}

type mihomoSettings struct {
	URL    string `json:"url"`
	Secret string `json:"secret"`
}

type autoSwitchSettings struct {
	Enabled                 bool                    `json:"enabled"`
	ThresholdBytesPerMinute int64                   `json:"thresholdBytesPerMinute"`
	CooldownSeconds         int64                   `json:"cooldownSeconds"`
	RestoreEnabled          bool                    `json:"restoreEnabled"`
	RestoreQuietMinutes     int64                   `json:"restoreQuietMinutes"`
	GroupTargets            []autoSwitchGroupTarget `json:"groupTargets"`
}

type autoSwitchGroupTarget struct {
	GroupName   string `json:"groupName"`
	TargetProxy string `json:"targetProxy"`
	Enabled     bool   `json:"enabled"`
}

type autoSwitchExecutionResult struct {
	GroupName   string `json:"groupName"`
	TargetProxy string `json:"targetProxy"`
	Status      string `json:"status"`
	Message     string `json:"message,omitempty"`
}

type autoSwitchEvent struct {
	ID          int64                       `json:"id"`
	TriggeredAt int64                       `json:"triggeredAt"`
	Host        string                      `json:"host"`
	TotalBytes  int64                       `json:"totalBytes"`
	WindowStart int64                       `json:"windowStart"`
	WindowEnd   int64                       `json:"windowEnd"`
	Results     []autoSwitchExecutionResult `json:"results"`
	Error       string                      `json:"error"`
}

type autoRestoreSession struct {
	GroupName          string `json:"groupName"`
	OriginalProxy      string `json:"originalProxy"`
	CurrentProxy       string `json:"currentProxy"`
	LastTriggeredAt    int64  `json:"lastTriggeredAt"`
	LastTriggeredHost  string `json:"lastTriggeredHost"`
	LastTriggeredBytes int64  `json:"lastTriggeredBytes"`
}

type controllableProxyGroup struct {
	Name string   `json:"name"`
	Type string   `json:"type"`
	Now  string   `json:"now"`
	All  []string `json:"all"`
}

type trafficLog struct {
	Timestamp     int64    `json:"timestamp"`
	SourceIP      string   `json:"sourceIP"`
	Host          string   `json:"host"`
	DestinationIP string   `json:"destinationIP"`
	Process       string   `json:"process"`
	Outbound      string   `json:"outbound"`
	Chains        []string `json:"chains"`
	Upload        int64    `json:"upload"`
	Download      int64    `json:"download"`
}

type aggregatedData struct {
	Label    string `json:"label"`
	Upload   int64  `json:"upload"`
	Download int64  `json:"download"`
	Total    int64  `json:"total"`
	Count    int64  `json:"count"`
}

type trendPoint struct {
	Timestamp int64 `json:"timestamp"`
	Upload    int64 `json:"upload"`
	Download  int64 `json:"download"`
}

type connectionDetail struct {
	DestinationIP string   `json:"destinationIP"`
	SourceIP      string   `json:"sourceIP"`
	Process       string   `json:"process"`
	Outbound      string   `json:"outbound"`
	Chains        []string `json:"chains"`
	Upload        int64    `json:"upload"`
	Download      int64    `json:"download"`
	Total         int64    `json:"total"`
	Count         int64    `json:"count"`
}

type connection struct {
	ID       string   `json:"id"`
	Upload   int64    `json:"upload"`
	Download int64    `json:"download"`
	Chains   []string `json:"chains"`
	Metadata struct {
		SourceIP      string `json:"sourceIP"`
		Host          string `json:"host"`
		DestinationIP string `json:"destinationIP"`
		Process       string `json:"process"`
	} `json:"metadata"`
}

type connectionsResponse struct {
	Connections   []connection `json:"connections"`
	UploadTotal   int64        `json:"uploadTotal"`
	DownloadTotal int64        `json:"downloadTotal"`
}

type mihomoProxyInfo struct {
	Type string   `json:"type"`
	Now  string   `json:"now"`
	All  []string `json:"all"`
}

type mihomoProxiesResponse struct {
	Proxies map[string]mihomoProxyInfo `json:"proxies"`
}

type service struct {
	db                *sql.DB
	client            *http.Client
	now               func() time.Time
	cfg               config
	mu                sync.Mutex
	mihomoSettings    mihomoSettings
	lastConnections   map[string]connection
	lastUploadTotal   int64
	lastDownloadTotal int64
	lastAutoSwitchAt  int64
	lastAutoSwitchMin int64
	lastCleanup       time.Time
	lastVacuum        time.Time
	aggregateBuffer   map[string]*aggregatedEntry
	hostMinuteWindows map[string]*hostTrafficWindow
}

type aggregatedEntry struct {
	BucketStart   int64
	BucketEnd     int64
	SourceIP      string
	Host          string
	DestinationIP string
	Process       string
	Outbound      string
	Chains        string
	Upload        int64
	Download      int64
	Count         int64
}

type hostTrafficWindow struct {
	BucketStart int64
	BucketEnd   int64
	TotalBytes  int64
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	db, err := openDatabase(defaultDatabasePath())
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer db.Close()

	runtimeSettings, err := resolveMihomoSettings(db, cfg.MihomoURL, cfg.MihomoSecret)
	if err != nil {
		log.Fatalf("resolve mihomo settings: %v", err)
	}

	svc := &service{
		db:                db,
		client:            &http.Client{Timeout: 10 * time.Second},
		now:               time.Now,
		cfg:               cfg,
		mihomoSettings:    runtimeSettings,
		lastConnections:   make(map[string]connection),
		lastVacuum:        time.Now(),
		aggregateBuffer:   make(map[string]*aggregatedEntry),
		hostMinuteWindows: make(map[string]*hostTrafficWindow),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)
		svc.runCollector(ctx)
	}()

	server := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           svc.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("traffic monitor listening on %s", cfg.ListenAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	cancel()
	select {
	case <-collectorDone:
	case <-time.After(5 * time.Second):
		log.Printf("collector shutdown timed out")
	}
	if err := svc.flushAggregateBuffer(); err != nil {
		log.Printf("flush aggregate buffer on shutdown: %v", err)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown server: %v", err)
	}
}

func loadConfig() (config, error) {
	cfg := config{
		ListenAddr:   getenv("TRAFFIC_MONITOR_LISTEN", defaultListenAddr),
		MihomoURL:    strings.TrimRight(getenv("MIHOMO_URL", ""), "/"),
		MihomoSecret: getenv("MIHOMO_SECRET", ""),
	}

	return cfg, nil
}

func defaultDatabasePath() string {
	if isContainerRuntime() {
		return "/data/traffic_monitor.db"
	}
	return "./data/traffic_monitor.db"
}

func openDatabase(path string) (*sql.DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	schema := `
	PRAGMA journal_mode=WAL;
	PRAGMA busy_timeout=5000;

	CREATE TABLE IF NOT EXISTS traffic_logs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		timestamp INTEGER NOT NULL,
		source_ip TEXT NOT NULL,
		host TEXT NOT NULL,
		destination_ip TEXT NOT NULL DEFAULT '',
		process TEXT NOT NULL,
		outbound TEXT NOT NULL,
		chains TEXT NOT NULL DEFAULT '[]',
		upload INTEGER NOT NULL,
		download INTEGER NOT NULL
	);

	CREATE TABLE IF NOT EXISTS traffic_aggregated (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		bucket_start INTEGER NOT NULL,
		bucket_end INTEGER NOT NULL,
		source_ip TEXT NOT NULL,
		host TEXT NOT NULL,
		destination_ip TEXT NOT NULL DEFAULT '',
		process TEXT NOT NULL,
		outbound TEXT NOT NULL,
		chains TEXT NOT NULL DEFAULT '[]',
		upload INTEGER NOT NULL,
		download INTEGER NOT NULL,
		count INTEGER NOT NULL,
		UNIQUE(bucket_start, bucket_end, source_ip, host, destination_ip, process, outbound, chains)
	);

	CREATE INDEX IF NOT EXISTS idx_traffic_logs_timestamp ON traffic_logs(timestamp);
	CREATE INDEX IF NOT EXISTS idx_traffic_logs_source_ip ON traffic_logs(source_ip);
	CREATE INDEX IF NOT EXISTS idx_traffic_logs_host ON traffic_logs(host);
	CREATE INDEX IF NOT EXISTS idx_traffic_logs_process ON traffic_logs(process);
	CREATE INDEX IF NOT EXISTS idx_traffic_logs_outbound ON traffic_logs(outbound);

	CREATE INDEX IF NOT EXISTS idx_traffic_aggregated_bucket ON traffic_aggregated(bucket_start, bucket_end);
	CREATE INDEX IF NOT EXISTS idx_traffic_aggregated_source_ip ON traffic_aggregated(source_ip);
	CREATE INDEX IF NOT EXISTS idx_traffic_aggregated_host ON traffic_aggregated(host);
	CREATE INDEX IF NOT EXISTS idx_traffic_aggregated_process ON traffic_aggregated(process);
	CREATE INDEX IF NOT EXISTS idx_traffic_aggregated_outbound ON traffic_aggregated(outbound);

	CREATE TABLE IF NOT EXISTS app_settings (
		key TEXT PRIMARY KEY,
		value TEXT NOT NULL DEFAULT ''
	);

	CREATE TABLE IF NOT EXISTS auto_switch_group_targets (
		group_name TEXT PRIMARY KEY,
		target_proxy TEXT NOT NULL,
		enabled INTEGER NOT NULL DEFAULT 0
	);

	CREATE TABLE IF NOT EXISTS auto_switch_events (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		triggered_at INTEGER NOT NULL,
		host TEXT NOT NULL,
		total_bytes INTEGER NOT NULL,
		window_start INTEGER NOT NULL,
		window_end INTEGER NOT NULL,
		results_json TEXT NOT NULL DEFAULT '[]',
		error TEXT NOT NULL DEFAULT ''
	);

	CREATE TABLE IF NOT EXISTS auto_restore_sessions (
		group_name TEXT PRIMARY KEY,
		original_proxy TEXT NOT NULL,
		current_proxy TEXT NOT NULL,
		last_triggered_at INTEGER NOT NULL,
		last_triggered_host TEXT NOT NULL DEFAULT '',
		last_triggered_bytes INTEGER NOT NULL DEFAULT 0
	);

	CREATE INDEX IF NOT EXISTS idx_auto_switch_events_triggered_at ON auto_switch_events(triggered_at DESC);
	`

	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, err
	}

	for _, stmt := range []string{
		`ALTER TABLE traffic_logs ADD COLUMN destination_ip TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE traffic_logs ADD COLUMN chains TEXT NOT NULL DEFAULT '[]'`,
	} {
		if _, err := db.Exec(stmt); err != nil && !strings.Contains(err.Error(), "duplicate column name") {
			db.Close()
			return nil, err
		}
	}

	currentBucketStart := (time.Now().UnixMilli() / 60000) * 60000
	if err := backfillAggregatedLogs(db, currentBucketStart); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func normalizeMihomoSettings(settings mihomoSettings) mihomoSettings {
	settings.URL = strings.TrimRight(strings.TrimSpace(settings.URL), "/")
	settings.Secret = strings.TrimSpace(settings.Secret)
	return settings
}

func loadMihomoSettings(db *sql.DB) (mihomoSettings, error) {
	rows, err := db.Query(`SELECT key, value FROM app_settings WHERE key IN ('mihomo_url', 'mihomo_secret')`)
	if err != nil {
		return mihomoSettings{}, err
	}
	defer rows.Close()

	var settings mihomoSettings
	for rows.Next() {
		var (
			key   string
			value string
		)
		if err := rows.Scan(&key, &value); err != nil {
			return mihomoSettings{}, err
		}
		switch key {
		case "mihomo_url":
			settings.URL = value
		case "mihomo_secret":
			settings.Secret = value
		}
	}
	if err := rows.Err(); err != nil {
		return mihomoSettings{}, err
	}

	return normalizeMihomoSettings(settings), nil
}

func saveMihomoSettings(db *sql.DB, settings mihomoSettings) error {
	settings = normalizeMihomoSettings(settings)

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	for key, value := range map[string]string{
		"mihomo_url":    settings.URL,
		"mihomo_secret": settings.Secret,
	} {
		if _, err := tx.Exec(`
			INSERT INTO app_settings (key, value)
			VALUES (?, ?)
			ON CONFLICT(key) DO UPDATE SET value = excluded.value
		`, key, value); err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

func resolveMihomoSettings(db *sql.DB, envURL, envSecret string) (mihomoSettings, error) {
	stored, err := loadMihomoSettings(db)
	if err != nil {
		return mihomoSettings{}, err
	}

	resolved := stored
	if strings.TrimSpace(envURL) != "" {
		resolved.URL = envURL
	}
	if strings.TrimSpace(envSecret) != "" {
		resolved.Secret = envSecret
	}
	resolved = normalizeMihomoSettings(resolved)

	if strings.TrimSpace(envURL) != "" || strings.TrimSpace(envSecret) != "" {
		if err := saveMihomoSettings(db, resolved); err != nil {
			return mihomoSettings{}, err
		}
	}

	return resolved, nil
}

func loadAutoSwitchSettings(db *sql.DB) (autoSwitchSettings, error) {
	rows, err := db.Query(`
		SELECT key, value
		FROM app_settings
		WHERE key IN (
			'auto_switch_enabled',
			'auto_switch_threshold_bytes_per_minute',
			'auto_switch_cooldown_seconds',
			'auto_restore_enabled',
			'auto_restore_quiet_minutes'
		)
	`)
	if err != nil {
		return autoSwitchSettings{}, err
	}
	defer rows.Close()

	settings := autoSwitchSettings{}
	for rows.Next() {
		var key string
		var value string
		if err := rows.Scan(&key, &value); err != nil {
			return autoSwitchSettings{}, err
		}

		switch key {
		case "auto_switch_enabled":
			settings.Enabled = value == "1"
		case "auto_switch_threshold_bytes_per_minute":
			settings.ThresholdBytesPerMinute, err = strconv.ParseInt(value, 10, 64)
			if err != nil {
				return autoSwitchSettings{}, err
			}
		case "auto_switch_cooldown_seconds":
			settings.CooldownSeconds, err = strconv.ParseInt(value, 10, 64)
			if err != nil {
				return autoSwitchSettings{}, err
			}
		case "auto_restore_enabled":
			settings.RestoreEnabled = value == "1"
		case "auto_restore_quiet_minutes":
			settings.RestoreQuietMinutes, err = strconv.ParseInt(value, 10, 64)
			if err != nil {
				return autoSwitchSettings{}, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return autoSwitchSettings{}, err
	}

	groupRows, err := db.Query(`
		SELECT group_name, target_proxy, enabled
		FROM auto_switch_group_targets
		ORDER BY rowid ASC
	`)
	if err != nil {
		return autoSwitchSettings{}, err
	}
	defer groupRows.Close()

	for groupRows.Next() {
		var target autoSwitchGroupTarget
		var enabled int
		if err := groupRows.Scan(&target.GroupName, &target.TargetProxy, &enabled); err != nil {
			return autoSwitchSettings{}, err
		}
		target.Enabled = enabled == 1
		settings.GroupTargets = append(settings.GroupTargets, target)
	}
	if err := groupRows.Err(); err != nil {
		return autoSwitchSettings{}, err
	}

	return settings, nil
}

func saveAutoSwitchSettings(db *sql.DB, settings autoSwitchSettings) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	values := map[string]string{
		"auto_switch_enabled":                    "0",
		"auto_switch_threshold_bytes_per_minute": strconv.FormatInt(settings.ThresholdBytesPerMinute, 10),
		"auto_switch_cooldown_seconds":           strconv.FormatInt(settings.CooldownSeconds, 10),
		"auto_restore_enabled":                   "0",
		"auto_restore_quiet_minutes":             strconv.FormatInt(settings.RestoreQuietMinutes, 10),
	}
	if settings.Enabled {
		values["auto_switch_enabled"] = "1"
	}
	if settings.RestoreEnabled {
		values["auto_restore_enabled"] = "1"
	}

	for key, value := range values {
		if _, err := tx.Exec(`
			INSERT INTO app_settings (key, value)
			VALUES (?, ?)
			ON CONFLICT(key) DO UPDATE SET value = excluded.value
		`, key, value); err != nil {
			tx.Rollback()
			return err
		}
	}

	if _, err := tx.Exec(`DELETE FROM auto_switch_group_targets`); err != nil {
		tx.Rollback()
		return err
	}

	stmt, err := tx.Prepare(`
		INSERT INTO auto_switch_group_targets (group_name, target_proxy, enabled)
		VALUES (?, ?, ?)
	`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	for _, target := range settings.GroupTargets {
		enabled := 0
		if target.Enabled {
			enabled = 1
		}
		if _, err := stmt.Exec(target.GroupName, target.TargetProxy, enabled); err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

func insertAutoSwitchEvent(db *sql.DB, event autoSwitchEvent) error {
	resultsJSON, err := json.Marshal(event.Results)
	if err != nil {
		return err
	}

	_, err = db.Exec(`
		INSERT INTO auto_switch_events
		(triggered_at, host, total_bytes, window_start, window_end, results_json, error)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, event.TriggeredAt, event.Host, event.TotalBytes, event.WindowStart, event.WindowEnd, string(resultsJSON), event.Error)
	return err
}

func upsertAutoRestoreSession(db *sql.DB, session autoRestoreSession) error {
	_, err := db.Exec(`
		INSERT INTO auto_restore_sessions
		(group_name, original_proxy, current_proxy, last_triggered_at, last_triggered_host, last_triggered_bytes)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(group_name) DO UPDATE SET
			original_proxy = excluded.original_proxy,
			current_proxy = excluded.current_proxy,
			last_triggered_at = excluded.last_triggered_at,
			last_triggered_host = excluded.last_triggered_host,
			last_triggered_bytes = excluded.last_triggered_bytes
	`, session.GroupName, session.OriginalProxy, session.CurrentProxy, session.LastTriggeredAt, session.LastTriggeredHost, session.LastTriggeredBytes)
	return err
}

func listAutoRestoreSessions(db *sql.DB) ([]autoRestoreSession, error) {
	rows, err := db.Query(`
		SELECT group_name, original_proxy, current_proxy, last_triggered_at, last_triggered_host, last_triggered_bytes
		FROM auto_restore_sessions
		ORDER BY group_name ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	sessions := make([]autoRestoreSession, 0)
	for rows.Next() {
		var session autoRestoreSession
		if err := rows.Scan(
			&session.GroupName,
			&session.OriginalProxy,
			&session.CurrentProxy,
			&session.LastTriggeredAt,
			&session.LastTriggeredHost,
			&session.LastTriggeredBytes,
		); err != nil {
			return nil, err
		}
		sessions = append(sessions, session)
	}

	return sessions, rows.Err()
}

func deleteAutoRestoreSession(db *sql.DB, groupName string) error {
	_, err := db.Exec(`DELETE FROM auto_restore_sessions WHERE group_name = ?`, groupName)
	return err
}

func getAutoRestoreSession(db *sql.DB, groupName string) (autoRestoreSession, bool, error) {
	row := db.QueryRow(`
		SELECT group_name, original_proxy, current_proxy, last_triggered_at, last_triggered_host, last_triggered_bytes
		FROM auto_restore_sessions
		WHERE group_name = ?
	`, groupName)

	var session autoRestoreSession
	if err := row.Scan(
		&session.GroupName,
		&session.OriginalProxy,
		&session.CurrentProxy,
		&session.LastTriggeredAt,
		&session.LastTriggeredHost,
		&session.LastTriggeredBytes,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return autoRestoreSession{}, false, nil
		}
		return autoRestoreSession{}, false, err
	}

	return session, true, nil
}

func listAutoSwitchEvents(db *sql.DB, limit int) ([]autoSwitchEvent, error) {
	if limit <= 0 {
		limit = 20
	}

	rows, err := db.Query(`
		SELECT id, triggered_at, host, total_bytes, window_start, window_end, results_json, error
		FROM auto_switch_events
		ORDER BY triggered_at DESC, id DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := make([]autoSwitchEvent, 0)
	for rows.Next() {
		var event autoSwitchEvent
		var resultsJSON string
		if err := rows.Scan(
			&event.ID,
			&event.TriggeredAt,
			&event.Host,
			&event.TotalBytes,
			&event.WindowStart,
			&event.WindowEnd,
			&resultsJSON,
			&event.Error,
		); err != nil {
			return nil, err
		}

		if resultsJSON != "" {
			if err := json.Unmarshal([]byte(resultsJSON), &event.Results); err != nil {
				return nil, err
			}
		}

		events = append(events, event)
	}

	return events, rows.Err()
}

func normalizeProxyGroupType(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "selector", "select":
		return "select"
	case "fallback":
		return "fallback"
	default:
		return ""
	}
}

func (s *service) listControllableProxyGroups(settings mihomoSettings) ([]controllableProxyGroup, error) {
	if settings.URL == "" {
		return nil, errors.New("mihomo url is not configured")
	}

	req, err := http.NewRequest(http.MethodGet, settings.URL+"/proxies", nil)
	if err != nil {
		return nil, err
	}
	if settings.Secret != "" {
		req.Header.Set("Authorization", "Bearer "+settings.Secret)
	}

	client := s.client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	var payload mihomoProxiesResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}

	groups := make([]controllableProxyGroup, 0)
	for name, proxy := range payload.Proxies {
		groupType := normalizeProxyGroupType(proxy.Type)
		if groupType == "" {
			continue
		}

		groups = append(groups, controllableProxyGroup{
			Name: name,
			Type: groupType,
			Now:  strings.TrimSpace(proxy.Now),
			All:  append([]string(nil), proxy.All...),
		})
	}

	sort.Slice(groups, func(i, j int) bool {
		return groups[i].Name < groups[j].Name
	})

	return groups, nil
}

func (s *service) switchProxyGroup(settings mihomoSettings, group controllableProxyGroup, targetProxy string) error {
	group.Type = normalizeProxyGroupType(group.Type)
	if group.Type == "" {
		return errors.New("proxy group type is not controllable")
	}

	targetProxy = strings.TrimSpace(targetProxy)
	if targetProxy == "" {
		return errors.New("target proxy is required")
	}
	if !containsString(group.All, targetProxy) {
		return fmt.Errorf("target proxy %q is not available in group %q", targetProxy, group.Name)
	}

	body, err := json.Marshal(map[string]string{"name": targetProxy})
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPut, settings.URL+"/proxies/"+group.Name, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if settings.Secret != "" {
		req.Header.Set("Authorization", "Bearer "+settings.Secret)
	}

	client := s.client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return nil
}

func backfillAggregatedLogs(db *sql.DB, beforeMS int64) error {
	if beforeMS <= 0 {
		return nil
	}

	var lastBucketEnd sql.NullInt64
	if err := db.QueryRow(`SELECT MAX(bucket_end) FROM traffic_aggregated`).Scan(&lastBucketEnd); err != nil {
		return err
	}

	startMS := int64(0)
	if lastBucketEnd.Valid {
		startMS = lastBucketEnd.Int64
	}
	if startMS >= beforeMS {
		return nil
	}

	_, err := db.Exec(`
		INSERT INTO traffic_aggregated
		(bucket_start, bucket_end, source_ip, host, destination_ip, process, outbound, chains, upload, download, count)
		SELECT ((timestamp / 60000) * 60000) AS bucket_start,
		       ((timestamp / 60000) * 60000) + 60000 AS bucket_end,
		       source_ip,
		       host,
		       destination_ip,
		       process,
		       outbound,
		       chains,
		       COALESCE(SUM(upload), 0) AS upload,
		       COALESCE(SUM(download), 0) AS download,
		       COUNT(*) AS count
		FROM traffic_logs
		WHERE timestamp >= ? AND timestamp < ?
		GROUP BY bucket_start, source_ip, host, destination_ip, process, outbound, chains
		ON CONFLICT(bucket_start, bucket_end, source_ip, host, destination_ip, process, outbound, chains)
		DO UPDATE SET
			upload = excluded.upload,
			download = excluded.download,
			count = excluded.count
	`, startMS, beforeMS)
	return err
}

func (s *service) runCollector(ctx context.Context) {
	s.collectOnce(ctx)

	ticker := time.NewTicker(defaultPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.collectOnce(ctx)
		}
	}
}

func (s *service) collectOnce(ctx context.Context) {
	settings := s.currentMihomoSettings()
	if settings.URL == "" {
		return
	}

	resp, err := s.fetchConnections(ctx, settings)
	if err != nil {
		log.Printf("poll Mihomo connections: %v", err)
		return
	}

	if err := s.processConnections(resp); err != nil {
		log.Printf("process connections: %v", err)
	}
}

func (s *service) currentMihomoSettings() mihomoSettings {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mihomoSettings
}

func (s *service) setMihomoSettings(settings mihomoSettings) {
	settings = normalizeMihomoSettings(settings)

	s.mu.Lock()
	defer s.mu.Unlock()

	if settings != s.mihomoSettings {
		s.lastConnections = make(map[string]connection)
		s.lastUploadTotal = 0
		s.lastDownloadTotal = 0
	}

	s.mihomoSettings = settings
}

func (s *service) updateMihomoSettings(settings mihomoSettings) error {
	settings = normalizeMihomoSettings(settings)
	if err := saveMihomoSettings(s.db, settings); err != nil {
		return err
	}
	s.setMihomoSettings(settings)
	return nil
}

func (s *service) fetchConnections(ctx context.Context, settings mihomoSettings) (*connectionsResponse, error) {
	if settings.URL == "" {
		return nil, errors.New("mihomo url is not configured")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, settings.URL+"/connections", nil)
	if err != nil {
		return nil, err
	}

	if settings.Secret != "" {
		req.Header.Set("Authorization", "Bearer "+settings.Secret)
	}

	client := s.client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	var payload connectionsResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}

	return &payload, nil
}

func (s *service) currentTime() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

func (s *service) processConnections(payload *connectionsResponse) error {
	now := s.currentTime()
	nowMS := now.UnixMilli()

	s.mu.Lock()
	if payload.UploadTotal < s.lastUploadTotal || payload.DownloadTotal < s.lastDownloadTotal {
		log.Printf("detected Mihomo counter reset, clearing in-memory baselines")
		s.lastConnections = make(map[string]connection)
	}

	s.lastUploadTotal = payload.UploadTotal
	s.lastDownloadTotal = payload.DownloadTotal

	activeIDs := make(map[string]struct{}, len(payload.Connections))
	logs := make([]trafficLog, 0, len(payload.Connections))

	for _, conn := range payload.Connections {
		activeIDs[conn.ID] = struct{}{}

		prev, hasPrev := s.lastConnections[conn.ID]
		uploadDelta := conn.Upload
		downloadDelta := conn.Download

		if hasPrev {
			uploadDelta = conn.Upload - prev.Upload
			downloadDelta = conn.Download - prev.Download
		}

		if uploadDelta < 0 {
			uploadDelta = conn.Upload
		}
		if downloadDelta < 0 {
			downloadDelta = conn.Download
		}
		if uploadDelta == 0 && downloadDelta == 0 {
			s.lastConnections[conn.ID] = conn
			continue
		}

		logs = append(logs, trafficLog{
			Timestamp:     nowMS,
			SourceIP:      defaultString(conn.Metadata.SourceIP, "Inner"),
			Host:          defaultString(firstNonEmpty(conn.Metadata.Host, conn.Metadata.DestinationIP), "Unknown"),
			DestinationIP: strings.TrimSpace(conn.Metadata.DestinationIP),
			Process:       defaultString(conn.Metadata.Process, "Unknown"),
			Outbound:      outboundName(conn.Chains),
			Chains:        sanitizeChains(conn.Chains),
			Upload:        uploadDelta,
			Download:      downloadDelta,
		})

		s.lastConnections[conn.ID] = conn
	}

	for id := range s.lastConnections {
		if _, ok := activeIDs[id]; !ok {
			delete(s.lastConnections, id)
		}
	}
	s.mu.Unlock()

	if len(logs) > 0 {
		if err := s.addToAggregateBuffer(logs, nowMS); err != nil {
			return err
		}
		if err := s.evaluateAutoSwitch(logs, nowMS); err != nil {
			log.Printf("auto switch evaluation: %v", err)
		}
	}
	if err := s.evaluateAutoRestore(nowMS); err != nil {
		log.Printf("auto restore evaluation: %v", err)
	}

	if err := s.flushCompletedAggregateBuckets(nowMS); err != nil {
		log.Printf("flush aggregate buffer: %v", err)
	}

	if now.Sub(s.lastCleanup) >= time.Hour {
		if err := s.cleanupOldLogs(nowMS); err != nil {
			log.Printf("cleanup old logs: %v", err)
		} else {
			s.lastCleanup = now
		}
	}

	return nil
}

func (s *service) evaluateAutoSwitch(logs []trafficLog, nowMS int64) error {
	settings, err := loadAutoSwitchSettings(s.db)
	if err != nil {
		return err
	}
	if !settings.Enabled || settings.ThresholdBytesPerMinute <= 0 {
		return nil
	}

	enabledTargets := enabledAutoSwitchTargets(settings)
	if len(enabledTargets) == 0 {
		return nil
	}

	triggerHost, triggerGroup, triggerTotal, bucketStart, bucketEnd := s.updateHostMinuteWindows(
		logs,
		nowMS,
		settings.ThresholdBytesPerMinute,
		enabledTargets,
	)
	if triggerHost == "" {
		return nil
	}

	if s.isAutoSwitchSuppressed(bucketStart, nowMS, settings.CooldownSeconds) {
		return nil
	}

	results, execErr := s.executeAutoSwitch(settings, enabledTargets[triggerGroup], nowMS, triggerHost, triggerTotal)
	event := autoSwitchEvent{
		TriggeredAt: nowMS,
		Host:        triggerHost,
		TotalBytes:  triggerTotal,
		WindowStart: bucketStart,
		WindowEnd:   bucketEnd,
		Results:     results,
	}
	if execErr != nil {
		event.Error = execErr.Error()
	}
	if err := insertAutoSwitchEvent(s.db, event); err != nil {
		return err
	}

	s.mu.Lock()
	s.lastAutoSwitchAt = nowMS
	s.lastAutoSwitchMin = bucketStart
	s.mu.Unlock()
	return execErr
}

func enabledAutoSwitchTargets(settings autoSwitchSettings) map[string]autoSwitchGroupTarget {
	targets := make(map[string]autoSwitchGroupTarget, len(settings.GroupTargets))
	for _, target := range settings.GroupTargets {
		if !target.Enabled {
			continue
		}
		groupName := strings.TrimSpace(target.GroupName)
		if groupName == "" {
			continue
		}
		targets[groupName] = target
	}
	return targets
}

func (s *service) updateHostMinuteWindows(
	logs []trafficLog,
	nowMS int64,
	threshold int64,
	enabledTargets map[string]autoSwitchGroupTarget,
) (string, string, int64, int64, int64) {
	bucketStart := (nowMS / 60000) * 60000
	bucketEnd := bucketStart + 60000
	if threshold <= 0 {
		return "", "", 0, bucketStart, bucketEnd
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.hostMinuteWindows == nil {
		s.hostMinuteWindows = make(map[string]*hostTrafficWindow)
	}

	for _, entry := range logs {
		total := entry.Upload + entry.Download
		if total <= 0 {
			continue
		}

		target, ok := enabledTargets[strings.TrimSpace(entry.Outbound)]
		if !ok {
			continue
		}

		windowKey := fmt.Sprintf("%s\x00%s", entry.Host, target.GroupName)
		window := s.hostMinuteWindows[windowKey]
		if window == nil || window.BucketStart != bucketStart {
			window = &hostTrafficWindow{
				BucketStart: bucketStart,
				BucketEnd:   bucketEnd,
			}
			s.hostMinuteWindows[windowKey] = window
		}

		previousTotal := window.TotalBytes
		window.TotalBytes += total
		if previousTotal < threshold && window.TotalBytes >= threshold && s.lastAutoSwitchMin != bucketStart {
			return entry.Host, target.GroupName, window.TotalBytes, bucketStart, bucketEnd
		}
	}

	return "", "", 0, bucketStart, bucketEnd
}

func (s *service) isAutoSwitchSuppressed(bucketStart, nowMS, cooldownSeconds int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lastAutoSwitchMin == bucketStart {
		return true
	}
	if cooldownSeconds > 0 && s.lastAutoSwitchAt > 0 && nowMS < s.lastAutoSwitchAt+cooldownSeconds*1000 {
		return true
	}
	return false
}

func (s *service) executeAutoSwitch(
	settings autoSwitchSettings,
	target autoSwitchGroupTarget,
	triggeredAt int64,
	triggerHost string,
	triggerTotal int64,
) ([]autoSwitchExecutionResult, error) {
	mihomo := s.currentMihomoSettings()
	groups, err := s.listControllableProxyGroups(mihomo)
	if err != nil {
		return nil, err
	}

	enabledTargets := enabledAutoSwitchTargets(settings)
	groupByName := make(map[string]controllableProxyGroup, len(groups))
	for _, group := range groups {
		groupByName[group.Name] = group
	}

	results := make([]autoSwitchExecutionResult, 0, 1)
	visited := make(map[string]struct{})
	return s.executeAutoSwitchChain(
		settings,
		mihomo,
		groupByName,
		enabledTargets,
		target,
		triggeredAt,
		triggerHost,
		triggerTotal,
		visited,
		results,
	)
}

func (s *service) executeAutoSwitchChain(
	settings autoSwitchSettings,
	mihomo mihomoSettings,
	groupByName map[string]controllableProxyGroup,
	enabledTargets map[string]autoSwitchGroupTarget,
	target autoSwitchGroupTarget,
	triggeredAt int64,
	triggerHost string,
	triggerTotal int64,
	visited map[string]struct{},
	results []autoSwitchExecutionResult,
) ([]autoSwitchExecutionResult, error) {
	if _, seen := visited[target.GroupName]; seen {
		results = append(results, autoSwitchExecutionResult{
			GroupName:   target.GroupName,
			TargetProxy: target.TargetProxy,
			Status:      "error",
			Message:     "cyclic auto switch target chain",
		})
		return results, nil
	}
	visited[target.GroupName] = struct{}{}

	group, ok := groupByName[target.GroupName]
	if !ok {
		results = append(results, autoSwitchExecutionResult{
			GroupName:   target.GroupName,
			TargetProxy: target.TargetProxy,
			Status:      "error",
			Message:     "group not found",
		})
		return results, nil
	}

	session, hasSession, err := getAutoRestoreSession(s.db, target.GroupName)
	if err != nil {
		return results, err
	}

	if group.Now == target.TargetProxy {
		results = append(results, autoSwitchExecutionResult{
			GroupName:   target.GroupName,
			TargetProxy: target.TargetProxy,
			Status:      "skipped",
			Message:     "already selected",
		})
		if settings.RestoreEnabled && settings.RestoreQuietMinutes > 0 && hasSession {
			session.CurrentProxy = target.TargetProxy
			session.LastTriggeredAt = triggeredAt
			session.LastTriggeredHost = triggerHost
			session.LastTriggeredBytes = triggerTotal
			if err := upsertAutoRestoreSession(s.db, session); err != nil {
				return results, err
			}
		}
		return s.continueAutoSwitchChainIfNeeded(
			settings,
			mihomo,
			groupByName,
			enabledTargets,
			target.TargetProxy,
			triggeredAt,
			triggerHost,
			triggerTotal,
			visited,
			results,
		)
	}

	if err := s.switchProxyGroup(mihomo, group, target.TargetProxy); err != nil {
		results = append(results, autoSwitchExecutionResult{
			GroupName:   target.GroupName,
			TargetProxy: target.TargetProxy,
			Status:      "error",
			Message:     err.Error(),
		})
		return results, nil
	}

	results = append(results, autoSwitchExecutionResult{
		GroupName:   target.GroupName,
		TargetProxy: target.TargetProxy,
		Status:      "switched",
	})

	if settings.RestoreEnabled && settings.RestoreQuietMinutes > 0 {
		originalProxy := group.Now
		if hasSession && strings.TrimSpace(session.OriginalProxy) != "" {
			originalProxy = session.OriginalProxy
		}
		if err := upsertAutoRestoreSession(s.db, autoRestoreSession{
			GroupName:          target.GroupName,
			OriginalProxy:      originalProxy,
			CurrentProxy:       target.TargetProxy,
			LastTriggeredAt:    triggeredAt,
			LastTriggeredHost:  triggerHost,
			LastTriggeredBytes: triggerTotal,
		}); err != nil {
			return results, err
		}
	}

	group.Now = target.TargetProxy
	groupByName[target.GroupName] = group
	return s.continueAutoSwitchChainIfNeeded(
		settings,
		mihomo,
		groupByName,
		enabledTargets,
		target.TargetProxy,
		triggeredAt,
		triggerHost,
		triggerTotal,
		visited,
		results,
	)
}

func (s *service) continueAutoSwitchChainIfNeeded(
	settings autoSwitchSettings,
	mihomo mihomoSettings,
	groupByName map[string]controllableProxyGroup,
	enabledTargets map[string]autoSwitchGroupTarget,
	nextGroupName string,
	triggeredAt int64,
	triggerHost string,
	triggerTotal int64,
	visited map[string]struct{},
	results []autoSwitchExecutionResult,
) ([]autoSwitchExecutionResult, error) {
	nextTarget, ok := enabledTargets[strings.TrimSpace(nextGroupName)]
	if !ok {
		return results, nil
	}

	return s.executeAutoSwitchChain(
		settings,
		mihomo,
		groupByName,
		enabledTargets,
		nextTarget,
		triggeredAt,
		triggerHost,
		triggerTotal,
		visited,
		results,
	)
}

func autoRestoreEligible(nowMS, lastTriggeredAt, quietMinutes int64) bool {
	if lastTriggeredAt <= 0 || quietMinutes <= 0 {
		return false
	}

	currentBucketStart := (nowMS / 60000) * 60000
	triggerBucketStart := (lastTriggeredAt / 60000) * 60000
	return currentBucketStart >= triggerBucketStart+quietMinutes*60000
}

func (s *service) evaluateAutoRestore(nowMS int64) error {
	settings, err := loadAutoSwitchSettings(s.db)
	if err != nil {
		return err
	}
	if !settings.RestoreEnabled || settings.RestoreQuietMinutes <= 0 {
		return nil
	}

	sessions, err := listAutoRestoreSessions(s.db)
	if err != nil {
		return err
	}
	if len(sessions) == 0 {
		return nil
	}

	pending := make([]autoRestoreSession, 0, len(sessions))
	for _, session := range sessions {
		if autoRestoreEligible(nowMS, session.LastTriggeredAt, settings.RestoreQuietMinutes) {
			pending = append(pending, session)
		}
	}
	if len(pending) == 0 {
		return nil
	}

	mihomo := s.currentMihomoSettings()
	groups, err := s.listControllableProxyGroups(mihomo)
	if err != nil {
		return err
	}

	groupByName := make(map[string]controllableProxyGroup, len(groups))
	for _, group := range groups {
		groupByName[group.Name] = group
	}

	results := make([]autoSwitchExecutionResult, 0, len(pending))
	var eventErrs []string
	for _, session := range pending {
		group, ok := groupByName[session.GroupName]
		if !ok {
			results = append(results, autoSwitchExecutionResult{
				GroupName:   session.GroupName,
				TargetProxy: session.OriginalProxy,
				Status:      "restore_error",
				Message:     "group not found",
			})
			eventErrs = append(eventErrs, fmt.Sprintf("%s: group not found", session.GroupName))
			if err := deleteAutoRestoreSession(s.db, session.GroupName); err != nil {
				return err
			}
			continue
		}

		if group.Now != session.CurrentProxy {
			results = append(results, autoSwitchExecutionResult{
				GroupName:   session.GroupName,
				TargetProxy: session.OriginalProxy,
				Status:      "restore_skipped",
				Message:     "selection changed manually",
			})
			if err := deleteAutoRestoreSession(s.db, session.GroupName); err != nil {
				return err
			}
			continue
		}

		if !containsString(group.All, session.OriginalProxy) {
			results = append(results, autoSwitchExecutionResult{
				GroupName:   session.GroupName,
				TargetProxy: session.OriginalProxy,
				Status:      "restore_error",
				Message:     "original proxy is no longer available",
			})
			eventErrs = append(eventErrs, fmt.Sprintf("%s: original proxy unavailable", session.GroupName))
			if err := deleteAutoRestoreSession(s.db, session.GroupName); err != nil {
				return err
			}
			continue
		}

		if err := s.switchProxyGroup(mihomo, group, session.OriginalProxy); err != nil {
			results = append(results, autoSwitchExecutionResult{
				GroupName:   session.GroupName,
				TargetProxy: session.OriginalProxy,
				Status:      "restore_error",
				Message:     err.Error(),
			})
			eventErrs = append(eventErrs, fmt.Sprintf("%s: %v", session.GroupName, err))
			if err := deleteAutoRestoreSession(s.db, session.GroupName); err != nil {
				return err
			}
			continue
		}

		results = append(results, autoSwitchExecutionResult{
			GroupName:   session.GroupName,
			TargetProxy: session.OriginalProxy,
			Status:      "restored",
		})
		if err := deleteAutoRestoreSession(s.db, session.GroupName); err != nil {
			return err
		}
	}

	if len(results) == 0 {
		return nil
	}

	bucketStart := (nowMS / 60000) * 60000
	event := autoSwitchEvent{
		TriggeredAt: nowMS,
		Host:        "",
		TotalBytes:  0,
		WindowStart: bucketStart,
		WindowEnd:   bucketStart + 60000,
		Results:     results,
	}
	if len(eventErrs) > 0 {
		event.Error = strings.Join(eventErrs, "; ")
	}

	return insertAutoSwitchEvent(s.db, event)
}

func (s *service) insertLogs(logs []trafficLog) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(`
		INSERT INTO traffic_logs (timestamp, source_ip, host, destination_ip, process, outbound, chains, upload, download)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	for _, entry := range logs {
		chainsJSON, err := json.Marshal(sanitizeChains(entry.Chains))
		if err != nil {
			tx.Rollback()
			return err
		}

		if _, err := stmt.Exec(
			entry.Timestamp,
			entry.SourceIP,
			entry.Host,
			entry.DestinationIP,
			entry.Process,
			entry.Outbound,
			string(chainsJSON),
			entry.Upload,
			entry.Download,
		); err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

func (s *service) cleanupOldLogs(nowMS int64) error {
	if _, err := s.db.Exec(`DELETE FROM traffic_logs`); err != nil {
		return err
	}

	aggCutoff := nowMS - int64(aggregateRetention/time.Millisecond)
	if _, err := s.db.Exec(`DELETE FROM traffic_aggregated WHERE bucket_end < ?`, aggCutoff); err != nil {
		return err
	}

	// 定期执行VACUUM（每周一次）
	if time.Since(s.lastVacuum) >= 7*24*time.Hour {
		if _, err := s.db.Exec(`VACUUM`); err != nil {
			log.Printf("VACUUM failed: %v", err)
		} else {
			s.lastVacuum = time.Now()
		}
	}

	return nil
}

// normalizeHost returns the eTLD+1 of host, stripping subdomains.
// Bare IPs and single-label names are returned unchanged.
// This is the sole seam for a future Public Suffix List upgrade.
func normalizeHost(host string) string {
	// Strip port if present (e.g. "example.com:443").
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	// Return bare IPs unchanged.
	if net.ParseIP(host) != nil {
		return host
	}
	parts := strings.Split(host, ".")
	if len(parts) <= 2 {
		return host
	}
	return parts[len(parts)-2] + "." + parts[len(parts)-1]
}

func (s *service) addToAggregateBuffer(logs []trafficLog, nowMS int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	bucketStart := (nowMS / 60000) * 60000 // 1分钟桶
	bucketEnd := bucketStart + 60000

	for _, log := range logs {
		key := fmt.Sprintf("%d-%s-%s-%s-%s-%s-%s", bucketStart, log.SourceIP, log.Host, log.DestinationIP, log.Process, log.Outbound, strings.Join(log.Chains, ","))

		if entry, exists := s.aggregateBuffer[key]; exists {
			entry.Upload += log.Upload
			entry.Download += log.Download
			entry.Count++
		} else {
			chainsJSON, err := json.Marshal(sanitizeChains(log.Chains))
			if err != nil {
				return err
			}
			s.aggregateBuffer[key] = &aggregatedEntry{
				BucketStart:   bucketStart,
				BucketEnd:     bucketEnd,
				SourceIP:      log.SourceIP,
				Host:          log.Host,
				DestinationIP: log.DestinationIP,
				Process:       log.Process,
				Outbound:      log.Outbound,
				Chains:        string(chainsJSON),
				Upload:        log.Upload,
				Download:      log.Download,
				Count:         1,
			}
		}
	}

	return nil
}

func (s *service) flushCompletedAggregateBuckets(nowMS int64) error {
	currentBucketStart := (nowMS / int64(aggregateFlushInterval/time.Millisecond)) * int64(aggregateFlushInterval/time.Millisecond)
	return s.flushAggregateEntries(func(entry *aggregatedEntry) bool {
		return entry.BucketEnd <= currentBucketStart
	})
}

func (s *service) flushAggregateBuffer() error {
	return s.flushAggregateEntries(func(*aggregatedEntry) bool {
		return true
	})
}

func (s *service) flushAggregateEntries(shouldFlush func(*aggregatedEntry) bool) error {
	buffer := s.snapshotAggregateEntries(shouldFlush)
	if len(buffer) == 0 {
		return nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(`
		INSERT INTO traffic_aggregated
		(bucket_start, bucket_end, source_ip, host, destination_ip, process, outbound, chains, upload, download, count)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(bucket_start, bucket_end, source_ip, host, destination_ip, process, outbound, chains)
		DO UPDATE SET
			upload = traffic_aggregated.upload + excluded.upload,
			download = traffic_aggregated.download + excluded.download,
			count = traffic_aggregated.count + excluded.count
	`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	for _, entry := range buffer {
		if _, err := stmt.Exec(
			entry.BucketStart, entry.BucketEnd, entry.SourceIP, entry.Host, entry.DestinationIP,
			entry.Process, entry.Outbound, entry.Chains, entry.Upload, entry.Download, entry.Count,
		); err != nil {
			tx.Rollback()
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for key := range buffer {
		delete(s.aggregateBuffer, key)
	}
	return nil
}

func (s *service) snapshotAggregateEntries(shouldFlush func(*aggregatedEntry) bool) map[string]aggregatedEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	buffer := make(map[string]aggregatedEntry)
	for key, entry := range s.aggregateBuffer {
		if shouldFlush(entry) {
			buffer[key] = *entry
		}
	}
	return buffer
}

func (s *service) routes() http.Handler {
	mux := http.NewServeMux()
	staticFS, err := fs.Sub(webAssets, "web")
	if err != nil {
		panic(err)
	}
	fileServer := http.FileServer(http.FS(staticFS))
	mux.HandleFunc("/LICENSE", s.handleLicense)
	mux.Handle("/", fileServer)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/api/settings/mihomo", s.handleMihomoSettings)
	mux.HandleFunc("/api/auto-switch/settings", s.handleAutoSwitchSettings)
	mux.HandleFunc("/api/auto-switch/groups", s.handleAutoSwitchGroups)
	mux.HandleFunc("/api/auto-switch/events", s.handleAutoSwitchEvents)
	mux.HandleFunc("/api/traffic/aggregate", s.handleAggregate)
	mux.HandleFunc("/api/traffic/substats", s.handleSubstats)
	mux.HandleFunc("/api/traffic/proxy-stats", s.handleProxyStats)
	mux.HandleFunc("/api/traffic/devices-by-host", s.handleDevicesByHost)
	mux.HandleFunc("/api/traffic/devices-by-proxy-host", s.handleDevicesByProxyHost)
	mux.HandleFunc("/api/traffic/details", s.handleConnectionDetails)
	mux.HandleFunc("/api/traffic/trend", s.handleTrend)
	mux.HandleFunc("/api/traffic/logs", s.handleLogs)
	return s.withCORS(mux)
}

func (s *service) withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", defaultAllowedOrigin)
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, DELETE, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *service) handleLicense(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeMethodNotAllowed(w)
		return
	}

	content, err := webAssets.ReadFile("LICENSE")
	if err != nil {
		http.Error(w, "license unavailable", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	http.ServeContent(w, r, "LICENSE", time.Time{}, bytes.NewReader(content))
}

func (s *service) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *service) handleMihomoSettings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, s.currentMihomoSettings())
	case http.MethodPut:
		var payload mihomoSettings
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeError(w, http.StatusBadRequest, errors.New("invalid json body"))
			return
		}

		payload = normalizeMihomoSettings(payload)
		if payload.URL == "" {
			writeError(w, http.StatusBadRequest, errors.New("mihomo url is required"))
			return
		}

		if err := s.updateMihomoSettings(payload); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		writeJSON(w, http.StatusOK, s.currentMihomoSettings())
	default:
		writeMethodNotAllowed(w)
	}
}

func validateAutoSwitchSettings(settings autoSwitchSettings) error {
	if settings.Enabled && settings.ThresholdBytesPerMinute <= 0 {
		return errors.New("thresholdBytesPerMinute must be greater than 0 when auto switch is enabled")
	}
	if settings.CooldownSeconds < 0 {
		return errors.New("cooldownSeconds must be 0 or greater")
	}
	if settings.RestoreQuietMinutes < 0 {
		return errors.New("restoreQuietMinutes must be 0 or greater")
	}

	for _, target := range settings.GroupTargets {
		if strings.TrimSpace(target.GroupName) == "" {
			return errors.New("groupName is required")
		}
		if strings.TrimSpace(target.TargetProxy) == "" {
			return errors.New("targetProxy is required")
		}
	}
	return nil
}

func (s *service) handleAutoSwitchSettings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		settings, err := loadAutoSwitchSettings(s.db)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, settings)
	case http.MethodPut:
		var payload autoSwitchSettings
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeError(w, http.StatusBadRequest, errors.New("invalid json body"))
			return
		}
		if err := validateAutoSwitchSettings(payload); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if err := saveAutoSwitchSettings(s.db, payload); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, payload)
	default:
		writeMethodNotAllowed(w)
	}
}

func (s *service) handleAutoSwitchGroups(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}

	groups, err := s.listControllableProxyGroups(s.currentMihomoSettings())
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, groups)
}

func (s *service) handleAutoSwitchEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}

	events, err := listAutoSwitchEvents(s.db, 20)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, events)
}

func (s *service) handleAggregate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}

	dimension := r.URL.Query().Get("dimension")
	start, end, err := parseTimeRange(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	data, err := s.queryAggregate(dimension, start, end)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	writeJSON(w, http.StatusOK, data)
}

func (s *service) handleSubstats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}

	dimension := r.URL.Query().Get("dimension")
	label := r.URL.Query().Get("label")
	start, end, err := parseTimeRange(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if label == "" {
		writeError(w, http.StatusBadRequest, errors.New("label is required"))
		return
	}

	data, err := s.querySubstats(dimension, label, start, end)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, data)
}

func (s *service) handleProxyStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}

	dimension := r.URL.Query().Get("dimension")
	parentLabel := r.URL.Query().Get("parentLabel")
	host := r.URL.Query().Get("host")
	start, end, err := parseTimeRange(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if parentLabel == "" || host == "" {
		writeError(w, http.StatusBadRequest, errors.New("parentLabel and host are required"))
		return
	}

	data, err := s.queryProxyStats(dimension, parentLabel, host, start, end)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, data)
}

func (s *service) handleDevicesByHost(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}

	host := r.URL.Query().Get("host")
	start, end, err := parseTimeRange(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if host == "" {
		writeError(w, http.StatusBadRequest, errors.New("host is required"))
		return
	}

	data, err := s.queryByFilters("source_ip", "host = ?", []any{host}, start, end)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, data)
}

func (s *service) handleDevicesByProxyHost(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}

	proxy := r.URL.Query().Get("proxy")
	host := r.URL.Query().Get("host")
	start, end, err := parseTimeRange(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if proxy == "" || host == "" {
		writeError(w, http.StatusBadRequest, errors.New("proxy and host are required"))
		return
	}

	data, err := s.queryByFilters(
		"source_ip",
		"outbound = ? AND host = ?",
		[]any{proxy, host},
		start,
		end,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, data)
}

func (s *service) handleTrend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}

	start, end, err := parseTimeRange(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	bucket := parseInt64(r.URL.Query().Get("bucket"), 60000)
	if bucket <= 0 {
		writeError(w, http.StatusBadRequest, errors.New("bucket must be positive"))
		return
	}

	data, err := s.queryTrend(start, end, bucket)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, data)
}

func (s *service) handleConnectionDetails(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}

	dimension := r.URL.Query().Get("dimension")
	primary := r.URL.Query().Get("primary")
	secondary := r.URL.Query().Get("secondary")
	start, end, err := parseTimeRange(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if primary == "" || secondary == "" {
		writeError(w, http.StatusBadRequest, errors.New("primary and secondary are required"))
		return
	}

	data, err := s.queryConnectionDetails(dimension, primary, secondary, start, end)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, data)
}

func (s *service) handleLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		writeMethodNotAllowed(w)
		return
	}

	tx, err := s.db.Begin()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if _, err := tx.Exec(`DELETE FROM traffic_logs`); err != nil {
		tx.Rollback()
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if _, err := tx.Exec(`DELETE FROM traffic_aggregated`); err != nil {
		tx.Rollback()
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	s.mu.Lock()
	s.aggregateBuffer = make(map[string]*aggregatedEntry)
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *service) queryAggregate(dimension string, start, end int64) ([]aggregatedData, error) {
	column, err := dimensionColumn(dimension)
	if err != nil {
		return nil, err
	}
	return s.queryByFilters(column, "", nil, start, end)
}

func (s *service) querySubstats(dimension, label string, start, end int64) ([]aggregatedData, error) {
	column, err := dimensionColumn(dimension)
	if err != nil {
		return nil, err
	}
	if column == "host" {
		return nil, errors.New("host is not supported for substats")
	}
	return s.queryByFilters("host", column+" = ?", []any{label}, start, end)
}

func (s *service) queryProxyStats(dimension, parentLabel, host string, start, end int64) ([]aggregatedData, error) {
	column, err := dimensionColumn(dimension)
	if err != nil {
		return nil, err
	}
	if column == "host" {
		return nil, errors.New("host is not supported for proxy stats")
	}
	return s.queryByFilters("outbound", column+" = ? AND host = ?", []any{parentLabel, host}, start, end)
}

func (s *service) queryByFilters(groupColumn, extraFilter string, extraArgs []any, start, end int64) ([]aggregatedData, error) {
	merged := make(map[string]*aggregatedData)

	items, err := s.queryByFiltersFromAggregates(groupColumn, extraFilter, extraArgs, start, end)
	if err != nil {
		return nil, err
	}
	mergeAggregatedDataRows(merged, items)

	mergeAggregatedDataRows(merged, s.queryByFiltersFromBuffer(groupColumn, extraFilter, extraArgs, start, end))

	return sortedAggregatedDataRows(merged), nil
}

func (s *service) queryConnectionDetails(dimension, primary, secondary string, start, end int64) ([]connectionDetail, error) {
	filter, args, err := detailFilter(dimension, primary, secondary)
	if err != nil {
		return nil, err
	}

	rows, err := s.db.Query(`
		SELECT destination_ip,
		       source_ip,
		       process,
		       outbound,
		       chains,
		       COALESCE(SUM(upload), 0) AS upload,
		       COALESCE(SUM(download), 0) AS download,
		       COALESCE(SUM(upload + download), 0) AS total,
		       COALESCE(SUM(count), 0) AS count
		FROM traffic_aggregated
		WHERE bucket_end > ? AND bucket_start <= ?
		  AND `+filter+`
		GROUP BY destination_ip, source_ip, process, outbound, chains
	`, append([]any{start, end}, args...)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	merged := make(map[string]*connectionDetail)
	for rows.Next() {
		var (
			item      connectionDetail
			chainsRaw string
		)
		if err := rows.Scan(
			&item.DestinationIP,
			&item.SourceIP,
			&item.Process,
			&item.Outbound,
			&chainsRaw,
			&item.Upload,
			&item.Download,
			&item.Total,
			&item.Count,
		); err != nil {
			return nil, err
		}
		item.Chains = parseChains(chainsRaw)
		mergeConnectionDetailRows(merged, []connectionDetail{item})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	mergeConnectionDetailRows(merged, s.queryConnectionDetailsFromBuffer(filter, args, start, end))
	return sortedConnectionDetailRows(merged), nil
}

func (s *service) queryTrend(start, end, bucket int64) ([]trendPoint, error) {
	buckets := make(map[int64]trendPoint)

	items, err := s.queryTrendFromAggregates(start, end, bucket)
	if err != nil {
		return nil, err
	}
	mergeTrendPoints(buckets, items)
	mergeTrendPoints(buckets, s.queryTrendFromBuffer(start, end, bucket))

	points := make([]trendPoint, 0, (end-start)/bucket+1)
	for t := start; t <= end; t += bucket {
		key := (t / bucket) * bucket
		if point, ok := buckets[key]; ok {
			points = append(points, point)
			continue
		}
		points = append(points, trendPoint{Timestamp: key})
	}
	return points, nil
}

func (s *service) queryByFiltersFromRaw(groupColumn, extraFilter string, extraArgs []any, start, end int64) ([]aggregatedData, error) {
	return s.queryByFiltersFromTable(
		"traffic_logs",
		"timestamp BETWEEN ? AND ?",
		[]any{start, end},
		groupColumn,
		extraFilter,
		extraArgs,
		"COUNT(*)",
	)
}

func (s *service) queryByFiltersFromAggregates(groupColumn, extraFilter string, extraArgs []any, start, endExclusive int64) ([]aggregatedData, error) {
	return s.queryByFiltersFromTable(
		"traffic_aggregated",
		"bucket_end > ? AND bucket_start <= ?",
		[]any{start, endExclusive},
		groupColumn,
		extraFilter,
		extraArgs,
		"COALESCE(SUM(count), 0)",
	)
}

func (s *service) queryByFiltersFromBuffer(groupColumn, extraFilter string, extraArgs []any, start, end int64) []aggregatedData {
	items := s.snapshotAggregateEntries(func(entry *aggregatedEntry) bool {
		return aggregateEntryOverlapsRange(*entry, start, end)
	})

	merged := make(map[string]*aggregatedData)
	for _, entry := range items {
		if !matchesAggregateEntryFilters(entry, extraFilter, extraArgs) {
			continue
		}
		label := aggregateEntryFieldValue(entry, groupColumn)
		if label == "" {
			continue
		}
		row, ok := merged[label]
		if !ok {
			row = &aggregatedData{Label: label}
			merged[label] = row
		}
		row.Upload += entry.Upload
		row.Download += entry.Download
		row.Total += entry.Upload + entry.Download
		row.Count += entry.Count
	}

	return sortedAggregatedDataRows(merged)
}

func (s *service) queryByFiltersFromTable(table, timeFilter string, timeArgs []any, groupColumn, extraFilter string, extraArgs []any, countExpr string) ([]aggregatedData, error) {
	base := `
		SELECT ` + groupColumn + ` AS label,
		       COALESCE(SUM(upload), 0) AS upload,
		       COALESCE(SUM(download), 0) AS download,
		       COALESCE(SUM(upload + download), 0) AS total,
		       ` + countExpr + ` AS count
		FROM ` + table + `
		WHERE ` + timeFilter

	args := append([]any{}, timeArgs...)
	if extraFilter != "" {
		base += " AND " + extraFilter
		args = append(args, extraArgs...)
	}
	base += `
		GROUP BY ` + groupColumn + `
		ORDER BY total DESC, label ASC
	`

	rows, err := s.db.Query(base, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]aggregatedData, 0)
	for rows.Next() {
		var item aggregatedData
		if err := rows.Scan(&item.Label, &item.Upload, &item.Download, &item.Total, &item.Count); err != nil {
			return nil, err
		}
		results = append(results, item)
	}
	return results, rows.Err()
}

func (s *service) queryTrendFromRaw(start, end, bucket int64) ([]trendPoint, error) {
	rows, err := s.db.Query(`
		SELECT ((timestamp / ?) * ?) AS bucket_start,
		       COALESCE(SUM(upload), 0) AS upload,
		       COALESCE(SUM(download), 0) AS download
		FROM traffic_logs
		WHERE timestamp BETWEEN ? AND ?
		GROUP BY bucket_start
		ORDER BY bucket_start ASC
	`, bucket, bucket, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]trendPoint, 0)
	for rows.Next() {
		var point trendPoint
		if err := rows.Scan(&point.Timestamp, &point.Upload, &point.Download); err != nil {
			return nil, err
		}
		results = append(results, point)
	}
	return results, rows.Err()
}

func (s *service) queryTrendFromAggregates(start, endExclusive, bucket int64) ([]trendPoint, error) {
	rows, err := s.db.Query(`
		SELECT ((bucket_start / ?) * ?) AS bucket_start,
		       COALESCE(SUM(upload), 0) AS upload,
		       COALESCE(SUM(download), 0) AS download
		FROM traffic_aggregated
		WHERE bucket_end > ? AND bucket_start <= ?
		GROUP BY bucket_start
		ORDER BY bucket_start ASC
	`, bucket, bucket, start, endExclusive)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]trendPoint, 0)
	for rows.Next() {
		var point trendPoint
		if err := rows.Scan(&point.Timestamp, &point.Upload, &point.Download); err != nil {
			return nil, err
		}
		results = append(results, point)
	}
	return results, rows.Err()
}

func (s *service) queryTrendFromBuffer(start, end, bucket int64) []trendPoint {
	items := s.snapshotAggregateEntries(func(entry *aggregatedEntry) bool {
		return aggregateEntryOverlapsRange(*entry, start, end)
	})

	merged := make(map[int64]trendPoint)
	for _, entry := range items {
		key := (entry.BucketStart / bucket) * bucket
		point := merged[key]
		point.Timestamp = key
		point.Upload += entry.Upload
		point.Download += entry.Download
		merged[key] = point
	}

	results := make([]trendPoint, 0, len(merged))
	for _, item := range merged {
		results = append(results, item)
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Timestamp < results[j].Timestamp
	})
	return results
}

func (s *service) queryConnectionDetailsFromBuffer(filter string, args []any, start, end int64) []connectionDetail {
	items := s.snapshotAggregateEntries(func(entry *aggregatedEntry) bool {
		return aggregateEntryOverlapsRange(*entry, start, end)
	})

	merged := make(map[string]*connectionDetail)
	for _, entry := range items {
		if !matchesAggregateEntryFilters(entry, filter, args) {
			continue
		}
		item := connectionDetail{
			DestinationIP: entry.DestinationIP,
			SourceIP:      entry.SourceIP,
			Process:       entry.Process,
			Outbound:      entry.Outbound,
			Chains:        parseChains(entry.Chains),
			Upload:        entry.Upload,
			Download:      entry.Download,
			Total:         entry.Upload + entry.Download,
			Count:         entry.Count,
		}
		mergeConnectionDetailRows(merged, []connectionDetail{item})
	}

	return sortedConnectionDetailRows(merged)
}

func aggregateEntryOverlapsRange(entry aggregatedEntry, start, end int64) bool {
	return entry.BucketEnd > start && entry.BucketStart <= end
}

func matchesAggregateEntryFilters(entry aggregatedEntry, filter string, args []any) bool {
	if filter == "" {
		return true
	}

	clauses := strings.Split(filter, " AND ")
	if len(clauses) != len(args) {
		return false
	}

	for i, clause := range clauses {
		column := strings.TrimSpace(strings.TrimSuffix(clause, "= ?"))
		column = strings.TrimSpace(strings.TrimSuffix(column, " = ?"))
		if column == "" {
			return false
		}
		if aggregateEntryFieldValue(entry, column) != fmt.Sprint(args[i]) {
			return false
		}
	}

	return true
}

func aggregateEntryFieldValue(entry aggregatedEntry, column string) string {
	switch column {
	case "source_ip":
		return entry.SourceIP
	case "host":
		return entry.Host
	case "destination_ip":
		return entry.DestinationIP
	case "process":
		return entry.Process
	case "outbound":
		return entry.Outbound
	case "chains":
		return entry.Chains
	default:
		return ""
	}
}

func fullMinuteBucketRange(start, end int64) (int64, int64) {
	aggregateStart := ((start + 60000 - 1) / 60000) * 60000
	aggregateEndExclusive := ((end + 1) / 60000) * 60000
	if aggregateStart < 0 {
		aggregateStart = 0
	}
	if aggregateEndExclusive < 0 {
		aggregateEndExclusive = 0
	}
	if aggregateStart > aggregateEndExclusive {
		return aggregateStart, aggregateStart
	}
	return aggregateStart, aggregateEndExclusive
}

func mergeAggregatedDataRows(target map[string]*aggregatedData, items []aggregatedData) {
	for _, item := range items {
		existing, ok := target[item.Label]
		if !ok {
			copyItem := item
			target[item.Label] = &copyItem
			continue
		}
		existing.Upload += item.Upload
		existing.Download += item.Download
		existing.Total += item.Total
		existing.Count += item.Count
	}
}

func sortedAggregatedDataRows(items map[string]*aggregatedData) []aggregatedData {
	results := make([]aggregatedData, 0, len(items))
	for _, item := range items {
		results = append(results, *item)
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].Total == results[j].Total {
			return results[i].Label < results[j].Label
		}
		return results[i].Total > results[j].Total
	})
	return results
}

func mergeConnectionDetailRows(target map[string]*connectionDetail, items []connectionDetail) {
	for _, item := range items {
		key := connectionDetailKey(item)
		existing, ok := target[key]
		if !ok {
			copyItem := item
			target[key] = &copyItem
			continue
		}
		existing.Upload += item.Upload
		existing.Download += item.Download
		existing.Total += item.Total
		existing.Count += item.Count
	}
}

func sortedConnectionDetailRows(items map[string]*connectionDetail) []connectionDetail {
	results := make([]connectionDetail, 0, len(items))
	for _, item := range items {
		results = append(results, *item)
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].Total == results[j].Total {
			if results[i].DestinationIP == results[j].DestinationIP {
				if results[i].SourceIP == results[j].SourceIP {
					return results[i].Process < results[j].Process
				}
				return results[i].SourceIP < results[j].SourceIP
			}
			return results[i].DestinationIP < results[j].DestinationIP
		}
		return results[i].Total > results[j].Total
	})
	return results
}

func connectionDetailKey(item connectionDetail) string {
	return strings.Join([]string{
		item.DestinationIP,
		item.SourceIP,
		item.Process,
		item.Outbound,
		strings.Join(item.Chains, "\x1f"),
	}, "\x00")
}

func mergeTrendPoints(target map[int64]trendPoint, items []trendPoint) {
	for _, item := range items {
		existing := target[item.Timestamp]
		existing.Timestamp = item.Timestamp
		existing.Upload += item.Upload
		existing.Download += item.Download
		target[item.Timestamp] = existing
	}
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func parseTimeRange(r *http.Request) (int64, int64, error) {
	start := parseInt64(r.URL.Query().Get("start"), 0)
	end := parseInt64(r.URL.Query().Get("end"), 0)
	if start <= 0 || end <= 0 {
		return 0, 0, errors.New("start and end are required")
	}
	if end < start {
		return 0, 0, errors.New("end must be greater than or equal to start")
	}
	return start, end, nil
}

func dimensionColumn(dimension string) (string, error) {
	switch dimension {
	case "sourceIP":
		return "source_ip", nil
	case "host":
		return "host", nil
	case "process":
		return "process", nil
	case "outbound":
		return "outbound", nil
	default:
		return "", fmt.Errorf("unsupported dimension %q", dimension)
	}
}

func detailFilter(dimension, primary, secondary string) (string, []any, error) {
	switch dimension {
	case "sourceIP":
		return "source_ip = ? AND host = ?", []any{primary, secondary}, nil
	case "host":
		return "host = ? AND source_ip = ?", []any{primary, secondary}, nil
	case "outbound":
		return "outbound = ? AND host = ?", []any{primary, secondary}, nil
	case "process":
		return "process = ? AND host = ?", []any{primary, secondary}, nil
	default:
		return "", nil, fmt.Errorf("unsupported dimension %q", dimension)
	}
}

func outboundName(chains []string) string {
	if len(chains) == 0 || chains[0] == "" {
		return "DIRECT"
	}
	return chains[0]
}

func sanitizeChains(chains []string) []string {
	if len(chains) == 0 {
		return []string{"DIRECT"}
	}

	cleaned := make([]string, 0, len(chains))
	for _, chain := range chains {
		chain = strings.TrimSpace(chain)
		if chain != "" {
			cleaned = append(cleaned, chain)
		}
	}
	if len(cleaned) == 0 {
		return []string{"DIRECT"}
	}
	return cleaned
}

func parseChains(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return []string{"DIRECT"}
	}

	var chains []string
	if err := json.Unmarshal([]byte(raw), &chains); err != nil {
		return []string{raw}
	}
	return sanitizeChains(chains)
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func getenv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func parseInt64(value string, fallback int64) int64 {
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func writeMethodNotAllowed(w http.ResponseWriter) {
	writeError(w, http.StatusMethodNotAllowed, errors.New("method not allowed"))
}
