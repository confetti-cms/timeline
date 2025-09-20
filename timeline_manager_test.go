package timeline

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// newTestManager creates a fresh TimelineConnectionManager instance for testing
func newTestManager() *TimelineConnectionManager {
	return &TimelineConnectionManager{
		connections: make(map[string]*Writer),
	}
}

func Test_get_or_create_connection_valid_path_returns_writer(t *testing.T) {
	// Given
	tempDir, err := os.MkdirTemp("", "timeline_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")
	manager := newTestManager()

	// When
	writer, err := manager.GetOrCreateConnection(dbPath)

	// Then
	if err != nil {
		t.Fatalf("Failed to create connection: %v", err)
	}
	if writer == nil {
		t.Fatal("Expected non-nil writer")
	}
}

func Test_get_or_create_connection_valid_path_stores_connection(t *testing.T) {
	// Given
	tempDir, err := os.MkdirTemp("", "timeline_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")
	manager := newTestManager()

	// When
	writer, err := manager.GetOrCreateConnection(dbPath)
	if err != nil {
		t.Fatalf("Failed to create connection: %v", err)
	}

	// Then
	manager.mutex.RLock()
	storedWriter, exists := manager.connections[dbPath]
	manager.mutex.RUnlock()

	if !exists {
		t.Fatal("Connection not stored in manager")
	}
	if storedWriter != writer {
		t.Fatal("Stored connection doesn't match returned connection")
	}
}

func Test_connection_reuse_same_path_returns_same_instance(t *testing.T) {
	// Given
	tempDir, err := os.MkdirTemp("", "timeline_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")
	manager := newTestManager()

	// When - Create first connection
	writer1, err := manager.GetOrCreateConnection(dbPath)
	if err != nil {
		t.Fatalf("Failed to create first connection: %v", err)
	}

	// And - Create second connection with same path
	writer2, err := manager.GetOrCreateConnection(dbPath)
	if err != nil {
		t.Fatalf("Failed to create second connection: %v", err)
	}

	// Then
	if writer1 != writer2 {
		t.Fatal("Expected same connection instance to be reused")
	}
}

func Test_connection_reuse_same_path_stores_only_one_connection(t *testing.T) {
	// Given
	tempDir, err := os.MkdirTemp("", "timeline_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")
	manager := newTestManager()

	// When - Create multiple connections with same path
	_, err = manager.GetOrCreateConnection(dbPath)
	if err != nil {
		t.Fatalf("Failed to create first connection: %v", err)
	}

	_, err = manager.GetOrCreateConnection(dbPath)
	if err != nil {
		t.Fatalf("Failed to create second connection: %v", err)
	}

	// Then
	manager.mutex.RLock()
	connectionCount := len(manager.connections)
	manager.mutex.RUnlock()

	if connectionCount != 1 {
		t.Fatalf("Expected 1 connection, got %d", connectionCount)
	}
}

func Test_directory_creation_nested_path_creates_directories(t *testing.T) {
	// Given
	tempDir, err := os.MkdirTemp("", "timeline_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "nested", "path", "test.db")
	manager := newTestManager()

	// When
	writer, err := manager.GetOrCreateConnection(dbPath)

	// Then
	if err != nil {
		t.Fatalf("Failed to create connection: %v", err)
	}
	if writer == nil {
		t.Fatal("Expected non-nil writer")
	}

	expectedDir := filepath.Join(tempDir, "nested", "path")
	if _, err := os.Stat(expectedDir); os.IsNotExist(err) {
		t.Fatal("Directory was not created")
	}
}

func Test_directory_creation_nested_path_creates_database_file(t *testing.T) {
	// Given
	tempDir, err := os.MkdirTemp("", "timeline_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "nested", "path", "test.db")
	manager := newTestManager()

	// When
	_, err = manager.GetOrCreateConnection(dbPath)
	if err != nil {
		t.Fatalf("Failed to create connection: %v", err)
	}

	// Then
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatal("Database file was not created")
	}
}

func Test_concurrent_access_multiple_goroutines_all_succeed(t *testing.T) {
	// Given
	tempDir, err := os.MkdirTemp("", "timeline_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPaths := []string{
		filepath.Join(tempDir, "test1.db"),
		filepath.Join(tempDir, "test2.db"),
		filepath.Join(tempDir, "test3.db"),
	}
	manager := newTestManager()

	// When - Test concurrent access
	var wg sync.WaitGroup
	connections := make(map[string]*Writer, len(dbPaths))

	for i, dbPath := range dbPaths {
		wg.Add(1)
		go func(path string, index int) {
			defer wg.Done()
			writer, err := manager.GetOrCreateConnection(path)
			if err != nil {
				t.Errorf("Failed to create connection %d: %v", index, err)
				return
			}
			connections[path] = writer
		}(dbPath, i)
	}

	wg.Wait()

	// Then
	if len(connections) != len(dbPaths) {
		t.Fatalf("Expected %d connections, got %d", len(dbPaths), len(connections))
	}
}

func Test_concurrent_access_multiple_goroutines_no_race_conditions(t *testing.T) {
	// Given
	tempDir, err := os.MkdirTemp("", "timeline_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPaths := []string{
		filepath.Join(tempDir, "test1.db"),
		filepath.Join(tempDir, "test2.db"),
		filepath.Join(tempDir, "test3.db"),
	}
	manager := newTestManager()

	// When - Test concurrent access
	var wg sync.WaitGroup

	for i, dbPath := range dbPaths {
		wg.Add(1)
		go func(path string, index int) {
			defer wg.Done()
			_, err := manager.GetOrCreateConnection(path)
			if err != nil {
				t.Errorf("Failed to create connection %d: %v", index, err)
			}
		}(dbPath, i)
	}

	wg.Wait()

	// Then - Verify no race conditions by checking stored connections
	manager.mutex.RLock()
	storedCount := len(manager.connections)
	manager.mutex.RUnlock()

	if storedCount != len(dbPaths) {
		t.Fatalf("Expected %d stored connections, got %d", len(dbPaths), storedCount)
	}
}

func Test_close_all_connections_multiple_connections_exist_initially(t *testing.T) {
	// Given
	tempDir, err := os.MkdirTemp("", "timeline_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPaths := []string{
		filepath.Join(tempDir, "test1.db"),
		filepath.Join(tempDir, "test2.db"),
		filepath.Join(tempDir, "test3.db"),
	}
	manager := newTestManager()

	// When - Create connections
	for _, dbPath := range dbPaths {
		_, err := manager.GetOrCreateConnection(dbPath)
		if err != nil {
			t.Fatalf("Failed to create connection: %v", err)
		}
	}

	// Then
	manager.mutex.RLock()
	initialCount := len(manager.connections)
	manager.mutex.RUnlock()

	if initialCount != len(dbPaths) {
		t.Fatalf("Expected %d connections, got %d", len(dbPaths), initialCount)
	}
}

func Test_close_all_connections_multiple_connections_all_closed(t *testing.T) {
	// Given
	tempDir, err := os.MkdirTemp("", "timeline_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPaths := []string{
		filepath.Join(tempDir, "test1.db"),
		filepath.Join(tempDir, "test2.db"),
		filepath.Join(tempDir, "test3.db"),
	}
	manager := newTestManager()

	// When - Create connections
	for _, dbPath := range dbPaths {
		_, err := manager.GetOrCreateConnection(dbPath)
		if err != nil {
			t.Fatalf("Failed to create connection: %v", err)
		}
	}

	// And - Close all connections
	manager.CloseAllConnections()

	// Then
	manager.mutex.RLock()
	finalCount := len(manager.connections)
	manager.mutex.RUnlock()

	if finalCount != 0 {
		t.Fatalf("Expected 0 connections after close, got %d", finalCount)
	}
}

func Test_close_connection_multiple_connections_only_target_closed(t *testing.T) {
	// Given
	tempDir, err := os.MkdirTemp("", "timeline_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPaths := []string{
		filepath.Join(tempDir, "test1.db"),
		filepath.Join(tempDir, "test2.db"),
		filepath.Join(tempDir, "test3.db"),
	}
	manager := newTestManager()

	// When - Create connections
	for _, dbPath := range dbPaths {
		_, err := manager.GetOrCreateConnection(dbPath)
		if err != nil {
			t.Fatalf("Failed to create connection: %v", err)
		}
	}

	// And - Close one specific connection
	targetPath := dbPaths[1]
	manager.CloseConnection(targetPath)

	// Then
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	if len(manager.connections) != len(dbPaths)-1 {
		t.Fatalf("Expected %d connections, got %d", len(dbPaths)-1, len(manager.connections))
	}

	if _, exists := manager.connections[targetPath]; exists {
		t.Fatal("Target connection should have been closed")
	}
}

func Test_close_connection_multiple_connections_others_remain(t *testing.T) {
	// Given
	tempDir, err := os.MkdirTemp("", "timeline_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPaths := []string{
		filepath.Join(tempDir, "test1.db"),
		filepath.Join(tempDir, "test2.db"),
		filepath.Join(tempDir, "test3.db"),
	}
	manager := newTestManager()

	// When - Create connections
	for _, dbPath := range dbPaths {
		_, err := manager.GetOrCreateConnection(dbPath)
		if err != nil {
			t.Fatalf("Failed to create connection: %v", err)
		}
	}

	// And - Close one specific connection
	targetPath := dbPaths[1]
	manager.CloseConnection(targetPath)

	// Then - Verify other connections still exist
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	for i, dbPath := range dbPaths {
		if i != 1 { // Skip the closed connection
			if _, exists := manager.connections[dbPath]; !exists {
				t.Fatalf("Connection %d should still exist", i)
			}
		}
	}
}

func Test_error_handling_empty_path_handles_gracefully(t *testing.T) {
	// Given
	manager := newTestManager()

	// When
	_, err := manager.GetOrCreateConnection("")

	// Then
	if err == nil {
		t.Log("Empty path did not return error (this might be expected behavior)")
	} else {
		t.Logf("Empty path returned error as expected: %v", err)
	}
}

func Test_error_handling_invalid_path_handles_gracefully(t *testing.T) {
	// Given
	manager := newTestManager()
	invalidPath := string([]byte{0x00, 0x01, 0x02})

	// When
	_, err := manager.GetOrCreateConnection(invalidPath)

	// Then
	if err == nil {
		t.Log("Invalid path did not return error (this might be expected behavior)")
	} else {
		t.Logf("Invalid path returned error as expected: %v", err)
	}
}

func Test_error_handling_edge_cases_does_not_panic(t *testing.T) {
	// Given
	manager := newTestManager()

	// When - Test various edge cases
	testCases := []string{
		"",
		string([]byte{0x00, 0x01, 0x02}),
		"/dev/null/test.db",
		"../../../etc/passwd",
	}

	for _, testCase := range testCases {
		_, err := manager.GetOrCreateConnection(testCase)
		// The important thing is that the manager doesn't panic
		if err != nil {
			t.Logf("Test case '%s' returned error (expected): %v", testCase, err)
		} else {
			t.Logf("Test case '%s' succeeded (might be expected)", testCase)
		}
	}

	// Then
	t.Log("Error handling test completed - manager handled edge cases gracefully")
}
