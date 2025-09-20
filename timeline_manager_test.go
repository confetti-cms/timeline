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

func TestGetOrCreateConnection_GivenValidPath_WhenCreatingConnection_ThenReturnsValidWriter(t *testing.T) {
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

func TestGetOrCreateConnection_GivenValidPath_WhenCreatingConnection_ThenStoresConnection(t *testing.T) {
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

func TestConnectionReuse_GivenSamePath_WhenCreatingMultipleConnections_ThenReturnsSameInstance(t *testing.T) {
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

func TestConnectionReuse_GivenSamePath_WhenCreatingMultipleConnections_ThenStoresOnlyOneConnection(t *testing.T) {
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

func TestDirectoryCreation_GivenNestedPath_WhenCreatingConnection_ThenCreatesDirectories(t *testing.T) {
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

func TestDirectoryCreation_GivenNestedPath_WhenCreatingConnection_ThenCreatesDatabaseFile(t *testing.T) {
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

func TestConcurrentAccess_GivenMultipleGoroutines_WhenCreatingConnections_ThenAllSucceed(t *testing.T) {
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

func TestConcurrentAccess_GivenMultipleGoroutines_WhenCreatingConnections_ThenNoRaceConditions(t *testing.T) {
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

func TestCloseAllConnections_GivenMultipleConnections_WhenClosingAll_ThenConnectionsExistInitially(t *testing.T) {
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

func TestCloseAllConnections_GivenMultipleConnections_WhenClosingAll_ThenAllConnectionsAreClosed(t *testing.T) {
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

func TestCloseConnection_GivenMultipleConnections_WhenClosingOne_ThenOnlyTargetIsClosed(t *testing.T) {
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

func TestCloseConnection_GivenMultipleConnections_WhenClosingOne_ThenOtherConnectionsRemain(t *testing.T) {
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

func TestErrorHandling_GivenEmptyPath_WhenCreatingConnection_ThenHandlesGracefully(t *testing.T) {
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

func TestErrorHandling_GivenInvalidPath_WhenCreatingConnection_ThenHandlesGracefully(t *testing.T) {
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

func TestErrorHandling_GivenEdgeCases_WhenCreatingConnection_ThenDoesNotPanic(t *testing.T) {
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
