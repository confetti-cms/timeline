package timeline

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// TimelineConnectionManager manages timeline database connections across multiple function calls
type TimelineConnectionManager struct {
	connections map[string]*Writer
	mutex       sync.RWMutex
}

// Global instance of the connection manager
var timelineConnManager = &TimelineConnectionManager{
	connections: make(map[string]*Writer),
}

// GetTimelineConnectionManager returns the global timeline connection manager instance
func GetTimelineConnectionManager() *TimelineConnectionManager {
	return timelineConnManager
}

// GetOrCreateConnection returns an existing connection or creates a new one for the given dbPath
func (m *TimelineConnectionManager) GetOrCreateConnection(dbPath string) (*Writer, error) {
	m.mutex.RLock()
	if writer, exists := m.connections[dbPath]; exists {
		m.mutex.RUnlock()
		return writer, nil
	}
	m.mutex.RUnlock()

	// Connection doesn't exist, create a new one
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Double-check in case another goroutine created it while we were waiting
	if writer, exists := m.connections[dbPath]; exists {
		return writer, nil
	}

	// Ensure the directory exists
	dbDir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", dbDir, err)
	}

	// Create new connection
	writer, err := NewStorageClient(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create timeline storage client for %s: %w", dbPath, err)
	}

	m.connections[dbPath] = writer
	return writer, nil
}

// CloseAllConnections closes all managed connections
// This should be called during application shutdown or when connections need to be refreshed
func (m *TimelineConnectionManager) CloseAllConnections() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for dbPath, writer := range m.connections {
		writer.Close()
		delete(m.connections, dbPath)
	}
}

// CloseConnection closes a specific connection by dbPath
func (m *TimelineConnectionManager) CloseConnection(dbPath string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if writer, exists := m.connections[dbPath]; exists {
		writer.Close()
		delete(m.connections, dbPath)
	}
}
