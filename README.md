# Timeline Database Client

A powerful Go library for timeline database operations using DuckDB, featuring automatic schema evolution and intelligent type promotion.

## Features

### 🚀 From Any String to Statistics Database (DuckDB)
Transform any string data into structured, queryable statistics using DuckDB's powerful analytical capabilities:
- Automatic data type detection and conversion
- JSON flattening and normalization
- Time series data handling
- Statistical analysis and aggregation support

### 📈 Intelligent Type Promotion System
Automatically promote database column types as your data grows:
```
Null → Utinyint → Smallint → Integer → Bigint → Hugeint
```

The system intelligently handles type conversions to prevent data loss while optimizing storage and performance.

## Installation

### Prerequisites
- Go 1.21 or higher
- DuckDB C++ libraries (automatically handled by the Go driver)

### Install
```bash
go get github.com/confetti-cms/timeline
```

### Quick Start

```go
package main

import (
    "fmt"
    "time"
    "github.com/confetti-cms/timeline"
)

func main() {
    // Create an in-memory timeline client
    writer, err := timeline.NewMemoryClient()
    if err != nil {
        panic(err)
    }
    defer writer.Close()

    // Write some data
    row := timeline.NewRow(time.Now(), map[string]any{
        "user_id": 123,
        "event": "login",
        "metadata": map[string]any{
            "ip": "192.168.1.1",
            "user_agent": "Mozilla/5.0",
        },
    })

    err = writer.Write("user_events", row)
    if err != nil {
        panic(err)
    }

    fmt.Println("Data written successfully!")
}
```

## Advanced Usage

### Storage Client with Persistent Database

```go
package main

import (
    "github.com/confetti-cms/timeline"
)

func main() {
    // Create a persistent storage client
    writer, err := timeline.NewStorageClient("./data/timeline.db")
    if err != nil {
        panic(err)
    }
    defer writer.Close()

    // Use the timeline manager for connection pooling
    manager := timeline.GetTimelineConnectionManager()

    // Get or create a connection (reuses existing connections)
    writer2, err := manager.GetOrCreateConnection("./data/timeline.db")
    if err != nil {
        panic(err)
    }

    // Your application code here...
}
```

### Type Promotion Examples

The library automatically handles type promotion as your data evolves:

```go
// Initial data with small numbers
row1 := timeline.NewRow(time.Now(), map[string]any{
    "user_id": 1,        // Starts as UTINYINT
    "score": 100,        // Starts as USMALLINT
    "count": 1000,       // Starts as UINTEGER
})

// Later data with larger numbers - automatically promoted
row2 := timeline.NewRow(time.Now(), map[string]any{
    "user_id": 100000,   // Promoted to UINTEGER
    "score": 1000000,    // Promoted to UINTEGER
    "count": 10000000,   // Promoted to UBIGINT
})

// Mixed signed/unsigned - intelligently promoted
row3 := timeline.NewRow(time.Now(), map[string]any{
    "user_id": -1000,    // Promoted to INTEGER
    "score": 5000000,    // Promoted to BIGINT
})
```

### JSON Data Handling

```go
// JSON objects are automatically flattened
row := timeline.NewRow(time.Now(), map[string]any{
    "user": map[string]any{
        "id": 123,
        "name": "John Doe",
        "profile": map[string]any{
            "age": 30,
            "city": "New York",
        },
    },
    "tags": []string{"premium", "active"},
})

// Automatically creates columns:
// - user_id: INTEGER
// - user_name: VARCHAR
// - user_profile_age: INTEGER
// - user_profile_city: VARCHAR
// - tags: JSON
```

### Time Series Data

```go
// Time data is automatically detected and handled
row := timeline.NewRow(time.Now(), map[string]any{
    "event_date": "2023-12-25",           // DATE
    "event_time": "14:30:00",             // TIME
    "timestamp": "2023-12-25 14:30:00",   // TIMESTAMP
    "duration": "02:30:00.500",           // TIME with microseconds
})
```

## API Reference

### Core Types

#### `Writer`
Main interface for writing timeline data.

```go
type Writer struct {
    // Internal fields
}
```

**Methods:**
- `Write(table string, row Row) error` - Write a row to the specified table
- `Close() error` - Close the database connection
- `Checkpoint() error` - Force a database checkpoint

#### `Row`
Represents a single row of data.

```go
type Row map[string]any
```

**Functions:**
- `NewRow(timestamp time.Time, data map[string]any) Row` - Create a new row with automatic timestamp handling

### Connection Management

#### `TimelineConnectionManager`
Manages multiple database connections with automatic reuse.

```go
type TimelineConnectionManager struct {
    // Internal fields
}
```

**Methods:**
- `GetOrCreateConnection(dbPath string) (*Writer, error)` - Get existing or create new connection
- `CloseAllConnections()` - Close all managed connections
- `CloseConnection(dbPath string)` - Close specific connection

**Functions:**
- `GetTimelineConnectionManager() *TimelineConnectionManager` - Get the global connection manager

### Client Creation Functions

- `NewMemoryClient() (*Writer, error)` - Create an in-memory database client
- `NewStorageClient(dbPath string) (*Writer, error)` - Create a persistent storage client

## Supported Data Types

The library automatically detects and handles these DuckDB data types:

### Numeric Types
- `UTINYINT` (0-255)
- `USMALLINT` (0-65,535)
- `UINTEGER` (0-4,294,967,295)
- `UBIGINT` (0-18,446,744,073,709,551,615)
- `TINYINT` (-128 to 127)
- `SMALLINT` (-32,768 to 32,767)
- `INTEGER` (-2,147,483,648 to 2,147,483,647)
- `BIGINT` (-9,223,372,036,854,775,808 to 9,223,372,036,854,775,807)
- `HUGEINT` (large integers)
- `FLOAT` (single precision)
- `DOUBLE` (double precision)

### Temporal Types
- `DATE` (calendar dates)
- `TIME` (time of day)
- `TIMESTAMP` (date and time)

### Other Types
- `BOOLEAN` (true/false)
- `VARCHAR` (text data)
- `UUID` (UUID values)
- `JSON` (JSON data)

## Schema Evolution

The library automatically handles schema changes:

1. **New Columns**: Automatically added when new fields are encountered
2. **Type Promotion**: Existing columns are promoted to larger types as needed
3. **JSON Flattening**: Nested objects are flattened into separate columns
4. **Array Handling**: Arrays are stored as JSON

## Performance Features

- **Connection Pooling**: Reuse database connections efficiently
- **Automatic Checkpointing**: Periodic checkpointing every 200ms
- **Memory Management**: Proper cleanup and resource management
- **Concurrent Access**: Thread-safe operations

## Error Handling

The library provides detailed error messages for:
- Database connection issues
- Type promotion conflicts
- Schema evolution problems
- Data validation errors

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For questions and support, please open an issue on GitHub.

---

**Built with ❤️ for high-performance timeline data processing**