package timeline

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

func NewClient() (*Writer, error) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	return &Writer{DB: db}, nil
}

func NewClientWithPath(dbPath string) (*Writer, error) {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database %s: %w", dbPath, err)
	}
	return &Writer{DB: db}, nil
}

type Row map[string]any

func NewRow(timestamp time.Time, data map[string]any) Row {
	// The user can override the timestamp column value
	ts, exists := data["timestamp"]
	if !exists || duckDbTypeFromInput(ts) != Timestamp {
		data["timestamp"] = timestamp
	}
	return data
}

type Writer struct {
	DB *sql.DB
}

func (w *Writer) Close() error {
	return w.DB.Close()
}

// with datetime object (not string)
func (w *Writer) Write(table string, row Row) error {

	// If row is empty or only contains timestamp, do nothing
	if len(row) <= 1 {
		return nil
	}

	// Get existing columns
	cols, err := w.getCurrentColumns(table)
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	// Ensure table exists
	if err := w.ensureTableExists(table, cols); err != nil {
		return fmt.Errorf("failed to ensure table exists: %w", err)
	}

	// Flatten json maps into separate columns
	row = flattenJsonMaps(row)

	// Promote column types if needed
	cols, err = w.promoteColumns(table, cols, row)
	if err != nil {
		return fmt.Errorf("before insert new row: %w", err)
	}

	// Add any missing columns
	if err := w.addMissingColumns(table, cols, row); err != nil {
		return fmt.Errorf("failed to add missing columns: %w", err)
	}

	row = w.preprocessRow(row, cols)

	// fmt.Printf("Inserting into %s: %+v\n", table, row) print json endocded
	rowJson, _ := json.Marshal(row)
	fmt.Printf("Inserting into %s: %s\n", table, string(rowJson))

	if err := w.insertRow(table, row); err != nil {
		return fmt.Errorf("failed to insert row: %w", err)
	}

	return nil
}

func flattenJsonMaps(row Row) Row {
	// only when row is a map[string]any, flatten it
	resultRow := make(Row)
	for k, v := range row {
		if vMap, ok := v.(map[string]any); ok {
			for mmk, mmv := range flattenJsonMaps(vMap) {
				newKey2 := k + "_" + mmk
				resultRow[newKey2] = mmv
			}
		} else if mvMap, ok := v.([]any); ok {
			// Json encoded the array
			jsonBytes, err := json.Marshal(mvMap)
			if err != nil {
				resultRow[k] = fmt.Sprintf("%v", mvMap)
			} else {
				resultRow[k] = string(jsonBytes)
			}
		} else {
			resultRow[k] = v
		}
	}
	return resultRow
}

func (w *Writer) promoteColumns(table string, existingCols map[string]ColumnType, row Row) (map[string]ColumnType, error) {
	for col, value := range row {
		oldType, exists := existingCols[col]
		if !exists {
			continue // Column does not exist yet, will be created later
		}
		givenType := duckDbTypeFromInput(value)

		if givenType == oldType {
			continue // No promotion needed
		}

		promoteType, err := oldType.PromoteTo(givenType)
		if err != nil {
			return existingCols, fmt.Errorf("failed get promotion type for column %s from %s to %s given %s: %w", col, oldType, promoteType, givenType, err)
		}

		// Only promote if the type actually changes
		if promoteType == oldType {
			continue
		}
		if err := w.promoteColumn(table, col, oldType, promoteType); err != nil {
			return existingCols, fmt.Errorf("from %s to %s given %s: %w", oldType, promoteType, givenType, err)
		}
		existingCols[col] = promoteType
	}
	return existingCols, nil
}

func (w *Writer) promoteColumn(table, col string, oldType, promoteType ColumnType) error {
	// Convert Time to Timestamp by combining with date part of existing timestamp column
	if oldType == Time && promoteType == Timestamp {
		alterSQL := fmt.Sprintf(`
			ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s
			USING (date_trunc('day', timestamp) + %s::TIME);
		`, table, col, promoteType, col) // use column timestamp to get the date part

		// Promote column type
		if _, err := w.DB.Exec(alterSQL); err != nil {
			return fmt.Errorf("failed to promote column %s to %s: %w", col, promoteType, err)
		}
		return nil
	}

	alterSQL := fmt.Sprintf(`
		ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s
		USING TRY_CAST(%s AS %s);
	`, table, col, promoteType, col, promoteType)

	// Promote column type
	if _, err := w.DB.Exec(alterSQL); err != nil {
		return fmt.Errorf("failed to promote column %s to %s: %w", col, promoteType, err)
	}
	return nil
}

func (w *Writer) insertRow(table string, row Row) error {
	columns := ""
	valuePlaceholder := ""
	values := []any{}
	i := 1
	for col, val := range row {
		if columns != "" {
			columns += ", "
			valuePlaceholder += ", "
		}
		columns += col
		valuePlaceholder += "?"
		values = append(values, val)
		i++
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, columns, valuePlaceholder)
	if _, err := w.DB.Exec(insertSQL, values...); err != nil {
		return fmt.Errorf("failed to execute: %w", err)
	}
	return nil
}

func (w *Writer) preprocessRow(row Row, cols map[string]ColumnType) Row {
	for col, val := range row {
		if col != "timestamp" && cols[col] == Timestamp {
			row[col] = preprocessTimestamp(val, row)
		}
	}
	return row
}

func preprocessTimestamp(value any, row Row) any {
	// if value is string 00:00:00 or 00:00:00.000 or 00:00:00.000000 or other time, prefix it with the date of the timestamp column
	strVal, ok := value.(string)
	if !ok {
		return value
	}
	if len(strVal) >= 8 && strVal[2] == ':' && strVal[5] == ':' {
		ts, ok := getDateFromTimestamp(row["timestamp"])
		if !ok {
			return value
		}
		// Prefix with date of timestamp column
		return ts[:10] + " " + strVal
	}
	return value
}

func getDateFromTimestamp(ts any) (string, bool) {
	if t, ok := ts.(time.Time); ok {
		return t.Format("2006-01-02"), true
	} else if t, ok := ts.(string); ok && len(t) >= 10 {
		return t[:10], true
	}
	return "", false
}

// getCurrentColumns returns a map of existing columns for the table
// key is column name, value is ColumnType
func (w *Writer) getCurrentColumns(table string) (map[string]ColumnType, error) {
	existingCols := make(map[string]ColumnType)

	rows, err := w.DB.Query(
		"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ?",
		table,
	)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, _type string
		if err := rows.Scan(&name, &_type); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}
		existingCols[name] = ColumnType(_type)
	}

	return existingCols, nil
}

// ensureTableExists creates the table if it does not exist
func (w *Writer) ensureTableExists(table string, existingCols map[string]ColumnType) error {
	if len(existingCols) == 0 {
		createSQL := fmt.Sprintf("CREATE TABLE %s (%s)", table, "timestamp TIMESTAMP")
		if _, err := w.DB.Exec(createSQL); err != nil {
			return fmt.Errorf("failed to create table %s: %w", table, err)
		}
		existingCols["timestamp"] = Timestamp
	}
	return nil
}

// addMissingColumns adds columns that are in the row but not in the table yet
func (w *Writer) addMissingColumns(table string, existingCols map[string]ColumnType, row Row) error {
	for col := range row {
		if _, exists := existingCols[col]; !exists {
			_type := duckDbTypeFromInput(row[col])
			columnsToAdd := map[string]ColumnType{col: _type}
			// If field has a map, create new columns for each field in the map
			if _type == JsonMap {
				columnsToAdd = getFieldsFromMap(row[col], col)
			}
			// Add columns
			for col, _type := range columnsToAdd {
				alterSQL := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, col, _type)
				if _, err := w.DB.Exec(alterSQL); err != nil {
					return fmt.Errorf("failed to add column %s: %w", col, err)
				}
			}
		}
	}
	return nil
}

// getFieldsFromMap transforms user:{id:123} to user_id:123
func getFieldsFromMap(value any, parentKey string) map[string]ColumnType {
	fields := make(map[string]ColumnType)
	if m, ok := value.(map[string]any); ok {
		for k, v := range m {
			newKey := parentKey + "_" + k
			_type := duckDbTypeFromInput(v)
			fields[newKey] = _type
		}
	}
	return fields
}

type ColumnType string

const (
	// null
	Null ColumnType = "BIT" // A bit represents only null values
	// false/true
	Boolean ColumnType = "BOOLEAN"
	// 0 to 255
	Utinyint ColumnType = "UTINYINT"
	// 0 to 65,535
	Usmallint ColumnType = "USMALLINT"
	// 0 to 4,294,967,295
	Uinteger ColumnType = "UINTEGER"
	// 0 to 18,446,744,073,709,551,615
	Ubigint ColumnType = "UBIGINT"
	// -128 to 127
	Tinyint ColumnType = "TINYINT"
	// -32,768 to 32,767
	Smallint ColumnType = "SMALLINT"
	// -2,147,483,648 to 2,147,483,647
	Integer ColumnType = "INTEGER"
	// -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807
	Bigint ColumnType = "BIGINT"
	// -170,141,183,460,469,231,731,687,303,715,884,105,727 to 170,141,183,460,469,231,731,687,303,715,884,105,727
	Hugeint ColumnType = "HUGEINT"
	// ~-3.4e38 to ~3.4e38
	Float ColumnType = "FLOAT"
	// ~-1.7e308 to ~1.7e308
	Double ColumnType = "DOUBLE"
	// 0001-01-01 to 9999-12-31
	Date ColumnType = "DATE"
	// 00:00:00 to 23:59:59.999999
	Time ColumnType = "TIME"
	// 0001-01-01 00:00:00 to 9999-12-31 23:59:59.999999
	Timestamp ColumnType = "TIMESTAMP"
	// 00000000-0000-0000-0000-000000000000 to ffffffff-ffff-ffff-ffff-ffffffffffff
	Uuid ColumnType = "UUID"
	// "" (empty string) to ~
	Varchar ColumnType = "VARCHAR"
	Json    ColumnType = "JSON"
	// We do not save this value. But we convert user.id to user_id
	JsonMap       ColumnType = "JSON_MAP"
	UnknownInt    ColumnType = "UNKNOWN_INT"
	UnknownFloat  ColumnType = "UNKNOWN_FLOAT"
	UnknownString ColumnType = "UNKNOWN_STRING"
	Unknown       ColumnType = "UNKNOWN"
)

// PromoteTo determines the promoted type
// The promoteType is not always the given type or current type
// e.g. promoting from utinyint to tinyint results in smallint
func (old ColumnType) PromoteTo(given ColumnType) (ColumnType, error) {
	switch old {
	case Null: // Nil can be promoted to any type
		return given, nil
	case Boolean:
		switch given {
		case Null, Boolean:
			return Boolean, nil
		case Utinyint, Usmallint, Uinteger, Ubigint, Tinyint, Smallint, Integer, Bigint, Hugeint, Float, Double:
			return given, nil
		case Date, Time, Timestamp, Uuid, Varchar, Json:
			return Varchar, nil
		}
	case Utinyint:
		switch given {
		case Null, Boolean, Utinyint:
			return Utinyint, nil
		case Usmallint, Uinteger, Ubigint, Float, Double:
			return given, nil
		case Tinyint:
			// 255 & -128
			return Smallint, nil
		case Smallint:
			// 255 & -32,768
			return Integer, nil
		case Integer:
			// 255 & -2,147,483,648
			return Bigint, nil
		case Bigint, Hugeint:
			// 255 & -9,223,372,036,854,775,808
			return Hugeint, nil
		case Date, Time, Timestamp, Uuid, Varchar, Json:
			return Varchar, nil
		}
	case Usmallint:
		switch given {
		case Null, Boolean, Utinyint, Usmallint:
			return Usmallint, nil
		case Uinteger, Ubigint, Float, Double:
			return given, nil
		case Tinyint:
			// 65,535 & -128
			return Integer, nil
		case Smallint:
			// 65,535 & -32,768
			return Integer, nil
		case Integer:
			// 65,535 & -2,147,483,648
			return Bigint, nil
		case Bigint, Hugeint:
			// 65,535 & -9,223,372,036,854,775,808
			return Hugeint, nil
		case Date, Time, Timestamp, Uuid, Varchar, Json:
			return Varchar, nil
		}
	case Uinteger:
		switch given {
		case Null, Boolean, Utinyint, Usmallint, Uinteger:
			return Uinteger, nil
		case Ubigint:
			return Ubigint, nil
		case Tinyint:
			// 4,294,967,295 & -128
			return Bigint, nil
		case Smallint:
			// 4,294,967,295 & -32,768
			return Bigint, nil
		case Integer:
			// 4,294,967,295 & -2,147,483,648
			return Bigint, nil
		case Bigint, Hugeint:
			// 4,294,967,295 & -9,223,372,036,854,775,808
			return Hugeint, nil
		case Float, Double:
			return given, nil
		case Date, Time, Timestamp, Uuid, Varchar, Json:
			return Varchar, nil
		}
	case Ubigint:
		switch given {
		case Null, Boolean, Utinyint, Usmallint, Uinteger, Ubigint:
			return Ubigint, nil
		case Tinyint:
			// 18,446,744,073,709,551,615 & -128
			return Hugeint, nil
		case Smallint:
			// 18,446,744,073,709,551,615 & -32,768
			return Hugeint, nil
		case Integer:
			// 18,446,744,073,709,551,615 & -2,147,483,648
			return Hugeint, nil
		case Bigint, Hugeint:
			// 18,446,744,073,709,551,615 & -9,223,372,036,854,775,808
			return Hugeint, nil
		case Float, Double:
			return given, nil
		case Date, Time, Timestamp, Uuid, Varchar, Json:
			return Varchar, nil
		}
	case Tinyint:
		switch given {
		case Null, Boolean, Tinyint:
			return Tinyint, nil
		case Utinyint:
			// -128 & 255
			return Smallint, nil
		case Usmallint:
			// -128 & 65,535
			return Integer, nil
		case Uinteger:
			// -128 & 4,294,967,295
			return Bigint, nil
		case Ubigint:
			// -128 & 18,446,744,073,709,551,615
			return Hugeint, nil
		case Smallint, Integer, Bigint, Hugeint, Float, Double:
			return given, nil
		case Date, Time, Timestamp, Uuid, Varchar, Json:
			return Varchar, nil
		}
	case Smallint:
		switch given {
		case Null, Boolean, Tinyint, Smallint, Utinyint:
			return Smallint, nil
		case Usmallint:
			// -32,768 & 65,535
			return Integer, nil
		case Uinteger:
			// -32,768 & 4,294,967,295
			return Bigint, nil
		case Ubigint:
			// -32,768 & 18,446,744,073,709,551,615
			return Hugeint, nil
		case Integer, Bigint, Hugeint, Float, Double:
			return given, nil
		case Date, Time, Timestamp, Uuid, Varchar, Json:
			return Varchar, nil
		}
	case Integer:
		switch given {
		case Null, Boolean, Tinyint, Smallint, Integer, Utinyint, Usmallint:
			return Integer, nil
		case Uinteger:
			// -2,147,483,648 & 4,294,967,295
			return Bigint, nil
		case Ubigint:
			// -2,147,483,648 & 18,446,744,073,709,551,615
			return Hugeint, nil
		case Bigint, Hugeint, Float, Double:
			return given, nil
		case Date, Time, Timestamp, Uuid, Varchar, Json:
			return Varchar, nil
		}
	case Bigint:
		switch given {
		case Null, Boolean, Tinyint, Smallint, Integer, Bigint, Utinyint, Usmallint:
			return Bigint, nil
		case Uinteger:
			// -9,223,372,036,854,775,808 & 4,294,967,295
			return Hugeint, nil
		case Ubigint:
			// -9,223,372,036,854,775,808 & 18,446,744,073,709,551,615
			return Hugeint, nil
		case Hugeint, Float, Double:
			return given, nil
		case Date, Time, Timestamp, Uuid, Varchar, Json:
			return Varchar, nil
		}
	case Hugeint:
		switch given {
		case Null, Boolean, Tinyint, Smallint, Integer, Bigint, Hugeint, Utinyint, Usmallint, Uinteger, Ubigint:
			return Hugeint, nil
		case Float, Double, Date, Time, Timestamp, Uuid, Varchar, Json:
			return Varchar, nil
		}
	case Float:
		switch given {
		case Null, Boolean, Utinyint, Usmallint, Uinteger, Ubigint, Tinyint, Smallint, Integer, Bigint, Float:
			return Float, nil
		case Double:
			return Double, nil
		case Hugeint, Date, Time, Timestamp, Uuid, Varchar, Json:
			return Varchar, nil
		}
	case Double:
		switch given {
		case Null, Boolean, Utinyint, Usmallint, Uinteger, Ubigint, Tinyint, Smallint, Integer, Bigint, Float, Double:
			return Double, nil
		case Hugeint, Date, Time, Timestamp, Uuid, Varchar, Json:
			return Varchar, nil
		}
	case Date:
		switch given {
		case Null, Date:
			return Date, nil
		case Time, Timestamp:
			return Timestamp, nil
		case Boolean, Utinyint, Usmallint, Uinteger, Ubigint, Tinyint, Smallint, Integer, Bigint, Hugeint, Float, Double, Uuid, Varchar, Json:
			return Varchar, nil
		}
	case Time:
		switch given {
		case Null, Time:
			return Time, nil
		case Date, Timestamp:
			return Timestamp, nil
		case Boolean, Utinyint, Usmallint, Uinteger, Ubigint, Tinyint, Smallint, Integer, Bigint, Hugeint, Float, Double, Uuid, Varchar, Json:
			return Varchar, nil
		}
	case Timestamp:
		switch given {
		case Null, Timestamp, Date, Time:
			return Timestamp, nil
		case Boolean, Utinyint, Usmallint, Uinteger, Ubigint, Tinyint, Smallint, Integer, Bigint, Hugeint, Float, Double, Uuid, Varchar, Json:
			return Varchar, nil
		}
	case Uuid:
		switch given {
		case Null, Uuid:
			return Uuid, nil
		case Boolean, Utinyint, Usmallint, Uinteger, Ubigint, Tinyint, Smallint, Integer, Bigint, Hugeint, Float, Double, Date, Time, Timestamp, Varchar, Json:
			return Varchar, nil
		}
	case Varchar:
		switch given {
		case Null, Varchar:
			return Varchar, nil
		case Boolean, Utinyint, Usmallint, Uinteger, Ubigint, Tinyint, Smallint, Integer, Bigint, Hugeint, Float, Double, Date, Time, Timestamp, Uuid, Json:
			return Varchar, nil
		}
	case Json:
		switch given {
		case Null, Json:
			return Json, nil
		case Boolean, Utinyint, Usmallint, Uinteger, Ubigint, Tinyint, Smallint, Integer, Bigint, Hugeint, Float, Double, Date, Time, Timestamp, Uuid, Varchar:
			return Varchar, nil
		}
	}
	return Unknown, fmt.Errorf("no case for old type %s", old)

}

func duckDbTypeFromInput(value any) ColumnType {
	if value == nil {
		return Null
	}

	switch v := value.(type) {
	case bool:
		return Boolean
	case int:
		return typeFromInt64(int64(v))
	case int8:
		return typeFromInt64(int64(v))
	case int16:
		return typeFromInt64(int64(v))
	case int32:
		return typeFromInt64(int64(v))
	case int64:
		return typeFromInt64(v)
	case float32:
		return typeFromFloat64(float64(v))
	case float64:
		return typeFromFloat64(v)
	case time.Time:
		return Timestamp
	case string:
		return typeFromString(v)
	case []any:
		return Json
	case map[string]any:
		return JsonMap
	default:
		return Unknown
	}
}

func typeFromString(v string) ColumnType {
	length := len(v)
	// Match: 2023-01-01
	if length == 10 && v[4] == '-' && v[7] == '-' {
		return Date
	}
	// Match: 12:00:00
	if length == 8 && v[2] == ':' && v[5] == ':' {
		return Time
	}
	// Match: 12:00:00.123 or 12:00:00.123456
	if (length == 12 || length == 15) && v[2] == ':' && v[5] == ':' && v[8] == '.' {
		return Time
	}
	// Match: 2023-01-01 12:00:00
	if length == 19 && v[4] == '-' && v[7] == '-' && v[10] == ' ' && v[13] == ':' && v[16] == ':' {
		return Timestamp
	}
	// Match: 2023-01-01 12:00:00.123 or 2023-01-01 12:00:00.123456
	if (length == 23 || length == 26) && v[4] == '-' && v[7] == '-' && v[10] == ' ' && v[13] == ':' && v[16] == ':' && v[19] == '.' {
		return Timestamp
	}
	return Varchar
}

func typeFromFloat64(v float64) ColumnType {
	switch {
	case v >= -3.4e38 && v <= 3.4e38:
		return Float
	case v >= -1.7e308 && v <= 1.7e308:
		return Double
	default:
		return UnknownFloat
	}
}

// helper for signed integers
func typeFromInt64(v int64) ColumnType {
	switch {
	case v >= 0 && v <= 255:
		return Utinyint
	case v >= 0 && v <= 65535:
		return Usmallint
	case v >= 0 && v <= 4294967295:
		return Uinteger
	case v >= 0:
		return Ubigint
	case v >= -128 && v <= -1:
		return Tinyint
	case v >= -32768 && v <= -129:
		return Smallint
	case v >= -2147483648 && v <= -32769:
		return Integer
	case v <= -2147483649:
		// Here we cannot determine if it is Bigint or Hugeint
		// Only when this field is Bigint and value is minus, and other value is bigger than Bigint
		// we need HugeintRank, but we cannot determine that here with a single value
		return Bigint
	default:
		return UnknownInt
	}
}
