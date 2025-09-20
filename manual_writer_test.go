package timeline

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matryer/is"
)

func setup(t *testing.T) (*is.I, *Writer) {
	is := is.New(t)
	writer, err := NewClient()
	if err != nil {
		t.Fatalf("failed to init client: %v", err)
	}

	t.Cleanup(func() {
		writer.Close()
	})

	return is, writer
}

func Test_do_not_create_table_on_empty_row(t *testing.T) {
	is, writer := setup(t)

	// Schrijf een lege row
	err := writer.Write("timeline", NewRow(time.Now().UTC(), Row{}))
	is.NoErr(err)

	// Controleer dat de tabel niet bestaat
	var count int
	err = writer.DB.QueryRow(`SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'timeline'`).Scan(&count)
	is.NoErr(err)
	is.Equal(count, 0)
}

func Test_create_table_on_first_row(t *testing.T) {
	is, w := setup(t)

	err := w.Write("timeline", NewRow(time.Now().UTC(), Row{"title": "my title"}))

	columns := getColumns(t, w)
	is.NoErr(err)
	is.Equal(len(columns), 2)
	is.Equal(columns[0], "timestamp")
	is.Equal(columns[1], "title")
}

// check that the timestamp type is correct
func Test_create_table_with_timestamp_column(t *testing.T) {
	is, w := setup(t)

	err := w.Write("timeline", NewRow(time.Now().UTC(), Row{"title": "my title"}))

	var dataType string
	err = w.DB.QueryRow(`SELECT data_type FROM information_schema.columns WHERE table_name = 'timeline' AND column_name = 'timestamp'`).Scan(&dataType)
	is.NoErr(err)
	is.Equal(dataType, "TIMESTAMP")
}

func Test_create_column_name_when_not_exists(t *testing.T) {
	is, w := setup(t)

	err := w.Write("timeline", NewRow(time.Now().UTC(), Row{"title": "my title"}))

	is.NoErr(err)
	columns := getColumns(t, w)
	is.NoErr(err)
	is.Equal(len(columns), 2)
	is.Equal(columns[1], "title") // first column is the "timestamp" column
}

func Test_create_multiple_column_names_when_not_exists(t *testing.T) {
	is, w := setup(t)

	err := w.Write("timeline", NewRow(time.Now().UTC(), Row{"title": "my title", "description": "my description"}))

	is.NoErr(err)
	columns := getColumns(t, w)
	is.NoErr(err)
	is.Equal(len(columns), 3)
	is.Equal(columns[0], "description")
	is.Equal(columns[1], "timestamp")
	is.Equal(columns[2], "title")
}

// One exists but the other does not, create the one that does not exist
func Test_create_one_of_two_column_names_when_not_exists(t *testing.T) {
	is, w := setup(t)
	mockColumn(t, w, "timeline", "title", Varchar)

	err := w.Write("timeline", NewRow(time.Now().UTC(), Row{"description": "my description"}))

	is.NoErr(err)
	columns := getColumns(t, w)
	if len(columns) != 3 {
		fmt.Printf("Columns: %+v\n", columns)
	}
	is.Equal(len(columns), 3)
	is.Equal(columns[0], "description") // test description was created
	is.Equal(columns[1], "timestamp")   // test timestamp exists
	is.Equal(columns[2], "title")       // test title exists
}

func Test_set_timestamp_column_by_current_time(t *testing.T) {
	is, w := setup(t)

	now := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)
	err := w.Write("timeline", NewRow(now, Row{"title": "my title"}))

	is.NoErr(err)

	// Assert type
	is.Equal(getCurrentType(t, w, "timeline", "timestamp"), Timestamp)

	// Assert value
	rows := getValues(t, w, "timeline", "timestamp")
	is.Equal(len(rows), 1)
	is.Equal(rows[0], now)
}

func Test_set_timestamp_column_by_user_value(t *testing.T) {
	is, w := setup(t)

	currentTime := time.Now().UTC()
	userTime := time.Date(2022, 12, 31, 23, 59, 59, 0, time.UTC)
	err := w.Write("timeline", NewRow(currentTime, Row{"timestamp": userTime, "title": "my title"}))

	is.NoErr(err)
	rows := getValues(t, w, "timeline", "timestamp")
	is.Equal(len(rows), 1)
	is.Equal(rows[0], userTime)
}

func Test_set_timestamp_but_rename_if_not_a_timestamp_value(t *testing.T) {
	is, w := setup(t)

	currentTime := time.Now().UTC()
	err := w.Write("timeline", NewRow(currentTime, Row{"timestamp": "not a timestamp", "title": "my title"}))

	is.NoErr(err)
	rows := getValues(t, w, "timeline", "timestamp")
	is.Equal(len(rows), 1)
	is.Equal(rows[0], currentTime) // result != expected
}

func Test_store_string_value(t *testing.T) {
	is, w := setup(t)

	err := w.Write("timeline", NewRow(time.Now().UTC(), Row{"string_column": "my string"}))

	is.NoErr(err)
	got := getCurrentType(t, w, "timeline", "string_column")
	is.Equal(got, Varchar)
}

func mockColumn(t *testing.T, w *Writer, table, column string, _type ColumnType) {
	_, err := w.DB.Exec(`CREATE TABLE IF NOT EXISTS ` + table + ` (timestamp TIMESTAMP )`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	// if _type, allow nulls
	_, err = w.DB.Exec(fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS "%s" %s`, table, column, _type))
	if err != nil {
		t.Fatalf("failed to add column: %v", err)
	}
}

func getColumns(t *testing.T, writer *Writer) []string {
	var columns []string
	rows, err := writer.DB.Query(`SELECT column_name FROM information_schema.columns WHERE table_name = 'timeline' ORDER BY column_name`)
	if err != nil {
		// t.Fatalf("failed to get columns: %v", err) log test name
		t.Fatalf("failed to get columns in test %s: %v", t.Name(), err)
	}
	defer rows.Close()

	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			if err == sql.ErrNoRows {
				t.Fatalf("no columns found in the database after running test %s", t.Name())
			}
			t.Fatalf("failed to scan column: %v", err)
		}
		columns = append(columns, column)
	}
	return columns
}
