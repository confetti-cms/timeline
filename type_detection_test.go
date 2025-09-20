package timeline

import (
	"fmt"
	"testing"
	"time"

	"github.com/matryer/is"
)

func Test_create_columns_with_duckdb_type(t *testing.T) {
	// Given
	testCases := []struct {
		name     string
		value    any
		expected ColumnType
	}{
		{"null_column", nil, Null},
		{"boolean_column", true, Boolean},
		{"utinyint_column", int(1), Utinyint},
		{"usmallint_column", int(256), Usmallint},
		{"uinteger_column", int(65536), Uinteger},
		{"ubigint_column", int64(4294967296), Ubigint},
		{"tinyint_column", int(-1), Tinyint},
		{"smallint_column", int(-129), Smallint},
		{"integer_column", int(-32769), Integer},
		{"bigint_column", int64(-2147483649), Bigint},
		// The input can't be a Hugeint because go int64 max is smaller than Hugeint min
		// But the database should still create a Hugeint column because when a bigint
		// is the previous type, the column needs to store the lower and upper bound (in a Hugeint)
		{"hugeint_column", int64(-9223372036854775808), Bigint},
		{"float_column", float32(3.4e+38), Float},
		{"double_column", float64(1.7e+308), Double},
		{"date_column", "2023-01-01", Date},
		{"time_column", "12:00:00", Time},
		{"time_with_ms_column", "12:00:00.123", Time},
		{"time_with_us_column", "12:00:00.123456", Time},
		{"timestamp_column", "2023-01-01 12:00:00", Timestamp},
		{"timestamp_with_ms_column", "2023-01-01 12:00:00.123", Timestamp},
		{"timestamp_with_us_column", "2023-01-01 12:00:00.123456", Timestamp},
		{"timestamp_by_type", time.Now(), Timestamp},
		{"string_column", "my string", Varchar},
		{"json_list_column", []any{1, 2, 3}, Json},
	}

	// Collect assertions (so the output is in sequential order)
	assertions := make([]func(), 0)
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			is, w := setup(t)

			// When
			err := w.Write(tc.name+"_table", NewRow(time.Now(), Row{tc.name: tc.value}))

			// Then
			got := getCurrentType(t, w, tc.name+"_table", tc.name)
			assertions = append(assertions, func() {
				is.NoErr(err)
				is.Equal(got, tc.expected)
			})
		})
	}
	for _, assert := range assertions {
		assert()
	}
}

func Test_create_columns_with_multiple_fields(t *testing.T) {
	is, w := setup(t)

	err := w.Write("timeline", NewRow(time.Now(), Row{
		"user": map[string]any{"id": 123},
	}))

	is.NoErr(err)
	is.Equal(getCurrentType(t, w, "timeline", "user_id"), Utinyint)
	val := getValues(t, w, "timeline", "user_id")
	is.Equal(len(val), 1)
	is.Equal(val[0], uint8(123))
}

func Test_create_columns_with_map_in_map(t *testing.T) {
	is, w := setup(t)

	err := w.Write("timeline", NewRow(time.Now(), Row{
		"user": map[string]any{
			"info": map[string]any{
				"name": "Alice",
			},
		},
	}))

	is.NoErr(err)
	is.Equal(getCurrentType(t, w, "timeline", "user_info_name"), Varchar)
	val := getValues(t, w, "timeline", "user_info_name")
	is.Equal(len(val), 1)
	is.Equal(val[0], "Alice")
}

var typeTransformations = []struct {
	old       ColumnType
	given     ColumnType
	promotion ColumnType
}{
	// Nil can be promoted to any type
	{Null, Null, Null},
	{Null, Boolean, Boolean},
	{Null, Utinyint, Utinyint},
	{Null, Usmallint, Usmallint},
	{Null, Uinteger, Uinteger},
	{Null, Ubigint, Ubigint},
	{Null, Tinyint, Tinyint},
	{Null, Smallint, Smallint},
	{Null, Integer, Integer},
	{Null, Bigint, Bigint},
	{Null, Hugeint, Hugeint},
	{Null, Float, Float},
	{Null, Double, Double},
	{Null, Date, Date},
	{Null, Time, Time},
	{Null, Timestamp, Timestamp},
	{Null, Uuid, Uuid},
	{Null, Varchar, Varchar},
	{Null, Json, Json},

	// Boolean
	{Boolean, Null, Boolean},
	{Boolean, Boolean, Boolean},
	{Boolean, Utinyint, Utinyint},
	{Boolean, Usmallint, Usmallint},
	{Boolean, Uinteger, Uinteger},
	{Boolean, Ubigint, Ubigint},
	{Boolean, Tinyint, Tinyint},
	{Boolean, Smallint, Smallint},
	{Boolean, Integer, Integer},
	{Boolean, Bigint, Bigint},
	{Boolean, Hugeint, Hugeint},
	{Boolean, Float, Float},
	{Boolean, Double, Double},
	{Boolean, Date, Varchar},
	{Boolean, Time, Varchar},
	{Boolean, Timestamp, Varchar},
	{Boolean, Uuid, Varchar},
	{Boolean, Varchar, Varchar},
	{Boolean, Json, Varchar},

	// Utinyint
	{Utinyint, Null, Utinyint},
	{Utinyint, Boolean, Utinyint},
	{Utinyint, Utinyint, Utinyint},
	{Utinyint, Usmallint, Usmallint},
	{Utinyint, Uinteger, Uinteger},
	{Utinyint, Ubigint, Ubigint},
	// 255 & -128
	{Utinyint, Tinyint, Smallint},
	// 255 & -32,768
	{Utinyint, Smallint, Integer},
	// 255 & -2,147,483,648
	{Utinyint, Integer, Bigint},
	// 255 & -9,223,372,036,854,775,808
	{Utinyint, Bigint, Hugeint},
	{Utinyint, Hugeint, Hugeint},
	{Utinyint, Float, Float},
	{Utinyint, Double, Double},
	{Utinyint, Date, Varchar},
	{Utinyint, Time, Varchar},
	{Utinyint, Timestamp, Varchar},
	{Utinyint, Uuid, Varchar},
	{Utinyint, Varchar, Varchar},
	{Utinyint, Json, Varchar},

	// Usmallint
	{Usmallint, Null, Usmallint}, // No promotion
	{Usmallint, Boolean, Usmallint},
	{Usmallint, Utinyint, Usmallint},
	{Usmallint, Usmallint, Usmallint},
	{Usmallint, Uinteger, Uinteger},
	{Usmallint, Ubigint, Ubigint},
	// 65,535 & -128
	{Usmallint, Tinyint, Integer},
	// 65,535 & -32,768
	{Usmallint, Smallint, Integer},
	// 65,535 & -2,147,483,648
	{Usmallint, Integer, Bigint},
	// 65,535 & -9,223,372,036,854,775,808
	{Usmallint, Bigint, Hugeint},
	{Usmallint, Hugeint, Hugeint},
	{Usmallint, Float, Float},
	{Usmallint, Double, Double},
	{Usmallint, Date, Varchar},
	{Usmallint, Time, Varchar},
	{Usmallint, Timestamp, Varchar},
	{Usmallint, Uuid, Varchar},
	{Usmallint, Varchar, Varchar},
	{Usmallint, Json, Varchar},

	// Uinteger
	{Uinteger, Null, Uinteger},
	{Uinteger, Boolean, Uinteger},
	{Uinteger, Utinyint, Uinteger},
	{Uinteger, Usmallint, Uinteger},
	{Uinteger, Uinteger, Uinteger},
	{Uinteger, Ubigint, Ubigint},
	// 4,294,967,295 & -128
	{Uinteger, Tinyint, Bigint},
	// 4,294,967,295 & -32,768
	{Uinteger, Smallint, Bigint},
	// 4,294,967,295 & -2,147,483,648
	{Uinteger, Integer, Bigint},
	// 4,294,967,295 & -9,223,372,036,854,775,808
	{Uinteger, Bigint, Hugeint},
	{Uinteger, Hugeint, Hugeint},
	{Uinteger, Float, Float},
	{Uinteger, Double, Double},
	{Uinteger, Date, Varchar},
	{Uinteger, Time, Varchar},
	{Uinteger, Timestamp, Varchar},
	{Uinteger, Uuid, Varchar},
	{Uinteger, Varchar, Varchar},
	{Uinteger, Json, Varchar},

	// Ubigint
	{Ubigint, Null, Ubigint},
	{Ubigint, Boolean, Ubigint},
	{Ubigint, Utinyint, Ubigint},
	{Ubigint, Usmallint, Ubigint},
	{Ubigint, Uinteger, Ubigint},
	{Ubigint, Ubigint, Ubigint},
	// 18,446,744,073,709,551,615 & -128
	{Ubigint, Tinyint, Hugeint},
	// 18,446,744,073,709,551,615 & -32,768
	{Ubigint, Smallint, Hugeint},
	// 18,446,744,073,709,551,615 & -2,147,483,648
	{Ubigint, Integer, Hugeint},
	// 18,446,744,073,709,551,615 & -9,223,372,036,854,775,808
	{Ubigint, Bigint, Hugeint},
	{Ubigint, Hugeint, Hugeint},
	{Ubigint, Float, Float},
	{Ubigint, Double, Double},
	{Ubigint, Date, Varchar},
	{Ubigint, Time, Varchar},
	{Ubigint, Timestamp, Varchar},
	{Ubigint, Uuid, Varchar},
	{Ubigint, Varchar, Varchar},
	{Ubigint, Json, Varchar},

	// Tinyint
	{Tinyint, Null, Tinyint},
	{Tinyint, Boolean, Tinyint},
	// -128 & 255
	{Tinyint, Utinyint, Smallint},
	// -128 & 65,535
	{Tinyint, Usmallint, Integer},
	// -128 & 4,294,967,295
	{Tinyint, Uinteger, Bigint},
	// -128 & 18,446,744,073,709,551,615
	{Tinyint, Ubigint, Hugeint},
	{Tinyint, Tinyint, Tinyint},
	{Tinyint, Smallint, Smallint},
	{Tinyint, Integer, Integer},
	{Tinyint, Bigint, Bigint},
	{Tinyint, Hugeint, Hugeint},
	{Tinyint, Float, Float},
	{Tinyint, Double, Double},
	{Tinyint, Date, Varchar},
	{Tinyint, Time, Varchar},
	{Tinyint, Timestamp, Varchar},
	{Tinyint, Uuid, Varchar},
	{Tinyint, Varchar, Varchar},
	{Tinyint, Json, Varchar},

	// Smallint
	{Smallint, Null, Smallint},
	{Smallint, Boolean, Smallint},
	// -32,768 & 255
	{Smallint, Utinyint, Smallint},
	// -32,768 & 65,535
	{Smallint, Usmallint, Integer},
	// -32,768 & 4,294,967,295
	{Smallint, Uinteger, Bigint},
	// -32,768 & 18,446,744,073,709,551,615
	{Smallint, Ubigint, Hugeint},
	{Smallint, Tinyint, Smallint},
	{Smallint, Smallint, Smallint},
	{Smallint, Integer, Integer},
	{Smallint, Bigint, Bigint},
	{Smallint, Hugeint, Hugeint},
	{Smallint, Float, Float},
	{Smallint, Double, Double},
	{Smallint, Date, Varchar},
	{Smallint, Time, Varchar},
	{Smallint, Timestamp, Varchar},
	{Smallint, Uuid, Varchar},
	{Smallint, Varchar, Varchar},
	{Smallint, Json, Varchar},

	// Integer
	{Integer, Null, Integer},
	{Integer, Boolean, Integer},
	// -2,147,483,648 & 255
	{Integer, Utinyint, Integer},
	// -2,147,483,648 & 65,535
	{Integer, Usmallint, Integer},
	// -2,147,483,648 & 4,294,967,295
	{Integer, Uinteger, Bigint},
	// -2,147,483,648 & 18,446,744,073,709,551,615
	{Integer, Ubigint, Hugeint},
	{Integer, Tinyint, Integer},
	{Integer, Smallint, Integer},
	{Integer, Integer, Integer},
	{Integer, Bigint, Bigint},
	{Integer, Hugeint, Hugeint},
	{Integer, Float, Float},
	{Integer, Double, Double},
	{Integer, Date, Varchar},
	{Integer, Time, Varchar},
	{Integer, Timestamp, Varchar},
	{Integer, Uuid, Varchar},
	{Integer, Varchar, Varchar},
	{Integer, Json, Varchar},

	// Bigint
	{Bigint, Null, Bigint},
	{Bigint, Boolean, Bigint},
	// -9,223,372,036,854,775,808 & 255
	{Bigint, Utinyint, Bigint},
	// -9,223,372,036,854,775,808 & 65,535
	{Bigint, Usmallint, Bigint},
	// -9,223,372,036,854,775,808 & 4,294,967,295
	{Bigint, Uinteger, Hugeint},
	// -9,223,372,036,854,775,808 & 18,446,744,073,709,551,615
	{Bigint, Ubigint, Hugeint},
	{Bigint, Tinyint, Bigint},
	{Bigint, Smallint, Bigint},
	{Bigint, Integer, Bigint},
	{Bigint, Bigint, Bigint},
	{Bigint, Hugeint, Hugeint},
	{Bigint, Float, Float},
	{Bigint, Double, Double},
	{Bigint, Date, Varchar},
	{Bigint, Time, Varchar},
	{Bigint, Timestamp, Varchar},
	{Bigint, Uuid, Varchar},
	{Bigint, Varchar, Varchar},
	{Bigint, Json, Varchar},

	// Hugeint
	{Hugeint, Null, Hugeint},
	{Hugeint, Boolean, Hugeint},
	// -170,141,183,460,469,231,731... & 255
	{Hugeint, Utinyint, Hugeint},
	// -170,141,183,460,469,231,731... & 65,535
	{Hugeint, Usmallint, Hugeint},
	// -170,141,183,460,469,231,731... & 4,294,967,295
	{Hugeint, Uinteger, Hugeint},
	// -170,141,183,460,469,231,731... & 18,446,744,073,709,551,615
	{Hugeint, Ubigint, Hugeint},
	{Hugeint, Tinyint, Hugeint},
	{Hugeint, Smallint, Hugeint},
	{Hugeint, Integer, Hugeint},
	{Hugeint, Bigint, Hugeint},
	{Hugeint, Hugeint, Hugeint},
	{Hugeint, Float, Varchar},
	{Hugeint, Double, Varchar},
	{Hugeint, Date, Varchar},
	{Hugeint, Time, Varchar},
	{Hugeint, Timestamp, Varchar},
	{Hugeint, Uuid, Varchar},
	{Hugeint, Varchar, Varchar},
	{Hugeint, Json, Varchar},

	// Float
	{Float, Null, Float},
	{Float, Boolean, Float},
	{Float, Utinyint, Float},
	{Float, Usmallint, Float},
	{Float, Uinteger, Float},
	{Float, Ubigint, Float},
	{Float, Tinyint, Float},
	{Float, Smallint, Float},
	{Float, Integer, Float},
	{Float, Bigint, Float},
	{Float, Hugeint, Varchar},
	{Float, Float, Float},
	{Float, Double, Double},
	{Float, Date, Varchar},
	{Float, Time, Varchar},
	{Float, Timestamp, Varchar},
	{Float, Uuid, Varchar},
	{Float, Varchar, Varchar},
	{Float, Json, Varchar},

	// Double
	{Double, Null, Double},
	{Double, Boolean, Double},
	{Double, Utinyint, Double},
	{Double, Usmallint, Double},
	{Double, Uinteger, Double},
	{Double, Ubigint, Double},
	{Double, Tinyint, Double},
	{Double, Smallint, Double},
	{Double, Integer, Double},
	{Double, Bigint, Double},
	{Double, Hugeint, Varchar},
	{Double, Float, Double},
	{Double, Double, Double},
	{Double, Date, Varchar},
	{Double, Time, Varchar},
	{Double, Timestamp, Varchar},
	{Double, Uuid, Varchar},
	{Double, Varchar, Varchar},
	{Double, Json, Varchar},

	// Date
	{Date, Null, Date},
	{Date, Boolean, Varchar},
	{Date, Utinyint, Varchar},
	{Date, Usmallint, Varchar},
	{Date, Uinteger, Varchar},
	{Date, Ubigint, Varchar},
	{Date, Tinyint, Varchar},
	{Date, Smallint, Varchar},
	{Date, Integer, Varchar},
	{Date, Bigint, Varchar},
	{Date, Hugeint, Varchar},
	{Date, Float, Varchar},
	{Date, Double, Varchar},
	{Date, Date, Date},
	{Date, Time, Timestamp},
	{Date, Timestamp, Timestamp},
	{Date, Uuid, Varchar},
	{Date, Varchar, Varchar},
	{Date, Json, Varchar},

	// Time
	{Time, Null, Time},
	{Time, Boolean, Varchar},
	{Time, Utinyint, Varchar},
	{Time, Usmallint, Varchar},
	{Time, Uinteger, Varchar},
	{Time, Ubigint, Varchar},
	{Time, Tinyint, Varchar},
	{Time, Smallint, Varchar},
	{Time, Integer, Varchar},
	{Time, Bigint, Varchar},
	{Time, Hugeint, Varchar},
	{Time, Float, Varchar},
	{Time, Double, Varchar},
	{Time, Date, Timestamp},
	{Time, Time, Time},
	{Time, Timestamp, Timestamp},
	{Time, Uuid, Varchar},
	{Time, Varchar, Varchar},
	{Time, Json, Varchar},

	// Timestamp
	{Timestamp, Null, Timestamp},
	{Timestamp, Boolean, Varchar},
	{Timestamp, Utinyint, Varchar},
	{Timestamp, Usmallint, Varchar},
	{Timestamp, Uinteger, Varchar},
	{Timestamp, Ubigint, Varchar},
	{Timestamp, Tinyint, Varchar},
	{Timestamp, Smallint, Varchar},
	{Timestamp, Integer, Varchar},
	{Timestamp, Bigint, Varchar},
	{Timestamp, Hugeint, Varchar},
	{Timestamp, Float, Varchar},
	{Timestamp, Double, Varchar},
	{Timestamp, Date, Timestamp},
	{Timestamp, Time, Timestamp},
	{Timestamp, Timestamp, Timestamp},
	{Timestamp, Uuid, Varchar},
	{Timestamp, Varchar, Varchar},
	{Timestamp, Json, Varchar},

	// Uuid
	{Uuid, Null, Uuid},
	{Uuid, Boolean, Varchar},
	{Uuid, Utinyint, Varchar},
	{Uuid, Usmallint, Varchar},
	{Uuid, Uinteger, Varchar},
	{Uuid, Ubigint, Varchar},
	{Uuid, Tinyint, Varchar},
	{Uuid, Smallint, Varchar},
	{Uuid, Integer, Varchar},
	{Uuid, Bigint, Varchar},
	{Uuid, Hugeint, Varchar},
	{Uuid, Float, Varchar},
	{Uuid, Double, Varchar},
	{Uuid, Date, Varchar},
	{Uuid, Time, Varchar},
	{Uuid, Timestamp, Varchar},
	{Uuid, Uuid, Uuid},
	{Uuid, Varchar, Varchar},
	{Uuid, Json, Varchar},

	// Varchar
	{Varchar, Null, Varchar},
	{Varchar, Boolean, Varchar},
	{Varchar, Utinyint, Varchar},
	{Varchar, Usmallint, Varchar},
	{Varchar, Uinteger, Varchar},
	{Varchar, Ubigint, Varchar},
	{Varchar, Tinyint, Varchar},
	{Varchar, Smallint, Varchar},
	{Varchar, Integer, Varchar},
	{Varchar, Bigint, Varchar},
	{Varchar, Hugeint, Varchar},
	{Varchar, Float, Varchar},
	{Varchar, Double, Varchar},
	{Varchar, Date, Varchar},
	{Varchar, Time, Varchar},
	{Varchar, Timestamp, Varchar},
	{Varchar, Uuid, Varchar},
	{Varchar, Varchar, Varchar},
	{Varchar, Json, Varchar},

	// Json
	{Json, Null, Json},
	{Json, Boolean, Varchar},
	{Json, Utinyint, Varchar},
	{Json, Usmallint, Varchar},
	{Json, Uinteger, Varchar},
	{Json, Ubigint, Varchar},
	{Json, Tinyint, Varchar},
	{Json, Smallint, Varchar},
	{Json, Integer, Varchar},
	{Json, Bigint, Varchar},
	{Json, Hugeint, Varchar},
	{Json, Float, Varchar},
	{Json, Double, Varchar},
	{Json, Date, Varchar},
	{Json, Time, Varchar},
	{Json, Timestamp, Varchar},
	{Json, Uuid, Varchar},
	{Json, Varchar, Varchar},
	{Json, Json, Json},
}

func Test_get_promote_type_based_on_current_and_given_type(t *testing.T) {
	for _, tc := range typeTransformations {
		t.Run(fmt.Sprintf("old_%s_given_%v_promotion_%v", tc.old, tc.given, tc.promotion), func(t *testing.T) {
			t.Parallel()
			is := is.New(t)
			result, err := tc.old.PromoteTo(tc.given)

			// Then
			is.NoErr(err)
			is.Equal(result, tc.promotion)
		})
	}
}

func Test_promote_existing_column(t *testing.T) {
	w, err := NewMemoryClient()
	if err != nil {
		t.Fatalf("failed to init client: %v", err)
	}
	t.Cleanup(func() {
		w.Close()
	})

	for _, tc := range typeTransformations {
		if tc.old == tc.given {
			continue
		}
		name := fmt.Sprintf("%s_to_%v_expects_%v", tc.old, tc.given, tc.promotion)

		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Given
			is := is.New(t)
			// First create a column with the old type
			mockColumn(t, w, name+"_table", "column_to_promote", tc.old)
			// Insert a value of the given type
			value := getExampleValueByType(t, tc.old)
			err := w.Write(name+"_table", NewRow(time.Now(), Row{"column_to_promote": value}))
			if err != nil {
				t.Fatalf("failed to write initial value of type %s: %v", tc.old, err)
			}

			// When
			err = w.promoteColumn(name+"_table", "column_to_promote", tc.old, tc.promotion)

			// Then
			is.NoErr(err)
			got := getCurrentType(t, w, name+"_table", "column_to_promote")
			if err != nil {
				t.Logf("Test failed for old type %s, promote to %s: %v", tc.old, tc.promotion, err)
				is.Equal(got, tc.promotion) // result != expected
			}
		})
	}
}

func getExampleValueByType(t *testing.T, colType ColumnType) any {
	switch colType {
	case Null:
		return nil
	case Boolean:
		return true
	case Utinyint:
		return int(1)
	case Usmallint:
		return int(256)
	case Uinteger:
		return int(65536)
	case Ubigint:
		return int64(4294967296)
	case Tinyint:
		return int(-1)
	case Smallint:
		return int(-129)
	case Integer:
		return int(-32769)
	case Bigint:
		return int64(-2147483649)
	case Hugeint:
		return int64(-9223372036854775808)
	case Float:
		return float32(3.4e+38)
	case Double:
		return float64(1.7e+308)
	case Date:
		return "2023-01-04"
	case Time:
		return "12:32:43"
	case Timestamp:
		return "2023-06-02 12:54:31.123456"
	case Uuid:
		return "550e8400-e29b-41d4-a716-446655440000"
	case Varchar:
		return "my string"
	case Json:
		return []any{1, 2, 3}
	default:
		t.Fatalf("unsupported column type: %s", colType)
		return nil
	}
}

func Test_promote_boolean_column_to_utinyint_when_value_is_int(t *testing.T) {
	is, w := setup(t)
	// First create a boolean column
	mockColumn(t, w, "timeline", "column_with_bool_to_1", Boolean)
	value := 1

	err := w.Write("timeline", NewRow(time.Now(), Row{"column_with_bool_to_1": value}))

	is.NoErr(err)
	is.Equal(getCurrentType(t, w, "timeline", "column_with_bool_to_1"), Utinyint) // result != expected
}

func Test_no_promotion_of_column_when_value_fits_current_type(t *testing.T) {
	is, w := setup(t)
	// First create a bigint column
	mockColumn(t, w, "timeline", "column_with_int", Utinyint)

	err := w.Write("timeline", NewRow(time.Now(), Row{"column_with_int": 120}))

	is.NoErr(err)
	is.Equal(getCurrentType(t, w, "timeline", "column_with_int"), Utinyint) // result != expected
}

func Test_use_date_when_promoting_time_to_timestamp(t *testing.T) {
	is, w := setup(t)
	// Given
	// First create a column with the old type
	mockColumn(t, w, "timeline", "column_to_promote", Time)

	// insert time
	now := time.Date(2444, 4, 4, 23, 55, 2, 0, time.UTC)
	w.Write("timeline", NewRow(now, Row{"column_to_promote": "12:00:00"}))

	// Insert a value of the given type
	err := w.Write("timeline", NewRow(time.Now(), Row{"column_to_promote": "2333-03-03"}))
	if err != nil {
		t.Fatalf("failed to write initial value of type %s: %v", Time, err)
	}

	// get all values
	rows := getValues(t, w, "timeline", "column_to_promote")
	is.Equal(len(rows), 2)
	is.Equal(fmt.Sprintf("%v", rows[0]), "2444-04-04 12:00:00 +0000 UTC")
	is.Equal(fmt.Sprintf("%v", rows[1]), "2333-03-03 00:00:00 +0000 UTC")
}

func Test_use_time_when_promoting_date_to_timestamp(t *testing.T) {
	is, w := setup(t)
	// Given
	// First create a column with the old type
	mockColumn(t, w, "timeline", "column_to_promote", Date)

	// insert date
	now := time.Date(2444, 4, 4, 23, 55, 2, 0, time.UTC)
	w.Write("timeline", NewRow(now, Row{"column_to_promote": "2022-02-02"}))

	// Insert a value of the given type
	err := w.Write("timeline", NewRow(time.Now(), Row{"column_to_promote": "2444-04-04 12:00:00"}))
	if err != nil {
		t.Fatalf("failed to write initial value of type %s: %v", Date, err)
	}

	// get all values
	rows := getValues(t, w, "timeline", "column_to_promote")
	is.Equal(len(rows), 2)
	is.Equal(fmt.Sprintf("%v", rows[0]), "2022-02-02 00:00:00 +0000 UTC")
	is.Equal(fmt.Sprintf("%v", rows[1]), "2444-04-04 12:00:00 +0000 UTC")
}

func Test_use_the_timestamp_column_when_date_is_missing(t *testing.T) {
	is, w := setup(t)

	// First create a time column
	mockColumn(t, w, "timeline", "column_with_time", Timestamp)
	now := time.Date(2023, 1, 2, 23, 55, 2, 299000000, time.UTC)

	err := w.Write("timeline", NewRow(now, Row{"column_with_time": "10:00:00"}))

	is.NoErr(err)
	is.Equal(getCurrentType(t, w, "timeline", "column_with_time"), Timestamp) // result != expected
	// get value string
	rows := getValues(t, w, "timeline", "column_with_time")
	is.Equal(len(rows), 1)
	is.Equal(rows[0].(time.Time).Format("2006-01-02 15:04:05"), "2023-01-02 10:00:00")
}

func Test_use_the_timestamp_column_when_time_is_missing(t *testing.T) {
	is, w := setup(t)
	// First create a time column
	mockColumn(t, w, "timeline", "column_with_date", Timestamp)
	now := time.Date(2023, 1, 2, 23, 55, 2, 299000000, time.UTC)

	err := w.Write("timeline", NewRow(now, Row{"column_with_date": "2023-01-02"}))

	is.NoErr(err)
	is.Equal(getCurrentType(t, w, "timeline", "column_with_date"), Timestamp) // result != expected
	// get value string
	rows := getValues(t, w, "timeline", "column_with_date")
	is.Equal(len(rows), 1)
	is.Equal(rows[0], time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC)) // date only, time is 00:00:00
}

func getCurrentType(t *testing.T, writer *Writer, table, column string) ColumnType {
	var dataType string
	err := writer.DB.QueryRow(`SELECT data_type FROM information_schema.columns WHERE table_name = ? AND column_name = ?`, table, column).Scan(&dataType)
	if err != nil {
		t.Fatalf("failed to get column type: %v", err)
	}
	return ColumnType(dataType)
}

func getValues(t *testing.T, writer *Writer, table, column string) []any {
	rows, err := writer.DB.Query(`SELECT ` + column + ` FROM ` + table)
	if err != nil {
		t.Fatalf("failed to get column values: %v", err)
	}
	defer rows.Close()

	var values []any
	for rows.Next() {
		var value any
		if err := rows.Scan(&value); err != nil {
			t.Fatalf("failed to scan value: %v", err)
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows error: %v", err)
	}
	return values
}
