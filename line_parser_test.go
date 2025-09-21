package timeline

import (
	"testing"

	"github.com/matryer/is"
)

func Test_parse_empty_json_object(t *testing.T) {
	is := is.New(t)
	line := `{}`

	data := ParseLineToValues(line)

	// Empty JSON object should result in no data
	is.True(len(data) == 0)
}

func Test_parse_json_line_with_one_value(t *testing.T) {
	is := is.New(t)
	line := `{"title": "my title"}`

	data := ParseLineToValues(line)

	is.Equal(data["title"], "my title")
}

func Test_parse_json_line_with_invalid_json(t *testing.T) {
	is := is.New(t)
	line := `{"title": "my title"`

	data := ParseLineToValues(line)

	is.Equal(data["message"], `{"title": "my title"`)
}

func Test_parse_empty_json_line(t *testing.T) {
	is := is.New(t)
	line := ``

	data := ParseLineToValues(line)

	// Empty line should result in no data
	is.True(len(data) == 0)
}

func Test_parse_json_line_with_int_value(t *testing.T) {
	is := is.New(t)
	line := `{"count": 42}`

	data := ParseLineToValues(line)

	is.Equal(len(data), 1)
	// Check that count is int, not float
	if v, ok := data["count"].(int); ok {
		is.Equal(v, 42)
	} else {
		t.Errorf("Expected int, got %T", data["count"])
	}
}

func Test_parse_json_line_with_float_value(t *testing.T) {
	is := is.New(t)
	line := `{"price": 42.5}`

	data := ParseLineToValues(line)

	is.Equal(len(data), 1)
	// Check that price is float
	if v, ok := data["price"].(float64); ok {
		is.Equal(v, 42.5)
	} else {
		t.Errorf("Expected float64, got %T", data["price"])
	}
}

func Test_parse_json_line_with_zero_int(t *testing.T) {
	is := is.New(t)
	line := `{"count": 0}`

	data := ParseLineToValues(line)

	is.Equal(len(data), 1)
	if v, ok := data["count"].(int); ok {
		is.Equal(v, 0)
	} else {
		t.Errorf("Expected int, got %T", data["count"])
	}
}

func Test_parse_json_line_with_negative_int(t *testing.T) {
	is := is.New(t)
	line := `{"count": -42}`

	data := ParseLineToValues(line)

	is.Equal(len(data), 1)
	if v, ok := data["count"].(int); ok {
		is.Equal(v, -42)
	} else {
		t.Errorf("Expected int, got %T", data["count"])
	}
}

func Test_parse_json_line_with_large_int(t *testing.T) {
	is := is.New(t)
	line := `{"big_number": 9223372036854775807}` // max int64

	data := ParseLineToValues(line)

	is.Equal(len(data), 1)
	if v, ok := data["big_number"].(int); ok {
		is.Equal(v, 9223372036854775807)
	} else {
		t.Errorf("Expected int, got %T", data["big_number"])
	}
}

func Test_parse_json_line_with_scientific_float(t *testing.T) {
	is := is.New(t)
	line := `{"scientific": 1.23e-4}`

	data := ParseLineToValues(line)

	is.Equal(len(data), 1)
	if v, ok := data["scientific"].(float64); ok {
		is.Equal(v, 1.23e-4)
	} else {
		t.Errorf("Expected float64, got %T", data["scientific"])
	}
}

func Test_parse_syslog_rfc3164_line(t *testing.T) {
	is := is.New(t)
	line := `<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/8`

	data := ParseLineToValues(line)

	is.Equal(len(data), 7)
	is.Equal(data["priority"], 34)
	is.Equal(data["timestamp"], "Oct 11 22:14:15")
	is.Equal(data["hostname"], "mymachine")
	is.Equal(data["tag"], "su")
	is.Equal(data["message"], "'su root' failed for lonvick on /dev/pts/8")
	is.Equal(data["facility"], 4)
	is.Equal(data["severity"], 2)
}

func Test_parse_syslog_rfc5424_line(t *testing.T) {
	is := is.New(t)
	line := `<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut="3" eventSource="Application"] BOMAn application event log entry...`

	data := ParseLineToValues(line)

	is.Equal(len(data), 11)
	is.Equal(data["priority"], 165)
	is.Equal(data["version"], 1)
	is.Equal(data["timestamp"], "2003-10-11T22:14:15.003Z")
	is.Equal(data["hostname"], "mymachine.example.com")
	is.Equal(data["app_name"], "evntslog")
	is.Equal(data["procid"], "-")
	is.Equal(data["msgid"], "ID47")

	// Check structured_data is parsed as map
	if sd, ok := data["structured_data"].(map[string]any); ok {
		is.Equal(sd["sd_id"], "exampleSDID@32473")
		is.Equal(sd["iut"], "3")
		is.Equal(sd["eventSource"], "Application")
	} else {
		t.Errorf("Expected structured_data to be map[string]any, got %T", data["structured_data"])
	}

	is.Equal(data["message"], "BOMAn application event log entry...")
	is.Equal(data["facility"], 20)
	is.Equal(data["severity"], 5)
}

func Test_parse_syslog_rfc3164_minimal_line(t *testing.T) {
	is := is.New(t)
	line := `<13>Jun 15 10:30:00 localhost test: hello world`

	data := ParseLineToValues(line)

	is.Equal(len(data), 7)
	is.Equal(data["priority"], 13)
	is.Equal(data["timestamp"], "Jun 15 10:30:00")
	is.Equal(data["hostname"], "localhost")
	is.Equal(data["tag"], "test")
	is.Equal(data["message"], "hello world")
	is.Equal(data["facility"], 1)
	is.Equal(data["severity"], 5)
}

func Test_parse_clf_standard_line(t *testing.T) {
	is := is.New(t)
	line := `127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326`

	data := ParseLineToValues(line)

	// With nil values, remote_logname should not be present since it was "-"
	// Should have 8 fields: remote_host, remote_user, timestamp, method, path, protocol, status, response_size
	is.Equal(len(data), 8)
	is.Equal(data["remote_host"], "127.0.0.1")
	// remote_logname should not be present (nil)
	_, exists := data["remote_logname"]
	is.Equal(exists, false)
	is.Equal(data["remote_user"], "frank")
	is.Equal(data["timestamp"], "10/Oct/2000:13:55:36 -0700")
	is.Equal(data["method"], "GET")          // HTTP method
	is.Equal(data["path"], "/apache_pb.gif") // Request path
	is.Equal(data["protocol"], "HTTP/1.0")   // Protocol version
	is.Equal(data["status"], 200)
	is.Equal(data["response_size"], 2326)
}

func Test_parse_clf_line_with_dash_size(t *testing.T) {
	is := is.New(t)
	line := `192.168.1.1 - - [15/Dec/2023:10:30:45 +0000] "POST /api/login HTTP/1.1" 401 -`

	data := ParseLineToValues(line)

	// With nil values, remote_logname and remote_user should not be present since they were "-"
	// Should have 7 fields: remote_host, timestamp, method, path, protocol, status, response_size
	is.Equal(len(data), 7)
	is.Equal(data["remote_host"], "192.168.1.1")
	// remote_logname should not be present (nil)
	_, exists := data["remote_logname"]
	is.Equal(exists, false)
	// remote_user should not be present (nil)
	_, exists = data["remote_user"]
	is.Equal(exists, false)
	is.Equal(data["response_size"], 0)
}

func Test_parse_clf_line_with_hostname(t *testing.T) {
	is := is.New(t)
	line := `example.com - alice [20/Jan/2024:14:20:30 +0200] "GET /index.html HTTP/1.1" 304 178`

	data := ParseLineToValues(line)

	// With nil values, remote_logname should not be present since it was "-"
	// Should have 8 fields: remote_host, remote_user, timestamp, method, path, protocol, status, response_size
	is.Equal(len(data), 8)
	is.Equal(data["remote_host"], "example.com")
	// remote_logname should not be present (nil)
	_, exists := data["remote_logname"]
	is.Equal(exists, false)
	is.Equal(data["remote_user"], "alice")
	is.Equal(data["timestamp"], "20/Jan/2024:14:20:30 +0200")
	is.Equal(data["method"], "GET")        // HTTP method
	is.Equal(data["path"], "/index.html")  // Request path
	is.Equal(data["protocol"], "HTTP/1.1") // Protocol version
	is.Equal(data["status"], 304)
	is.Equal(data["response_size"], 178)
}

func Test_parse_clf_invalid_line(t *testing.T) {
	is := is.New(t)
	line := `127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200`

	data := ParseLineToValues(line)

	// Should fall back to message since it's not a valid CLF line (missing size)
	is.Equal(len(data), 1)
	is.Equal(data["message"], line)
}

func Test_parse_combined_log_format_standard_line(t *testing.T) {
	is := is.New(t)
	line := `127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326 "http://www.example.com/start.html" "Mozilla/4.08 [en] (Win98; I ;Nav)"`

	data := ParseLineToValues(line)

	// With nil values, remote_logname should not be present since it was "-"
	// Should have 10 fields: remote_host, remote_user, timestamp, method, path, protocol, status, response_size, referer, user_agent
	is.Equal(len(data), 10)
	is.Equal(data["remote_host"], "127.0.0.1")
	// remote_logname should not be present (nil)
	_, exists := data["remote_logname"]
	is.Equal(exists, false)
	is.Equal(data["remote_user"], "frank")
	is.Equal(data["timestamp"], "10/Oct/2000:13:55:36 -0700")
	is.Equal(data["method"], "GET")          // HTTP method
	is.Equal(data["path"], "/apache_pb.gif") // Request path
	is.Equal(data["protocol"], "HTTP/1.0")   // Protocol version
	is.Equal(data["status"], 200)
	is.Equal(data["response_size"], 2326)
	is.Equal(data["referer"], "http://www.example.com/start.html")
	is.Equal(data["user_agent"], "Mozilla/4.08 [en] (Win98; I ;Nav)")
}

func Test_parse_combined_log_format_with_dash_referer(t *testing.T) {
	is := is.New(t)
	line := `192.168.1.100 - alice [15/Dec/2023:10:30:45 +0000] "POST /api/login HTTP/1.1" 200 1234 "-" "curl/7.68.0"`

	data := ParseLineToValues(line)

	// referer should not be present (nil) since it was "-"
	_, exists := data["referer"]
	is.Equal(exists, false)
}

func Test_parse_combined_log_format_minimal(t *testing.T) {
	is := is.New(t)
	line := `example.com - - [20/Jan/2024:14:20:30 +0200] "GET /index.html HTTP/1.1" 304 178 "https://www.google.com/" "Mozilla/5.0 (compatible; Googlebot/2.1)"`

	data := ParseLineToValues(line)

	// With nil values, remote_logname and remote_user should not be present since they were "-"
	// Should have 9 fields: remote_host, timestamp, method, path, protocol, status, response_size, referer, user_agent
	is.Equal(len(data), 9)
	is.Equal(data["remote_host"], "example.com")
	// remote_logname should not be present (nil)
	_, exists := data["remote_logname"]
	is.Equal(exists, false)
	// remote_user should not be present (nil)
	_, exists = data["remote_user"]
	is.Equal(exists, false)
	is.Equal(data["timestamp"], "20/Jan/2024:14:20:30 +0200")
	is.Equal(data["method"], "GET")        // HTTP method
	is.Equal(data["path"], "/index.html")  // Request path
	is.Equal(data["protocol"], "HTTP/1.1") // Protocol version
	is.Equal(data["status"], 304)
	is.Equal(data["response_size"], 178)
	is.Equal(data["referer"], "https://www.google.com/")
	is.Equal(data["user_agent"], "Mozilla/5.0 (compatible; Googlebot/2.1)")
}

func Test_parse_logfmt_standard_line(t *testing.T) {
	is := is.New(t)
	line := `time=2025-09-19T20:35:00Z level=info service=user-api msg="User login successful" user_id=123`

	data := ParseLineToValues(line)

	is.Equal(len(data), 5)
	is.Equal(data["time"], "2025-09-19T20:35:00Z")
	is.Equal(data["level"], "info")
	is.Equal(data["service"], "user-api")
	is.Equal(data["msg"], "User login successful")
	is.Equal(data["user_id"], 123)
}

func Test_parse_logfmt_with_numbers_and_quotes(t *testing.T) {
	is := is.New(t)
	line := `service=user-api status=200 response_time=0.45 method=POST path="/api/login" user_id=456`

	data := ParseLineToValues(line)

	is.Equal(len(data), 6)
	is.Equal(data["service"], "user-api")
	is.Equal(data["status"], 200)
	is.Equal(data["response_time"], 0.45)
	is.Equal(data["method"], "POST")
	is.Equal(data["path"], "/api/login")
	is.Equal(data["user_id"], 456)
}

func Test_parse_logfmt_simple_unquoted(t *testing.T) {
	is := is.New(t)
	line := `level=debug msg=simple_message count=42`

	data := ParseLineToValues(line)

	is.Equal(len(data), 3)
	is.Equal(data["level"], "debug")
	is.Equal(data["msg"], "simple_message")
	is.Equal(data["count"], 42)
}

func Test_parse_logfmt_invalid_line(t *testing.T) {
	is := is.New(t)
	line := `this is not a logfmt line`

	data := ParseLineToValues(line)

	// Should fall back to message since it's not valid logfmt
	is.Equal(len(data), 1)
	is.Equal(data["message"], line)
}

func Test_parse_extended_clf_forwarded_for(t *testing.T) {
	tests := []struct {
		name        string
		line        string
		expected    any
		shouldExist bool
	}{
		{
			name:        "with forwarded_for IP",
			line:        `10.10.2.2 - - [20/Sep/2025:23:41:41 +0000] "GET / HTTP/1.1" 200 39689 "-" "Mozilla/5.0" "10.10.2.1"`,
			expected:    "10.10.2.1",
			shouldExist: true,
		},
		{
			name:        "with dash forwarded_for",
			line:        `192.168.1.100 - alice [15/Dec/2023:10:30:45 +0000] "POST /api/login HTTP/1.1" 200 1234 "https://example.com/login" "curl/7.68.0" "-"`,
			expected:    nil,
			shouldExist: false,
		},
		{
			name:        "with IPv6 forwarded_for",
			line:        `2001:db8::1 - - [15/Dec/2023:10:30:45 +0000] "GET /api/data HTTP/1.1" 200 567 "https://example.com/" "Mozilla/5.0" "192.168.1.1"`,
			expected:    "192.168.1.1",
			shouldExist: true,
		},
		{
			name:        "with empty forwarded_for",
			line:        `10.10.2.2 - - [20/Sep/2025:23:41:41 +0000] "GET / HTTP/1.1" 200 39689 "-" "Mozilla/5.0" ""`,
			expected:    "",
			shouldExist: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			data := ParseLineToValues(tt.line)

			if tt.shouldExist {
				is.Equal(data["forwarded_for"], tt.expected)
			} else {
				_, exists := data["forwarded_for"]
				is.Equal(exists, false)
			}
		})
	}
}

func Test_parse_clf_line_without_brackets(t *testing.T) {
	is := is.New(t)
	line := `10.10.2.11 -  21/Sep/2025:19:41:57 +0000 "GET /init.php" 200`

	data := ParseLineToValues(line)

	// Should have 7 fields: remote_host, timestamp, method, path, protocol, status, response_size
	is.Equal(len(data), 7)
	is.Equal(data["remote_host"], "10.10.2.11")
	// remote_logname should not be present (nil)
	_, exists := data["remote_logname"]
	is.Equal(exists, false)
	// remote_user should not be present (nil)
	_, exists = data["remote_user"]
	is.Equal(exists, false)
	is.Equal(data["timestamp"], "21/Sep/2025:19:41:57 +0000")
	is.Equal(data["method"], "GET")        // HTTP method
	is.Equal(data["path"], "/init.php")    // Request path
	is.Equal(data["protocol"], "HTTP/1.0") // Protocol version (default when missing)
	is.Equal(data["status"], 200)
	is.Equal(data["response_size"], 0) // Default when missing (no response_size in this format)
}

func Test_stripAnsiCodes_basic_colors(t *testing.T) {
	is := is.New(t)

	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "green text",
			input:    "Done in \x1b[32m32ms\x1b[39m",
			expected: "Done in 32ms",
		},
		{
			name:     "red text",
			input:    "Error: \x1b[31mfailed\x1b[39m",
			expected: "Error: failed",
		},
		{
			name:     "bold text",
			input:    "\x1b[1mbold text\x1b[22m",
			expected: "bold text",
		},
		{
			name:     "multiple codes",
			input:    "\x1b[1m\x1b[32mSuccess\x1b[39m\x1b[22m",
			expected: "Success",
		},
		{
			name:     "no ansi codes",
			input:    "plain text",
			expected: "plain text",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "complex ansi sequence",
			input:    "\x1b[38;5;196m\x1b[48;5;15mColored text\x1b[0m",
			expected: "Colored text",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := stripAnsiCodes(tc.input)
			is.Equal(result, tc.expected)
		})
	}
}

func Test_parse_unmatched_format_with_ansi_codes(t *testing.T) {
	is := is.New(t)

	// Test that ANSI codes are stripped when no format matches
	line := "Done in \x1b[32m32ms\x1b[39m"
	data := ParseLineToValues(line)

	is.Equal(len(data), 1)
	is.Equal(data["message"], "Done in 32ms")
}

func Test_parse_unmatched_format_mixed_ansi(t *testing.T) {
	is := is.New(t)

	// Test with multiple ANSI codes
	line := "\x1b[1m\x1b[31mError:\x1b[39m\x1b[22m Something went wrong"
	data := ParseLineToValues(line)

	is.Equal(len(data), 1)
	is.Equal(data["message"], "Error: Something went wrong")
}
