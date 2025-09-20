package timeline

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
)

func ParseLineToValues(l string) Row {
	if l == "" {
		return Row{}
	}

	if result := parseJSON(l); result != nil {
		return result
	}

	if result := parseSyslog(l); result != nil {
		return result
	}

	if result := parseCLF(l); result != nil {
		return result
	}

	if result := parseLogfmt(l); result != nil {
		return result
	}

	return Row{"message": l}
}

// parseJSON parses a JSON-formatted log line.
// Supports standard JSON objects with automatic type conversion for numbers.
// json.Number values are converted to int if possible, otherwise float64.
// Example: {"level": "info", "message": "User logged in", "user_id": 123, "timestamp": "2023-01-01T12:00:00Z"}
// Fields: all JSON keys with their corresponding values and types preserved
func parseJSON(l string) Row {
	var data map[string]interface{}
	decoder := json.NewDecoder(bytes.NewReader([]byte(l)))
	decoder.UseNumber()
	err := decoder.Decode(&data)
	if err != nil {
		return nil
	}

	// Convert json.Number to int if possible, otherwise float64
	result := make(Row)
	for k, v := range data {
		if num, ok := v.(json.Number); ok {
			if i, err := num.Int64(); err == nil {
				result[k] = int(i)
			} else if f, err := num.Float64(); err == nil {
				result[k] = f
			} else {
				result[k] = num.String()
			}
		} else {
			result[k] = v
		}
	}
	return result
}

// parseSyslog parses syslog-formatted log lines (both RFC3164 and RFC5424).
// RFC3164 format: <priority>timestamp hostname tag: message
// RFC5424 format: <priority>version timestamp hostname app-name procid msgid [structured-data] message
// Examples:
//
//	RFC3164: <34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/8
//	RFC5424: <165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut="3"] BOMAn application event log entry...
//
// Fields: priority, facility, severity, version (RFC5424), timestamp, hostname, app_name (RFC5424), procid (RFC5424), msgid (RFC5424), tag (RFC3164), structured_data (RFC5424 as map[string]any), message
func parseSyslog(l string) Row {
	if !strings.HasPrefix(l, "<") {
		return nil
	}

	// Find the end of priority
	endPri := strings.Index(l, ">")
	if endPri == -1 {
		return nil
	}

	priStr := l[1:endPri]
	priority, err := strconv.Atoi(priStr)
	if err != nil {
		return nil
	}

	rest := l[endPri+1:]
	result := make(Row)
	result["priority"] = priority
	result["facility"] = priority / 8
	result["severity"] = priority % 8

	// Check if RFC5424 (has version)
	if len(rest) > 0 && rest[0] >= '0' && rest[0] <= '9' {
		// RFC5424
		parts := strings.Fields(rest)
		if len(parts) < 7 {
			return nil
		}

		result["version"], _ = strconv.Atoi(parts[0])
		result["timestamp"] = parts[1]
		result["hostname"] = parts[2]
		result["app_name"] = parts[3]
		result["procid"] = parts[4]
		result["msgid"] = parts[5]

		// Find structured data
		sdStart := strings.Index(rest, "[")
		sdEnd := strings.Index(rest, "]")
		if sdStart != -1 && sdEnd != -1 && sdEnd > sdStart {
			sdContent := rest[sdStart+1 : sdEnd]
			result["structured_data"] = parseStructuredData(sdContent)
			result["message"] = strings.TrimSpace(rest[sdEnd+1:])
		} else {
			result["structured_data"] = map[string]any{}
			result["message"] = strings.TrimSpace(rest)
		}
	} else {
		// RFC3164
		// Format: timestamp hostname tag: message
		parts := strings.Fields(rest)
		if len(parts) < 4 {
			return nil
		}

		timestamp := parts[0] + " " + parts[1] + " " + parts[2]
		hostname := parts[3]
		remaining := strings.Join(parts[4:], " ")

		// Find tag:
		colon := strings.Index(remaining, ":")
		if colon == -1 {
			return nil
		}

		tag := strings.TrimSpace(remaining[:colon])
		message := strings.TrimSpace(remaining[colon+1:])

		result["timestamp"] = timestamp
		result["hostname"] = hostname
		result["tag"] = tag
		result["message"] = message
	}

	return result
}

// parseStructuredData parses RFC5424 syslog structured data format.
// Format: key="value" pairs separated by spaces, with optional SD-ID prefix.
// Example: exampleSDID@32473 iut="3" eventSource="Application"
// Returns a map[string]any with parsed key-value pairs.
func parseStructuredData(sd string) map[string]any {
	result := make(map[string]any)

	// Split by spaces to get individual key="value" pairs
	parts := strings.Fields(sd)
	if len(parts) == 0 {
		return result
	}

	// First part might be SD-ID (contains @)
	if strings.Contains(parts[0], "@") {
		result["sd_id"] = parts[0]
		parts = parts[1:]
	}

	// Parse remaining key="value" pairs
	for _, part := range parts {
		if eqIndex := strings.Index(part, "="); eqIndex != -1 {
			key := part[:eqIndex]
			value := part[eqIndex+1:]

			// Remove surrounding quotes if present
			if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
				value = value[1 : len(value)-1]
			}

			result[key] = value
		}
	}

	return result
}

// parseCLF parses a Common Log Format (CLF) or Combined Log Format line.
// CLF is the standard format for Apache HTTP server access logs.
// Combined Log Format extends CLF with referer and user-agent fields.
// CLF Format: %h %l %u [%t] "%r" %>s %b
// Combined Format: %h %l %u [%t] "%r" %>s %b "%{Referer}i" "%{User-agent}i"
// Examples:
//
//	CLF: 127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326
//	Combined: 127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326 "http://www.example.com/" "Mozilla/5.0"
//
// Fields: remote_host, remote_logname, remote_user, timestamp, request, status, response_size, referer (Combined only), user_agent (Combined only)
func parseCLF(l string) Row {
	// CLF/Combined regex: ^(\S+) (\S+) (\S+) \[([^\]]+)\] "([^"]*)" (\d+) (\d+|-)(?: "([^"]*)" "([^"]*)")?$`
	re := regexp.MustCompile(`^(\S+) (\S+) (\S+) \[([^\]]+)\] "([^"]*)" (\d+) (\d+|-)(?: "([^"]*)" "([^"]*)")?$`)
	matches := re.FindStringSubmatch(l)
	if matches == nil {
		return nil
	}

	result := make(Row)
	result["remote_host"] = matches[1]
	result["remote_logname"] = matches[2]
	result["remote_user"] = matches[3]
	result["timestamp"] = matches[4]
	result["request"] = matches[5]
	if status, err := strconv.Atoi(matches[6]); err == nil {
		result["status"] = status
	}
	if matches[7] != "-" {
		if size, err := strconv.Atoi(matches[7]); err == nil {
			result["response_size"] = size
		}
	} else {
		result["response_size"] = 0
	}

	// Check if Combined Log Format (has referer and user-agent)
	if len(matches) > 8 && matches[8] != "" {
		result["referer"] = matches[8]
	}
	if len(matches) > 9 && matches[9] != "" {
		result["user_agent"] = matches[9]
	}

	return result
}

// parseLogfmt parses logfmt-formatted log lines.
// Logfmt is a structured logging format with key=value pairs separated by spaces.
// Values can be quoted or unquoted, with quoted values supporting spaces.
// Examples:
//
//	time=2025-09-19T20:35:00Z level=info msg="User login successful" user_id=123
//	service=user-api status=200 response_time=0.45
//
// Fields: all key-value pairs with automatic type conversion for numbers
func parseLogfmt(l string) Row {
	result := make(Row)

	// Split by spaces, but be careful with quoted values
	parts := strings.Fields(l)
	if len(parts) == 0 {
		return nil
	}

	i := 0
	for i < len(parts) {
		part := parts[i]

		// Find the equals sign
		eqIndex := strings.Index(part, "=")
		if eqIndex == -1 {
			// Not a key=value pair, skip
			i++
			continue
		}

		key := part[:eqIndex]
		value := part[eqIndex+1:]

		// Check if value starts with quote
		if strings.HasPrefix(value, "\"") {
			// Handle quoted value that might span multiple parts
			if strings.HasSuffix(value, "\"") && len(value) > 1 {
				// Simple quoted value
				value = value[1 : len(value)-1]
			} else {
				// Multi-part quoted value
				value = value[1:] // Remove opening quote
				i++
				for i < len(parts) {
					nextPart := parts[i]
					if strings.HasSuffix(nextPart, "\"") {
						// This is the last part of the quoted value
						value += " " + nextPart[:len(nextPart)-1]
						break
					} else {
						value += " " + nextPart
					}
					i++
				}
			}
		}

		// Try to convert to number
		if intVal, err := strconv.Atoi(value); err == nil {
			result[key] = intVal
		} else if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
			result[key] = floatVal
		} else {
			result[key] = value
		}

		i++
	}

	// Only return result if we actually parsed some key-value pairs
	if len(result) > 0 {
		return result
	}
	return nil
}
