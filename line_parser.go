package timeline

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
)

// stripAnsiCodes removes ANSI color codes from a string.
// ANSI color codes follow the pattern: \x1b[XXm where XX is a color/style code.
func stripAnsiCodes(s string) string {
	// Match ANSI escape sequences: \x1b[ followed by any number of parameters separated by ; and ending with m
	ansiRegex := regexp.MustCompile(`\x1b\[[0-9;]*m`)
	return ansiRegex.ReplaceAllString(s, "")
}

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

	if result := parseMonolog(l); result != nil {
		return result
	}

	return Row{"message": stripAnsiCodes(l)}
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
// Extended Format: %h %l %u [%t] "%r" %>s %b "%{Referer}i" "%{User-agent}i" "%{X-Forwarded-For}i"
// Also supports format without brackets around timestamp: %h %l %u %t "%r" %>s %b
// Examples:
//
//	CLF: 127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326
//	Combined: 127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326 "http://www.example.com/" "Mozilla/5.0"
//	Extended: 10.10.2.2 - - [20/Sep/2025:23:41:41 +0000] "GET / HTTP/1.1" 200 39689 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36" "10.10.2.1"
//	Without brackets: 10.10.2.11 -  21/Sep/2025:19:41:57 +0000 "GET /init.php" 200
//
// Fields: remote_host, remote_logname, remote_user, timestamp, request, status, response_size, referer (Combined only), user_agent (Combined only), forwarded_for (Extended only)
func parseCLF(l string) Row {
	// Split line by spaces to handle variable spacing
	parts := strings.Fields(l)
	if len(parts) < 6 {
		return nil
	}

	result := make(Row)

	// Find the request by looking for quoted string
	requestIndex := -1
	for i, part := range parts {
		if strings.HasPrefix(part, "\"") {
			requestIndex = i
			break
		}
	}

	if requestIndex == -1 || requestIndex < 3 {
		return nil
	}

	// Parse first three fields: remote_host, remote_logname, remote_user
	// Set fields only if they're not "-"
	if parts[0] != "-" {
		result["remote_host"] = parts[0]
	}
	if len(parts) > 1 && parts[1] != "-" {
		result["remote_logname"] = parts[1]
	}

	// Check if we have a bracketed timestamp format by looking at parts[3]
	// If parts[3] starts with '[', then it's a bracketed timestamp and parts[2] is remote_user
	// If parts[3] doesn't start with '[', then it's a non-bracketed timestamp and parts[2] is part of timestamp
	if len(parts) > 3 && strings.HasPrefix(parts[3], "[") {
		// Bracketed format: parts[2] is remote_user
		if len(parts) > 2 && parts[2] != "-" {
			result["remote_user"] = parts[2]
		}
		// Timestamp starts from parts[3]
		timestampStart := 3
		if requestIndex > timestampStart {
			timestampParts := parts[timestampStart:requestIndex]
			timestamp := strings.Join(timestampParts, " ")

			// Remove surrounding brackets if present
			if len(timestamp) >= 2 && timestamp[0] == '[' && timestamp[len(timestamp)-1] == ']' {
				timestamp = timestamp[1 : len(timestamp)-1]
			}

			result["timestamp"] = timestamp
		}
	} else {
		// Non-bracketed format: parts[2] is part of timestamp, no remote_user
		// Timestamp starts from parts[2]
		timestampStart := 2
		if requestIndex > timestampStart {
			timestampParts := parts[timestampStart:requestIndex]
			timestamp := strings.Join(timestampParts, " ")
			result["timestamp"] = timestamp
		}
	}

	// Parse request (combine quoted parts if needed)
	request := parts[requestIndex]
	if !strings.HasSuffix(request, "\"") {
		// Multi-part quoted request - find the closing quote
		for i := requestIndex + 1; i < len(parts); i++ {
			request += " " + parts[i]
			if strings.HasSuffix(parts[i], "\"") {
				break
			}
		}
	}

	// Calculate the actual end of the request (for status parsing)
	actualRequestEndIndex := requestIndex
	if !strings.HasSuffix(parts[requestIndex], "\"") {
		// Multi-part request - find where it ends
		for i := requestIndex + 1; i < len(parts); i++ {
			actualRequestEndIndex = i
			if strings.HasSuffix(parts[i], "\"") {
				break
			}
		}
	}

	// Remove surrounding quotes from request
	if len(request) >= 2 && request[0] == '"' && request[len(request)-1] == '"' {
		request = request[1 : len(request)-1]
	}

	// Parse request into method, path, and protocol
	requestParts := strings.Split(request, " ")
	if len(requestParts) >= 3 {
		result["method"] = requestParts[0]
		result["path"] = requestParts[1]
		result["protocol"] = requestParts[2]
	} else if len(requestParts) >= 2 {
		// Handle requests without protocol (e.g., "GET /init.php")
		result["method"] = requestParts[0]
		result["path"] = requestParts[1]
		result["protocol"] = "HTTP/1.0" // Default protocol when missing
	} else {
		// Fallback to storing full request if parsing fails
		result["request"] = request
	}

	// Parse status (should be right after request)
	statusIndex := actualRequestEndIndex + 1
	if statusIndex < len(parts) {
		if status, err := strconv.Atoi(parts[statusIndex]); err == nil {
			result["status"] = status
		}
	}

	// Parse response size (should be right after status)
	// Only parse if we have more parts and the next part looks like a number
	sizeIndex := actualRequestEndIndex + 2
	if sizeIndex < len(parts) && parts[sizeIndex] != "-" {
		if size, err := strconv.Atoi(parts[sizeIndex]); err == nil {
			result["response_size"] = size
		}
	} else {
		// If we don't have a response size field, check if this is a valid CLF line
		// For bracketed format, response size is required
		// For non-bracketed format, response size is optional and defaults to 0
		if sizeIndex >= len(parts) {
			// Check if this is a non-bracketed format (timestamp doesn't start with '[')
			if len(parts) > 3 && !strings.HasPrefix(parts[3], "[") {
				// Non-bracketed format - response size is optional
				result["response_size"] = 0
			} else {
				// Bracketed format - response size is required, this is not a valid CLF line
				return nil
			}
		} else {
			result["response_size"] = 0
		}
	}

	// Handle remaining optional fields (referer, user_agent, forwarded_for)
	remainingStart := actualRequestEndIndex + 3
	if remainingStart < len(parts) {
		quotedFields := parseQuotedFieldsFromSlice(parts[remainingStart:])

		// Check if Combined Log Format (has referer and user-agent)
		if len(quotedFields) > 0 && quotedFields[0] != "-" && quotedFields[0] != "" {
			result["referer"] = quotedFields[0]
		}
		if len(quotedFields) > 1 && quotedFields[1] != "-" && quotedFields[1] != "" {
			result["user_agent"] = quotedFields[1]
		}

		// Check if Extended Log Format (has forwarded_for)
		if len(quotedFields) > 2 && quotedFields[2] != "-" {
			result["forwarded_for"] = quotedFields[2]
		}
	}

	return result
}

// parseQuotedFieldsFromSlice parses quoted fields from a slice of strings.
// Returns a slice of field values, handling quoted strings properly.
func parseQuotedFieldsFromSlice(parts []string) []string {
	var fields []string

	i := 0
	for i < len(parts) {
		part := parts[i]

		// Check if this part starts with a quote
		if strings.HasPrefix(part, "\"") {
			var fieldValue strings.Builder
			fieldValue.WriteString(part)

			// Check if this quoted string spans multiple parts
			if !strings.HasSuffix(part, "\"") {
				// Multi-part quoted string
				i++
				for i < len(parts) {
					nextPart := parts[i]
					fieldValue.WriteString(" ")
					fieldValue.WriteString(nextPart)

					if strings.HasSuffix(nextPart, "\"") {
						break
					}
					i++
				}
			}

			// Extract the content between quotes
			quotedStr := fieldValue.String()
			if len(quotedStr) >= 2 && quotedStr[0] == '"' && quotedStr[len(quotedStr)-1] == '"' {
				// Handle empty quoted strings
				if len(quotedStr) == 2 {
					fields = append(fields, "")
				} else {
					fields = append(fields, quotedStr[1:len(quotedStr)-1])
				}
			}
		} else {
			// Unquoted field
			fields = append(fields, part)
		}

		i++
	}

	return fields
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

// parseMonolog parses Monolog-formatted log lines (Laravel/PHP logging format).
// Monolog format: [timestamp] channel.level: message {json_data}
// Examples:
//
//	[2025-09-21 22:35:12] local.DEBUG: User logged in {"id":1,"email":"john@example.com"}
//	[2025-09-21 22:35:12] production.ERROR: Database connection failed
//
// Fields: timestamp, channel, level, message, and any JSON data fields
func parseMonolog(l string) Row {
	// Check if line starts with timestamp in brackets
	if !strings.HasPrefix(l, "[") {
		return nil
	}

	// Find the end of timestamp
	endTime := strings.Index(l, "]")
	if endTime == -1 {
		return nil
	}

	timestamp := l[1:endTime]
	rest := strings.TrimSpace(l[endTime+1:])

	if rest == "" {
		return nil
	}

	result := make(Row)
	result["timestamp"] = timestamp

	// Find the colon that separates channel.level from message
	colonIndex := strings.Index(rest, ":")
	if colonIndex == -1 {
		return nil
	}

	// Parse channel.level
	channelLevel := strings.TrimSpace(rest[:colonIndex])
	if channelLevel == "" {
		return nil
	}

	// Split channel and level - must have exactly one dot
	parts := strings.Split(channelLevel, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return nil
	}

	result["channel"] = parts[0]
	result["level"] = parts[1]

	// Parse message and JSON data
	messageAndJSON := strings.TrimSpace(rest[colonIndex+1:])
	if messageAndJSON == "" {
		return nil
	}

	// Must have a space after the colon for valid Monolog format
	if colonIndex+1 >= len(rest) || rest[colonIndex+1] != ' ' {
		return nil
	}

	// Check if there's JSON data at the end
	// Look for the last occurrence of { that could be the start of JSON data
	braceIndex := strings.LastIndex(messageAndJSON, "{")
	if braceIndex != -1 && strings.HasSuffix(messageAndJSON, "}") {
		// Extract potential JSON part
		jsonPart := messageAndJSON[braceIndex:]
		messagePart := strings.TrimSpace(messageAndJSON[:braceIndex])

		// Only try to parse as JSON if the message part doesn't end with a colon
		// This helps avoid false positives where the message contains JSON-like content
		if !strings.HasSuffix(messagePart, ":") {
			// Parse JSON data
			var jsonData map[string]interface{}
			if err := json.Unmarshal([]byte(jsonPart), &jsonData); err == nil {
				// Add JSON fields to result
				for k, v := range jsonData {
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

				result["message"] = messagePart
				return result
			} else {
				// JSON parsing failed, log the error for debugging
				// For now, just fall through to treat as message
			}
		}
	}

	// No JSON data, whole thing is message
	result["message"] = messageAndJSON

	return result
}
