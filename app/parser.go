package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// readRESPArray parses a RESP array from the reader.
// It returns the array elements as a slice of strings, the number of bytes read, or an error.
func readRESPArray(r *bufio.Reader) ([]string, int, error) {
	// 1. Read the array header: *<number_of_elements>\r\n
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, 0, err
	}

	// Update replication offset if this instance is a replica
	if isReplica {
		offset += len(line)
	}

	readOffset := len(line)

	line = strings.TrimSpace(line)
	// Validate RESP array format
	if len(line) == 0 || line[0] != '*' {
		return nil, 0, fmt.Errorf("invalid RESP array")
	}

	// Parse the number of elements in the array
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, 0, err
	}

	result := make([]string, 0, n)

	for i := 0; i < n; i++ {
		// Read bulk string header: $<length>\r\n
		line, err := r.ReadString('\n')
		if err != nil {
			return nil, 0, err
		}

		if isReplica {
			offset += len(line)
		}

		readOffset += len(line)

		line = strings.TrimSpace(line)
		if line[0] != '$' {
			return nil, 0, fmt.Errorf("expected bulk string")
		}

		// Parse the length of the string
		l, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, 0, err
		}

		// Read the exact number of bytes for the string content + \r\n
		buf := make([]byte, l+2)
		nRead, err := io.ReadFull(r, buf)
		if err != nil {
			return nil, 0, err
		}

		if isReplica {
			offset += nRead
		}

		readOffset += nRead

		// Append string content (excluding \r\n) to result
		result = append(result, string(buf[:l]))
	}

	return result, 0, nil
}

// BulkStringArrayToStringArray converts a raw RESP array of bulk strings into a Go slice of strings.
func BulkStringArrayToStringArray(RESPArray []byte) []string {
	// Split by CRLF
	splitArray := strings.Split(string(RESPArray), "\r\n")

	// Parse the number of elements from the first line
	numberOfElements, _ := strconv.Atoi(splitArray[0][1:])

	stringArray := make([]string, 0, numberOfElements)

	// Iterate over the split array
	for i := 2; i < len(splitArray); i += 2 {
		if splitArray[i] == "" {
			break
		}

		stringArray = append(stringArray, splitArray[i])
	}

	return stringArray
}

// StringArrayToBulkStringArray encodes a Go string slice into a RESP Array.
func StringArrayToBulkStringArray(StringArray []string) []byte {
	// Start with array header
	BulkStringArray := "*" + strconv.Itoa(len(StringArray)) + "\r\n"

	// Append each element as a Bulk String
	for _, v := range StringArray {
		BulkStringArray += "$" + strconv.Itoa(len(v)) + "\r\n" + v + "\r\n"
	}

	return []byte(BulkStringArray)
}

// StringToBulkString encodes a single string as a RESP Bulk String.
func StringToBulkString(String string) []byte {
	return []byte("$" + strconv.Itoa(len(String)) + "\r\n" + String + "\r\n")
}

// EncodeArray converts a slice of typed ArrayElements into RESP.
func EncodeArray(elements []ArrayElement) []byte {
	resp := "*" + strconv.Itoa(len(elements)) + "\r\n"

	for _, el := range elements {
		switch el.Type {
		case BulkString:
			resp += "$" + strconv.Itoa(len(el.Value)) + "\r\n" + el.Value + "\r\n"
		case Integer:
			resp += ":" + el.Value + "\r\n"
		}
	}

	return []byte(resp)
}

// encodeBulkString for the recursive encoder
func encodeBulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

// encodeInteger for the recursive encoder
func encodeInteger(i int) string {
	return fmt.Sprintf(":%d\r\n", i)
}

// stringsToInterfaceArray converts []string to []interface{} so it can be passed
// to the recursive encodeArray function.
func stringsToInterfaceArray(arr []string) []interface{} {
	result := make([]interface{}, len(arr))
	for i, s := range arr {
		result[i] = s
	}
	return result
}

// Recursive encoder
func encodeArray(arr []interface{}) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("*%d\r\n", len(arr)))

	for _, elem := range arr {
		switch v := elem.(type) {
		case string:
			sb.WriteString(encodeBulkString(v))
		case int:
			sb.WriteString(encodeInteger(v))
		case []interface{}:
			sb.WriteString(encodeArray(v))
		case []string:
			sb.WriteString(encodeArray(stringsToInterfaceArray(v)))
		default:
			// If type is unknown, we skip it
		}
	}

	return sb.String()
}
