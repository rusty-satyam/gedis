package main

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// valueType represents a stored value with an optional expiration time in the 'data' map.
type valueType struct {
	valueString string
	expiry      *time.Time
}

// Client holds the state for a connected TCP client.
type Client struct {
	SubscribedMode     bool
	Authenticated      bool
	Username           string
	SubscribedChannels map[string]struct{}
	Connection         net.Conn
	Reader             *bufio.Reader
}

type streamEntry map[string]string

type ArrayElementType int

const (
	BulkString ArrayElementType = iota
	Integer
)

type ArrayElement struct {
	Type  ArrayElementType
	Value string
}

// ACLUser defines user permissions and credentials
type ACLUser struct {
	Flags     map[string]bool
	Passwords []string // List of valid SHA-256 password hashes
}

// Data stores for different Redis data types
var streams = make(map[string][]streamEntry)
var data = make(map[string]*valueType)
var listData = make(map[string][]string)

// Replication state
var offset = 0 // Tracks the replication offset (bytes processed)
var emptyRDBBase64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
var emptyRDB []byte

// Configuration defaults
var dir = ""
var dbfilename = ""
var port = "6379"

// maps channel names to a list of client connections
var channelSubscribers = make(map[string][]net.Conn)

// ACL Users initialization (default user has no password)
var users = map[string]*ACLUser{
	"default": {Flags: map[string]bool{"nopass": true}, Passwords: []string{}},
}

// Commands allowed while a client is in Pub/Sub mode
var allowedInSubscribeMode = map[string]bool{
	"subscribe":    true,
	"unsubscribe":  true,
	"psubscribe":   true,
	"punsubscribe": true,
	"ping":         true,
	"quit":         true,
}

// Commands that modify data (used to determine if propagation is needed)
var writeCommand = map[string]bool{
	"set": true,
	"del": true,
}

// handleConnection manages the lifecycle of a client connection.
// If connectionToPrimary is true, it performs the replication handshake first.
func handleConnection(conn net.Conn, connectionToPrimary bool) {
	inTransaction := false
	var queuedCommands []Command

	reader := bufio.NewReader(conn)

	// Replication Handshake Logic (if acting as a replica)
	if connectionToPrimary {
		// Step 1: Send PING to verify connection
		_, err := conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
		if err != nil {
			fmt.Println("Failed to send PING to primary:", err)
			return
		}
		reader.ReadString('\n') // Consume PONG

		// Step 2: Inform primary of the listening port
		_, err = conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" + strconv.Itoa(len(port)) + "\r\n" + port + "\r\n"))
		if err != nil {
			fmt.Println("Failed to send REPLCONF listening-port:", err)
			return
		}
		reader.ReadString('\n') // Consume OK

		// Step 3: Inform primary of capabilities (psync2 support)
		_, err = conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
		if err != nil {
			fmt.Println("Failed to send REPLCONF capa psync2:", err)
			return
		}
		reader.ReadString('\n') // Consume OK

		// Step 4: Initiate synchronization
		_, err = conn.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))
		if err != nil {
			fmt.Println("Failed to send PSYNC:", err)
			return
		}

		// Read PSYNC response
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Failed to read PSYNC response:", err)
			return
		}
		line = strings.TrimSpace(line)
		fmt.Println("PSYNC response:", line)

		// Step 5: Handle Full Resynchronization (RDB transfer)
		if strings.HasPrefix(line, "+FULLRESYNC") {
			// Read RDB header (starts with $)
			rdbHeader, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println("Failed to read RDB header:", err)
				return
			}

			if len(rdbHeader) == 0 || rdbHeader[0] != '$' {
				fmt.Println("Expected RDB bulk string, got:", rdbHeader)
				return
			}

			// Parse RDB size
			rdbLen, err := strconv.Atoi(strings.TrimSpace(rdbHeader[1:]))
			if err != nil {
				fmt.Println("Invalid RDB length:", err)
				return
			}

			// Read the actual RDB binary data
			rdb := make([]byte, rdbLen)
			if _, err := io.ReadFull(reader, rdb); err != nil {
				fmt.Println("Failed to read RDB:", err)
				return
			}

			fmt.Println("RDB fully received, size:", rdbLen)
		}
	}

	// Initialize client state
	client := &Client{
		Connection:         conn,
		SubscribedChannels: make(map[string]struct{}),
		Authenticated:      users["default"].Flags["nopass"],
		Username:           "default",
		Reader:             reader,
	}

	// Main Loop
	for {
		// Parse the next command from the client
		commandStringArray, commandOffset, err := readRESPArray(reader)
		if err != nil {
			if err == io.EOF {
				return
			}
			fmt.Println("read error:", err)
			return
		}

		commandName := strings.ToLower(commandStringArray[0])

		command := Command{
			StringArray: commandStringArray,
			Name:        commandName,
			Offset:      commandOffset,
		}

		switch commandName {

		case "replconf":
			// Handle replication ACKs
			if len(commandStringArray) >= 2 {
				// Replica responding to GETACK from primary to confirm offset
				if connectionToPrimary && strings.ToLower(commandStringArray[1]) == "getack" {
					// Respond with REPLCONF ACK <offset>
					conn.Write([]byte(StringArrayToBulkStringArray([]string{"REPLCONF", "ACK", strconv.Itoa(offset - 37)})))
					continue
				}
			}

			// Default response for other REPLCONF commands
			conn.Write([]byte("+OK\r\n"))

		case "multi":
			// Start a transaction
			inTransaction = true
			queuedCommands = nil
			conn.Write([]byte("+OK\r\n"))

		case "exec":
			// Process all queued commands in the transaction
			if !inTransaction {
				conn.Write([]byte("-ERR EXEC without MULTI\r\n"))
				continue
			}

			inTransaction = false

			results := make([][]byte, 0, len(queuedCommands))

			// Process every queued command
			for _, cmd := range queuedCommands {
				reply := ProcessCommand(client, cmd)
				results = append(results, reply)
			}

			queuedCommands = nil

			// Build array response for EXEC
			response := "*" + strconv.Itoa(len(results)) + "\r\n"
			for _, r := range results {
				response += string(r)
			}

			conn.Write([]byte(response))

		case "discard":
			// Discard the transaction
			if !inTransaction {
				conn.Write([]byte("-ERR DISCARD without MULTI\r\n"))
				continue
			}

			inTransaction = false
			queuedCommands = nil
			conn.Write([]byte("+OK\r\n"))

		default:
			if inTransaction {
				// Queue command if inside a transaction
				queuedCommands = append(queuedCommands, command)
				conn.Write([]byte("+QUEUED\r\n"))
			} else {
				// Process immediately
				response := ProcessCommand(client, command)

				// Replicas should not reply to commands sent by primary
				if !connectionToPrimary {
					conn.Write(response)
				}
			}
		}

	}
}

func main() {
	var err error
	// Decode the hardcoded empty RDB file for initializing replicas
	emptyRDB, err = base64.StdEncoding.DecodeString(emptyRDBBase64)
	if err != nil {
		fmt.Println("Failed to decode Base64 RDB:", err.Error())
	}

	// Argument Parsing
	args := os.Args
	for i := 1; i < len(args); i++ {
		switch args[i] {

		case "--port":
			if i+1 < len(args) {
				port = args[i+1]
				i++
			}

		case "--replicaof":
			if i+1 < len(args) {
				isReplica = true
				replicaHost = strings.Split(args[i+1], " ")[0]
				replicaPort = strings.Split(args[i+1], " ")[1]
				i++
			}

		case "--dir":
			if i+1 < len(args) {
				dir = args[i+1]
				i++
			}

		case "--dbfilename":
			if i+1 < len(args) {
				dbfilename = args[i+1]
				i++
			}
		}
	}

	// Start TCP Listener
	l, err := net.Listen("tcp", "0.0.0.0:"+port)
	if err != nil {
		fmt.Println("Failed to bind to port ", port)
		os.Exit(1)
	}

	// If configured as a replica, connect to the primary instance immediately
	if isReplica {
		conn, err := net.Dial("tcp", replicaHost+":"+replicaPort)
		if err != nil {
			fmt.Println("Failed to connect to primary:", err)
			return
		}

		go handleConnection(conn, true)
	}

	// Accept incoming connections
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(conn, false)
	}
}
