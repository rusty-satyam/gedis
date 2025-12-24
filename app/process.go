package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Command represents a parsed RESP command
type Command struct {
	StringArray []string // The full command arguments
	Name        string   // The command name (normalized to lowercase)
	Offset      int      // Byte offset in the stream (used for replication tracking)
}

// ProcessCommand is the central logic for the application.
// It routes the command to the correct logic, handles authentication middleware,
// and manages replication propagation for write operations.
func ProcessCommand(client *Client, command Command) []byte {
	commandName := command.Name
	commandStringArray := command.StringArray

	// If the user hasn't authenticated (and isn't sending an AUTH command)
	if !client.Authenticated && commandName != "auth" {
		return []byte("-NOAUTH Authentication required\r\n")
	}

	// If a client is in "Subscribe Mode", they are restricted to a subset of commands.
	if client.SubscribedMode && !allowedInSubscribeMode[commandName] {
		return []byte("-ERR Can't execute '" + commandName +
			"': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n")
	}

	// If this is a Primary node and the command is a "Write" (modifies data),
	// we must forward it to all connected Replicas to keep them in sync.
	if !isReplica && writeCommand[commandName] {
		replOffset += command.Offset
		PropagateWriteCommandToReplicas(commandStringArray)
	}

	switch commandName {

	case "ping":
		if client.SubscribedMode {
			return []byte("*2\r\n$4\r\npong\r\n$0\r\n\r\n")
		}
		return []byte("+PONG\r\n")

	case "echo":
		return StringToBulkString(commandStringArray[1])

	case "config":
		// Handles 'CONFIG GET dir' or 'CONFIG GET dbfilename'
		if len(commandStringArray) >= 3 && strings.ToLower(commandStringArray[1]) == "get" {
			param := commandStringArray[2]
			var value string

			switch param {
			case "dir":
				value = dir
			case "dbfilename":
				value = dbfilename
			}
			return StringArrayToBulkStringArray([]string{param, value})
		}

	case "set":
		fmt.Println("setting")
		key := commandStringArray[1]

		data[key] = &valueType{valueString: commandStringArray[2]}

		// Handle Expiry: SET key val px <milliseconds>
		if len(commandStringArray) > 3 {
			if strings.ToLower(commandStringArray[3]) == "px" {
				ms, err := strconv.Atoi(commandStringArray[4])

				if err != nil {
					fmt.Println("Error reading command: ", err.Error())
					os.Exit(1)
				}

				t := time.Now().Add(time.Duration(ms) * time.Millisecond)
				data[key].expiry = &t
			}
		}
		return []byte("+OK\r\n")

	case "get":
		value, ok := data[commandStringArray[1]]

		if ok {
			// Check if key has expired before returning
			if value.expiry == nil || time.Now().Before(*value.expiry) {
				return StringToBulkString(value.valueString)
			}
		}

		return []byte("$-1\r\n")

	case "incr":
		key := commandStringArray[1]

		value, ok := data[key]
		if !ok {
			data[key] = &valueType{valueString: "1"}
			return []byte(":1\r\n")
		}

		currentValue, err := strconv.Atoi(value.valueString)
		if err != nil {
			return []byte("-ERR value is not an integer or out of range\r\n")
		}

		currentValue++
		value.valueString = strconv.Itoa(currentValue)

		return []byte(":" + strconv.Itoa(currentValue) + "\r\n")

	case "keys":
		pattern := commandStringArray[1]

		// Currently only supports "*" to list everything
		if pattern != "*" {
			return []byte("*0\r\n")
		}

		allKeys := make([]string, 0, len(data)+len(listData))
		for k := range data {
			allKeys = append(allKeys, k)
		}
		for k := range listData {
			allKeys = append(allKeys, k)
		}

		return StringArrayToBulkStringArray(allKeys)

	case "type":
		// Returns the data type of the key
		// Currently handle string and stream data types
		if len(commandStringArray) != 2 {
			return []byte("-ERR wrong number of arguments for 'type' command\r\n")
		}

		key := commandStringArray[1]

		if _, ok := data[key]; ok {
			return []byte("+string\r\n")
		}
		if _, ok := streams[key]; ok {
			return []byte("+stream\r\n")
		}

		return []byte("+none\r\n")

	// List Operations
	case "rpush":
		key := commandStringArray[1]
		values := commandStringArray[2:]
		listData[key] = append(listData[key], values...)
		return []byte(":" + strconv.Itoa(len(listData[key])) + "\r\n")

	case "lpush":
		key := commandStringArray[1]
		values := commandStringArray[2:]
		slices.Reverse(values)
		listData[key] = append(values, listData[key]...)
		return []byte(":" + strconv.Itoa(len(listData[key])) + "\r\n")

	case "llen":
		key := commandStringArray[1]
		list, ok := listData[key]
		var length int
		if ok {
			length = len(list)
		} else {
			length = 0
		}
		return []byte(":" + strconv.Itoa(length) + "\r\n")

	case "lpop":
		key := commandStringArray[1]
		list, ok := listData[key]

		if !ok || len(list) == 0 {
			return []byte("$-1\r\n")
		}

		numberOfElementsToRemove := 1
		if len(commandStringArray) >= 3 {
			numberOfElementsToRemove, err := strconv.Atoi(commandStringArray[2])
			if err != nil || numberOfElementsToRemove < 0 {
				fmt.Println("Error reading command: ", err.Error())
				os.Exit(1)
			}
		}

		if numberOfElementsToRemove > len(list) {
			numberOfElementsToRemove = len(list)
		}

		if numberOfElementsToRemove > 1 {
			poppedElements := list[:numberOfElementsToRemove]
			listData[key] = list[numberOfElementsToRemove:]
			return StringArrayToBulkStringArray(poppedElements)
		} else {
			poppedElement := list[0]
			listData[key] = list[1:]
			return StringToBulkString(poppedElement)
		}

	case "lrange":
		key := commandStringArray[1]
		start, err := strconv.Atoi(commandStringArray[2])
		if err != nil {
			fmt.Println("Error reading command: ", err.Error())
			os.Exit(1)
		}
		stop, err := strconv.Atoi(commandStringArray[3])
		if err != nil {
			fmt.Println("Error reading command: ", err.Error())
			os.Exit(1)
		}

		list, ok := listData[key]
		if !ok {
			return []byte("*0\r\n")
		}

		length := len(list)
		if start < 0 {
			start += length
		}
		if stop < 0 {
			stop += length
		}
		if start < 0 {
			start = 0
		}
		if stop < 0 {
			stop = 0
		}
		if stop >= length {
			stop = length - 1
		}

		if start > stop || start >= length {
			return []byte("*0\r\n")
		}

		resultList := list[start : stop+1]
		return StringArrayToBulkStringArray(resultList)

	// Publisher / Subscriber operations
	case "subscribe":
		channel := commandStringArray[1]

		// Track subscription
		client.SubscribedChannels[channel] = struct{}{}
		client.SubscribedMode = true
		count := len(client.SubscribedChannels)

		// Add connection to global subscriber map
		subscribers := channelSubscribers[channel]
		alreadySubscribed := false
		for _, c := range subscribers {
			if c == client.Connection {
				alreadySubscribed = true
				break
			}
		}
		if !alreadySubscribed {
			channelSubscribers[channel] = append(channelSubscribers[channel], client.Connection)
		}

		return EncodeArray([]ArrayElement{
			{Type: BulkString, Value: "subscribe"},
			{Type: BulkString, Value: channel},
			{Type: Integer, Value: strconv.Itoa(count)},
		})

	case "publish":
		channel := commandStringArray[1]
		message := commandStringArray[2]
		subscribers := channelSubscribers[channel]

		// Broadcast message to all listening connections
		for _, c := range subscribers {
			c.Write(EncodeArray([]ArrayElement{
				{Type: BulkString, Value: "message"},
				{Type: BulkString, Value: channel},
				{Type: BulkString, Value: message},
			}))
		}
		return []byte(":" + strconv.Itoa(len(subscribers)) + "\r\n")

	case "unsubscribe":
		channel := commandStringArray[1]

		subscribers := channelSubscribers[channel]
		for i, c := range subscribers {
			if c == client.Connection {
				channelSubscribers[channel] = append(subscribers[:i], subscribers[i+1:]...)
				continue
			}
		}

		delete(client.SubscribedChannels, channel)

		return EncodeArray([]ArrayElement{
			{Type: BulkString, Value: "unsubscribe"},
			{Type: BulkString, Value: channel},
			{Type: Integer, Value: strconv.Itoa(len(client.SubscribedChannels))},
		})

	// Sorted Sets
	case "zadd":
		key := commandStringArray[1]
		score, err := strconv.ParseFloat(commandStringArray[2], 64)
		if err != nil {
			return []byte("-ERR score must be a float\r\n")
		}
		member := commandStringArray[3]
		added := zadd(key, score, member)
		return []byte(":" + strconv.Itoa(added) + "\r\n")

	case "zrank":
		key := commandStringArray[1]
		member := commandStringArray[2]
		rank := zrank(key, member)
		if rank == nil {
			return []byte("$-1\r\n")
		}
		return []byte(":" + strconv.Itoa(*rank) + "\r\n")

	case "zrange":
		key := commandStringArray[1]
		start, err := strconv.Atoi(commandStringArray[2])
		if err != nil {
			return []byte("-ERR invalid start index\r\n")
		}
		stop, err := strconv.Atoi(commandStringArray[3])
		if err != nil {
			return []byte("-ERR invalid stop index\r\n")
		}
		members := zrange(key, start, stop)
		return StringArrayToBulkStringArray(members)

	case "zcard":
		key := commandStringArray[1]
		count := zcard(key)
		return []byte(":" + strconv.Itoa(count) + "\r\n")

	case "zscore":
		key := commandStringArray[1]
		member := commandStringArray[2]
		return zscore(key, member)

	case "zrem":
		key := commandStringArray[1]
		member := commandStringArray[2]
		return zrem(key, member)

	// Geospatial Commands
	case "geoadd":
		// Encodes Lat/Lon into a 52-bit integer and stores it as the ZSET Score.
		if len(commandStringArray) < 5 {
			return []byte("-ERR wrong number of arguments for 'geoadd'\r\n")
		}

		longitude, err := strconv.ParseFloat(commandStringArray[2], 64)
		if err != nil {
			return []byte("-ERR invalid longitude\r\n")
		}
		latitude, err := strconv.ParseFloat(commandStringArray[3], 64)
		if err != nil {
			return []byte("-ERR invalid latitude\r\n")
		}

		// Validate Coordinates
		if longitude < -180 || longitude > 180 || latitude < -85.05112878 || latitude > 85.05112878 {
			return []byte(fmt.Sprintf("-ERR invalid longitude,latitude pair %.6f,%.6f\r\n", longitude, latitude))
		}

		key := commandStringArray[1]
		member := commandStringArray[4]

		score := GeospatialEncode(latitude, longitude)

		if sortedSets[key] == nil {
			sortedSets[key] = make(map[string]sortedSetMember)
		}
		sortedSets[key][member] = sortedSetMember{
			Member: member,
			Score:  float64(score),
		}
		return []byte(":1\r\n")

	case "geopos":
		// Decodes the 52-bit score back into Lat/Lon coordinates
		key := commandStringArray[1]
		members := commandStringArray[2:]
		response := "*" + strconv.Itoa(len(members)) + "\r\n"
		zset, keyExists := sortedSets[key]

		for _, memberName := range members {
			if !keyExists {
				response += "*-1\r\n"
				continue
			}
			member, exists := zset[memberName]
			if !exists {
				response += "*-1\r\n"
				continue
			}

			coordinates := GeospatialDecode(uint64(member.Score))
			longitude := strconv.FormatFloat(coordinates.Longitude, 'f', -1, 64)
			latitude := strconv.FormatFloat(coordinates.Latitude, 'f', -1, 64)

			response += "*2\r\n"
			response += "$" + strconv.Itoa(len(longitude)) + "\r\n" + longitude + "\r\n"
			response += "$" + strconv.Itoa(len(latitude)) + "\r\n" + latitude + "\r\n"
		}
		return []byte(response)

	case "geodist":
		// Calculates Haversine distance between two members
		key := commandStringArray[1]
		m1 := commandStringArray[2]
		m2 := commandStringArray[3]

		zset, ok := sortedSets[key]
		if !ok {
			return []byte("$-1\r\n")
		}

		sm1, ok1 := zset[m1]
		sm2, ok2 := zset[m2]
		if !ok1 || !ok2 {
			return []byte("$-1\r\n")
		}

		c1 := GeospatialDecode(uint64(sm1.Score))
		c2 := GeospatialDecode(uint64(sm2.Score))
		distance := GeoDistance(c1, c2)

		distanceString := strconv.FormatFloat(distance, 'f', -1, 64)
		return StringToBulkString(distanceString)

	case "geosearch":
		// Finds members within a radius of a target point
		key := commandStringArray[1]
		zset, ok := sortedSets[key]
		if !ok {
			return []byte("*0\r\n")
		}

		longitude, _ := strconv.ParseFloat(commandStringArray[3], 64)
		latitude, _ := strconv.ParseFloat(commandStringArray[4], 64)
		radius, _ := strconv.ParseFloat(commandStringArray[6], 64)
		unit := strings.ToLower(commandStringArray[7])

		radiusMeters := RadiusToMeters(radius, unit)
		center := Coordinates{
			Latitude:  latitude,
			Longitude: longitude,
		}

		results := []string{}
		for _, member := range zset {
			coords := GeospatialDecode(uint64(member.Score))
			dist := GeoDistance(center, coords)
			if dist <= radiusMeters {
				results = append(results, member.Member)
			}
		}
		return StringArrayToBulkStringArray(results)

	// ACL (Access Control List)
	case "acl":
		if len(commandStringArray) < 2 {
			return []byte("-ERR wrong ACL syntax\r\n")
		}
		aclSubCmd := strings.ToUpper(commandStringArray[1])

		switch aclSubCmd {
		case "SETUSER":
			if len(commandStringArray) < 4 {
				return []byte("-ERR wrong number of arguments for ACL SETUSER\r\n")
			}
			username := commandStringArray[2]
			passwordRule := commandStringArray[3]
			client.Authenticated = true
			return []byte(setUserPassword(username, passwordRule))

		case "GETUSER":
			if len(commandStringArray) < 3 {
				return []byte("-ERR wrong number of arguments for ACL GETUSER\r\n")
			}
			username := commandStringArray[2]
			return encodeACLGetUser(username)

		case "WHOAMI":
			return []byte(StringToBulkString("default"))

		default:
			return []byte("-ERR unknown ACL subcommand\r\n")
		}

	case "auth":
		if len(commandStringArray) != 3 {
			return []byte("-ERR wrong number of arguments for 'auth' command\r\n")
		}
		username := commandStringArray[1]
		password := commandStringArray[2]

		user := users[username]
		if user == nil {
			return []byte("-WRONGPASS invalid username-password pair or user is disabled\r\n")
		}

		// Verify Password Hash
		hash := sha256.Sum256([]byte(password))
		hashHex := hex.EncodeToString(hash[:])

		for _, pwHash := range user.Passwords {
			if pwHash == hashHex {
				client.Authenticated = true
				client.Username = username
				return []byte("+OK\r\n")
			}
		}
		return []byte("-WRONGPASS invalid username-password pair or user is disabled\r\n")

	// Replication Handshake
	case "psync":
		if len(commandStringArray) != 3 {
			return []byte("-ERR wrong number of arguments for 'psync' command\r\n")
		}

		if commandStringArray[1] == "?" && commandStringArray[2] == "-1" {
			client.Connection.Write([]byte("+FULLRESYNC " + replID + " 0\r\n"))

			rdbLength := len(emptyRDB)
			client.Connection.Write([]byte("$" + strconv.Itoa(rdbLength) + "\r\n"))
			client.Connection.Write(emptyRDB)

			replicaClients = append(replicaClients, *client)
			return nil
		}

	// Streams (XADD)
	case "xadd":
		if len(commandStringArray) < 4 || len(commandStringArray)%2 == 0 {
			return []byte("-ERR wrong number of arguments for 'xadd' command\r\n")
		}

		key := commandStringArray[1]
		entryID := commandStringArray[2]

		var ms int64
		var seq int64
		var err error

		// ID Generation
		if entryID == "*" {
			// Full Auto-Generate (*)
			ms = time.Now().UnixNano() / int64(time.Millisecond)
			seq = 0

			for i := len(streams[key]) - 1; i >= 0; i-- {
				lastID := streams[key][i]["id"]
				lastParts := strings.Split(lastID, "-")
				lastMS, _ := strconv.ParseInt(lastParts[0], 10, 64)
				if lastMS == ms {
					lastSeq, _ := strconv.ParseInt(lastParts[1], 10, 64)
					seq = lastSeq + 1
					break
				} else if lastMS < ms {
					break
				}
			}
			entryID = fmt.Sprintf("%d-%d", ms, seq)

		} else if strings.HasSuffix(entryID, "-*") {
			// Partial Auto-Generate
			parts := strings.Split(entryID, "-")
			if len(parts) != 2 {
				return []byte("-ERR invalid stream ID format\r\n")
			}

			ms, err = strconv.ParseInt(parts[0], 10, 64)
			if err != nil || ms < 0 {
				return []byte("-ERR invalid milliseconds part in ID\r\n")
			}

			seq = 0
			for i := len(streams[key]) - 1; i >= 0; i-- {
				lastID := streams[key][i]["id"]
				lastParts := strings.Split(lastID, "-")
				lastMS, _ := strconv.ParseInt(lastParts[0], 10, 64)
				if lastMS == ms {
					lastSeq, _ := strconv.ParseInt(lastParts[1], 10, 64)
					seq = lastSeq + 1
					break
				}
			}

			// 0-0 is invalid, so 0-* starts at 0-1
			if ms == 0 && seq == 0 {
				seq = 1
			}
			entryID = fmt.Sprintf("%d-%d", ms, seq)

		} else {
			parts := strings.Split(entryID, "-")
			if len(parts) != 2 {
				return []byte("-ERR invalid stream ID format\r\n")
			}
			ms, err1 := strconv.ParseInt(parts[0], 10, 64)
			seq, err2 := strconv.ParseInt(parts[1], 10, 64)
			if err1 != nil || err2 != nil || ms < 0 || seq < 0 {
				return []byte("-ERR invalid stream ID values\r\n")
			}

			if ms == 0 && seq == 0 {
				return []byte("-ERR The ID specified in XADD must be greater than 0-0\r\n")
			}

			var lastID string
			if len(streams[key]) > 0 {
				lastEntry := streams[key][len(streams[key])-1]
				lastID = lastEntry["id"]
			}

			if lastID != "" {
				lastParts := strings.Split(lastID, "-")
				lastMS, _ := strconv.ParseInt(lastParts[0], 10, 64)
				lastSeq, _ := strconv.ParseInt(lastParts[1], 10, 64)

				if ms < lastMS || (ms == lastMS && seq <= lastSeq) {
					return []byte("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
				}
			}
		}

		entry := make(map[string]string)
		entry["id"] = entryID
		for i := 3; i < len(commandStringArray); i += 2 {
			field := commandStringArray[i]
			value := commandStringArray[i+1]
			entry[field] = value
		}

		streams[key] = append(streams[key], entry)
		return []byte("$" + strconv.Itoa(len(entryID)) + "\r\n" + entryID + "\r\n")
	}

	return []byte("-ERR unknown command\r\n")
}
