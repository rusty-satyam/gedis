package main

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// setUserPassword updates the credentials for a user.
// It adheres to the Redis ACL syntax where a password rule starting with '>'
// indicates a new password should be added.
func setUserPassword(username, passwordRule string) string {
	// Check if the user exists in the global 'users' map
	user, ok := users[username]
	if !ok {
		return "-ERR no such user\r\n"
	}

	// Validate the rule syntax.
	if !strings.HasPrefix(passwordRule, ">") {
		return "-ERR invalid rule\r\n"
	}

	// Extract the plain text password by stripping the leading '>'
	password := passwordRule[1:]

	// Hash the password for storage using SHA-256.
	hash := sha256.Sum256([]byte(password))
	hashHex := hex.EncodeToString(hash[:])

	// Update user state:
	user.Passwords = append(user.Passwords, hashHex)
	delete(user.Flags, "nopass")

	return "+OK\r\n"
}

// encodeACLGetUser implements the logic for the 'ACL GETUSER <username>' command.
// It gathers the user's flags and password hashes and serializes them into a RESP array.
func encodeACLGetUser(username string) []byte {
	user := users[username]
	if user == nil {
		return []byte("-ERR user does not exist\r\n")
	}

	flagsArray := []string{}
	for flag := range user.Flags {
		flagsArray = append(flagsArray, flag)
	}

	passwordsArray := make([]string, len(user.Passwords))
	copy(passwordsArray, user.Passwords)

	// Construct the final output.
	// [ "flags", [flag1, flag2...], "passwords", [hash1, hash2...] ]
	return []byte(encodeArray([]interface{}{
		"flags",
		flagsArray,
		"passwords",
		passwordsArray,
	}))
}
