package main

import (
	"fmt"
	"sort"
	"strconv"
)

// sortedSetMember represents a single element in a Sorted Set (ZSET).
// It holds the string member and its associated floating-point score.
type sortedSetMember struct {
	Member string
	Score  float64
}

// sortedSets is the global storage for all ZSETs.
var sortedSets = make(map[string]map[string]sortedSetMember)

// zadd adds a member with a specific score to the sorted set stored at key.
// If the member already exists, its score is updated.
// Returns 1 if the element is new, 0 if it was updated.
func zadd(key string, score float64, member string) int {
	if sortedSets[key] == nil {
		sortedSets[key] = make(map[string]sortedSetMember)
	}

	_, exists := sortedSets[key][member]

	sortedSets[key][member] = sortedSetMember{
		Member: member,
		Score:  score,
	}

	if exists {
		return 0
	} else {
		return 1
	}
}

// zrank returns the 0-based index (rank) of the member in the sorted set.
// The rank is determined by ordering members by Score (low to high).
// Returns nil if the member or key does not exist.
func zrank(key string, member string) *int {
	set, ok := sortedSets[key]
	if !ok {
		return nil
	}

	// Flatten map to slice
	members := make([]sortedSetMember, 0, len(set))
	for _, m := range set {
		members = append(members, m)
	}

	// Sort slice by Score then Member
	sort.Slice(members, func(i, j int) bool {
		if members[i].Score == members[j].Score {
			return members[i].Member < members[j].Member
		}
		return members[i].Score < members[j].Score
	})

	// Iterate to find the requested member's index
	for idx, m := range members {
		if m.Member == member {
			return &idx
		}
	}

	return nil
}

// zrange returns a range of members from the sorted set, given start and stop indices.
func zrange(key string, start, stop int) []string {
	set, ok := sortedSets[key]
	if !ok {
		return []string{}
	}

	// Flatten map to slice
	members := make([]sortedSetMember, 0, len(set))
	for _, m := range set {
		members = append(members, m)
	}

	// Sort slice by Score then Member
	sort.Slice(members, func(i, j int) bool {
		if members[i].Score == members[j].Score {
			return members[i].Member < members[j].Member
		}
		return members[i].Score < members[j].Score
	})

	length := len(members)

	// Handle negative indices
	if start < 0 {
		start += length
	}
	if stop < 0 {
		stop += length
	}

	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}

	if start > stop || start >= length {
		return []string{}
	}

	// Return the range
	result := make([]string, stop-start+1)
	for i := start; i <= stop; i++ {
		result[i-start] = members[i].Member
	}

	return result
}

// zcard returns the number of elements (cardinality) in the sorted set.
func zcard(key string) int {
	set, ok := sortedSets[key]
	if !ok {
		return 0
	}
	return len(set)
}

// zscore returns the score of a member in the sorted set as a Bulk String.
func zscore(key, member string) []byte {
	set, ok := sortedSets[key]
	if !ok {
		return []byte("$-1\r\n")
	}

	m, ok := set[member]
	if !ok {
		return []byte("$-1\r\n")
	}

	scoreStr := fmt.Sprintf("%g", m.Score)
	return []byte("$" + strconv.Itoa(len(scoreStr)) + "\r\n" + scoreStr + "\r\n")
}

// zrem removes a member from the sorted set.
// Returns 1 if removed, 0 if not found.
func zrem(key, member string) []byte {
	set, ok := sortedSets[key]
	if !ok {
		return []byte(":0\r\n")
	}

	_, memberExists := set[member]
	if !memberExists {
		return []byte(":0\r\n")
	}

	// Remove from inner map
	delete(set, member)

	// If the set is empty, remove the key entirely
	if len(set) == 0 {
		delete(sortedSets, key)
	}

	return []byte(":1\r\n")
}
