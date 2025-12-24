package main

import "fmt"

// isReplica indicates if this instance is running in replica mode or primary mode
var isReplica = false

// Configuration for connecting to the primary instance if isReplica is true.
var replicaHost = ""
var replicaPort = ""

// Replication ID
var replID = ""

// replOffset tracks the amount of replication stream data processed by this instance.
var replOffset = 0

// replicaClients holds the connections to all downstream replicas.
var replicaClients []Client

// PropagateWriteCommandToReplicas sends a write command (like SET, DEL) to all connected replicas.
func PropagateWriteCommandToReplicas(commandStringArray []string) {
	if isReplica {
		return
	}

	// Iterate over all connected replicas and send the command.
	for _, replica := range replicaClients {
		_, err := replica.Connection.Write([]byte(StringArrayToBulkStringArray(commandStringArray)))
		if err != nil {
			fmt.Println("Error propagating command to replica:", err)
		}
	}
}
