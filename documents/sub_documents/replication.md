# Replication process

This process is triggered in 2 scenarios:

- The new file is newly written (uploaded) by client
- A Data node is down

Each scenario has separate data node selection strategy.

## 1. In-charge components

- Current Master
- `N` Data Nodes

Configuration `N` is applied universally the network and is specified as start-up.

## 2. Flow

### Scenario 1: New file is newly written (uploaded) by client

Assume the current Master wants to replicate the file X in Data node A.

1. Given a file to replicate, current Master selects the suitable Data node to store (denote as B)
2. Current Master notifies A to send file X to B [packet *RequestSendReplica*]
3. A sends X to B [packet *SendReplica*]
4. Both A and B send ACK to current Master [packet *SendReplicaAck*]

**_Data node selection_**

Data nodes are selected by current Master node to store replication. The selection occurs as follow:

1. Master ranks Data nodes by:

   - number of currently holding files
   - node id

2. First node in sorted list will directly receive file sent from client and this node will send a replica to each of the following node within top `N` of the aforementioned sorted list

### Scenario 2: A Data node is disconnected from Master

Assume the current Master detects the disconnection of Data node A and Data node A and B are holding file X. So Master selects a new Data node C, then triggers Replication with C and B.

1. Current Master selects the new suitable Data node to store
2. Current Master notifies B to send file X to C [packet *RequestSendReplica*]
3. B sends X to C [packet *SendReplica*]
4. Both C and B send ACK to current Master [packet *SendReplicaAck*]
5. Master updates in-memory database about which nodes are holding file X

**_Data node selection_**

Data nodes are selected by current Master node to store replication. The selection occurs as follow:

1. Master ranks Data nodes by:

   - number of currently holding files
   - node id

This ranked list doesn't contain Data node A.

2. First node in sorted list will directly receive file sent from client and this node will send a replica to each of the following node within top `N` of the aforementioned sorted list
