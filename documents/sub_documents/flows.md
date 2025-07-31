# Flows

This section describes the flows (actions) among components within the system as well as in-charge packets.

### 1. Initialization

**In-charge components**: DNS, Master, Data

**Flow**:
For Master:

1. Master sends Notify to DNS

For Data:

1. Data sends AskIP to DNS
2. DNS replies by AskIP containing the socket address of current Master
3. Data sends Notify to Master

### 2. Read data

**In-charge components**: Client; DNS; Current Master;1 Data node

**Flow**:

1. Client connects to DNS to ask the IP of current Master [packet AskIP]
2. DNS responses the IP [packet *AskIPAck*]
3. Client connects to the current Master to ask to read specific file: [packet *RequestFromClient*]
4. Master responses the IP of the Data node is holding file replica [packet *ResponseNodeIp*]
5. Client connects to Data node to get file [packet *RequestFromClient*]
6. Data node responses the file binary [packet *DataNodeSendData*]
7. Data node sends ACK for finishing reading process [packet *ClientRequestAck*]

### 3. Send data

**In-charge components**: Client;DNS;Current Master;2 Data nodes

**Flow**:

1. Client connects to DNS to ask the IP of current Master [packet *AskIp*]
2. DNS responses the IP [packet *AskIpAck*]
3. Client connects to the current Master to ask to write specific file [packet *RequestFromClient*]
4. Master responses the IP of a receiving node (this node can either a Data node or Master node) is ready to receive file [packet *ResponseNodeIp*]

In this step, Master can designates itself or another Data node inside the network and embeds that node's address to packet _ResponseNodeIp_.

5. Client connects to Receiving node to write file [packet *ClientUpload*]
6. When client finishes writing, Receiving node:
   1. notifies the Master node the writing process is done [packet *ClientRequestAck*]
   1. sends ACK to client [packet *ClientUploadAck*]
7. The current Master node selects the suitable Data node different from the one which is newly received file from the client and triggers [**_Replication process_**](#replication).

### 4. Heartbeat

**In-charge components**: Current Master ; Backup Master ; all Data nodes

**Flow**:
Periodically, each Data node sends a heartbeat to report the health status to Master.

1. Node A sends heartbeat to B [packet *Heartbeat*]
2. B sends ACK back to A [packet *HeartbeatAck*]

### 5. Fault tolerance on Data node

**In-charge components**: Current Master; 2 Data nodes

**Flow**:

1. After 2 heartbeats from current Master but the Data node (denote A) doesn’t response, the current Master looks up for each file X held by A, which Data node (denote B) is holding a replica
2. For each X and B, the current Master triggers the [**_Replication_**](#7-replication) process.

### 6. Fault tolerance on Master node

**In-charge components**: Current Master;backup Master

**Flow**:

1. After 2 heartbeats from backup Master but the current Master doesn’t response, the backup Master notifies all Data nodes that it now is becoming the current Data node
2. New current Master node updates its IP to DNS server

### 7. State synchronization

**In-charge components**: Current Master node; backup Master node

**Flow**:

1. After **_30 seconds_**, the current Master Node synchronizes its current state to the backup Master [packet *StateSync*]
2. After receiving, the backup Master sends ACK [packet *StateSyncAck*]

### 8. Replication

[Link](replication.md)

### 9. Graceful shutdown of nodes/clients

Each node/client, during starting up, initializes 3 different threads to handle 3 dedicated tasks. Thus, graceful shutdown is procedure to gracefully shutdown those threads.

[Link](gracefully_shutdown.md)
