# Replication process

This process is triggered in 2 scenarios:

- The new file is newly written (uploaded) by client
- A Data node is down

## 1. In-charge components

- Current Master
- `N` Data Nodes

Configuration `N` is applied universally the network and is specified as start-up.

## 2. Flow

Assume we have 2 Data nodes. Denote each as A and B. Assume A is holding the file to be replicated.

1. Current Master selects the new suitable Data node to store file
2. Current Master notifies A to send file X to B [packet *RequestSendReplica*]
3. A sends X to B [packet *SendReplica*]
4. Both A and B send ACK to current Master [packet *SendReplicaAck*]
5. Master updates in-memory database about which nodes are holding file X

## 3. Data node selection

Data node is selected by Master based on the number of file it is controlling. In particular, Master chooses the Data node holding the least number of file.

This strategy is applied in both Send file flow and Replication flow.
