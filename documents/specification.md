<p style="text-align: center;font-size: 35px;font-weight: 900">System specification</p>

# Overview

This file distributed system allows the user to send/receive file from a distributed file system. In this system, each file will have at least 2 replicas stored in two separate nodes.

## Intro

- Architecture: 2 types of nodes: Master and Data node. Master keeps files replica and controls. Data node keeps the files replica only. DNS server keeps the IP of the current Master node
- Communication: bare socket-based solution
- State: During working, the current Master node and the backup Master node always sync the metadata.
- Coordination: DNS server is used to get the IP of the current Master node. When the current Master is down, there is an algorithm that makes the backup Master node and the current.
- Data consistency: Master node only permits 2 scenarios for each file:

  - multiple read
  - zero read, single write

- Replication: Each file has at least 2 replicas stored in different Data nodes. After one Data node successfully receives a file uploaded from client, the Replication process begins.
- Fault tolerance: With Data node and Master node, each has a distinct Fault tolerance strategy when it is down.
  - for Data node: [Fault tolerance on Data node](#fault-tolerance-on-data-node).
  - for Master node: check section [Fault tolerance on Master node](#fault-tolerance-on-master-node).

## Functional requirements

User:

- can send file to specific node within the network, the file will later be replicated to another chosen node
- can upload and read file

Nodes can communicate with each other to exchange:

- Files
- Health status (heartbeat)
- The metadata of which node is holding which file
- When a node is down, other nodes must exchange the replica of the files held by the down one to ensure the replica of every file must be at least 2

# System architecture

## User

User is a program which can send TCP requests to Master and Data node in the system. User connects to DNS to get the IP of the current Master node.
Assumption(s):

- We assume user has 2 permissions:
  - read file
  - write file

## DNS

DNS stores the IP of the current Master node.
Assumption(s):

- We assume that DNS is never down.

## Master node

Master node has several roles:

- Store files’ replica
- keep track of which file is located on which data node
- keep track of Data Node alive status via heartbeat
- trigger replication process when a node is down

There are 2 Master nodes, but only one of them does the roles listed above. The other Master node is just backup. During working, the actual Master node exchanges the metadata to the backup to ensure when it is down, the backup can replace and become the working Master node.

## Data node

Data node has only one target: keep the files’ replica.
Periodically, it sends heartbeat message to Master node to keep track health status.
During **Replication** process, Data node receives the IP address of another data node to send data.

# Consistency guarantee

For each file, Master node permits 2 scenarios from clients:

- multi read: concurrently read by multiple clients
- no-read-one-write: during a client is writing (uploading) the file, no other client can read it

# Fault tolerance

There are 2 distinct scenarios of fault tolerance for Master and Data.

- For Master fault tolerance: When the current Master node is down, it can be only determined by the backup Master node via 2 consecutive non-response heartbeats. In this case, the backup Master node triggers the Master node fault tolerance process (described in [Fault tolerance](#fault-tolerance-on-master-node))
- For Data node fault tolerance: When the current Master node detects a Data node doesn’t response 2 consecutive heartbeats, for each file whose the replica is held by the down Data node, the current Master node chooses an alternative Data node and triggers the [Replication process](#replication).

# Coordination

DNS server is used to store the IP of the current Master node. All other Data nodes after initializing and Client, which want to know the IP of the current Master node, must connect to the DNS.
When the current Master node is down and the backup Master node becomes the current Master node, the new Master node has responsibility to update its IP to the DNS server.

# Packet format

The network's working is based on a tailored protocol. For more info about packets used in protocol, please visit: [Link](sub_documents/flows.md)

# Flow

This section introduces flows of the network.

[Link](sub_documents/flows.md)

# In-memory database

[Link](sub_documents/sqlite_db.md)
