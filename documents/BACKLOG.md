# BACKLOG

- Eliminate the role of DNS
- Client as connect to get data must know as least one IP of a node in a network, client holds a list of IPs of several nodes inside the network
- Add authentication feature for every requests
- Not specify role `master\\data\\dns`, when node joins the network, the network will decide the role
- For `addr_current`, use machine IP
- Remove all `panic`
- Convert all clone used for `addr` to other more memory efficient form
- In Flow Send file, if Master designates itself as a data storing node, no need to
- Handle corner case in Replication:
  - No. current nodes less than `NUM_REPLICAS`: Re-run Replication when new node joins
  -
