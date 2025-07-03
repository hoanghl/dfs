# SQLite database

At each node (Master or Data), 2 SQLite databases are maintained to keep track of information

1. File info database

```sql
filename        TEXT    PRIMARY KEY
,is_local       BOOLEAN NOT NULL
,node           TEXT    NOT NULL
,last_updated   TEXT    NOT NULL
```

## 1. FileInfoDB

Schema:

```sql
filename        TEXT    PRIMARY KEY
,is_local       BOOLEAN NOT NULL
,path           TEXT
,node           TEXT    NOT NULL
,last_updated   TEXT    NOT NULL
```
