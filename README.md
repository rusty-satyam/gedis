# ⚡ Gedis

![Go Version](https://img.shields.io/badge/Go-1.23-00ADD8?style=flat&logo=go)
![License](https://img.shields.io/badge/license-MIT-green)

**Gedis** is a lightweight Redis clone written entirely in **Go**. It is built from scratch to explore the internals of database systems.

## ✨ Features & Commands

Gedis supports a wide subset of Redis commands across various categories:

### 🔑 Key-Value & Strings
* `SET key value [px ms]`: Store string values with optional expiry.
* `GET key`: Retrieve values.
* `INCR key`: Atomic increment operations.
* `TYPE`: Determine the type of stored data.

### 📜 Lists
* `LPUSH`, `RPUSH`: Add elements to the head or tail.
* `LPOP`: Remove and return elements.
* `LRANGE`: Retrieve a range of elements.
* `LLEN`: Get list length.

### 📊 Sorted Sets (ZSets)
* `ZADD`: Add members with scores.
* `ZRANK`: Get the rank of a member.
* `ZRANGE`: Query members by index range.
* `ZCARD`, `ZSCORE`, `ZREM`: Set metadata and modification.

### 🌍 Geospatial
* `GEOADD`: Encodes Lat/Lon into a **52-bit integer Geohash**
* `GEODIST`: Calculates distance between points using the **Haversine formula**.
* `GEOSEARCH`: Finds members within a specific radius.
* `GEOPOS`: Decodes Geohashes back to coordinates.

### 📡 Publisher/Subscriber & Streams
* `SUBSCRIBE`, `PUBLISH`, `UNSUBSCRIBE`: Real-time messaging.
* `XADD`: Basic Stream support

### ⚙️ System & Replication
* `MULTI`, `EXEC`, `DISCARD`: Transactions
* `REPLCONF`, `PSYNC`: Replication handshakes and offset tracking.
* `ACL SETUSER`, `ACL GETUSER`, `AUTH`: User management and authentication.
* `CONFIG GET`: Retrieve server configuration.

---

## 🏃 Getting Started

### Installation
```bash
git clone [https://github.com/rusty-satyam/gedis.git](https://github.com/rusty-satyam/gedis.git)
cd gedis
go build -o gedis ./app
```

### Running a Primary Node

By default, Gedis runs on port 6379.
```Bash
./gedis --port 6379
```

### Running a Replica

To start a second instance that replicates the primary:
```Bash
./gedis --port 6380 --replicaof "localhost 6379"
```

Testing with Redis-CLI

You can use the standard redis-cli tool to interact with Gedis:
```Bash
redis-cli -p 6379
> SET mykey "Hello Gedis"
OK
> GET mykey
"Hello Gedis"
```
---
## 🤝 Contributing

Contributions are welcome! Feel free to open issues or submit pull requests for new commands or performance improvements.

## 📄 License

This project is licensed under the MIT License.