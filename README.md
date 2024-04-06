# lamination
lamination is a lightweight,  LSM (Log-Structured Merge-tree) based Key-Value store engine implemented in Java.

## Features
- **High performance key-value read / write**
- **Efficient disk space management through level-based compaction**
- **Handle failures gracefully, ensuring data integrity and reliability**

## Usage
```java
Engine db = Engine.open("/tmp/db");
db.put("key", "value");

// Retrieve the value associated with a key
String value = db.get("key");

// Delete a key-value pair
db.delete("key");
```

## Acknowledgments
- [A tutorial of building an LSM-Tree storage engine in a week!](https://skyzh.github.io/mini-lsm/)
- [The Log-Structured Merge-Tree (LSM-Tree)](https://www.cs.umb.edu/~poneil/lsmtree.pdf)
- [smhasher](https://github.com/aappleby/smhasher/tree/master)
