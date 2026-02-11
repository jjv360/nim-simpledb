# SimpleDB

![](https://img.shields.io/badge/status-beta-orange)
![](https://img.shields.io/badge/platforms-native%20only-orange)

A very simple NoSQL JSON document database written on top of SQLite.

## Usage

```nim
import simpledb
import json

# Open or create a database
var db = SimpleDB.init("database.db")

# Write a document
db.put(%* {
    "id": "1234",
    "timestamp": 123456,
    "type": "example",
    "text": "Hello world!"
})

# Get a specific document by it's ID (null if not found)
var doc = db.get("1234")

# Fetch a document with a query
var doc = db.query().where("type", "==", "example").get()

# Fetch a list of documents with a query
var docs = db.query()
    .where("timestamp", ">=", 1000)
    .where("timestamp", "<=", 2000)
    .limit(5)
    .offset(2)
    .list()

# Update a specific document by ID
db.update("1234", %* { "text": "Updated text!", "views": 42 })

# Update multiple documents matching a query
db.query()
    .where("type", "==", "example")
    .update(%* { "status": "archived" })

# Delete documents
db.remove("1234")
db.query().where("type", "==", "example").remove()

# Batch modifications
db.batch do():
    db.put(%* { "name": "item1" })
    db.put(%* { "name": "item2" })
    db.put(%* { "name": "item3" })

# Close the database
db.close()
```

See [tests.nim](tests/tests.nim) for more examples.