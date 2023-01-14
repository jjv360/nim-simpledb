# This is just an example to get you started. You may wish to put all of your
# tests into a single file, or separate them into multiple `test1`, `test2`
# etc. files (better names are recommended, just make sure the name starts with
# the letter 't').
#
# To run these tests, simply execute `nimble test`.

import simpledb
import std/times
import std/json
import std/random
import std/os
import std/terminal



# Helpers for testing
proc group(str: string) = styledEcho "\n", fgBlue, "+ ", fgDefault, str
proc test(str: string) = styledEcho fgGreen, "  + ", fgDefault, str
proc warn(str: string) = styledEcho fgRed, "    ! ", fgDefault, str




# Remove existing database if it exists
group "Cleanup"
test "Remove existing database"
if fileExists("test.db"):
    removeFile("test.db")

# Open the database
group "Database tests"
test "Open database"
var db = SimpleDB.init("test.db")

# Add a document
test "Add a document"
db.put(%* {
    "id": "1234",
    "type": "replaced",
})

# Update a document
test "Update a document"
db.put(%* {
    "id": "1234",
    "type": "example",
    "data": "123456",
    "timestamp": cpuTime(),
    "isExample": true,
    "otherInfo": nil
})

# Batch add documents
test "Batch updates"
randomize()
let batchedCount = 1000
db.batch:
    for i in 0 .. batchedCount:
        db.put(%* { "type": "batched", "index": i, "random": rand(1.0) })

# Close and reopen the database
test "Close and reopen database"
db.close()
db = SimpleDB.init("test.db")

# Fetch a specific document
test "Fetch a document by ID"
let doc = db.get("1234")

# Test results
if doc == nil: raiseAssert("Unable to read document.")
if doc{"type"}.getStr() != "example": raiseAssert("Invalid data")

# Do a complex query
test "Complex queries"
let docs = db.query()
    .where("type", "==", "batched")
    .where("index", ">=", 100)
    .where("index", "<", 120)
    .sort("index", ascending = false)
    .offset(5)
    .limit(2)
    .list()

# Test results
if docs.len != 2: raiseAssert("Wrong number of documents returned")
if docs[0]["index"].getInt() != 114: raiseAssert("Wrong document returned")
if docs[1]["index"].getInt() != 113: raiseAssert("Wrong document returned")

# Iterator proc test
test "Iterator"
var count = 0
for doc in db.query().where("type", "==", "batched").list():
    count += 1
    if doc{"type"}.getStr() != "batched": raiseAssert("Wrong document returned")
    if count > 5: break

# Delete a single item
test "Delete a single document"
db.remove("1234")

# Delete multiple items
test "Delete multiple documents"
let removedCount = db.query()
    .where("type", "==", "batched")
    .where("index", ">", 100)
    .remove()

# Test results
if removedCount != batchedCount - 100: raiseAssert("Different number of documents were removed than expected. expected=" & $(batchedCount - 100) & " removed=" & $removedCount)