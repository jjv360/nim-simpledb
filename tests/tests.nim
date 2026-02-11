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
import std/terminal



# Helpers for testing
proc group(str: string) = styledEcho "\n", fgBlue, "+ ", fgDefault, str
proc test(str: string) = styledEcho fgGreen, "  + ", fgDefault, str
proc warn(str: string) = styledEcho fgRed, "    ! ", fgDefault, str




# Open the database
group "Database tests"
test "Open database"
var db = SimpleDB.init(":memory:")




# Add a document
test "Add a document"
db.put(%* {
    "id": "1234",
    "type": "replaced",
})





# Update a document
test "Replace a document"
db.put(%* {
    "id": "1234",
    "type": "example",
    "data": "123456",
    "timestamp": cpuTime(),
    "isExample": true,
    "otherInfo": nil
})





# Merge update a document
test "Update a document"
db.put(%* {
    "id": "1234",
    "otherInfo": "test"
}, merge = true)

# Test it
let exampleDoc = db.get("1234")
if exampleDoc{"type"}.getStr() != "example": raiseAssert("Wrong document returned.")
if exampleDoc{"otherInfo"}.getStr() != "test": raiseAssert("Merged content was not saved.")
if exampleDoc{"data"}.getStr() != "123456": raiseAssert("Content was not merged correctly.")





# Batch add documents
test "Batch updates"
randomize()
let batchedCount = 1000
db.batch do():
    for i in 0 .. batchedCount:
        db.put(%* { "type": "batched", "index": i, "random": rand(1.0) })






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





# Iterator test
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





# Test the new update functionality
group "Update tests"

# Add a test document
test "Add document for update test"
db.put(%* {
    "id": "update-test-1",
    "name": "bob",
    "type": "user",
    "age": 25,
    "status": "active"
})






# Update by ID
test "Update document by ID"
db.update("update-test-1", %* { "name": "alice", "age": 30 })

# Verify update
let updatedDoc = db.get("update-test-1")
if updatedDoc{"name"}.getStr() != "alice": raiseAssert("Name was not updated")
if updatedDoc{"age"}.getInt() != 30: raiseAssert("Age was not updated")
if updatedDoc{"type"}.getStr() != "user": raiseAssert("Type should not have changed")
if updatedDoc{"status"}.getStr() != "active": raiseAssert("Status should not have changed")






# Add more documents for batch update test
test "Add documents for batch update"
db.put(%* { "id": "user-1", "name": "user1", "type": "user", "score": 100 })
db.put(%* { "id": "user-2", "name": "user2", "type": "user", "score": 200 })
db.put(%* { "id": "user-3", "name": "user3", "type": "admin", "score": 300 })






# Update multiple documents using query
test "Update multiple documents with query"
let updatedCount = db.query()
    .where("type", "==", "user")
    .where("id", ">=", "user-")
    .update(%* { "status": "updated", "score": 999 })

if updatedCount != 2: raiseAssert("Expected 2 documents updated, got " & $updatedCount)

# Verify updates via query (tests that index columns are updated)
let updatedUsers = db.query()
    .where("type", "==", "user")
    .where("status", "==", "updated")
    .list()
if updatedUsers.len != 2: raiseAssert("Expected 2 users with updated status, got " & $updatedUsers.len)

# Verify scores were updated
let highScores = db.query().where("score", "==", 999.0).list()
if highScores.len != 2: raiseAssert("Expected 2 users with score 999, got " & $highScores.len)






# Update with filters
test "Update with complex filter"
let adminUpdated = db.query()
    .where("type", "==", "admin")
    .where("score", ">=", 250)
    .update(%* { "level": "super" })

if adminUpdated != 1: raiseAssert("Expected 1 admin updated, got " & $adminUpdated)

let superAdmin = db.get("user-3")
if superAdmin{"level"}.getStr() != "super": raiseAssert("Level was not updated for admin")






# Cleanup test documents
test "Cleanup update test documents"
db.remove("update-test-1")
db.remove("user-1")
db.remove("user-2")
db.remove("user-3")