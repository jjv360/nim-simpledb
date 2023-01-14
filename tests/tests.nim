# This is just an example to get you started. You may wish to put all of your
# tests into a single file, or separate them into multiple `test1`, `test2`
# etc. files (better names are recommended, just make sure the name starts with
# the letter 't').
#
# To run these tests, simply execute `nimble test`.

import unittest
import simpledb
import std/times
import std/json
import std/random
import std/os

# Platform specific imports
when defined(js):

    # Code for Javascript
    const fgBlue = "\u001b[34m"
    const fgGreen = "\u001b[32m"
    const fgRed = "\u001b[31m"
    const fgDefault = "\u001b[0m"
    proc styledEcho(v: varargs[string]) = echo v.join("")

    const platformName = "js"

else:

    # Native code
    import asyncdispatch
    import terminal
    
    const platformName = "native"



# Helpers for testing
proc group(str: string) = styledEcho "\n", fgBlue, "+ ", fgDefault, str, " (" & platformName & " compiler)"
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
    "type": "example",
    "data": "123456",
    "timestamp": cpuTime(),
    "isExample": true,
    "otherInfo": nil
})

# Update a document
test "Update a document"
warn "Not implemented yet"

# Batch add documents
test "Batch updates"
randomize()
db.batch:
    for i in 0 .. 100000:
        db.put(%* { "type": "batched", "index": i, "random": rand(1.0) })

# Close and reopen the database
test "Close and reopen database"
db.close()
db = SimpleDB.init("test.db")

# Fetch a specific document
test "Fetch a document by ID"
let doc = db.get("1234")
if doc == nil: raiseAssert("Unable to read document.")
if doc{"type"}.getStr() != "example": raiseAssert("Invalid data")