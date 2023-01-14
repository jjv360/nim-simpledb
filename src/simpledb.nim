import classes
import std/db_sqlite
import std/json
import std/oids
import std/strutils
import std/sequtils


##
## Query filter info
class SimpleDBFilter:
    var field = ""
    var operation = ""
    var value = ""
    var fieldIsNumber = false


##
## Query builder
class SimpleDBQuery:

    ## Reference to the database
    var db: RootRef

    ## List of filters
    var filters : seq[SimpleDBFilter]

    ## Sort field
    var sortField = ""
    var sortAscending = true
    var sortIsNumber = true

    ## Limit
    var pLimit = -1

    ## Offset
    var pOffset = 0

    ## (chainable) Add a filter. Operation is one of: `==` `!=` `<` `<=` `>` `>=`
    method where(field: string, operation: string, value: string): SimpleDBQuery =

        # Check input
        if field.len == 0: raiseAssert("No field provided")
        if operation.len == 0: raiseAssert("No operation provided")
        if operation != "==" and operation != "!=" and operation != "<" and operation != "<=" and operation != ">" and operation != ">=": raiseAssert("Unknown operation '" & operation & "'")

        # Add it
        let filter = SimpleDBFilter(field: field, operation: operation, value: value, fieldIsNumber: false)
        this.filters.add(filter)
        return this


    ## (chainable) Add a filter. Operation is one of: `==` `!=` `<` `<=` `>` `>=`
    method where(field: string, operation: string, value: float): SimpleDBQuery =

        # Check input
        if field.len == 0: raiseAssert("No field provided")
        if operation.len == 0: raiseAssert("No operation provided")
        if operation != "==" and operation != "!=" and operation != "<" and operation != "<=" and operation != ">" and operation != ">=": raiseAssert("Unknown operation '" & operation & "'")

        # Add it
        let filter = SimpleDBFilter(field: field, operation: operation, value: $value, fieldIsNumber: true)
        this.filters.add(filter)
        return this
    

    ## (chainable) Set sort field
    method sort(field: string, ascending: bool = true, isNumber: bool = true): SimpleDBQuery =

        # Check input
        if field.len == 0: raiseAssert("No field provided")
        
        # Store it
        this.sortField = field
        this.sortAscending = ascending
        this.sortIsNumber = isNumber
        return this
    

    ## (chainable) Set the maximum number of documents to return, or -1 to return all documents.
    method limit(count: int): SimpleDBQuery =

        # Check input
        if count < -1: raiseAssert("Cannot use negative numbers for the limit")

        # Store it
        this.pLimit = count
        return this


    ## (chainable) Set the number of documents to skip
    method offset(count: int): SimpleDBQuery =

        # Check input
        if count < 0: raiseAssert("Cannot use negative numbers for the offset")

        # Store it
        this.pOffset = count




##
## A simple NoSQL database written in Nim.
class SimpleDB:

    ## (private) Database connection
    var conn : DbConn

    ## (private) True if the database has been prepared yet
    var hasPrepared = false

    ## (private) Extra columns that have been created for indexing
    var extraColumns: seq[string] = @["id_TEXT"]

    ## (private) List of hashes of generated indexes
    var createdIndexHashes: seq[string]

    ## Constructor
    method init(filename: string) =

        # Create the database connection
        this.conn = open(filename, "", "", "")


    ## Close the database
    method close() =

        # Close database
        if this.conn != nil:
            this.conn.close()
            this.conn = nil


    ## (private) Prepare the datatabase for use
    method prepareDB() =

        # Only do once
        if this.hasPrepared: return
        this.hasPrepared = true

        # Create main table if it doesn't exist
        this.conn.exec(sql"CREATE TABLE IF NOT EXISTS documents (id_TEXT TEXT PRIMARY KEY, _json TEXT)")

        # Get list of all columns in the table
        for row in this.conn.rows(sql"PRAGMA table_info(documents)"):

            # Add to the extra columns array
            let columnName = row[1]
            if columnName == "_json": continue
            if not this.extraColumns.contains(columnName):
                this.extraColumns.add(columnName)


    ## Put a new document
    method put(document: JsonNode) =

        # Check input
        if document == nil: raiseAssert("Cannot put a null document into the database.")

        # Prepare database
        this.prepareDB()

        # Create query including all fields
        let str = "INSERT INTO documents (_json, " & this.extraColumns.join(", ") & ") VALUES (?, " & this.extraColumns.mapIt("?").join(", ") & ")"
        let cmd = sql(str)

        # First field is the JSON content
        var args = @[ $document ]

        # Add fields for the extra columns
        for columnName in this.extraColumns:
            args.add document{columnName}.getStr()

        # Bind and execute the query
        this.conn.exec(cmd, args)


    ## Execute a batch of transactions. Either they all succeed, or the database will not be updated. This is also much faster when saving lots of documents at once.
    method batch(code: proc()) =

        # Prepate database
        this.prepareDB()

        # Start a transaction
        this.conn.exec sql"BEGIN TRANSACTION"

        # Catch errors
        try:

            # Execute the caller's code
            code()

        except:

            # Rollback the transaction
            this.conn.exec sql"ROLLBACK TRANSACTION"

            # Pass the error on to the caller
            raise getCurrentException()

        # Complete the transaction
        this.conn.exec sql"COMMIT TRANSACTION"


    ## Start a query
    method query(): SimpleDBQuery =

        # Prepare database
        this.prepareDB()

        # Create query object
        let q = SimpleDBQuery.init()
        q.db = this
        return q


    ## (private) Ensure column exists for the specified field
    method createIndexableColumnForField(name: string, sqlName: string, sqlType: string) =

        # Stop if already created
        if this.extraColumns.contains(sqlName):
            return

        # Begin an update transaction
        this.batch:

            # Create new field on the table
            let str = "ALTER TABLE documents ADD \"" & sqlName & "\" " & sqlType
            this.conn.exec(sql(str))

            # Fetch all existing documents ... this is heavy, but we can't iterate and modify at the same time
            let sqlAllRows = sql"SELECT id, _json FROM documents"
            let sqlUpdateRow = sql("UPDATE documents SET \"" & sqlName & "\" = ? WHERE id = ?")
            for row in this.conn.getAllRows(sqlAllRows):

                # Parse this document
                let id = row[0]
                let json = parseJson(row[1])

                # Get field value
                let value = json{name}.getStr()

                # Set row value
                if value.len > 0:
                    this.conn.exec(sqlUpdateRow, value, id)

        # Done, update extra columns
        this.extraColumns.add(sqlName)


    ## (private) Create an index for the specified query, if needed
    method createIndex(query: SimpleDBQuery) =

        # Stop if no index is needed, ie this query returns all data directly
        if query.sortField == "" and query.filters.len == 0:
            return

        # Check if index created
        let indexHash = query.filters.mapIt(it.field).join("_") & query.sortField
        if this.createdIndexHashes.contains(indexHash):
            return
        
        # Create SQL
        var sqlStr = "CREATE INDEX IF NOT EXISTS \"documents_" & indexHash & "\" ON documents ("

        # Add filter fields
        var addedFirst = false
        for filter in query.filters:

            # Get SQL column info
            var sqlType = if filter.fieldIsNumber: "REAL" else: "TEXT"
            var sqlName = filter.field & "_" & sqlType
            
            # Add the separator if this is not the first filter
            if addedFirst: sqlStr &= ", "
            addedFirst = true

            # Add the filter
            sqlStr &= "\"" & sqlName & "\""

        # Add sort field
        if query.sortField.len > 0:

            # Get SQL column info
            var sqlType = if query.sortIsNumber: "REAL" else: "TEXT"
            var sqlName = query.sortField & "_" & sqlType
            
            # Add the separator if this is not the first filter
            if addedFirst: sqlStr &= ", "
            addedFirst = true

            # Add the filter
            sqlStr &= "\"" & sqlName & "\""

        # Close the SQL
        sqlStr &= ")"

        # Execute it
        this.conn.exec(sql(sqlStr))

        # Done, store index hash
        this.createdIndexHashes.add(indexHash)



## Execute the query and return all documents.
proc prepareSelect(this: SimpleDBQuery): SqlPrepared =

    # Get database reference
    let db = cast[SimpleDB](this.db)
    
    # Build query
    var bindValues : seq[string]
    var sqlStr = "SELECT _json FROM documents"

    # Add filters
    if this.filters.len > 0:

        # Add WHERE clause
        sqlStr &= " WHERE "
        var addedFirst = false
        for filter in this.filters:

            # Get SQL column info
            var sqlType = if filter.fieldIsNumber: "REAL" else: "TEXT"
            var sqlName = filter.field & "_" & sqlType

            # Ensure an indexable column exists for this field
            db.createIndexableColumnForField(filter.field, sqlName, sqlType)

            # Add the 'AND' if this is not the first filter
            if addedFirst: sqlStr &= " AND "
            addedFirst = true

            # Add the filter
            sqlStr &= "\"" & sqlName & "\" " & filter.operation & " ?"
            bindValues.add(filter.value)
            
        # Add sort
        if this.sortField.len > 0:

            # Get SQL column info
            var sqlType = if this.sortIsNumber: "REAL" else: "TEXT"
            var sqlName = this.sortField & "_" & sqlType

            # Ensure an indexable column exists for this field
            db.createIndexableColumnForField(this.sortField, sqlName, sqlType)
            
            # Add the sort
            sqlStr &= " ORDER BY \"" & sqlName & "\" " & (if this.sortAscending: "asc" else: "desc")
            
        # Add limit
        if this.pLimit >= 0:
            sqlStr &= " LIMIT " & $this.pLimit

        # Add offset
        if this.pOffset > 0:
            sqlStr &= " OFFSET " & $this.pOffset

        # Create index for this query if needed
        db.createIndex(this)

        # Done, prepare and bind the query
        var prepared = db.conn.prepare(sqlStr)
        for i in 0 ..< bindValues.len: prepared.bindParam(i, bindValues[i])
        return prepared


## Execute the query and return all documents.
proc list*(this: SimpleDBQuery): seq[JsonNode] =

    # Get database reference
    let db = cast[SimpleDB](this.db)

    # Prepare the query
    let preparedSql = prepareSelect(this)

    # Run the query
    var docs : seq[JsonNode]
    for row in db.conn.rows(preparedSql):

        # Parse JSON for each result
        docs.add(parseJson(row[0]))

    # Done
    return docs


## Execute the query and iterate through the resulting documents.
iterator list*(this: SimpleDBQuery): JsonNode =

    # Get database reference
    let db = cast[SimpleDB](this.db)

    # Prepare the query
    let preparedSql = prepareSelect(this)

    # Run the query
    for row in db.conn.rows(preparedSql):

        # Parse JSON for each result and yield it
        yield parseJson(row[0])


## Execute the query and return the first document found, or null if not found.
proc get*(this: SimpleDBQuery): JsonNode =

    # Limit to one
    this.pLimit = 1

    # Execute query
    let docs = this.list()
    if docs.len == 0:
        return nil
    else:
        return docs[0]


## Helper: Get a document with the specified ID, or return nil if not found
proc get*(this: SimpleDB, id: string): JsonNode = this.query().where("id", "==", id).get()