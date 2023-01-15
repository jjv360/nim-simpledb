# Package

version       = "0.1.0"
author        = "jjv360"
description   = "A simple NoSQL JSON document database"
license       = "MIT"
srcDir        = "src"


# Dependencies

requires "nim >= 1.6.10"
requires "classes >= 0.2.13"


# Test script
task test, "Test":

    # Compile a native and JavaScript binary
    exec "nim compile --out:test.exe --path:src test.nim "
    exec "nim js --out:test.js --path:src test.nim"

    # Run the compiled binaries
    exec "./test.exe"
    exec "chromerunner --headless ./test.js"