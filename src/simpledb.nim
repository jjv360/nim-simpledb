
# Check which platform to import
when defined(js):

    # Import the JS code
    discard

else:

    # Import the native code
    import simpledb/native
    export native