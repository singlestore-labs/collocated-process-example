# This is a simple test program that demonstrates an external function using
# the binary protocol and ROWDAT_1 row format.  It simply returns each row it
# receives with the prefix "HELLO ".
#
# To create and run the external function in the database, do this:
# 
#    CREATE DATABASE test;
#
#    CREATE TABLE foobar(a TEXT);
#
#    INSERT INTO foobar(a) VALUES ('one'), ('two'), ('three'), ('four'), ('five'), ('six'), ('seven'), ('eight'), ('nine'), ('ten');
#
#    CREATE EXTERNAL FUNCTION xfunc(a text) RETURNS TEXT AS COLLOCATED SERVICE '/tmp/xfunc_pipe' FORMAT ROWDAT_1;
#
#    SELECT xfunc(a) FROM foobar;
#

import ctypes
import struct
import socket
import mmap
import array
import os
import threading
import time

# This is the request handler.  It is invoked when we accept a connection.
# It will called once per segment.
#
def handle_request(connection, client_address):
    print('*** Connection from', str(connection).split(", ")[0][-4:])

    # Receive the request header.  Format:
    #   server version:          uint64
    #   length of function name: uint64
    #
    buf = connection.recv(16)
    version, namelen = struct.unpack("<qq", buf)

    # Python's recvmsg returns a tuple.  We only really care about the first
    # two parts.  The recvmsg call has a weird way of specifying the size for
    # the file descriptor array; basically, we're indicating we want to read
    # two 32-bit ints (for the input and output files).
    #
    fd_model = array.array("i", [0, 0])
    msg, ancdata, flags, addr = connection.recvmsg(
        namelen, 
        socket.CMSG_LEN(2 * fd_model.itemsize))
    assert len(ancdata) == 1

    # The function's name will be in the "message" area of the recvmsg response.
    # It will be populated with `namelen` bytes.
    #
    name = msg
    print("Calling function:", name)

    # Two file descriptors are transferred to us from the database via the
    # `sendmsg` protocol.  These are for reading the input rows and writing
    # the output rows, respectively.
    #
    fd0, fd1 = struct.unpack("<ii", ancdata[0][2])
    ifile = os.fdopen(fd0, "rb")
    ofile = os.fdopen(fd1, "wb")

    # Keep receiving data on this socket until we run out.
    #
    while True:
        # Read in the length of this batch, a uint64.  No data means we're done
        # receiving.
        #
        connection.settimeout(5)
        try:
            recvd = connection.recv(8)
            if len(recvd) == 0:
                break
            length = struct.unpack("<q", recvd)[0]
            if length == 0:
                break
        except socket.timeout:
            break

        # Map in the input shared memory segment from the fd we received via
        # recvmsg.
        #
        mem = mmap.mmap(
            ifile.fileno(),
            length,
            mmap.MAP_SHARED,
            mmap.PROT_READ)

        # Read rows while there's data left.
        #
        cursor = 0
        response_size = 0
        ofile.truncate(max(128*1024, response_size))
        ofile.seek(0)
        while cursor < length:
            # Read the row's ID (uint64).
            #
            row_id = struct.unpack("<q", mem.read(8))[0]
            cursor += 8

            # Each field has two parts:
            #   Is Null?:      byte (0=false, 1=true)
            #   Value:
            #      uint64/bytes - for string types (length-prefixed)
            #      int64        - for integer types
            #      double       - for floating point types
            #
            # In this example, we are only dealing with strings, so we will
            # read the 64-bit length first and then the actual string itself.
            #
            # We are also only dealing with one field.  If there were more,
            # we would wrap this next part in a loop.
            #
            row_isnull = struct.unpack("<B", mem.read(1))[0]
            cursor += 1
            row_len = struct.unpack("<q", mem.read(8))[0]
            cursor += 8
            row_value = mem.read(row_len)
            cursor += row_len
            print("    Value: {}".format(row_value))

            # Write out a dummy response for this row into the output shared
            # memory.  Format is same is input row:  id, isnull, value length
            #
            response_val = b"HELLO " + row_value
            response_hdr = struct.pack("<qBq", row_id, 0, len(response_val))
            ofile.write(response_hdr)
            response_size += len(response_hdr)
            ofile.write(response_val)
            response_size += len(response_val)
            ofile.flush()

        assert cursor == length, "CURSOR={}, LENGTH={}".format(cursor, length)

        # Close the shared memory object.
        #
        mem.close()

        # Complete the request by send back the status as two uint64s on the
        # socket: 
        #     - http status
        #     - size of data in output shared memory
        #
        connection.send(struct.pack("<qq", 200, response_size))

    # Close shared memory files.
    #
    ifile.close()
    ofile.close()

    # Close the connection
    #
    connection.close()

# Main routine.
#
if __name__ == '__main__':
    # Set the path for the Unix socket.
    #
    socket_path = '/tmp/xfunc_pipe'

    # Remove the socket file if it already exists.
    #
    try:
        os.unlink(socket_path)
    except OSError:
        if os.path.exists(socket_path):
            raise
    
    # Create the Unix socket server.
    #
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    
    # Bind our server to the path.
    #
    server.bind(socket_path)
    
    # Listen for incoming connections.  Argument is the number of connections to 
    # keep in the backlog before we begin refusing them; 32 is plenty for this
    # simple case.
    #
    server.listen(32)
    
    # Accept connections forever.
    #
    try:
        while True:
            # Listen for the next connection on our port.
            #
            print('Server is listening for incoming connections...')
            connection, client_address = server.accept()
    
            # Handle the connection in a separate thread.
            #
            t = threading.Thread(
                target=handle_request,
                args=(connection, client_address))
            t.start()
        
            # NOTE:  The following line forces this process to handle requests
            # serially.  This makes it easier to understand what's going on.
            # In real life, though, parallel is much faster.  To use parallel
            # handling, just comment out the next line.
            #
            t.join()
    finally:
        # Remove the socket file before we exit.
        #
        os.unlink(socket_path)

