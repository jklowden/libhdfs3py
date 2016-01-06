#! /usr/bin/env python
import os
import hdfs3py as hd
port = 9000
host = '54.174.167.189'

def connect():
    builder = hd.newBuilder()
    hd.builderSetNameNodePort(builder, port)
    hd.builderSetNameNode(builder, host)
    fs = hd.builderConnect(builder);
    print( 'connected to %s:%s' % (host, port) )
    return fs

def rw(fs, name):
    buffer = bytes("Nothing to see here. Move along.", 'utf-8')

    # create a new file to write
    WRONLY = os.O_WRONLY
    if not hd.exists(fs, name):
        WRONLY += os.O_CREAT
        print('will create %s' % name)
    fout = hd.openFile(fs, name, WRONLY, 0, 0, 0)
    print('opened %s for write' % name)

    # write into a file
    erc = hd.write(fs, fout, buffer, len(buffer))
    print('wrote to %s' % name)

    # close a file
    erc = hd.flush(fs, fout)
    print('flushed %s' % name)
    erc = hd.closeFile(fs, fout)
    print('closed %s' % name)

    # open a file to read
    fin = hd.openFile(fs, name, os.O_RDONLY, 0, 0, 0)
    print('opened %s for reading' % name)

    # read a file
    done = hd.read(fs, fin, buffer, len(buffer))
    print('read %s' % name)

    # close a file
    erc = hd.closeFile(fs, fout)
    print('closed %s' % name)

fs = connect()
rw(fs, '/jkltest')

hd.freeBuilder(builder)

