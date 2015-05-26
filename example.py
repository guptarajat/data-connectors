#!/usr/bin/python

from sftp_connector import *

myConnectionInfo = SFTPConnectionInfo('localhost',22,'dev')
myConnection = SFTPConnection(myConnectionInfo)
print("Trying to establish connection")
myConnection.connect()
print("Connection established")
myReader = SFTPReader(myConnection)
myPartitionReader = myReader.createPartitionReader()
myPartitionReader.read('./Desktop')

# myWriter = SFTPWriter(myConnection)
# myPartitionWriter = myWriter.createPartitionWriter()
# myPartitionWriter.write('holla','./Desktop')