#!/usr/bin/python

from sftp_connector import *

myConnectionInfo = SFTPConnectionInfo('localhost','22','dev','')
myConnection = SFTPConnection(myConnectionInfo)
myConnection.connect()
myReader = SFTPReader(myConnection)
myPartitionReader = myReader.createPartitionReader()
myPartitionReader.read('./')