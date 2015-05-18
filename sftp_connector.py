#!/usr/bin/python

import paramiko
import time
import datetime
import os
import subprocess
import optparse
import stat
from connectors import *

class SFTPConnectionInfo(ConnectionInfo):
	pass
	#same as default class. Can modify later

class SFTPConnection(Connection):
	def connect(self):
		print "Trying to connect to: " + self.connectionInfo.host + " with port " + str(self.connectionInfo.port) 
		self.transport = paramiko.Transport((self.connectionInfo.host, self.connectionInfo.port))
		self.transport.connect(username = self.connectionInfo.username, password = self.connectionInfo.password)
		print "Connection Successful"
						
	def close(self):
		self.transport.close()

class SFTPReader(Reader):
	def createPartitionReader(self):
		sftpPartitionReader = SFTPPartitionReader()
		sftpPartitionReader.transport = self.connectionObj.transport
		return sftpPartitionReader

class SFTPPartitionReader(PartitionReader):
	def read(self, sftppath):
		local_path = os.getcwd() # local path - can be changed later
		sftp = paramiko.SFTPClient.from_transport(self.transport)
		#assuming the remote path is a file, raises error if it is a directory for now, will add option to recursively download directories
		fileattr = sftp.lstat(sftppath)
		if not stat.S_ISDIR(fileattr.st_mode):
			sftp.get(sftppath, local_path+'/'+os.path.basename(sftppath))
		else:
			raise Exception('Cannot download directory yet')
		sftp.close()