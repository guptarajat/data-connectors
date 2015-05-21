#!/usr/bin/python

import paramiko
import time
import datetime
import os
import subprocess
import optparse
import stat
import errno
from connectors import *

class SFTPConnectionInfo(ConnectionInfo):
	def __init__(self, host, port, username, password=None, privatekeyfile=None):
		self.username = username
		self.password = password
		self.host = host
		self.port = port
		self.privatekeyfile = privatekeyfile

class SFTPConnection(Connection):
	def connect(self):
		print "Trying to connect to: " + self.connectionInfo.host + " with port " + str(self.connectionInfo.port) 
		self.transport = paramiko.Transport((self.connectionInfo.host, self.connectionInfo.port))
		if self.connectionInfo.password is not None:
			self.transport.connect(username = self.connectionInfo.username, password = self.connectionInfo.password)
		else:
			if self.connectionInfo.privatekeyfile is None:
				key_path = os.path.expanduser('~/.ssh/id_rsa')
			else:
				key_path = os.path.expanduser(privatekeyfile)
			mykey = paramiko.RSAKey.from_private_key_file(key_path)
			self.transport.connect(username = self.connectionInfo.username, pkey = mykey)
		print "Connection Successful"
						
	def close(self):
		self.transport.close()

class SFTPReader(Reader):
	def createPartitionReader(self):
		sftpPartitionReader = SFTPPartitionReader()
		sftpPartitionReader.transport = self.connectionObj.transport
		return sftpPartitionReader

class SFTPPartitionReader(PartitionReader):
	def read(self, sftppath, localPath = None):
		if localPath is None:
			localPath = os.getcwd() # local path - can be changed later
		sftp = paramiko.SFTPClient.from_transport(self.transport)
		def recursiveRead(sftp, sftppath, localPath):
			fileattr = sftp.lstat(sftppath)
			if not stat.S_ISDIR(fileattr.st_mode): #it is a file
				sftp.get(sftppath, os.path.join(localPath, os.path.basename(sftppath)))
			else: #it is a directory
				try: #creating local directory, using try-catch to handle race conditions
					os.makedirs(os.path.join(localPath, os.path.basename(sftppath)))
				except OSError as exception:
					if exception.errno != errno.EEXIST:
						raise
				for file in sftp.listdir_attr(sftppath):
					recursiveRead(sftp, os.path.join(sftppath, file.filename), os.path.join(localPath, os.path.basename(sftppath)))
		recursiveRead(sftp, sftppath, localPath)
		sftp.close()

class SFTPWriter(Writer):
	def createPartitionWriter(self):
		sftpPartitionWriter = SFTPPartitionWriter()
		sftpPartitionWriter.transport = self.connectionObj.transport
		return sftpPartitionWriter

class SFTPPartitionWriter(PartitionWriter):
	def write(self, localPath, sftppath = None):
		localPath = os.path.expanduser(localPath)
		print 'Copying contents from ' + localPath + ' to ' + sftppath 
		if sftppath is None:
			sftppath = './'
		sftp = paramiko.SFTPClient.from_transport(self.transport)
		def recursiveWrite(sftp, localPath, sftppath):
			if os.path.isfile(localPath): # if given path is a file
				sftp.put(localPath, os.path.join(sftppath, os.path.basename(localPath)))
			else:
				print sftppath
				try:
					sftp.listdir(os.path.join(sftppath, os.path.basename(localPath)))  # Test if remote_path exists
				except IOError:
					sftp.mkdir(os.path.join(sftppath, os.path.basename(localPath)))  # Create remote_path
				for file in os.listdir(localPath):
					recursiveWrite(sftp, os.path.join(localPath, file), os.path.join(sftppath, os.path.basename(localPath)))
		recursiveWrite(sftp, localPath, sftppath)
		sftp.close()
