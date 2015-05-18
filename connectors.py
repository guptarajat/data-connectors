#!/usr/bin/python
class ConnectionInfo:
	attributes = dict()
	def __init__(self, host, port, username, password):
		self.username = username
		self.password = password
		self.host = host
		self.port = port

class Connection:
	def __init__(self, obj):
		self.connectionInfo = obj

	def connect(self):
		raise NotImplementedError("connect method is not defined")

	def close(self):
		pass
		#method to close connection

class Reader:
	def __init__(self, obj):
		self.connectionObj = obj

	def createPartitionReader(self):
		return PartitionReader()

class PartitionReader:
	def read(self, datablock):
		raise NotImplementedError("read method is not definded")