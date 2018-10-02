# from botocore.client import Config
import boto3, json
import psycopg2
from boto3.dynamodb.conditions import Key

def get_config(filename="aws_config.json"):
	con_file = open("aws_config.json")
	config = json.load(con_file)
	con_file.close()
	return config

class S3:
	def __init__(self):
		config = get_config()

		self.BUCKET_NAME = config['BUCKET_NAME']
		# cls.client = boto3.client('s3')

		# resource = boto3.resource('s3') #high-level object-oriented API
		self.resource = boto3.resource(
									's3',
									aws_access_key_id=config['ACCESS_KEY_ID'],
									aws_secret_access_key=config['ACCESS_SECRET_KEY']
									)

		
		#subsitute this for your s3 bucket name. 
		self.bucket = self.resource.Bucket(self.BUCKET_NAME)

	def get_bucket(self):
		return self.bucket

	def get_resource(self):
		return self.resource

	def get_bucketname(self):
		return self.BUCKET_NAME

class DynamoDB:
	def __init__(self, tablekey):

		config = get_config()

		self.TableName = config[tablekey]

		self.dynamodb = boto3.resource(
									"dynamodb", 
									region_name=config['REGION'],
									aws_access_key_id=config['ACCESS_KEY_ID'],
									aws_secret_access_key=config['ACCESS_SECRET_KEY']
									)

		self.table = self.dynamodb.Table(self.TableName)

	def get_table(self):
		return self.table

	def get_tablename(self):
		return self.TableName

	def insert_item(self, item):
		"""Insert an item to table"""
		response = self.table.put_item(Item=item)
		if response['ResponseMetadata']['HTTPStatusCode'] == 200:
			return True
		else:
			return False

	def get_item(self, key):
		response = self.table.get_item(Key=key) # Key={'fruitName':{'S':'Banana'}})
		return response.get('Item', None)

	def batch_write(self, items):
		"""
		Batch write items to given table name
		"""		
		with self.table.batch_writer() as batch:
			for item in items:
				batch.put_item(Item=item)
		return True

	def update_data(self, item):
		
		pass

	def query_data(self, key, value):
		index = None
		if key == 'scp_id':
			index = 'scp_id-index'
		else:
			index = 'arXiv_id-index'
		response = self.table.query(
							IndexName=index,			
							KeyConditionExpression=Key(key).eq(value)
							)
		items = response['Items']
		return items

class RDS:
	def __init__(self):
		config = get_config()

		self.DB_NAME = config['DB_NAME']
		self.DB_USER = config['DB_USER']
		self.DB_HOSTNAME = config['HOST_NAME']
		self.DB_PASSWORD = config['DB_PASSWORD']

		self.conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(self.DB_NAME, 
							self.DB_USER , self.DB_HOSTNAME, self.DB_PASSWORD))
		self.cur = self.conn.cursor()

	def get_conn(self):
		if self.conn.closed:
			self.conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(self.DB_NAME, 
							self.DB_USER , self.DB_HOSTNAME, self.DB_PASSWORD))
		return self.conn

	def get_cursor(self):
		if self.cur.closed:
			self.cur = self.get_conn().cursor()
		return self.cur

	def rollback(self):
		conn = self.get_conn()
		conn.rollback()

	def commit(self):
		conn = self.get_conn()
		conn.commit()

	def insert(self, sql, data):
		cur = self.get_cursor()
		try:
			cur.execute(sql, data)
		except psycopg2.InternalError as IE:
			cur.commit()
			cur.execute(sql, data)
		except psycopg2.IntegrityError as IE:
			print("[INSERT_ERROR] ", IE)
			return False
		return True

	def update(self, sql, data):
		return self.insert(sql, data)

	def select(self, sql, data):
		cur = self.get_cursor()
		try:
			cur.execute(sql, data)
		except psycopg2.InternalError as IE:
			cur.commit()
			cur.execute(sql, data)
		finally:
			return cur.fetchall()




