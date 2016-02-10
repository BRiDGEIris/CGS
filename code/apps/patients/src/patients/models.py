#Module copy-pasted from variants, might require some changes
from django.db import models

class QueryModel(models.Model):
	"""
	Represents a query from a raw user input to a formatted query string.
	For now, the formatted query string is temporary as I think this is 
	not the best representation for our needs. 
	"""	
	CGS_SYSTEM = "CGS"
	GOOGLE_GENOMICS = "GGEN"
	HBASE = "HBASE"
	HIVE = "HIVE"
	SUPPORTED_LANGUAGES = (
		(CGS_SYSTEM, 'CGS System'),
		(GOOGLE_GENOMICS, 'Google genomics'),
		(HBASE, 'HBase'),
		(HIVE, 'Hive'),
	)
	
	raw = models.CharField("Raw query", max_length=500)
	formatted = models.CharField("Formatted query", max_length=500)
	language = models.CharField(max_length=4,choices=SUPPORTED_LANGUAGES, default=CGS_SYSTEM)
	user_id = models.IntegerField("User id which created this query")
	creation_date = models.DateField("Query date", auto_now_add=True)
	execution_date = models.DateField("Execution date", auto_now_add=True)
	execution_time = models.IntegerField("Execution time in ms")

	def format_raw(self):
		""" TODO
		Formats the raw query in this object to a better one. Does not
		throw an error if the query is invalid.
		"""
		formatted = ""
		return formatted
		
	def check(self):
		""" TODO
		Checks the current formatted query to see if there is no problem.
		It includes verifications about: the format of the string, the
		tables called, the fields called, the eventual joints, etc.
		Does not check the authorization of the user to perform this 
		query.
		"""
		check = False
		return check
	
	def transform(self):
		""" TODO
		Transforms the formatted query into a list of hbase queries.
		"""
		result = ""
		return result
	
	def explain(self):
		""" TODO
		Explains the formatted query. It transforms the sql-like query
		into a list of readable hbase queries
		"""
		explanation = "Some explanations..."
		return explanation

	def execute(self):
		""" TODO
		Executes the current sql-like query and returns the results
		"""
		return ""
		
class JobModel(models.Model):
	""" TODO
	Manage the different jobs. A job can be constituted of a simple 
	query, but also constituted of multiples queries. We could eventually
	allow some R or Python scripts to be executed directly on the servers
	but it is not for the current version.  
	"""

class HistoryModel(models.Model):
	""" TODO
	Manage the different actions related to each query/job. It saves 
	some useful information, as the execution time, the memory consumption,
	the bandwidth used to return the data to the client, etc.
	It would be useful to be able to monitor which user uses most of the
	cluster resources, etc. 
	"""
