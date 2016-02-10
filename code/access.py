import sys
import requests

def access(operation, variant_id=None):
	session = requests.Session()
	r = session.get('http://quickstart.cloudera:8888/accounts/login/?next=/')
	token = r.cookies['csrftoken']
	
	# Connecting to hue
	data = {'username':'cloudera','password':'cloudera','csrfmiddlewaretoken':token}
	r = session.post('http://quickstart.cloudera:8888/accounts/login/?next=/',data=data,cookies=r.cookies)
	
	# Making our query
	if operation == 'select':	
		r = session.get('http://quickstart.cloudera:8888/variants/api/variants/'+variant_id+'/',cookies=r.cookies)
	elif operation == 'search':
		data = {
		      "username":"cloudera","password":"cloudera","csrfmiddlewaretoken":token,
		      "variantSetIds": ['stuff'],
		      "variantName": '',
		      "callSetIds": [],
		      "referenceName": 1,
		      "start": 0,
		      "end": 0, # Not supported
		      "pageToken": '',
		      "pageSize": 5000, # Not supported, but should be very big for the moment
		      "maxCalls": 5000 # Not supported
		    }
		r = session.get('http://quickstart.cloudera:8888/variants/api/variants/search/',json=data,cookies=r.cookies)
			
	elif operation == 'highlander_search':
		data = {
		      "username":"cloudera","password":"cloudera","csrfmiddlewaretoken":token,
		      "method": "SELECT",
		      "fields": "*",# list of fields separated like a sql query, by a comma
		      "condition": "", # list of conditions (WHERE clause) like a sql query
		      "limit": 5,
		      "offset": 0,
		      "order-by": ""
		    }
		r = session.get('http://quickstart.cloudera:8888/variants/api/variants/highlander_search/',json=data,cookies=r.cookies)
		
	return r.text

r = access(operation='select',variant_id='ulb|0|1|10177|A')
#r = access(operation='search')
#r = access(operation='highlander_search')

print(r)



