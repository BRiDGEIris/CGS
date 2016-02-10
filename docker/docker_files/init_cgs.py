import sys
import requests

def init():
	session = requests.Session()
	r = session.get('http://quickstart.cloudera:8888/accounts/login/?next=/')
	token = r.cookies['csrftoken']
	
	# Connecting to hue
	data = {'username':'cloudera','password':'cloudera','csrfmiddlewaretoken':token}
	r = session.post('http://quickstart.cloudera:8888/accounts/login/?next=/',data=data,cookies=r.cookies)
	
	# Making our query
	r = session.get('http://quickstart.cloudera:8888/variants/database/initialize/',cookies=r.cookies)
	
init()

