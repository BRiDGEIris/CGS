import requests

def login(output_file):
	r = requests.get('http://quickstart.cloudera:8888/accounts/login/?next=/')
	tmp = r.text.split('csrfmiddlewaretoken')
	tmp = tmp[1].split("value='")
	tmp = tmp[1].split("'")
	token = tmp[0]
	cookie = r.cookies
	
	data = {'username':'cloudera','password':'cloudera','csrfmiddlewaretoken':token}
	r = requests.post('http://quickstart.cloudera:8888/accounts/login/?next=/variants/api/variants/ulb|0|1|10177|A/',data=data,cookies=cookie)

	f = open(output_file,'w')
	f.write(r.text)
	f.close()

login('curl-results.txt')


