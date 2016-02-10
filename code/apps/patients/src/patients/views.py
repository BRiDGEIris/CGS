#!/usr/bin/env python

from desktop.lib.django_util import render
from django.views.decorators.csrf import csrf_exempt
import datetime
from forms import *
import pycurl
from StringIO import StringIO
from random import randrange
import bz2
import os
import inspect

from subprocess import *
import subprocess
import requests

import logging
import json
from desktop.context_processors import get_app_name
from desktop.lib.django_util import render
from django.http import HttpResponse
import time
import datetime
from beeswax.design import hql_query
from beeswax.server import dbms
from beeswax.server.dbms import get_query_server_config
from impala.models import Dashboard, Controller
import copy
from hadoop.fs.hadoopfs import Hdfs
from django.template.defaultfilters import stringformat, filesizeformat
from filebrowser.lib.rwx import filetype, rwx

def index(request):
    """ Display the first page of the application """

    return render('index.mako', request, locals())

'''
@csrf_exempt
def query_index_interface(request):
    """ Display the page which allows to launch queries or add data """

    if request.method == 'POST':
        form = query_form(request.POST)
    else:
        form = query_form()
    return render('query.index.interface.mako', request, locals())
'''
'''
""" DISPLAY FILES PREVIOUSLY UPLOADED TO ADD SAMPLE DATA """
def sample_index_interface(request):

    # We take the files in the current user directory
    init_path = directory_current_user(request)
    files = list_directory_content(request, init_path, ".vcf", False)
    total_files = len(files)

    return render('sample.index.interface.mako', request, locals())
'''
""" IMPORT DATA FOR PATIENT """
def patient_import_interface(request):
    """ Import the data of one or multiple patient in the database """
    '''
    error_get = False
    error_sample = False
    samples_quantity = 0

    # We take the file received
    if 'vcf' in request.GET:
        filename = request.GET['vcf']
    else:
        error_get = True
        return render('sample.insert.interface.mako', request, locals())

    # We take the files in the current user directory
    init_path = directory_current_user(request)
    files = list_directory_content(request, init_path, ".vcf", True)
    length = 0
    for f in files:
        new_name = f['path'].replace(init_path+"/","", 1)
        if new_name == filename:
            length = f['stats']['size']
            break

    if length == 0:
        # File not found
        error_get = True
        return render('sample.insert.interface.mako', request, locals())

    # We take the number of samples (and their name) in the vcf file
    samples = sample_insert_vcfinfo(request, filename, length)
    samples_quantity = len(samples)
    if samples_quantity == 0:
        error_sample = True
        return render('sample.insert.interface.mako', request, locals())

    # We take the list of questions the user has to answer, and as dict in python is not ordered, we use an intermediary list
    # We also receive the different files previously uploaded by the user
    questions, q, files = patient_import_questions(request)
    '''
    if request.method == 'POST':
        # Now we save the result
        fprint(str(request.POST))
        result = patient_import(request)
        result = json_to_dict(result)

    # We display the form
    return render('patient.import.interface.mako', request, locals())


def patient_import(request):
    """ Insert patient data to database """

    result = {'status': -1,'data': {}}

    '''
    # We take the file received
    if 'vcf' in request.GET:
        filename = request.GET['vcf']
    else:
        result['status'] = 0
        result['error'] = 'No vcf file was given. You have to give a GET parameter called "vcf" with the filename of your vcf in your hdfs directory.'
        return HttpResponse(json.dumps(result), mimetype="application/json")

    # We take the files in the current user directory
    init_path = directory_current_user(request)
    files = list_directory_content(request, init_path, ".vcf", True)
    length = 0
    for f in files:
        new_name = f['path'].replace(init_path+"/","", 1)
        if new_name == filename:
            length = f['stats']['size']
            break

    if length == 0:
        # File not found
        result['status'] = 0
        result['error'] = 'The vcf file given was not found in the cgs file system.'
        return HttpResponse(json.dumps(result), mimetype="application/json")

    # We take the number of samples (and their name) in the vcf file
    samples = sample_insert_vcfinfo(request, filename, length)
    samples_quantity = len(samples)
    if samples_quantity == 0:
        error_sample = True
        return render('sample.insert.interface.mako', request, locals())

    # Some checks first about the sample data
    if request.method != 'POST':
        result['status'] = 0
        result['error'] = 'You have to send a POST request.'
        return HttpResponse(json.dumps(result), mimetype="application/json")
    '''

    if not 'patient_data' in request.POST:
        result['status'] = 0
        result['error'] = 'The patient data were not given. You have to send a POST field called "patient_data" with the information about the related file given in parameter.'
        return HttpResponse(json.dumps(result), mimetype="application/json")

    raw_lines = request.POST['patient_data'].split(";")
    if not raw_lines[len(raw_lines)-1]:# We allow the final ';'
        raw_lines.pop()

    #questions, q, files = patient_import_questions(request)
    questions, q = patient_import_questions(request)

    questions_quantity = len(q)
    for raw_line in raw_lines:
        if len(raw_line.split(",")) != questions_quantity:
            result['status'] = 0
            result['error'] = 'The number of information sent does not correspond to the number of questions asked for each sample ('+str(len(raw_line.split(",")))+' vs '+str(questions_quantity)+').'
            return HttpResponse(json.dumps(result), mimetype="application/json")

    # Connexion to the db
    try:
        query_server = get_query_server_config(name='impala')
        db = dbms.get(request.user, query_server=query_server)
        dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        result['status'] = 0
        result['error'] = 'Sorry, an error occured: Impossible to connect to the db.'
        return HttpResponse(json.dumps(result), mimetype="application/json")

    # Now we analyze each sample information
    for raw_line in raw_lines:
        answers = raw_line.split(",")

        # We check each answer for each question
        current_sample = {}
        for key, answer in enumerate(answers):

            # We take the related field
            field = q[key]
            info = questions['patient_registration'][field]

            # We check if the information is correct
            if not type(info) is dict:
                pass # Nothing to do here, it's normal. We could compare the sample id received from the ones found in the file maybe.
            elif info['field'] == 'select':
                if not answer in info['fields']:
                    result['status'] = 0
                    result['error'] = 'The value "'+str(answer)+'" given for the field "'+field+'" is invalid (Valid values: '+str(info['fields'])+').'
                    return HttpResponse(json.dumps(result), mimetype="application/json")
            else:
                # TODO: make the different verification of the 'text' and 'date' format
                pass

            current_sample[field] = answer

        fprint(current_sample)
        if not 'sample_id' in current_sample:
            current_sample['sample_id'] = ''
        sample_id = str(current_sample['sample_id'])

        if not 'patient_id' in current_sample:
            current_sample['patient_id'] = ''
        patient_id = str(current_sample['patient_id'])

        if not 'sample_collection_date' in current_sample:
            current_sample['sample_collection_date'] = ''
        date_of_collection = str(current_sample['sample_collection_date'])

        if not 'original_sample_id' in current_sample:
            current_sample['original_sample_id'] = ''
        original_sample_id = str(current_sample['original_sample_id'])

        if not 'collection_status' in current_sample:
            current_sample['collection_status'] = ''
        status = str(current_sample['collection_status'])

        if not 'sample_type' in current_sample:
            current_sample['sample_type'] = ''
        sample_type = str(current_sample['sample_type'])

        if not 'biological_contamination' in current_sample:
            current_sample['biological_contamination'] = '0'
        biological_contamination = str(current_sample['biological_contamination'])

        if not 'sample_storage_condition' in current_sample:
            current_sample['sample_storage_condition'] = ''
        storage_condition = str(current_sample['sample_storage_condition'])

        if not 'biobank_id' in current_sample:
            current_sample['biobank_id'] = ''
        biobank_id = str(current_sample['biobank_id'])

        if not 'pn_id' in current_sample:
            current_sample['pn_id'] = ''
        pn_id = str(current_sample['pn_id'])

        # We insert the data
        fprint("INSERT INTO clinical_sample VALUES('"+sample_id+"', '"+patient_id+"', '"+date_of_collection+"', '"+original_sample_id+"', '"+status+"', '"+sample_type+"', '"+biological_contamination+"','"+storage_condition+"', '"+biobank_id+"', '"+pn_id+"');")
        query = hql_query("INSERT INTO clinical_sample VALUES('"+sample_id+"', '"+patient_id+"', '"+date_of_collection+"', '"+original_sample_id+"', '"+status+"', '"+sample_type+"', '"+biological_contamination+"','"+storage_condition+"', '"+biobank_id+"', '"+pn_id+"');")
        handle = db.execute_and_wait(query, timeout_sec=5.0)

    result['status'] = 1
    return HttpResponse(json.dumps(result), mimetype="application/json")


def patient_import_questions(request):
    """ Return the questions asked to insert the data """
    questions = {
        "patient_registration":{
            "main_title": "Patient",
            "patient_id": {"question": "Patient id", "field": "text", "regex": "0-9", "mandatory": True},
            "patient_family_name": {"question": "Family name", "field": "text", "regex": "a-zA-Z0-9_-", "mandatory": True},
            "patient_first_name": {"question": "First Name", "field": "text", "mandatory": True}
            "ngr_no": {"question": "NGR #", "field": "text", "regex": "0-9", "mandatory": True},
            "dossier_no": {"question": "Dossier #", "field": "text", "regex": "a-zA-Z0-9_-", "mandatory": True},
            "gender": {"question": "Gender", "field": "select", "fields":("Male","Female"), "mandatory": True},
            "date_of_birth": {"question": "Date of Birth", "field": "datetime", "mandatory": True},
            "date_of_decease": {"question": "Date of Decease", "field":"datetime"},
            "address": {"question": "Address", "field": "text", "mandatory": True},
            "zip_code": {"question": "ZIP code", "field": "text", "mandatory": True},
            "city": {"question": "City", "field": "text"},
            "country": {"question": "Country", "field": "text"},
            "email": {"question": "Email", "field": "text"},
            "contact_number": {"question": "Phone number", "field": "text", "regex": "0-9"},
            "ethnicity": {"question": "Ethnicity", "field": "select", "fields": ("Hispanic","Native American","East Asian", 
                                                                                 "Black", "Pacific Islander", "White"), 
                          "mandatory": True}
            "citizenship": {"question": "Citizenship", "field": "text", "mandatory": True},
            "registration_date": {"question": "Date of Registration", "field": "datetime", "mandatory": True},
        },
    }

    # A dict in python is not ordered so we need a list
    q = ("main_title", "patient_id", "patient_family_name", "patient_first_name", "ngr_no", "dossier_no", "gender", 
         "date_of_birth", "date_of_decease", "address", "zip_code", "city", "country", "email", "contact_number",
         "ethnicity", "citizenship", "registration_date")

    '''
    # We also load the files
    files = list_directory_content(request, directory_current_user(request), ".vcf", False)
    '''

    return questions, q#, files


""" INITIALIZE THE DATABASE """
def database_initialize(request):
    """ Install the tables for this application """

    # Connexion to the db
    query_server = get_query_server_config(name='impala')
    db = dbms.get(request.user, query_server=query_server)
    
    #Building the ethnicity table
    sql = """
--
-- Table structure for table `ethinicity`
--

DROP TABLE IF EXISTS `ethinicity`;
CREATE TABLE `ethnicity` (
  `ETHNIC_ID` int(11) NOT NULL auto_increment,
  `ETHNICITY` varchar(100) default NULL,
  `DATE_OF_INCLUSION` timestamp NULL default NULL,
  `DESCRIPTION` varchar(400) default NULL,
  PRIMARY KEY  (`ETHINIC_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `ethinicity`
--


/*!40000 ALTER TABLE `ethnicity` DISABLE KEYS */;
LOCK TABLES `ethnicity` WRITE;
INSERT INTO `ethnicity` VALUES (1,'Hispanic','2015-02-19 23:00:00','Cuban, Mexican, Puerto Rican, South or Central American, or other Spanish culture or origin, regardless of race. '),(2,'American Indian','2015-02-19 23:00:00','origins in any of the original peoples of North and South America (including Central America), and who maintains a tribal affiliation or community attachment'),(3,'Asian','2015-02-19 23:00:00','Far East, Southeast Asia, or the Indian subcontinent including, for example, Cambodia, China, India, Japan, Korea, Malaysia, Pakistan, the Philippine Islands, Thailand, and Vietnam'),(4,'Black ','2015-02-19 23:00:00','origins in any of the Black racial groups of Africa'),(5,'Pacific Islander','2015-02-19 23:00:00','original peoples of Hawaii, Guam, Samoa, or other Pacific islands'),(6,'White','2015-02-19 23:00:00','original peoples of Europe, the Middle East, or North Africa');
UNLOCK TABLES;
/*!40000 ALTER TABLE `ethnicity` ENABLE KEYS */;"""
  
    # The sql queries
    sql += """
DROP TABLE IF EXISTS `patient`;
CREATE TABLE `patient` (
  `PATIENT_ID` int(11) NOT NULL auto_increment,
  `PATIENT_FAMILY_NAME` varchar(100) NOT NULL default '',
  `DATE_OF_REGISTRATION` date default NULL,
  `GENDER` varchar(10) NOT NULL default '',
  `NGR_NO` int(11) NOT NULL default '0',
  `DOSSIER_NO` varchar(50) NOT NULL default '',
  `DATE_OF_BIRTH` date default NULL,
  `DATE_OF_DECEASE` date default NULL,
  `ADDRESS` varchar(400) default NULL,
  `ZIP_CODE` int(11) default NULL,
  `CITY` varchar(40) default NULL,
  `COUNTRY` varbinary(40) default NULL,
  `CITIZENSHIP_STATUS` varchar(20) default NULL,
  `ETHNIC_ID` int(11) NOT NULL,
  `PATIENT_FIRST_NAME` varchar(100) NOT NULL default '',
  `EMAIL` varchar(40) default NULL,
  `CONTACT_NUMBER` varchar(40) default NULL,
  `NATIONAL_ID` varchar(40) default NULL,
  `CARDIO_ID` varchar(20) default NULL,
  `GENETIC_CENTER_ID` varchar(20) default NULL,
  `FAMILY_NUMBER` int(10) unsigned default NULL,
  PRIMARY KEY  (`PATIENT_ID`),
  KEY `R_56` (`ETHNIC_ID`),
  CONSTRAINT `patient_ibfk_1` FOREIGN KEY (`ETHNIC_ID`) REFERENCES `ethnicity` (`ETHNIC_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""

    # Executing the different queries
    tmp = sql.split(";")
    for hql in tmp:
        hql = hql.strip()
        if hql:
            query = hql_query(hql)
            handle = db.execute_and_wait(query, timeout_sec=5.0)
     
    return render('database.initialize.mako', request, locals())

'''  
def init_example(request):
    """ Allow to make some test for the developers, to see if the insertion and the querying of data is correct """

    result = {'status': -1,'data': {}}

    query_server = get_query_server_config(name='impala')
    db = dbms.get(request.user, query_server=query_server)
  
    # Deleting the db
    hql = "DROP TABLE IF EXISTS val_test_2;"
    query = hql_query(hql)
    handle = db.execute_and_wait(query, timeout_sec=5.0)
  
    # Creating the db
    hql = "CREATE TABLE val_test_2 (id int, token string);"
    query = hql_query(hql)
    handle = db.execute_and_wait(query, timeout_sec=5.0)
  
    # Adding some data
    hql = " INSERT OVERWRITE val_test_2 values (1, 'a'), (2, 'b'), (-1,'xyzzy');"
    # hql = "INSERT INTO TABLE testset_bis VALUES (2, 25.0)"
    query = hql_query(hql)
    handle = db.execute_and_wait(query, timeout_sec=5.0)
  
    # querying the data
    hql = "SELECT * FROM val_test_2"
    query = hql_query(hql)
    handle = db.execute_and_wait(query, timeout_sec=5.0)
    if handle:
        data = db.fetch(handle, rows=100)
        result['data'] = list(data.rows())
        db.close(handle)
 
    return render('database.initialize.mako', request, locals())
'''




""" RETURN THE DATA FOR A PATIENT ID """
@csrf_exempt
def patient_search(request):
    """ Search the data related to a given patient id or dossier no """
    result = {'status': -1,'data': {}}

    if request.method != 'POST' or not request.POST or not request.POST['sample_id']:
        result['status'] = 0
        return HttpResponse(json.dumps(result), mimetype="application/json")
    
    
    hql = "SELECT patient.patient_id, patient.dossier_no, patient.patient_family_name, patient.patient_first_name, ethnicity.ethnicity FROM patient JOIN ethnicity ON patient.ethnic_id = ethnicity.ethnic_id WHERE "
    
    if "patient_id" in request.POST:
        patient_id = str(request.POST['patient_id'])
        hql += "patient.patient_id = '"+patient_id+"';"
    elif "dossier_no" in request.POST:
        dossier_no = str(request.POST['dossier_no'])
        hql += "patient.dossier_no = '"+dossier_no+"';"

    # Database connexion
    query_server = get_query_server_config(name='impala')
    db = dbms.get(request.user, query_server=query_server)
    customer_sample_id = str(request.user.id)+"_"+sample_id

    # Selecting the files related to the sample id
    
    query = hql_query(hql)
    handle = db.execute_and_wait(query, timeout_sec=5.0)
    if handle:
        data = db.fetch(handle, rows=100)
        result['status'] = 1
        result['data'] = list(data.rows())
        db.close(handle)

    # Returning the data
    return HttpResponse(json.dumps(result), mimetype="application/json")
    
""" METHODS TO IMPLEMENT """

def documentation(request):
    """ Display the main page of the user documentation """

    # START: TESTS FOR BENCHMARKS

    # END

    return render('documentation.mako', request, locals())




""" ********** """
""" BENCHMARKS """
""" ********** """


""" ************** """
""" SOME FUNCTIONS """
""" ************** """


def json_to_dict(text):
    """ convert a string received from an api query to a classical dict """
    text = str(text).replace('Content-Type: application/json', '').strip()
    return json.loads(text)


def current_line():
    """ Return the current line number """
    return inspect.currentframe().f_back.f_lineno
  
def fprint(txt):
    """ Print some text in a debug file """
    f = open('debug.txt', 'a')
    f.write("Line: "+str(current_line)+" in views.py: "+str(txt)+"\n")
    f.close()
    return True
