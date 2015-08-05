#!/usr/bin/env python

from desktop.lib.django_util import render
from django.views.decorators.csrf import csrf_exempt
from django.core.context_processors import csrf
from django.middleware.csrf import get_token
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
from hbase.api import HbaseApi
from django.template.defaultfilters import stringformat, filesizeformat
from filebrowser.lib.rwx import filetype, rwx
from hadoop.fs.hadoopfs import Hdfs

from django.contrib.auth.decorators import user_passes_test, login_required
from django.http import Http404, HttpResponse

from converters import *

def index(request):
    """ Display the first page of the application """

    return render('index.mako', request, locals())



"""
    The code below needs some refactoring
"""
def sample_insert_questions(request):
    """ Return the questions asked to insert the data """
    questions = {
        "sample_registration":{
            "main_title": "Sample",
            "original_sample_id": {"question": "Original sample id", "field": "text", "regex": "a-zA-Z0-9_-", "mandatory": True},
            "patient_id": {"question": "Patient id", "field": "text", "regex": "a-zA-Z0-9_-", "mandatory": True},
            "biobank_id": {"question": "Biobank id", "field": "text", "regex": "a-zA-Z0-9_-"},
            "prenatal_id": {"question": "Prenatal id", "field": "text", "regex": "a-zA-Z0-9_-"},
            "sample_collection_date": {"question": "Date of collection", "field": "date", "regex": "date"},
            "collection_status": {"question": "Collection status", "field": "select", "fields":("collected","not collected")},
            "sample_type": {"question": "Sample type", "field": "select", "fields":("serum","something else")},
            "biological_contamination": {"question": "Biological contamination", "field": "select", "fields":("no","yes")},
            "sample_storage_condition": {"question": "Storage condition", "field": "select", "fields":("0C","1C","2C","3C","4C")},
        },
    }

    # A dict in python is not ordered so we need a list
    q = ("main_title", "original_sample_id", "patient_id", "biobank_id", "prenatal_id", "sample_collection_date", "collection_status", "sample_type",
        "biological_contamination", "sample_storage_condition")

    # We also load the files
    files = list_directory_content(request, directory_current_user(request), ".vcf", False)

    return questions, q, files

@csrf_exempt
def query_index_interface(request):
    """ Display the page which allows to launch queries or add data """

    if request.method == 'POST':
        form = query_form(request.POST)
    else:
        form = query_form()
    return render('query.index.interface.mako', request, locals())

""" DISPLAY FILES PREVIOUSLY UPLOADED TO ADD SAMPLE DATA """
def sample_index_interface(request):

    # We take the files in the current user directory
    init_path = directory_current_user(request)
    files = list_directory_content(request, init_path, ".vcf", False)
    total_files = len(files)

    return render('sample.index.interface.mako', request, locals())

""" INSERT DATA FOR SAMPLE """
@csrf_exempt
def sample_insert_interface(request):
    """ Insert the data of one or multiple sample in the database """
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
    questions, q, files = sample_insert_questions(request)

    if request.method == 'POST':
        # Now we save the result
        fprint(str(request.POST))
        result = sample_insert(request)
        result = json_to_dict(result)

    # We display the form
    return render('sample.insert.interface.mako', request, locals())

@csrf_exempt
def sample_insert(request, current_analysis='analysis-id-todo'):
    """
        Insert sample data to database
        TODO: We should receive the id of the analysis attached to the submitted file
    """

    vcfSerializer = VCFSerializer()
    result = vcfSerializer.post(request=request, current_analysis=current_analysis)

    return HttpResponse(json.dumps(result), mimetype="application/json")

def sample_insert_vcfinfo(request, filename, total_length):
    """ Return the different samples found in the given vcf file """

    offset = 0
    length = min(1024*1024*5,total_length)
    path = directory_current_user(request)+"/"+filename

    # We read the text and analyze it
    while offset < total_length:
        text = request.fs.read(path, offset, length)
        lines = text.split("\n")
        samples = []
        for line in lines:
            info = line.split("\t")
            if info[0] == '#CHROM':

                # We add the samples information
                for i in xrange(9, len(info)):
                    if info[i]:
                        samples.append(info[i].strip())

                # We can stop it here
                break

        if len(samples) > 0:
            break
        else:
            offset = offset+length

    # We return the different samples in the file
    return samples








###############################
### INITIALIZE THE DATABASE ###
###############################











def database_create_variants(request, temporary=False):
    # Create the variant table. If temporary is True, it means we need to create a temporary structure as Text to be imported
    # to another variants table (that won't be temporary)
    result = {'value':True,'text':'Everything is alright'}
    # We install the tables for impala
    fc = formatConverters(input_file='stuff.vcf',output_file='stuff.json',output_type='json')
    fields = fc.getMappingPyvcfToText()
    pyvcf_fields = fc.getMappingPyvcfToJson()
    hbase_fields = fc.getMappingJsonToHBase()
    inversed_fields = {}
    max_value = 0
    for field in fields:
        if fields[field] > max_value:
            max_value = fields[field]
        future_field = hbase_fields[pyvcf_fields[field]].split('.')
        inversed_fields[fields[field]] = future_field.pop()

    variants_table = ["" for i in xrange(max_value+1)]
    for i in range(1, max_value + 1): # TODO: for now we simply choose the STRING type for every field
        variants_table[i] = inversed_fields[i]+" STRING"
        if i < max_value:
            variants_table[i] += ","
    variants_table[0] = "pk STRING, "

    # Connexion to the db
    query_server = get_query_server_config(name='impala')
    db = dbms.get(request.user, query_server=query_server)

    # Deleting the old table and creating the new one
    if temporary is True:
        handle = db.execute_and_wait(hql_query("DROP TABLE IF EXISTS variants_tmp_"+request.user.username+";"), timeout_sec=30.0)
        query = hql_query("CREATE TABLE variants_tmp_"+request.user.username+"("+"".join(variants_table)+") row format delimited fields terminated by ',' stored as textfile;")
        handle = db.execute_and_wait(query, timeout_sec=30.0)
    else:
        handle = db.execute_and_wait(hql_query("DROP TABLE IF EXISTS variants;"), timeout_sec=30.0)
        query = hql_query("CREATE TABLE variants("+"".join(variants_table)+") stored as parquet;")
        handle = db.execute_and_wait(query, timeout_sec=30.0)

    # We install the variant table for HBase
    try:
        hbaseApi = HbaseApi(user=request.user)
        currentCluster = hbaseApi.getClusters().pop()
        hbaseApi.createTable(cluster=currentCluster['name'],tableName='variants',columns=[{'properties':{'name':'I'}},{'properties':{'name':'R'}},{'properties':{'name':'F'}}])
    except:
        result['value'] = False
        result['text'] = 'A problem occured when connecting to HBase and creating a table. Check if HBase is activated. Note that this message will also appear if the \'variants\' table in HBase already exists. In that case you need to manually delete it.'


    return result

def database_initialize(request):
    """ Install the tables for this application """

    # The variant tables (impala and hbase)
    database_create_variants(request, temporary=False)

    # Connexion to the db
    query_server = get_query_server_config(name='impala')
    db = dbms.get(request.user, query_server=query_server)
  
    # The sql queries
    sql = "DROP TABLE IF EXISTS map_sample_id; CREATE TABLE map_sample_id (internal_sample_id STRING, customer_sample_id STRING, date_creation TIMESTAMP, date_modification TIMESTAMP);  DROP TABLE IF EXISTS sample_files; CREATE TABLE sample_files (id STRING, internal_sample_id STRING, file_path STRING, file_type STRING, date_creation TIMESTAMP, date_modification TIMESTAMP) row format delimited fields terminated by ',' stored as textfile;"

    # The clinical db
    sql += "DROP TABLE IF EXISTS clinical_sample; CREATE TABLE clinical_sample (sample_id STRING, patient_id STRING, date_of_collection STRING, original_sample_id STRING, status STRING, sample_type STRING, biological_contamination STRING, storage_condition STRING, biobank_id STRING, pn_id STRING) row format delimited fields terminated by ',' stored as textfile;"

    # Executing the different queries
    tmp = sql.split(";")
    for hql in tmp:
        hql = hql.strip()
        if hql:
            query = hql_query(hql)
            handle = db.execute_and_wait(query, timeout_sec=5.0)
     
    return render('database.initialize.mako', request, locals())
  
def init_example(request):
    """ Allow to make some test for the developpers, to see if the insertion and the querying of data is correct """

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







#########################
### General functions ###
#########################





def json_to_dict(text):
    """ convert a string received from an api query to a classical dict """
    text = str(text).replace('Content-Type: application/json', '').strip()
    return json.loads(text)

def get_cron_information(url, post_parameters=False):
    """ Make a cron request and return the result """

    buff = StringIO()
    # Adding some parameters to the url
    if "?" in url:
        url += "&user.name=cloudera"
    else:
        url += "?user.name=cloudera"
  
    c = pycurl.Curl()
    c.setopt(pycurl.URL, str(url))
    c.setopt(pycurl.HTTPHEADER, ['Accept: application/json'])
    c.setopt(c.WRITEFUNCTION, buff.write)
    #c.setopt(pycurl.VERBOSE, 0)
    c.setopt(pycurl.USERPWD, 'cloudera:cloudera')
    if post_parameters:
        c.setopt(c.POST, 1)
        c.setopt(c.HTTPPOST, post_parameters)
        c.perform()
        c.close()
    return buff.getvalue()

def upload_cron_information(url, filename):
    """ Upload a file with cron to a specific url """

    fout = StringIO()
  
    #Adding some parameters to the url
    if "?" in url:
        url += "&user.name=cloudera"
    else:
        url += "?user.name=cloudera"
  
    #Setting the headers to say that we are uploading a file. See http://www.saltycrane.com/blog/2012/08/example-posting-binary-data-using-pycurl/
    c = pycurl.Curl()
    c.setopt(pycurl.VERBOSE, 1)
    c.setopt(pycurl.WRITEFUNCTION, fout.write)
    c.setopt(pycurl.URL, str(url))
    c.setopt(pycurl.UPLOAD, 1)
    c.setopt(pycurl.READFUNCTION, open(filename, 'rb').read)
    c.setopt(pycurl.HTTPHEADER, ['Content-Type: application/octet-stream'])
    filesize = os.path.getsize(filename)
    c.setopt(pycurl.INFILESIZE, filesize)
    c.perform()
  
    result = c.getinfo(pycurl.RESPONSE_CODE)
    return result

def create_random_sample_id():
    """ Create the id of a new sample """

    now = datetime.datetime.now()
    y = now.year
    m = now.month
    d = now.day
    h = now.hour
    minute = now.minute
    if len(str(m)) == 1:
        m = "0"+str(m)
    if len(str(d)) == 1:
        d = "0"+str(d)
    if len(str(h)) == 1:
        h = "0"+str(h)
    if len(str(minute)) == 1:
        minute = "0"+str(minute)
  
    randomId = str(randrange(100000,999999))
    randomId += "_"+str(y)+str(m)+str(d)+str(h)+str(minute)
    return randomId
  
def create_random_file_id():
    """ Create a random file id """

    now = datetime.datetime.now()
    y = now.year
    m = now.month
    d = now.day
    if len(str(m)) == 1:
        m = "0"+str(m)
    if len(str(d)) == 1:
        d = "0"+str(d)
  
    randomId = str(randrange(100000,999999))
    randomId += "_"+str(y)+str(m)+str(d)
    return randomId
  
def copy_file(origin, destination):
    """ Copy a file from a given origin to a given destination """

    return True
  
def compress_file(path, destination):
    """ Compress a file sequentially """

    data = ""
  
    #Open a temporary file on the local file system (not a big deal) for the compression. It will be deleted after
    try:
        temporary_filename = "tmp."+str(randrange(0,100000))+".txt"
        f = open(temporary_filename,'w')
    except Exception:
        fprint("Impossible to open a temporary file on the local file system. Are you sure you give enough privileges to the usr/lib/hue directory?")
        return False
  
    #creating a compressor object for sequential compressing
    comp = bz2.BZ2Compressor()
  
    #We take the length of the file to compress
    try:
        file_status = get_cron_information("http://localhost:14000/webhdfs/v1/user/hdfs/"+path+"?op=GETFILESTATUS")
        file_status = json.loads(file_status)
        file_length = file_status[u"FileStatus"][u"length"]
    except Exception:
        fprint("Impossible to take the length of the file.")
        os.remove(temporary_filename)
        return False
    
    #We take part of the file to compress it
    offset=0
    while offset < file_length:
        length = min(file_length,1024*1024)
        txt = get_cron_information("http://localhost:14000/webhdfs/v1/user/hdfs/"+path+"?op=OPEN&offset="+str(offset)+"&length="+str(length)+"")
        data += comp.compress(txt)
    
        #If we have already compressed some data we write them
        if len(data) > 10*1024*1024:
            f.write(data)
            data = ""
    
        offset += min(length,1024*1024) #1Mo
   
    #Flushing the result
    data += comp.flush()
    f.write(data)
    f.close()
    
    #Saving the file to the new repository: http://hadoop.apache.org/docs/r1.0.4/webhdfs.html#APPEND
    result = upload_cron_information("http://localhost:14000/webhdfs/v1/user/hdfs/compressed_data/"+destination+"?op=CREATE&overwrite=false&data=true", temporary_filename)
  
    #We delete the temporary file anyway
    os.remove(temporary_filename)
  
    #If the upload was okay we stop here
    if result >= 200 and result < 400:
        return True
  
    #We have to delete the uploaded file as it may be corrupted
    fprint("Impossible to upload the compressed file to hdfs.")
    res = get_cron_information("http://localhost:14000/webhdfs/v1/user/hdfs/compressed_data/"+destination+"?op=DELETE")
    fprint("Deleting file: "+destination)
  
    return False

def current_line():
    """ Return the current line number """
    return inspect.currentframe().f_back.f_lineno
  
def fprint(txt):
    """ Print some text in a debug file """
    try:
        f = open('debug.txt', 'a')
        f.write("Line: "+str(current_line)+" in views.py: "+str(txt)+"\n")
        f.close()
    except:
        print("Not possible to open the debug file...")
    return True

def list_directory_content(request, first_path, extension, save_stats=False):
    """ Load the content of a directory and its subdirectories, according to the given extension. Find only files. """

    # Recursive functions are the root of all evil.
    paths = []
    paths.append(first_path)
    files = []
    while len(paths) > 0:
        current_path = paths.pop()
        stats = request.fs.listdir_stats(current_path)
        data = [_massage_stats(request, stat) for stat in stats]
        for f in data:
            if f['name'].endswith(extension) and f['type'] == 'file':
                destination_file = f['path'].replace(first_path+"/","",1)
                if save_stats == True:
                    files.append(f)
                else:
                    files.append(destination_file)
            elif f['type'] == 'dir' and f['name'] != '.Trash' and f['name'] != '.' and f['name'] != '..':
                paths.append(f['path'])

    return files

def directory_current_user(request):
    """ Return the current user directory """
    path = request.user.get_home_directory()
    try:
        if not request.fs.isdir(path):
            path = '/'
    except Exception:
        pass

    return path

def _massage_stats(request, stats):
    """
    Massage a stats record as returned by the filesystem implementation
    into the format that the views would like it in.
    """
    path = stats['path']
    normalized = Hdfs.normpath(path)
    return {
    'path': normalized,
    'name': stats['name'],
    'stats': stats.to_json_dict(),
    'mtime': datetime.datetime.fromtimestamp(stats['mtime']).strftime('%B %d, %Y %I:%M %p'),
    'humansize': filesizeformat(stats['size']),
    'type': filetype(stats['mode']),
    'rwx': rwx(stats['mode'], stats['aclBit']),
    'mode': stringformat(stats['mode'], "o")
    #'url': make_absolute(request, "view", dict(path=urlquote(normalized))),
    #'is_sentry_managed': request.fs.is_sentry_managed(path)
    }
