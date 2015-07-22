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
from hadoop.fs.hadoopfs import Hdfs
from django.template.defaultfilters import stringformat, filesizeformat
from filebrowser.lib.rwx import filetype, rwx

from django.contrib.auth.decorators import user_passes_test, login_required
from django.http import Http404, HttpResponse

from variants import api
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
def sample_insert(request):
    """ Insert sample data to database """

    result = {'status': -1,'data': {}}

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

    if not 'vcf_data' in request.POST:
        result['status'] = 0
        result['error'] = 'The vcf data were not given. You have to send a POST field called "vcf_data" with the information about the related file given in parameter.'
        return HttpResponse(json.dumps(result), mimetype="application/json")

    raw_lines = request.POST['vcf_data'].split(";")
    samples_quantity_received = len(raw_lines)
    if samples_quantity_received == samples_quantity + 1 and not raw_lines[len(raw_lines)-1]:# We allow the final ';'
        raw_lines.pop()
        samples_quantity_received = samples_quantity

    if samples_quantity !=  samples_quantity_received:
        fprint(request.POST['vcf_data'])
        result['status'] = 0
        result['error'] = 'The number of samples sent do not correspond to the number of samples found in the vcf file ('+str(samples_quantity_received)+' vs '+str(samples_quantity)+').'
        return HttpResponse(json.dumps(result), mimetype="application/json")

    questions, q, files = sample_insert_questions(request)

    questions_quantity = len(q)
    for raw_line in raw_lines:
        if len(raw_line.split(",")) != questions_quantity:
            result['status'] = 0
            result['error'] = 'The number of information sent do not correspond to the number of questions asked for each sample ('+str(len(raw_line.split(",")))+' vs '+str(questions_quantity)+').'
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
    tsv_content = ''
    for raw_line in raw_lines:
        answers = raw_line.split(",")

        # We check each answer for each question
        current_sample = {}
        for key, answer in enumerate(answers):

            # We take the related field
            field = q[key]
            info = questions['sample_registration'][field]

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

        # We create the tsv content
        tsv_content += sample_id + ','+ patient_id + ',' +date_of_collection+','+original_sample_id+','+status+','+sample_type+','+biological_contamination+','+storage_condition+','+biobank_id+','+pn_id+'\r\n'

    # We create the tsv file
    request.fs.create('/user/'+request.user.username+'/cgs_vcf_import.tsv', overwrite=True, data=tsv_content)

    # We insert the data
    query = hql_query("load data inpath '/user/"+request.user.username+"/cgs_vcf_import.tsv' into table clinical_sample;")
    handle = db.execute_and_wait(query, timeout_sec=30.0)

    # To analyze the content of the vcf, we need to get it from the hdfs to this node
    tmp_vcf = request.fs.read(path='/user/'+request.user.username+'/'+filename, offset=0, length=length)
    tmp_filename = 'cgs_import_'+request.user.username+'.vcf'
    f = open(tmp_filename,mode='w')
    f.write(tmp_vcf)
    f.close()

    # Now we try to analyze the vcf a little bit more with the correct tool
    json_filename = tmp_filename+'.cgs.json'
    convert = formatConverters(input_file=tmp_filename,output_file=json_filename,input_type='vcf',output_type='jsonflat')
    status = convert.convertVCF2FLATJSON()

    # TODO: do not load anymore the entire file in RAM in one-shot
    with open(json_filename, 'r') as content_file:
        json_content = content_file.read()
        request.fs.create('/user/'+request.user.username+'/'+json_filename, overwrite=True, data=json_content)

    # We convert the json to text
    convert = formatConverters(input_file=json_filename,output_file=json_filename+'.tsv',input_type='json',output_type='text')
    status = convert.convertJsonToText(request)

    # TODO: do not load anymore the entire file in RAM in one-shot
    with open(json_filename+'.tsv', 'r') as content_file:
        json_content = content_file.read()
        request.fs.create('/user/'+request.user.username+'/'+json_filename+'.tsv', overwrite=True, data=json_content)

    # We import the .tsv into impala, then we put it into a parquet table
    query = hql_query("load data inpath '/user/"+request.user.username+"/cgs_vcf_import.tsv' into table variants_text;")
    handle = db.execute_and_wait(query, timeout_sec=30.0)


    # We delete the temporary file previously created on this node
    os.remove(tmp_filename)
    os.remove(json_filename)
    os.remove(json_filename+'.tsv')

    if status == 'succeeded':
        result['status'] = 1
    else:
        result['status'] = 0

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
                        samples.append(info[i])

                # We can stop it here
                break

        if len(samples) > 0:
            break
        else:
            offset = offset+length

    # We return the different samples in the file
    return samples

""" INITIALIZE THE DATABASE """
def database_create_variants(request, temporary=False):
    # Create the variant table. If temporary is True, it means we need to create a temporary structure as Text to be imported
    # to another variants table (that won't be temporary)

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

    return True

def database_initialize(request):
    """ Install the tables for this application """


    # The (impala) variant table
    database_create_variants(request, temporary=False)

    # Connexion to the db
    query_server = get_query_server_config(name='impala')
    db = dbms.get(request.user, query_server=query_server)
  
    # The sql queries
    sql = "DROP TABLE IF EXISTS map_sample_id; CREATE TABLE map_sample_id (internal_sample_id STRING, customer_sample_id STRING, date_creation TIMESTAMP, date_modification TIMESTAMP);  DROP TABLE IF EXISTS sample_files; CREATE TABLE sample_files (id STRING, internal_sample_id STRING, file_path STRING, file_type STRING, date_creation TIMESTAMP, date_modification TIMESTAMP) row format delimited fields terminated by ',' stored as textfile;"

    # The clinical db
    sql += "DROP TABLE IF EXISTS clinical_sample; CREATE TABLE clinical_sample (sample_id STRING, patient_id STRING, date_of_collection STRING, original_sample_id STRING, status STRING, sample_type STRING, biological_contamination STRING, storage_condition STRING, biobank_id STRING, pn_id STRING) row format delimited fields terminated by ',' stored as textfile;"

    #DROP TABLE IF EXISTS variants; CREATE TABLE variants (id STRING, alternate_bases STRING, calls STRING, names STRING, info STRING, reference_bases STRING, quality DOUBLE, created TIMESTAMP, elem_start BIGINT, elem_end BIGINT, variantset_id STRING); DROP TABLE IF EXISTS variantsets;
    #CREATE TABLE variantsets (id STRING, dataset_id STRING, metadata STRING, reference_bounds STRING);
    #DROP TABLE IF EXISTS datasets; CREATE TABLE datasets (id STRING, is_public BOOLEAN, name STRING);'''
  
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

    




""" RETURN THE INFORMATION RELATED TO A VARIANT """
def variant_get(request, variant_id):
    """ Return the variant related to the given id """

    result = {'status': -1,'data': {}}
  
    #Connexion db
    query_server = get_query_server_config(name='impala')
    db = dbms.get(request.user, query_server=query_server)
    
    #Selecting the information related to the variant
    hql = "SELECT * FROM map_sample_id;"
    query = hql_query(hql)
    handle = db.execute_and_wait(query, timeout_sec=5.0)
    if handle:
        data = db.fetch(handle, rows=100)
        result['data'] = list(data.rows())
        result['status'] = 1
        db.close(handle)

    #Returning the data
    return HttpResponse(json.dumps(result), mimetype="application/json")

""" RETURN THE DATA FOR A SAMPLE ID """
@csrf_exempt
def sample_search(request):
    """ Search the data related to a given sample id """

    result = {'status': -1,'data': {}}

    if request.method != 'POST' or not request.POST or not request.POST['sample_id']:
        result['status'] = 0
        return HttpResponse(json.dumps(result), mimetype="application/json")

    sample_id = str(request.POST['sample_id'])

    # Database connexion
    query_server = get_query_server_config(name='impala')
    db = dbms.get(request.user, query_server=query_server)
    customer_sample_id = str(request.user.id)+"_"+sample_id

    # Selecting the files related to the sample id
    hql = "SELECT sample_files.id, sample_files.file_path FROM sample_files JOIN map_sample_id ON sample_files.internal_sample_id = map_sample_id.internal_sample_id WHERE map_sample_id.customer_sample_id = '"+customer_sample_id+"';"
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

def variant_search(request):
    """ Return the variant found regarding the post information received """

    result = {'status': -1,'data': {}}
  
    # Returning the data
    return HttpResponse(json.dumps(result), mimetype="application/json")
  
def variant_import(request):
    """ Import variant from the post/get/files information received """

    result = {'status': -1,'data': {}}
  
    #Returning the data
    return HttpResponse(json.dumps(result), mimetype="application/json")

""" OBSOLETE METHOD """
def api_insert_general(request):
    """ Insert some data to the hdfs """

    # we list the different file in the current directory
    info = get_cron_information("http://localhost:14000/webhdfs/v1/user/hdfs/data/?op=LISTSTATUS")
    files = json.loads(info)
    filesList = {}
    for f in files[u"FileStatuses"][u"FileStatus"]:
        if f[u"pathSuffix"].endswith(".vcf") or f[u"pathSuffix"].endswith(".bam") or f[u"pathSuffix"].endswith(".fastq") or f[u"pathSuffix"].endswith(".fq"):
            filesList[f[u"pathSuffix"]] = "data/"+f[u"pathSuffix"]
  
    final_result = {'status': -1,'data': 'Invalid data sent.'}
  
    # We check if we have received some data to import
    formValidated = False
    if request.method == 'POST':
        form = query_insert_form(request.POST, files=filesList)
        if form.is_valid():
            samples_ids = form.cleaned_data['samples_ids']
            selected_file = form.cleaned_data['import_file']
      
        if selected_file.endswith(".vcf"):
            file_type = "vcf"
        elif selected_file.endswith(".bam"):
            file_type = "bam"
        elif selected_file.endswith("fastq") or selected_file.endswith("fq"):
            file_type = "fastq"
        else:
            file_type = "unknown"
      
        #Generating the random id for the file
        file_random_id = create_random_file_id()
      
        #Compressing the file and writing it directly in the correct directory
        path = "data/"+selected_file.strip()
        destination = file_random_id+".bz2"
        try:
            result = compress_file(path, destination)
        except Exception:
            fprint("Impossible to compress and upload the file")
            result = False
            pass
      
        if not result:
            final_result['status'] = 0
            final_result['data'] = 'Sorry, an error occured: Impossible to find the file, compress it or upload it.'
        
        #If the compression was okay, we can insert the data in db
        if result:
            try:
                #Connexion to the db
                query_server = get_query_server_config(name='impala')
                db = dbms.get(request.user, query_server=query_server)
                dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                get_cron_information("http://localhost:14000/webhdfs/v1/user/hdfs/compressed_data/"+destination+"?op=DELETE")
                final_result['status'] = 0
                final_result['data'] = 'Sorry, an error occured: Impossible to connect to the db.'
                fprint("Impossible to connect to the database")
                result = False
                pass
          
        if result:
            #We insert the data to the db
            #TODO: Not very optimized if we have 100 samples ids to insert...
            tmp = samples_ids.split('\n')
            for current_id in tmp:
                current_id = current_id.strip()
                if len(current_id) > 0:
                    customer_sample_id = str(request.user.id)+"_"+current_id
                internal_sample_id = ""
            
                #We check if we already have an internal_sample_id for the customer_sample_id given
                query = hql_query("SELECT internal_sample_id FROM map_sample_id WHERE customer_sample_id = '"+customer_sample_id+"' LIMIT 1;")
                handle = db.execute_and_wait(query, timeout_sec=5.0)
            
                if handle:
                    #If yes, we take the same as before.
                    data = db.fetch(handle, rows=1)
                    tmp = list(data.rows())
                    if(len(tmp) > 0):
                        internal_sample_id = tmp.pop().pop()
            
                    if len(internal_sample_id) == 0:
                        #If not, we create a new customer_sample_id and save it
                        internal_sample_id = str(request.user.id)+"_"+create_random_sample_id()
              
                    query = hql_query("INSERT INTO map_sample_id VALUES('"+internal_sample_id+"', '"+customer_sample_id+"', '"+dt+"', '"+dt+"');")
                    handle = db.execute_and_wait(query, timeout_sec=5.0)
          
                    #We can insert the data now.
                    query = hql_query("INSERT INTO TABLE sample_files VALUES ('"+file_random_id+"', '"+internal_sample_id+"', '"+destination+"', '"+file_type+"', '"+dt+"', '"+dt+"')")
                    handle = db.execute_and_wait(query, timeout_sec=5.0)
       
            #End
            final_result['status'] = 1
            final_result['data'] = 'Data correctly added.'
      
    #Returning the data
    return HttpResponse(json.dumps(final_result), mimetype="application/json")



""" ********** """
""" BENCHMARKS """
""" ********** """

def benchmarks_variant_query(request, benchmark_table):
    result = {'status': -1,'query_time': 0, 'output_length': -1, 'output': ''}

    if not 'database' in request.GET or (request.GET['database'] != "impala_text" and request.GET['database'] != "impala_parquet" and request.GET['database'] != "hbase" and request.GET['database'] != "hive_text" and request.GET['database'] != "hive_parquet"):
        result['status'] = 0
        result['error'] = 'No "database" field received (or an invalid one). It should be "impala_text", "impala_parquet", "hbase", "hive_text", "hive_parquet'
        return HttpResponse(json.dumps(result), mimetype="application/json")
    else:
        database = str(request.GET['database'])

    if not 'query' in request.GET:
        result['status'] = 0
        result['error'] = 'No "query" field received.'
        return HttpResponse(json.dumps(result), mimetype="application/json")

    if not 'stupid_verification' in request.GET or request.GET['stupid_verification'] != 'hIOFE56fgeEGmiumiomiPO998qs':
        result['status'] = 0
        result['error'] = 'No "stupid_verification" field received.'
        return HttpResponse(json.dumps(result), mimetype="application/json")

    output_max = 10*1024*1024;
    if 'output' in request.GET and str(request.GET['output']) == '1':
        output_returned = True

        if 'output_max' in request.GET:
            output_max = int(request.GET['output_max'])
    else:
        output_returned = False

    # We define the table the query will work on (there will be no join so no problem to do that)
    query = str(request.GET['query'])
    if database == "hbase":
        target_table = 'gdegols_benchmarks_'+benchmark_table
    elif database == "hive_text" or database == "impala_text":
        target_table = 'gdegols_benchmarks_text_'+benchmark_table
    else:
        target_table = 'gdegols_benchmarks_parquet_'+benchmark_table

    query = query.replace('benchmarks', target_table)

    # We execute the query. Be careful, the time to launch the hbase shell is around ~7s
    #output = check_output(['echo scan \\\'gdegols_benchmarks_test\\\' | hbase shell'], shell=True)
    if database == "hbase" or database == "hive":
        if database == "hbase":
            command_line = 'echo '+query.replace('\'','\\\'')+' | hbase shell'
        else:
            command_line = 'hive -e \''+query+'\''

        # We make the query for hbase or hive
        if output_returned == True:
            st = time.time()
            output = check_output([command_line], shell=True)
            result['query_time'] = time.time() - st
            result['output_length'] = len(output)
            result['output'] = output[:min(len(output),output_max)]
        else:
            st = time.time()
            subprocess.call([command_line], shell=True)
            result['query_time'] = time.time() - st

    else:
        # We could use "impala-shell", but we can do something a little bit prettier

        #Connexion to the db
        try:
            query_server = get_query_server_config(name='impala')
            db = dbms.get(request.user, query_server=query_server)
        except Exception:
            result['status'] = 0
            result['error'] = 'Sorry, an error occurred: Impossible to connect to the db.'
            return HttpResponse(json.dumps(result), mimetype="application/json")

        # Executing the query
        st = time.time()
        hquery = hql_query(query)
        handle = db.execute_and_wait(hquery)
        result['query_time'] = time.time() - st
        if handle:
            data = db.fetch(handle, rows=output_max)
            result['output'] = list(data.rows())
            db.close(handle)



    # The end
    return HttpResponse(json.dumps(result), mimetype="application/json")

def check_output(*popenargs, **kwargs):
    process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
    output, unused_err = process.communicate()
    retcode = process.poll()
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        error = subprocess.CalledProcessError(retcode, cmd)
        error.output = output
        raise error
    return output

def benchmarks_variant_import(request, benchmark_table):
    # TODO: delete file after hbase import
    result = {'status': -1, 'query_time': 0, 'text_time':0, 'hdfs_time': 0, 'download_time': 0}
    result['info'] = request.GET

    if not 'database' in request.GET or (request.GET['database'] != "impala_text" and request.GET['database'] != "impala_parquet" and request.GET['database'] != "hbase" and request.GET['database'] != "hive"):
        result['status'] = 0
        result['error'] = 'No "database" field received (or an invalid one). It should be "impala_text", "impala_parquet", "hbase" or "hive". But in reality you can only use "impala_text" and "hbase".'
        return HttpResponse(json.dumps(result), mimetype="application/json")
    else:
        database = str(request.GET['database'])

    if not 'variants' in request.GET:
        result['status'] = 0
        result['error'] = 'No "variants" field received.'
        return HttpResponse(json.dumps(result), mimetype="application/json")

    if not 'patient' in request.GET:
        result['status'] = 0
        result['error'] = 'No "patient" field received.'
        return HttpResponse(json.dumps(result), mimetype="application/json")

    # We download the file
    variants_file = str(request.GET['variants'])
    try:
        st = time.time()
        r = requests.get(variants_file)
        result['download_time'] = time.time() - st
        variants = r.text
        result['download_length'] = len(variants)
    except:
        result['status'] = 0
        result['error'] = 'The download of the file '+variants_file+' failed.'
        return HttpResponse(json.dumps(result), mimetype="application/json")

    # We take the data
    variants = variants.replace('\\\"','\"')
    try:
        variants = json.loads(variants)
    except:
        result['status'] = 0
        result['error'] = 'Invalid json received.'
        result['json'] = variants
        return HttpResponse(json.dumps(result), mimetype="application/json")

    # We prepare the tsv file to be able to insert the data after that
    st = time.time()
    try:
        header, tsv = dict_to_tsv(variants, 'vcf')
        patient = str(request.GET['patient'])
        tsv_path = 'gdegols_benchmarks_'+benchmark_table+'_'+patient+'_'+str(create_random_file_id())+'.csv'
    except:
        result['status'] = 0
        result['error'] = 'Impossible to create the tsv file from the json'
        result['json'] = variants
        return HttpResponse(json.dumps(result), mimetype="application/json")
    result['text_time'] = time.time() - st

    # We create a file in hdfs and put the data there
    st = time.time()
    try:
        path = directory_current_user(request) + "/" + tsv_path
        request.fs.create(path, data=tsv)
    except:
        result['status'] = 0
        result['error'] = 'Impossible to create a file in hdfs.'
        return HttpResponse(json.dumps(result), mimetype="application/json")
    result['hdfs_time'] = time.time() - st

    # We try to load the data
    if database == 'hbase':

        # We simply use the shell
        target_table = 'gdegols_benchmarks_'+benchmark_table
        args = ['hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator=\';\' -Dimporttsv.columns=HBASE_ROW_KEY,'+header+' '+target_table+' '+path]
        st = time.time()
        p = subprocess.call(args, shell=True)
        #args = ["hadoop", "jar" , "/usr/lib/hbase/hbase-0.94.6-cdh4.3.0-security.jar", "importtsv", "-Dimporttsv.separator='\t'", "-Dimporttsv.columns=HBASE_ROW_KEY,f:count", target_table, tsv_path]
        result['query_time'] = time.time() - st

        # For hbase we need to delete the file
        request.fs._delete(path, recursive=False)

    elif database == 'impala_text':

        #Connexion to the db
        try:
            query_server = get_query_server_config(name='impala')
            db = dbms.get(request.user, query_server=query_server)
        except Exception:
            result['status'] = 0
            result['error'] = 'Sorry, an error occured: Impossible to connect to the db.'
            return HttpResponse(json.dumps(result), mimetype="application/json")

        target_table = 'gdegols_benchmarks_text_'+benchmark_table

        # Executing the query
        st = time.time()
        query = hql_query("LOAD DATA INPATH '"+path+"' INTO TABLE "+target_table+";")
        handle = db.execute_and_wait(query)
        result['query_time'] = time.time() - st

        # Impala automatically moves the original file to its metastore so we don't need to delete it

    elif database == 'impala_parquet':
        result['status'] = 0
        result['error'] = 'The "impala_parquet" database does not support the load data directly, you have to make the insert' \
                          'manually when the impala_text will be created.'
    else:
        result['status'] = 0
        result['error'] = 'No corresponding database...'

    result['tsv'] = path

    #Returning the result
    return HttpResponse(json.dumps(result), mimetype="application/json")

def dict_to_tsv(variants, column_family):
    """ convert a dict of variants (from a json object) to a tsv file to import it later, we also return the column description """
    fields_of_key, fields_of_filter, fields_of_value = table_configuration()
    lines = []
    header = ""

    # We format the header
    for field in fields_of_filter:
        if header:
            header += ","+column_family+":"+field[1]
        else:
            header = column_family+":"+field[1]

    for field in fields_of_value:
        if header:
            header += ","+column_family+":"+field[1]
        else:
            header = column_family+":"+field[1]

    # We format the variants
    for key in variants:
        variant = variants[key]

        # We create the 'key'
        """
        key = variant['readGroupSets']['readGroups']['sampleId'] + '-' + variant['variants']['id'] + '-' \
            + '0' + '-' + variant['variants']['info']['gene_symbol'] + '-' + variant['variants']['referenceName'] \
            + variant['variants']['start']
        """
        key = str(variant['readGroupSets.readGroups.sampleId']) + '-' + str(variant['variants.id']) + '-' \
            + '0' + '-' + str(variant['variants.info.gene_symbol']) + '-' + str(variant['variants.referenceName']) \
            + str(variant['variants.start'])

        # We create the different fields for the 'value'
        value = ""
        for field in fields_of_filter:
            if value:
                value += ";"+json_field_value(variant, field[0])
            else:
                value += json_field_value(variant, field[0])
        for field in fields_of_value:
            if value:
                value += ";"+json_field_value(variant, field[0])
            else:
                value += json_field_value(variant, field[0])

        lines.append(key + ";" + value)
    text = "\n".join(lines)

    return header, text

def json_field_value(variant, field):
    """ return the value of a specific field/column for the given variant, if none or 0, we return an empty string nonetheless """
    if field in variant:
        if str(variant[field]) != '0':
            return str(variant[field])
    return '0'

def table_configuration():
    """ Return the configuration of the table """
    fields_of_key = [('readGroupSets.readGroups.sampleID','SI', 'string'), ('variants.info.gene_symbol','GS','string'), ('variants.info.gene_ensembl','GIE','string'),
                ('variants.referenceName','C','string'), ('variants.start','P','int'), ('variants.id','ID','string'), ('variants.referenceBases', 'REF', 'string'),
                ('variants.alternateBases','ALT','string'),('variants.quality','QU','float'),('variants.fileformat','FF','string')]
    fields_of_filter = [('gatk','GAQ','string'),('variants.low_quality','LQ','string'),('variants.call.info.confidence_by_depth','AD','string'),
                ('variants.call.info.read_depth','DPF','int'), ('variants.calls.info.genotype_quality','GQ','float'),('variants.genotype','GTP','string'),
                ('variants.calls.info.genotype_likelihood','PL','string')]
    fields_of_value = [('variants.info.allele_num','AC','int'),('variants.allele_frequency','AF','float'),('variants.info.number_genes','AN','int'),
                    ('variants.info.rank_sum_test_base_qual','BQR','float'), ('variants.dbsnp','DB','int'), ('variants.calls.info.read_depth','DP','int'),
                    ('variants.info.downsampled','DS','int'), ('variants.info.fraction_spanning_deletions','DEL','float'), ('variants.info.fisher_strand_bias','FS','float'),
                    ('variants.info.largest_homopolymer','HR','int'), ('variants.info.haplotype_score','HSC','float'), ('variants.info.inbreeding_coefficient','IC','float'),
                    ('variants.info.mapping_quality','MQ','float'), ('variants.info.mapping_quality_zero_reads','MQ0','float'),('variants.info.rank_sum_test_read_mapping_qual','MQRS','int'),
                    ('variants.info.confidence_by_depth','QD','float'), ('variants.rank_sum_test_read_pos_bias','RPRS','float'), ('variants.strand_bias','SB','float'),
                    ('variants.info.mle_allele_count','MLC','int'), ('variants.info.mle_allele_frequency','MLF','float'), ('variants.vcf','SID','string'),
                    ('variants.rank_sum_test_alt_ref','CRS','float'),('variants.change_type','CT','NA'),('variants.calls.info.zygosity','ZY','NA')]

    return fields_of_key, fields_of_filter, fields_of_value

""" ************** """
""" SOME FUNCTIONS """
""" ************** """








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
