from rest_framework import serializers
from variants.models import *
from beeswax.design import hql_query
from beeswax.server import dbms
from beeswax.server.dbms import get_query_server_config
from hbase.api import HbaseApi
from converters import *
from models import *
import time
import os
import json
import threading

# The fields of the following serializers directly come from https://cloud.google.com/genomics/v1beta2/reference/

class VCFSerializer(serializers.Serializer):
    pk = serializers.IntegerField(read_only=True)
    filename = serializers.CharField(max_length=100)
    patients = serializers.CharField(max_length=1000) # Ids of the different patients inside the vcf, separated by a comma
    analyzed = serializers.BooleanField(default=False)

    def post(self, request, filename, current_analysis, current_organization):
        """
            Insert a new vcf file inside the database
        """
        result = {'status': -1,'data': {}}

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
            return result

        # We take the number of samples (and their name) in the vcf file
        samples = sample_insert_vcfinfo(request, filename, length)
        samples_quantity = len(samples)
        if samples_quantity == 0:
            error_sample = True
            result['status'] = 0
            result['error'] = 'No sample found in the given file'
            return result

        # Some checks first about the sample data
        if request.method != 'POST':
            result['status'] = 0
            result['error'] = 'You have to send a POST request.'
            return result

        if not 'vcf_data' in request.POST:
            result['status'] = 0
            result['error'] = 'The vcf data were not given. You have to send a POST field called "vcf_data" with the information about the related file given in parameter.'
            return result

        raw_lines = request.POST['vcf_data'].split(";")
        samples_quantity_received = len(raw_lines)
        if samples_quantity_received == samples_quantity + 1 and not raw_lines[len(raw_lines)-1]:# We allow the final ';'
            raw_lines.pop()
            samples_quantity_received = samples_quantity

        if samples_quantity != samples_quantity_received and False:# TODO: remove this part of code later
            fprint(request.POST['vcf_data'])

            result['status'] = 0
            result['error'] = 'The number of samples sent do not correspond to the number of samples found in the vcf file ('+str(samples_quantity_received)+' vs '+str(samples_quantity)+').'
            return result

        questions, q, files = sample_insert_questions(request)

        questions_quantity = len(q)
        for raw_line in raw_lines:
            if len(raw_line.split(",")) != questions_quantity and False:# TODO: remove this part of code later
                result['status'] = 0
                result['error'] = 'The number of information sent do not correspond to the number of questions asked for each sample ('+str(len(raw_line.split(",")))+' vs '+str(questions_quantity)+').'
                return result

        # Connexion to the db
        try:
            query_server = get_query_server_config(name='impala')
            db = dbms.get(request.user, query_server=query_server)
            dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            result['status'] = 0
            result['error'] = 'Sorry, an error occured: Impossible to connect to the db.'
            return result

        hbaseApi = HbaseApi(user=request.user)
        currentCluster = hbaseApi.getClusters().pop()

        # Now we analyze each sample information
        try:
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
                            return result
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
            tsv_path = '/user/cgs/cgs_'+request.user.username+'_vcf_import.tsv'
            request.fs.create(tsv_path, overwrite=True, data=tsv_content)
        except:
            pass

        # We insert the data
        query = hql_query("load data inpath '/user/cgs/cgs_"+request.user.username+"_vcf_import.tsv' into table clinical_sample;")
        handle = db.execute_and_wait(query, timeout_sec=30.0)

        # We analyze the vcf, then insert the data inside hbase & impala. We don't wait for the import to finish to return the page
        result['text'] = 'The import started correctly and the data from the vcf should be available soon.'
        #thr = threading.Thread(target=import_of_vcf, args=(request, filename, length), kwargs={})
        #thr.start()
        import_of_vcf(request, filename, length)

        if status == 'succeeded':
            result['status'] = 1
        else:
            result['status'] = 0

        return result

def import_of_vcf(request, filename, length):
    # It is in charge to import a vcf (convert the vcf to avro, etc.), and as it is not fast, we should call
    # this method asynchronously7

    # Connexion to the db
    try:
        query_server = get_query_server_config(name='impala')
        db = dbms.get(request.user, query_server=query_server)
    except Exception:
        return False

    hbaseApi = HbaseApi(user=request.user)
    currentCluster = hbaseApi.getClusters().pop()

    # To analyze the content of the vcf, we need to get it from the hdfs to this node
    buffer = min(length,1024*1024*512)
    tmp_filename = 'cgs_import_'+request.user.username+'.vcf'
    f = open(tmp_filename,mode='w')
    for offset in xrange(0, length, buffer):
        tmp_vcf = request.fs.read(path='/user/'+request.user.username+'/'+filename, offset=offset, length=buffer, bufsize=buffer)
        f.write(tmp_vcf)
    f.close()

    # Now we try to analyze the vcf a little bit more with the correct tool
    json_filename = tmp_filename+'.cgs.json'
    st = time.time()
    convert = formatConverters(input_file=tmp_filename,output_file=json_filename,input_type='vcf',output_type='jsonflat')
    status, columns, ids_of_samples, rowkeys = convert.convertVcfToFlatJson(request=request, initial_file=filename)
    f = open('superhello.txt','w')
    f.write('EXECUTION TIME to flat json:'+str(time.time()-st)+'\n')
    f.close()

    # We put the output on hdfs
    json_size = os.path.getsize(json_filename)
    buffer = min(json_size, 1024*1024*50)
    st = time.time()
    with open(json_filename, 'r') as content_file:
        request.fs.create('/user/cgs/cgs_'+request.user.username+'_'+json_filename, overwrite=True, data='')
        for offset in xrange(0, json_size, buffer):
            ftmp = open('superhello.txt','a')
            ftmp.write('Pushing flatjson to hdfs (/user/cgs/cgs_'+request.user.username+'_'+json_filename+')... '+str(time.time()-st)+'\n')
            ftmp.close()
            request.fs.append('/user/cgs/cgs_'+request.user.username+'_'+json_filename, data=content_file.read(buffer))

    # We eventually modify the avsc file with the new calls
    avro_schema = {}
    with open('myapps/variants/variants.avsc','r') as content_file:
        avro_schema = json.loads(content_file.read())
        with open('variants.avsc','w') as f:
            f.write(json.dumps(avro_schema))

    existing_columns = []
    for field in avro_schema['fields']:
        existing_columns.append(field['name'])
    modified_avro_schema = False
    specific_columns = [] # Used below for the import in impala/hive
    for sample_id in ids_of_samples:
        destination_field = 'I_CALL_'+sample_id

        if destination_field not in specific_columns:
            specific_columns.append(destination_field)

        if destination_field not in existing_columns:
            # The current sample does not exist yet in the avsc file, we need to add it
            call_schema = {"name":destination_field,"type":["string","null"],"doc":"Column for a specific sample"}
            avro_schema['fields'].append(call_schema)
            existing_columns.append(destination_field)
            modified_avro_schema = True

    if modified_avro_schema is True:
        # We need to save the new schema, first we save the old version, then we modify the current one
        with open('variants.avsc','r') as content_file:
            with open('variants.'+str(time.time())+'.avsc', 'w') as archive_file:
                archive_file.write(content_file.read())

        with open('variants.avsc','w') as content_file:
            content_file.write(json.dumps(avro_schema))

        request.fs.create('/user/cgs/cgs_variants.avsc', overwrite=True, data=json.dumps(avro_schema))

    # We convert the flat json to hbase (mostly a key mapping)
    st = time.time()
    convert = formatConverters(input_file=json_filename,output_file=json_filename+'.hbase',input_type='jsonflat',output_type='hbase')
    status = convert.convertFlatJsonToHbase()
    ftmp = open('superhello.txt','a')
    ftmp.write('Conversion from flatjson to hbase... '+str(time.time()-st)+'\n')
    ftmp.close()

    # We put the hbase file on hdfs
    hbase_length = os.path.getsize(json_filename+'.hbase')
    buffer = min(hbase_length,1024*1024*50)
    st = time.time()
    with open(json_filename+'.hbase', 'r') as content_file:
        request.fs.create('/user/cgs/cgs_'+request.user.username+'_'+json_filename+'.hbase', overwrite=True, data='')
        for offset in xrange(0, hbase_length, buffer):
            cont = content_file.read(buffer)
            ftmp = open('superhello.txt','a')
            ftmp.write('Pushing hbase to hdfs (/user/cgs/cgs_'+request.user.username+'_'+json_filename+'.hbase)... '+str(time.time()-st)+'\n')
            ftmp.close()
            request.fs.append('/user/cgs/cgs_'+request.user.username+'_'+json_filename+'.hbase', data=cont)

    # We convert the hbase to avro file
    st = time.time()
    convert = formatConverters(input_file=json_filename+'.hbase',output_file=json_filename+'.avro',input_type='jsonflat',output_type='avro')
    status = convert.convertHbaseToAvro(avscFile='variants.avsc')

    ftmp = open('superhello.txt','a')
    ftmp.write('Conversion from hbase to avro... '+str(time.time()-st)+'\n')
    ftmp.close()

    # We put the avro file on hdfs
    st = time.time()
    avro_length = os.path.getsize(json_filename+'.avro')
    buffer = min(avro_length, 1024*1024*50)
    with open(json_filename+'.avro', 'r') as content_file:
        request.fs.create('/user/cgs/cgs_'+request.user.username+'_'+json_filename+'.avro', overwrite=True, data='')
        request.fs.create('/user/cgs/cgs_'+request.user.username+'_'+json_filename+'.archive.avro', overwrite=True, data='')
        for offset in xrange(0, avro_length, buffer):
            cont = content_file.read(buffer)
            ftmp = open('superhello.txt','a')
            ftmp.write('Pushing avro to hdfs (/user/cgs/cgs_'+request.user.username+'_'+json_filename+'.avro)... '+str(time.time()-st)+'\n')
            ftmp.close()
            request.fs.append('/user/cgs/cgs_'+request.user.username+'_'+json_filename+'.avro', data=cont)
            request.fs.append('/user/cgs/cgs_'+request.user.username+'_'+json_filename+'.archive.avro', data=cont)

    """ For test only ""
    convert = formatConverters(input_file='myapps/variants/twitter-content.json',output_file='tmp.avro',input_type='jsonflat',output_type='avro')
    status = convert.convertHbaseToAvro(avscFile='myapps/variants/twitter.avsc',modify=False)
    with open('tmp.avro', 'r') as content_file:
        request.fs.create('/user/cgs/cgs_twitter.avro', overwrite=True, data=content_file.read())
    "" End test """

    tmpf = open('superhello.txt','a')
    # O: We get the columns from the parquet table to detect missing columns for the new calls we just created
    query = hql_query("show column stats variants")
    handle = db.execute_and_wait(query, timeout_sec=30.0)
    data = db.fetch(handle, rows=1000000)
    rows = list(data.rows())
    columns_for_new_calls = []
    existing_calls_columns = []
    for row in rows:
        current_column = row[0]
        if current_column.startswith('i_call_'):
            existing_calls_columns.append(str(current_column).lower())

    for current_sample in ids_of_samples:
        destination_field = str('I_CALL_'+current_sample).lower()
        if destination_field not in existing_calls_columns and destination_field not in columns_for_new_calls:
            columns_for_new_calls.append(destination_field)
    tmpf.write("Existing calls: "+json.dumps(existing_calls_columns)+"\r\n")
    tmpf.write("New calls: "+json.dumps(columns_for_new_calls))
    tmpf.close()

    # 1st: we create a temporary hive table with avro storage
    result, variants_table = database_create_variants(request, temporary=True, specific_columns=specific_columns)

    tmpf = open('superhello.txt','a')
    # 2nd: we import the previously created avro file inside the temporary avro table
    query_server = get_query_server_config(name='hive')
    hive_db = dbms.get(request.user, query_server=query_server)
    variants_columns = []
    for variants_column in variants_table:
        variants_columns.append(str(variants_column).split(' ').pop(0))

    query = hql_query("load data inpath '/user/cgs/cgs_"+request.user.username+"_"+json_filename+".avro' into table variants_tmp_"+request.user.username+";")
    handle = hive_db.execute_and_wait(query, timeout_sec=3600.0)

    # Necessary for impala to detect an hive table
    query = hql_query("invalidate metadata;")
    handle = db.execute_and_wait(query, timeout_sec=30.0)

    # 3rd: we eventually modify the global parquet table to add the eventual new columns for each call
    if len(columns_for_new_calls) > 0:
        query = hql_query("alter table variants add columns ("+' STRING, '.join(columns_for_new_calls)+" STRING)")
        handle = db.execute_and_wait(query, timeout_sec=3600.0)

    # 4th: we import the data from the temporary avro table to the global parquet table
    query = hql_query("insert into table variants ("+','.join(variants_columns)+") select "+','.join(variants_columns)+" from variants_tmp_"+request.user.username+" ;")
    handle = db.execute_and_wait(query, timeout_sec=3600.0)

    # 5th: we delete the temporary table
    #query = hql_query("drop table variants_tmp_"+request.user.username+";")
    #handle = hive_db.execute_and_wait(query, timeout_sec=30.0)


    # We put the data in HBase. For now we do it simply, but we should use the bulk upload (TODO)
    with open(json_filename+'.hbase', 'r') as content_file:
        for line in content_file:
            try:
                # We create the json content
                hbase_data = json.loads(line)
                rowkey = hbase_data['rowkey']
                del hbase_data['rowkey']
                del hbase_data['pk']

                # We can save the new variant
                hbaseApi.putRow(cluster=currentCluster['name'], tableName='variants', row=rowkey, data=hbase_data)
            except Exception as e:
                fprint("Error while reading the HBase json file")
                tmpf.write('Error ('+str(e.message)+'):/.')

    tmpf.close()

    # We delete the temporary file previously created on this node
    os.remove(tmp_filename)
    os.remove(json_filename)

    return True

"""
    Dataset
"""
class DatasetSerializer(serializers.Serializer):
    id = serializers.CharField()
    projectNumber = serializers.IntegerField()
    isPublic = serializers.BooleanField()
    name = serializers.CharField()

"""
    ReferenceSet
"""

class ReferenceSetSerializer(serializers.Serializer):
    id = serializers.CharField
    referenceIds = serializers.ListField()
    md5checksum = serializers.CharField()
    ncbiTaxonId = serializers.IntegerField()
    description = serializers.CharField()
    assemblyId = serializers.CharField()
    sourceURI = serializers.CharField()
    sourceAccessions = serializers.ListField()

"""
    Reference
"""

class ReferenceSerializer(serializers.Serializer):
    id = serializers.CharField()
    length = serializers.IntegerField()
    md5checksum = serializers.CharField()
    name = serializers.CharField()
    sourceURI = serializers.CharField()
    sourceAccessions = serializers.ListField()
    ncbiTaxonId = serializers.IntegerField()

"""
    ReadGroupSet and readGroup
"""
class ReadGroupExperimentSerializer(serializers.Serializer):
    # This object is only used by ReadGroupSerializer
    libraryId = serializers.CharField()
    platformUnit = serializers.CharField()
    sequencingCenter = serializers.CharField()
    instrumentModel = serializers.CharField()

class ReadGroupProgramSerializer(serializers.Serializer):
    # This object is only used by ReadGroupSerializer
    commandLine = serializers.CharField()
    id = serializers.CharField()
    name = serializers.CharField()
    prevProgramId = serializers.CharField()
    version = serializers.CharField()

class ReadGroupSerializer(serializers.Serializer):
    # This object is only used by ReadGroupSetSerializer
    id = serializers.CharField()
    datasetId = serializers.CharField()
    name = serializers.CharField()
    description = serializers.CharField()
    sampleId = serializers.CharField()
    experiment = ReadGroupExperimentSerializer()
    predictedInsertSize = serializers.IntegerField()
    programs = ReadGroupProgramSerializer(many=True)
    referenceSetId = serializers.CharField()
    info = serializers.DictField()

class ReadGroupSetSerializer(serializers.Serializer):
    id = serializers.CharField()
    datasetId = serializers.CharField()
    referenceSetId = serializers.CharField()
    name = serializers.CharField()
    filename = serializers.CharField()
    readGroups = ReadGroupSerializer(many=True)
    info = serializers.DictField()

"""
    Read
"""

class ReadAlignementPositionSerializer(serializers.Serializer):
    # This object is only used by ReadAlignementSerializer
    referenceName = serializers.CharField()
    position = serializers.IntegerField()
    reverseStrand = serializers.BooleanField()

class ReadAlignementCigarSerializer(serializers.Serializer):
    operation = serializers.CharField()
    operationLength = serializers.IntegerField()
    referenceSequence = serializers.CharField()

class ReadAlignmentSerializer(serializers.Serializer):
    # This object is only used by ReadSerializer()
    position = ReadAlignementPositionSerializer()
    mappingQuality = serializers.IntegerField()
    cigar = ReadAlignementCigarSerializer(many=True)

class ReadNextMatePositionSerializer(serializers.Serializer):
    referenceName = serializers.CharField()
    position = serializers.IntegerField()
    reverseStrand = serializers.BooleanField()

class ReadSerializer(serializers.Serializer):
    id = serializers.CharField()
    readGroupId = serializers.CharField()
    readGroupSetId = serializers.CharField()
    fragmentName = serializers.CharField()
    properPlacement = serializers.BooleanField()
    duplicateFragment = serializers.BooleanField()
    fragmentLength = serializers.IntegerField()
    readNumber = serializers.IntegerField()
    numberReads = serializers.IntegerField()
    failedVendorQualityChecks = serializers.BooleanField()
    alignment = ReadAlignmentSerializer()
    secondaryAlignement = serializers.BooleanField()
    supplementaryAlignment = serializers.BooleanField()
    alignedSequence = serializers.CharField()
    alignedQuality = serializers.ListField()
    nextMatePosition = ReadNextMatePositionSerializer()
    info = serializers.DictField()

"""
    VariantSet
"""
class VariantSetReferenceBoundSerializer(serializers.Serializer):
    # This object is only used by VariantSetSerializer()
    referenceName = serializers.CharField()
    upperBound = serializers.IntegerField()

class VariantSetMetadataSerializer(serializers.Serializer):
    # This object is only used by VariantSetSerializer()
    key = serializers.CharField()
    value = serializers.CharField()
    id = serializers.CharField()
    type = serializers.CharField()
    number = serializers.CharField()
    description = serializers.CharField()
    info = serializers.DictField()

class VariantSetSerializer(serializers.Serializer):
    datasetId = serializers.CharField()
    id = serializers.CharField()
    referenceBounds = VariantSetReferenceBoundSerializer()
    metadata = VariantSetMetadataSerializer(many=True)

"""
    Variant
"""

class VariantCallSerializer(serializers.Serializer):
    # This object is only used by VariantSerializer()
    callSetId = serializers.CharField(allow_blank=True)
    callSetName = serializers.CharField(allow_blank=True)
    genotype = serializers.ListField()
    phaseset = serializers.CharField(allow_blank=True)
    genotypeLikelihood = serializers.ListField()
    info = serializers.DictField()

    def __init__(self, variantcall_data, *args, **kwargs):
        # We load the data based on the information we receive from the database.

        d = {}
        # We already have the structure json data inside variantcall_data, we just need to modify
        # them a little bit
        json_data = {}
        for field in variantcall_data:
            json_data['variants.calls[].'+field] = variantcall_data[field]

        d = jsonToSerializerData(json_data, self.fields, 'variants.calls[]')

        # Now we can call the classical constructor
        kwargs['data'] = d
        super(VariantCallSerializer, self).__init__(*args, **kwargs)
        self.is_valid()

class VariantSerializer(serializers.Serializer):
    variantSetId = serializers.CharField()
    id = serializers.CharField()
    names = serializers.ListField()
    created = serializers.IntegerField()
    referenceName = serializers.CharField()
    start = serializers.IntegerField()
    end = serializers.IntegerField()
    referenceBases = serializers.CharField()
    alternateBases = serializers.ListField()
    quality = serializers.FloatField()
    filters = serializers.ListField()
    info = serializers.DictField()
    calls = VariantCallSerializer(variantcall_data='', many=True)


    def __init__(self, request=None, pk=None, impala_data=None, *args, **kwargs):
        if request is None and pk is None and impala_data is None:
            return super(VariantSerializer, self).__init__(*args, **kwargs)

        # We take the information in the database if don't have it. As we are interested in one variant, we use HBase
        if impala_data is None:
            # Documentation: https://github.com/cloudera/hue/blob/master/apps/hbase/src/hbase/api.py
            hbaseApi = HbaseApi(user=request.user)
            currentCluster = hbaseApi.getClusters().pop()

            # We arbitrary take 100 rows (to be able to catch every different alternate. Maybe we should take more, not sure about that (we cannot
            # set an endkey with the hbase api). Most of the times 100 rows will be way more than enough
            variant = hbaseApi.getRows(cluster=currentCluster['name'], tableName='variants', columns=['R','I','F'], startRowKey=pk, numRows=100, prefix=False)
        else:
            variant = "raw data we got from impala..."

        if variant is not None:
            # We load it in the current object
            if impala_data is None:
                json_data = hbaseToJson(variant)
            else:
                json_data = parquetToJson(impala_data)
            d = jsonToSerializerData(json_data, self.fields, 'variants')

            d['calls'] = []
            for variants_call in json_data['variants.calls[]']:
                call = VariantCallSerializer(variantcall_data=variants_call)
                d['calls'].append(call.data)

            # Load a specific variant
            kwargs['data'] = d
            super(VariantSerializer, self).__init__(*args, **kwargs)

            self.is_valid()

    def post(self, request):
        # Insert a new variant inside the database (Impala - HBase)

        # TODO: this method is OBSOLETE and will not work
        # TODO: it would be great to move the ';'.join() and json.dumps() to converters.py

        # Impala - We create the query to put the data
        query_data = ["" for i in range(dbmap_length()+1)]

        query_data[0] = self.variantSetId + '-' + self.referenceName + '-' + self.start + '-' + self.referenceBases + '-' + self.alternateBases

        query_data[dbmap('variants.variantSetId', order=True)] = self.variantSetId
        query_data[dbmap('variants.id', order=True)] = self.id
        query_data[dbmap('variants.names[]', order=True)] = ';'.join(self.names)
        query_data[dbmap('variants.created', order=True)] = self.created
        query_data[dbmap('variants.referenceName', order=True)] = self.created
        query_data[dbmap('variants.start', order=True)] = self.start
        query_data[dbmap('variants.end', order=True)] = self.end
        query_data[dbmap('variants.referenceBases', order=True)] = self.referenceBases
        query_data[dbmap('variants.alternateBases[]', order=True)] = ';'.join(self.alternateBases)
        query_data[dbmap('variants.quality', order=True)] = self.quality
        query_data[dbmap('variants.filters[]', order=True)] = ';'.join(self.filter)
        query_data[dbmap('variants.info{}', order=True)] = json.dumps(self.info)
        query_data[dbmap('variants.calls[]', order=True)] = "TODO" # TODO

        # Impala- We make the query
        query_server = get_query_server_config(name='impala')
        db = dbms.get(request.user, query_server=query_server)
        query = hql_query("INSERT INTO variant("+",".join(query_data)+")")
        handle = db.execute_and_wait(query, timeout_sec=5.0)
        if handle:
            db.close(handle)
        else:
            raise Exception("Impossible to create the variant...")

        # HBase - We add the data in that table too
        hbaseApi = HbaseApi(user=request.user)
        currentCluster = hbaseApi.getClusters().pop()
        rowkey = query_data[0]
        hbase_data = {}
        query_data[dbmap('variants.variantSetId', database="hbase", order=True)] = self.variantSetId
        query_data[dbmap('variants.id', database="hbase", order=True)] = self.id
        query_data[dbmap('variants.names[]', database="hbase", order=True)] = ';'.join(self.names)
        query_data[dbmap('variants.created', database="hbase", order=True)] = self.created
        query_data[dbmap('variants.referenceName', database="hbase", order=True)] = self.created
        query_data[dbmap('variants.start', database="hbase", order=True)] = self.start
        query_data[dbmap('variants.end', database="hbase", order=True)] = self.end
        query_data[dbmap('variants.referenceBases', database="hbase", order=True)] = self.referenceBases
        query_data[dbmap('variants.alternateBases[]', database="hbase", order=True)] = ';'.join(self.alternateBases)
        query_data[dbmap('variants.quality', database="hbase", order=True)] = self.quality
        query_data[dbmap('variants.filters[]', database="hbase", order=True)] = ';'.join(self.filter)
        query_data[dbmap('variants.info{}', database="hbase", order=True)] = json.dumps(self.info)
        query_data[dbmap('variants.calls[]', database="hbase", order=True)] = "TODO" # TODO

        hbaseApi.putRow(cluster=currentCluster['name'], tableName='variants', row=rowkey, data=hbase_data)

"""
    CallSet
"""

class CallSetSerializer(serializers.Serializer):
    id = serializers.CharField()
    name = serializers.CharField()
    sampleId = serializers.CharField()
    variantSetIds = VariantSetSerializer()
    created = serializers.IntegerField()
    info = serializers.DictField()


"""
    AnnotationSet
"""

class AnnotationSetSerializer(serializers.Serializer):
    id = serializers.CharField()
    datasetId = serializers.CharField()
    referenceSetId = serializers.CharField()
    name = serializers.CharField()
    sourceUri = serializers.CharField()
    type = serializers.CharField()
    info = serializers.DictField()

"""
    Annotation
"""

class AnnotationPositionSerializer(serializers.Serializer):
    # This serializer is only used by Annotation serializer
    referenceId = serializers.CharField()
    referenceName = serializers.CharField()
    start = serializers.IntegerField()
    end = serializers.IntegerField()
    reverseStrand = serializers.BooleanField()

class AnnotationVariantConditionsExternalIdSerializer(serializers.Serializer):
    # This serializer is only used by AnnotationVariantConditionsSerializer
    sourceName = serializers.CharField()
    id = serializers.CharField()

class AnnotationVariantConditionSerializer(serializers.Serializer):
    # This serializer is only used by AnnotationVariantSerializer
    names = serializers.ListField()
    externalIds = AnnotationVariantConditionsExternalIdSerializer(many=True) # Super-Ugly isn't it?
    conceptId = serializers.CharField()
    omimId = serializers.CharField()

class AnnotationTranscriptExonsFrameSerializer(serializers.Serializer):
    # This serializer is only used by AnnotationTranscriptExonsSerializer()
    value = serializers.IntegerField()

class AnnotationTranscriptExonSerializers(serializers.Serializer):
    # This serializer is only used by AnnotationTranscriptSerializer()
    start = serializers.IntegerField()
    end = serializers.IntegerField()
    frame = AnnotationTranscriptExonsFrameSerializer() # Super ugly again!

class AnnotationTranscriptCodingSequenceSerializer(serializers.Serializer):
    # This serializer is only used by AnnotationTranscriptSerializer
    start = serializers.CharField()
    end = serializers.CharField()

class AnnotationTranscriptSerializer(serializers.Serializer):
    # This serializer is only used by AnnotationSerializer()
    geneId = serializers.CharField()
    exons = AnnotationTranscriptExonSerializers(many=True)
    codingSequence = AnnotationTranscriptCodingSequenceSerializer()

class AnnotationVariantSerializer(serializers.Serializer):
    # This serializer is only used by Annotation serializer. Maybe we could use the VariantSerializer directly
    # but Google seems to have some reasons to not do it, and they're smarter than us.
    type = serializers.CharField()
    effect = serializers.CharField()
    alternateBases = serializers.CharField()
    geneId = serializers.CharField()
    transcriptIds = serializers.ListField()
    conditions = AnnotationVariantConditionSerializer(many=True)
    clinicalSignificance = serializers.CharField()

class AnnotationSerializer(serializers.Serializer):
    id = serializers.CharField()
    annotationSetId = serializers.CharField()
    name = serializers.CharField()
    position = AnnotationPositionSerializer()
    type = serializers.CharField()
    variant = AnnotationVariantSerializer()
    transcript = AnnotationTranscriptSerializer()
    info = serializers.DictField()

"""
    Job
"""

class JobRequestSerializer(serializers.Serializer):
    # This object is used by JobSerializer only
    type = serializers.CharField()
    source = serializers.ListField()
    destination = serializers.ListField()

class JobSerializer(serializers.Serializer):
    id = serializers.CharField()
    projectNumber = serializers.IntegerField()
    status = serializers.CharField()
    detailedStatus = serializers.CharField()
    importedIds = serializers.ListField()
    errors = serializers.ListField()
    warnings = serializers.ListField()
    created = serializers.IntegerField()
    request = JobRequestSerializer()
