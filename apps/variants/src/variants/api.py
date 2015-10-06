#!/usr/bin/env python

import json
import logging

from django.http import HttpResponse

from beeswax.design import hql_query
from beeswax.server import dbms
from beeswax.server.dbms import get_query_server_config

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.renderers import JSONRenderer
from serializers import *
from models import *

import os
from settings import ROOT_PATH

LOG = logging.getLogger(__name__)

### debugging
# def current_line():
#     """ Return the current line number """
#     return inspect.currentframe().f_back.f_lineno



class VCFDetail(APIView):
    """
        This view allow to have information about a specific user, create a new one, etc.
    """
    def get(self, request, pk=-1):
        result = {'status':1,'text':'Everything is alright.'}

        # Basic verifications
        try:
            pk = int(pk)
            if pk < 0:
                result['status'] = 0
                result['text'] = 'Invalid pk'
        except:
            result['status'] = 0
            result['text'] = 'Invalid pk'

        if result['status'] == 0:
            return HttpResponse(json.dumps(result), content_type="application/json")

        # We get the information and format the result (TODO)
        #p = VCF.objects.get(pk=pk)
        #result = VCFSerializer(p, context={'request': request})

        return Response(result.data)


    def post(self, request, filename, current_analysis, current_organization, from_views=False):
        """
            Receive a new vcf to analyze
        :param request:
        :param format:
        :return:
        """
        result = {'status':1,'text':'Everything is alright.'}

        v = VCFSerializer()
        result = v.post(request=request, filename=filename, current_analysis=current_analysis, current_organization=current_organization)
        if result['status'] == 1:
            if from_views is True:
                return result
            else:
                return Response(result, status=status.HTTP_201_CREATED)

        result = {'status':1,'text':'Something went wrong.'}

        if from_views is True:
            return result
        else:
            return Response(result, status=status.HTTP_400_BAD_REQUEST)

class SampleDetail(APIView):
    """
        Manage a sample
    """

    def get(self, request, pk=-1):
        # Information on a specific sample
        result = {'status':1,'text':'Everything is alright.'}
        return Response(result)

    def delete(self, request, pk=-1):
        # Delete a specific sample
        result = {'status':1,'text':'Everything is alright.'}
        return Response(result)

    def search(self, request, terms=''):
        # Search for a specific sample
        result = {'status':1,'text':'Everything is alright.'}
        return Response(result)

class SampleList(APIView):
    """
        Return information on a list of samples
    """
    def get(self, request):
        result = {'status':1,'text':'Everything is alright.'}
        return Response(result)

class DatasetDetail(APIView):

    def get(self, request, pk):
        # Information on a specific dataset
        result = {'status':1,'text':'Everything is alright.'}
        return Response(result)

    def delete(self, request, pk):
        # Delete a specific dataset
        result = {'status':1,'text':'Everything is alright.'}
        return Response(result)

class DatasetList(APIView):
    """
        Return information on a list of datasets
    """
    def get(self, request):
        result = {'status':1,'text':'Everything is alright.'}
        return Response(result)

class ReferenceDetail(APIView):

    def get(self, request, pk):
        # Information on a specific reference
        result = {'status':1,'text':'Everything is alright.'}
        return Response(result)

    def search(self, request):
        result = {'status':1,'text':'Everything is alright.'}
        return Response(result)

class ReferenceSetDetail(APIView):

    def get(self, request, pk):
        # Information on a specific referenceset
        result = {'status':1,'text':'Everything is alright.'}
        return Response(result)

    def search(self, request):
        result = {'status':1,'text':'Everything is alright.'}
        return Response(result)

class VariantSetDetail(APIView):

    def get(self, request, pk=-1):
        if pk == "search":
            return self.search(request=request)

        # Information on a specific variantset
        result = {'status':1,'text':'Everything is alright.'}
        return Response(result)

    def action(self, request, pk, action_type):
        if action_type == 'export':
            return self.action_export(request=request, pk=pk)
        elif action_type == 'importVariants':
            return self.action_import_variants(request=request, pk=pk)
        elif action_type == 'mergeVariants':
            return self.action_merge_variants(request=request, pk=pk)

        result = {'status':0,'text':'Invalid action.'}
        return HttpResponse(json.dumps(result), mimetype="application/json")

    def action_export(self, request, pk):
        result = {'status':1,'text':'To implement.'}
        return HttpResponse(json.dumps(result), mimetype="application/json")

    def action_import_variants(self, request, pk):
        result = {'status':1,'text':'To implement.'}
        return HttpResponse(json.dumps(result), mimetype="application/json")

    def action_merge_variants(self, request, pk):
        result = {'status':1,'text':'To implement.'}
        return HttpResponse(json.dumps(result), mimetype="application/json")

    def search(self, request):
        result = {'status':1,'text':'To implement.'}
        return HttpResponse(json.dumps(result), mimetype="application/json")

    def post(self, request, format=None):
        """ Creates variant data by asynchronously importing the provided information into HBase.
        HTTP request: POST https://<your.site>/variants/variantsets/variantSetId/importVariants

        Parameters:
         - variantSetId: string Required. The variant set to which variant data should be imported.
         - sourceUris: a list of URIs pointing to VCF files in the cluster
         - format: "VCF"

        ##
        """
        result = {'status':1,'text':'Everything is alright.'}
        return Response(result)

class VariantDetail(APIView):

    renderer_classes = (JSONRenderer, )

    def get(self, request, pk=""):
        # Returns some information on a specific variant
        if pk == 'search':
            return self.search(request=request)
        elif pk == 'highlander_search':
            return self.highlander_search(request=request)

        if len(pk) == 0:
            return Response(json.dumps({'status':-1,'error':'Variant id not given.'}))

        # We ask some information
        variant = VariantSerializer(request=request, pk=pk)

        #return Response(json.dumps({'status':-1,'error':'Variant id invalid or problem while loading the variant.'}))
        return Response(variant.data)

    def post(self, request, data=""):
        # Create a new variant
        if data == 'search':
            return self.search(request)
        elif data == 'highlander_search':
            return self.highlander_search(request)

        status = -1

        variant_form = VariantSerializer(data=request.data)
        if variant_form.is_valid():
            try:
                variant_form.post(request)
                status = 1
            except:
                status = 0

        return Response({'status':status})

    def search(self, request):
        # Search for a specific variant. See https://cloud.google.com/genomics/v1beta2/reference/variants/search

        result = {'status':1,'text':'Everything is alright.'}
        data = request.data

        """ For test only """
        if len(data) == 0:
            data = {
              "variantSetIds": ['0|tiny_sample.vcf'],
              "variantName": '',
              "callSetIds": [],
              "referenceName": 1,
              "start": 1,
              "end": 0, # Not supported (TODO: add this field in the rowkey?)
              "pageToken": '',
              "pageSize": 5000, # Not supported, but should be very big for the moment
              "maxCalls": 5000 # Not supported
            }

        """ End test """

        # First we check the data and set default value like google genomics
        if 'variantSetIds' not in data:
            data['variantSetIds'] = []

        if 'variantName' not in data:
            data['variantName'] = ''

        if 'callSetIds' not in data:
            data['callSetIds'] = []

        if 'referenceName' not in data:
            result = {'status':-1,'text':'You need to set a value for the attribute "referenceName".'}
            return Response(result)

        if 'start' not in data:
            data['start'] = 0

        if 'end' not in data:
            data['end'] = 0

        if 'pageToken' not in data:
            data['pageToken'] = ''

        if 'pageSize' not in data:
            data['pageSize'] = 5000

        if 'maxCalls' not in data:
            data['maxCalls'] = 5000

        if len(data['variantSetIds']) == 0 and len(data['callSetIds']) == 0:
            result = {'status':-1,'text':'You need to set a value for the attribute "variantSetIds" or "callSetIds" at least.'}
            return Response(result)

        # We get the keys associated to the json we received
        fc = formatConverters(input_file='stuff.vcf',output_file='stuff.json')
        mapping = fc.getMappingJsonToParquet()

        tableNames = {
            'variantSetIds': mapping['variants.variantSetId'],
            'variantName': mapping['variants.names[]'],
            'callSetIds': mapping['variants.calls[].callSetId'],
            'referenceName': mapping['variants.referenceName'],
            'start': mapping['variants.start'],
            'end': mapping['variants.end'],
            'pageToken': mapping['variants.id'], # Equivalent to rowkey
        }

        # We prepare the query
        where = []

        if len(data['variantSetIds']) > 0:# CF. google doc, "at most one variantSetIds must be provided"
            where.append(tableNames['variantSetIds']+" = '"+data['variantSetIds'][0]+"'")

        if len(data['variantName']) > 0:
            where.append(tableNames['variantName']+" = '"+data['variantName']+"'")

        if len(data['callSetIds']) > 0:
            # TODO
            pass

        where.append(tableNames['referenceName']+" = '"+str(data['referenceName'])+"'")

        if data['start'] > 0 and False:
            where.append(tableNames['start']+" >= "+str(data['start'])+"")

        if data['end'] > 0:
            # TODO: not supported yet
            where.append(tableNames['end']+" < "+str(data['end'])+"")

        if len(data['pageToken']) > 0:
            where.append(tableNames['pageToken']+" >= '"+str(data['pageToken'])+"'")

        query = "SELECT * FROM variants WHERE "+" AND ".join(where)

        # We execute the query on parquet
        query_server = get_query_server_config(name='impala')
        db = dbms.get(request.user, query_server=query_server)
        handle = db.execute_and_wait(hql_query(query), timeout_sec=360.0)
        result_set = []
        last_pk = ''
        if handle:
            raw_data = db.fetch(handle, rows=data['pageSize'] + 1) # + 1 as we want to know there are multiple pages
            columns = raw_data.cols()

            # We need to separate the different variants based on the rowkeys
            good_variants = {}
            number_of_good_variants = 0
            for raw_variant in raw_data.rows():
                interesting_rowkey = raw_variant[0].split('|')
                interesting_rowkey.pop()
                interesting_rowkey = '|'.join(interesting_rowkey)+'|'
                if interesting_rowkey not in good_variants:
                    good_variants[interesting_rowkey] = []
                    number_of_good_variants += 1

                # We map the column names and the list of data for the current row
                mapped_variant = {}
                for i in xrange(len(columns)):
                    mapped_variant[columns[i]] = raw_variant[i]

                # We save the modified variant
                good_variants[interesting_rowkey].append(mapped_variant)

            # We have the different variants correctly separated
            for rowkey_part in good_variants:

                # We generate the data for the variant (we give multiple rows but they are all related to the same variant)
                current_variant = VariantSerializer(request=request, pk=good_variants[rowkey_part][0]['pk'], impala_data=good_variants[rowkey_part])

                # We store the variant (only the data, not the object)
                if len(result_set) < number_of_good_variants:
                    result_set.append(current_variant.data)
                else: # The last row  is used to check if there are still variants to list
                    last_pk = good_variants[rowkey_part][len(good_variants[rowkey_part])-1]['id']
            db.close(handle)

        # We format the results and send them back
        result['variants'] = result_set
        result['nextPageToken'] = last_pk

        return Response(result)

    def highlander_search(self, request):
        """
        Realize an advanced search on the impala data. We receive a query made by Highlander
        so the fields might be very specific.
        :param request:
        :return:
        """

        result = {'status':1,'text':'Everything is alright.'}
        data = request.data

        """ For test only """
        if len(data) == 0:
            data = {
                "method": "SELECT",
                "fields": "count(*)",# list of fields separated like a sql query, by a comma
                "condition": "variants.referenceName = '1'", # list of conditions (WHERE clause) like a sql query
                "limit": 5000,
                "offset": 0,
                "order-by": ""
            }

        """ End test """

        # TODO add verification for the code we receive

        # We get the keys associated to the json we received
        fc = formatConverters(input_file='stuff.vcf',output_file='stuff.json')
        mapping = fc.getMappingJsonToParquet()

        # We prepare the query
        query_data = {}
        query_data['method'] = data['method']
        if query_data['method'] != 'SELECT':
            query_data['method'] = 'SELECT'

        fields = data['fields']
        for json_parameter in mapping:
            fields = fields.replace(json_parameter, mapping[json_parameter])
        query_data['fields'] = fields

        condition = data['condition']
        for json_parameter in mapping:
            condition = condition.replace(json_parameter, mapping[json_parameter])
        if len(condition) > 0:
            query_data['condition'] = "WHERE "+condition
        else:
            query_data['condition'] = ""

        if data['limit'] > 0:
            query_data['limit'] = "LIMIT "+str(data['limit'])
        else:
            query_data['limit'] = ""

        if data['offset'] > 0:
            query_data['offset'] = "OFFSET "+str(data['offset'])
        else:
            query_data['offset'] = ""

        order_by = data['order-by']
        for json_parameter in mapping:
            order_by = order_by.replace(json_parameter, mapping[json_parameter])
        query_data['order-by'] = order_by

        # TODO format the query when it involves specific samples (a little bit tricky to do that...)!

        # We format the final query (which might be invalid but we don't care right now)
        query = query_data['method']+" "+query_data['fields']+" FROM variants "+query_data['condition']+" "+query_data['limit']+" "+query_data['offset']

        f = open('superhello-queries.txt','a')
        f.write(query+'\n')
        f.close()

        # We execute the query on parquet
        query_server = get_query_server_config(name='impala')
        db = dbms.get(request.user, query_server=query_server)
        try:
            handle = db.execute_and_wait(hql_query(query), timeout_sec=360.0)
        except Exception as e:
            result['status'] = 0
            result['text'] = 'Impossible to execute the query.'
            result['detail'] = str(e)
            return Response(result)

        result_set = []
        last_pk = ''
        if handle:
            raw_data = db.fetch(handle, rows=1000000 + 1) # + 1 as we want to know there are multiple pages
            columns = raw_data.cols()

            # We need to separate the different variants based on the rowkeys
            good_variants = {}
            number_of_good_variants = 0
            ints = []
            for raw_variant in raw_data.rows():
                if isinstance(raw_variant[0], (int,long)): # we made a select count(*) so we only got a number, not multiple variants
                    ints.append(raw_variant[0])
                    continue

                interesting_rowkey = raw_variant[0].split('|')
                interesting_rowkey.pop()
                interesting_rowkey = '|'.join(interesting_rowkey)+'|'
                if interesting_rowkey not in good_variants:
                    good_variants[interesting_rowkey] = []
                    number_of_good_variants += 1

                # We map the column names and the list of data for the current row
                mapped_variant = {}
                for i in xrange(len(columns)):
                    mapped_variant[columns[i]] = raw_variant[i]

                # We save the modified variant
                good_variants[interesting_rowkey].append(mapped_variant)

            # If we have a list of ints, we return the result
            if len(ints) > 0:
                result['values'] = ints
                return Response(result)

            # We have the different variants correctly separated
            for rowkey_part in good_variants:

                # We generate the data for the variant (we give multiple rows but they are all related to the same variant)
                current_variant = VariantSerializer(request=request, pk=good_variants[rowkey_part][0]['pk'], impala_data=good_variants[rowkey_part])

                # We store the variant (only the data, not the object)
                if len(result_set) < number_of_good_variants:
                    result_set.append(current_variant.data)
                else: # The last row  is used to check if there are still variants to list
                    last_pk = good_variants[rowkey_part][len(good_variants[rowkey_part])-1]['id']
            db.close(handle)

        # We format the results and send them back
        result['variants'] = result_set
        result['nextPageToken'] = last_pk

        return Response(result)

class CallSetDetail(APIView):

    def get(self, request, pk=-1):
        # Information on a specific callset
        if pk == 'search':
            return self.search(request=request)
        result = {'status':1,'text':'To implement.'}

        return HttpResponse(json.dumps(result), mimetype="application/json")

    def search(self, request):
        result = {'status':1,'text':'To implement.'}

        return HttpResponse(json.dumps(result), mimetype="application/json")
