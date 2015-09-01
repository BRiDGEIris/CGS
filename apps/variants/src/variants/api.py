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

        data = request.data # For prod

        """ For test only ""
        data = {
          "variantSetIds": ['NA'],
          "variantName": '',
          "callSetIds": [],
          "referenceName": 1,
          "start": 1,
          "end": 0,
          "pageToken": '',
          "pageSize": 30,
          "maxCalls": 30
        }

        "" End test """

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

        if data['start'] > 0:
            where.append(tableNames['start']+" >= "+str(data['start'])+"")

        if data['end'] > 0:
            # TODO: compute length of the reference like in google genomics
            where.append(tableNames['end']+" < "+str(data['end'])+"")

        if len(data['pageToken']) > 0:
            where.append(tableNames['pageToken']+" >= '"+str(data['pageToken'])+"'")

        tmpf = open('superhello.txt','w')
        query = "SELECT * FROM variants WHERE "+" AND ".join(where)
        tmpf.write(query)
        tmpf.close()

        # We execute the query on parquet
        query_server = get_query_server_config(name='impala')
        db = dbms.get(request.user, query_server=query_server)
        handle = db.execute_and_wait(hql_query(query), timeout_sec=360.0)
        result_set = []
        last_pk = ''
        if handle:
            raw_data = db.fetch(handle, rows=data['pageSize'] + 1) # + 1 as we want to know there are multiple pages
            columns = raw_data.cols()
            for raw_variant in raw_data.rows():

                # We map the column names and the list of data for the current row
                mapped_variant = {}
                for i in xrange(len(columns)):
                    mapped_variant[columns[i]] = raw_variant[i]

                # We generate the data for the variant
                current_variant = VariantSerializer(request=request, pk=raw_variant[0], impala_data=mapped_variant)

                # We store the variant (only the data, not the object)
                if len(result_set) < data['pageSize']:
                    result_set.append(current_variant.data)
                else: # The last row  is used to check if there are still variants to list
                    last_pk = current_variant.data['id']
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
