#!/usr/bin/env python

import json
import logging

from django.core.urlresolvers import reverse
from django.utils.translation import ugettext as _
from django.http import HttpResponse

from beeswax.design import hql_query
from beeswax.server import dbms
from beeswax.server.dbms import get_query_server_config
from impala.models import Dashboard, Controller

#from desktop.lib.django_util import JsonResponse
from desktop.lib.rest.http_client import RestException
from exception import handle_rest_exception

from variants.decorators import api_error_handler

from rest_framework.permissions import IsAuthenticated
from rest_framework import routers, serializers, viewsets
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from serializers import *

LOG = logging.getLogger(__name__)

### debugging
# def current_line():
#     """ Return the current line number """
#     return inspect.currentframe().f_back.f_lineno




"""
    Old code which needs refactoring as it does not use DRF
"""
def fprint(txt):
    """ Print some text in a debug file """
    f = open('/home/cloudera/debug.txt', 'a')
    f.write(str(txt)+"\n")
    f.close()
    return True

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


    def post(self, request, format=None):
        """
            Receive a new vcf to analyze
        :param request:
        :param format:
        :return:
        """
        result = {'status':1,'text':'Everything is alright.'}

        v = VCFSerializer(data=request.data)
        if v.is_valid():
            return Response(result, status=status.HTTP_201_CREATED)

        result = {'status':1,'text':'Something went wrong.'}
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

    def get(self, request, pk=-1):
        # Information on a specific dataset
        result = {'status':1,'text':'Everything is alright.'}
        return Response(result)

    def delete(self, request, pk=-1):
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

class ReadGroupSetDetail(APIView):

    def get(self, request, pk=-1):
        # Information on a specific readgroupset
        result = {'status':1,'text':'Everything is alright.'}
        return Response(result)

class VariantSetDetail(APIView):

    def get(self, request, pk=-1):
        # Information on a specific variantset
        result = {'status':1,'text':'Everything is alright.'}
        return Response(result)

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

    def get(self, request, pk=-1):
        # Information on a specific variant
        result = {'status':1,'text':'Everything is alright.'}

        ## config.yml and reference.yml in CGSCONFIG (see the config file in the root directory of this package)
        ## reading the config.yml file to define the datastructure (looking for HBase with "variant" in names)

        ## reading the reference.yml file to define the link between the API resource and HBase/impala fields
        ## impalaFields =
        ## hbaseFields =

        ## request HBase through Impala with the ID (or row key)
        ##Connexion db
        # query_server = get_query_server_config(name='impala')
        # db = dbms.get(request.user, query_server=query_server)
        # hql = "SELECT ... FROM ..."
        # query = hql_query(hql)
        # handle = db.execute_and_wait(query, timeout_sec=5.0)
        # if handle:
        #     data = db.fetch(handle, rows=1)
        #     result['data'] = list(data.rows())
        #     result['status'] = 1
        #     db.close(handle)

        return Response(result)

    def search(self, request, terms=''):
        # Search for a specific variant
        result = {'status':1,'text':'Everything is alright.'}

        # > Code below NEEDS REFACTORING

        """ Search a variant by some criteria (see doc for more details and if not https://cloud.google.com/genomics/v1beta2/reference/variants/search)

        To test it type in your bash prompt:
        See the documentation

        """
        result = {'status': -1}
        # ## check request
        if request.method != 'POST':
            result['status'] = -1
            result['error'] = "The method should be POST."
            return HttpResponse(json.dumps(result), mimetype="application/json")
            ##return JsonResponse(result)

        criteria_json = request.POST['criteria']
        fprint("============")
        fprint(criteria_json)
        #callsetid = json.loads(criteria)['callSetIds']
        try:
            criteria = json.loads(criteria_json)
        except Exception:
            result['status'] = -1
            result['error'] = "Sorry, an error occured: Impossible to load the criteria. Please check that they are in a JSON format."
            return HttpResponse(json.dumps(result), mimetype="application/json")

        criteria_keys = [str(s) for s in criteria.keys()]
        fprint(criteria['callSetIds'])
        if 'callSetIds' not in criteria_keys:
        #if 'callSetIds' not in request.POST.keys():
            result['status'] = -1
            #result['keys'] = request.POST.keys()
            #result['criteria'] = callsetid
            result['error'] = "Information about the callSetsIds (sample information should be available)."
            return HttpResponse(json.dumps(result), mimetype="application/json")
            ##return JsonResponse(result)

        callsetids = [str(s) for s in criteria['callSetIds']]

        fprint(','.join(callsetids))
        ## getting data from DB
        try:
            query_server = get_query_server_config(name='impala')
            db = dbms.get(request.user, query_server=query_server)
        except Exception:
            result['status'] = 0
            result['error'] = "Sorry, an error occured: Impossible to connect to the db."
            return HttpResponse(json.dumps(result), mimetype="application/json")

        try:
            ## TODO:
            ## - the variant_table should be defined from the config files not as a string as here
            ## - the callsetids should map to a specific field (here readGroupSets_readGroups_info_patientId) in the variant table, this info should be read from the config files, not as a string as here.
            ## the listVars should be read from the config files as well
            variants_table = "jpoullet_1000genomes_1E7rows_bis"
            listVars = ["id","readGroupSets_readGroups_info_patientId"]
            fprint(variants_table)
            #fprint(str(criteria.keys()[0]))
            searchCriteriaList = list()
            for k in criteria.keys():
                fprint(criteria[str(k)])
                if str(k) == 'callSetIds':
                    refvar = "readGroupSets_readGroups_info_patientId" ## TODO: this must be read from the config files
                elif str(k) == 'referenceName':
                    refvar = 'variants_referenceName' ## TODO: this must be read from the config files
                else:
                    pass # TODO: there should be an error when the user chooses some inexisting variable
                fprint(refvar + " in ('" + "','".join([str(s) for s in criteria[str(k)]]) + "')")
                searchCriteriaList.append(refvar + " in ('" + "','".join([str(s) for s in criteria[str(k)]]) + "')")

            searchCriteriaTxt = " AND ".join(searchCriteriaList)
            fprint(searchCriteriaTxt)

            hql = "SELECT " + ",".join(listVars) + " FROM " + variants_table + " WHERE " + searchCriteriaTxt
            #hql = "SELECT " + ",".join(listVars) + " FROM " + variant_table + " WHERE readGroupSets_readGroups_info_patientId IN ('" + "','".join(callsetids) + "')"
            fprint(hql)

        except Exception:
            result['status'] = 0
            result['error'] = "Sorry, an error occured: a syntax error appears in the definition of the criteria. The query could not be built."
            return HttpResponse(json.dumps(result), mimetype="application/json")

        try:
            query = hql_query(hql)
            handle = db.execute_and_wait(query, timeout_sec=5.0)

        except Exception:
            result['status'] = 0
            result['error'] = "The query cannot be performed: %s" % hql
            return HttpResponse(json.dumps(result), mimetype="application/json")

        if handle:
            data = db.fetch(handle)
            ## TODO: rebuild the variant resource such as defined in the API
            ## field parser that would take the config files as input to retrieve the generate back the structured json
            ## results['variants'] = getStructuredJson(list(data.rows))
            result['variants'] = list(data.rows())
            result['status'] = 1
            db.close(handle)

        else:
            result['error'] = 'No result found.'
            return HttpResponse(json.dumps(result), mimetype="application/json")

        return Response(result)

## callsets
@api_error_handler
def callsets_get(request):
    """ Gets a call set by ID
    """

    pass
