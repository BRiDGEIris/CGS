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


    def post(self, request, filename, current_analysis, from_views=False):
        """
            Receive a new vcf to analyze
        :param request:
        :param format:
        :return:
        """
        result = {'status':1,'text':'Everything is alright.'}

        v = VCFSerializer()
        result = v.post(request=request, filename=filename, current_analysis=current_analysis)
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
        call = VariantCallSerializer(variantcall_data='')

        #return Response(json.dumps({'status':-1,'error':'Variant id invalid or problem while loading the variant.'}))
        return Response(variant.data)

    def post(self, request):
        # Create a new variant
        status = -1

        variant_form = VariantSerializer(data=request.data)
        if variant_form.is_valid():
            try:
                variant_form.post(request)
                status = 1
            except:
                status = 0

        return Response(json.dumps({'status':status}))

    def search(self, request):
        # Search for a specific variant
        result = {'status':1,'text':'Everything is alright.'}

        # > Code below NEEDS REFACTORING

        """ Search a variant by some criteria (see doc for more details and if not https://cloud.google.com/genomics/v1beta2/reference/variants/search )

        To test it type in your bash prompt:
        See the documentation

        """
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
            #Find the location of config.yml (locally then globally)
            local_config_path = os.path.join(ROOT_PATH,'config.yml')
            if not os.path.exists(local_config_path):
                result['status'] = 0
                result['error'] = "The local config file of the variants app could not be found. Please check your installation."
                return HttpResponse(json.dumps(result), mimetype="application/json")
            local_config_file = open(local_config_path)
            global_config_path = yaml.load(local_config_file.read())["CGSCONFIG"]
            local_config_file.close()
            if not os.path.exists(global_config_path):
                result['status'] = 0
                result['error'] = "The global config file could not be found. Please check your installation."
                return HttpResponse(json.dumps(result), mimetype="application/json")
            global_config_file = open(global_config_path)
            global_config = yaml.load(global_config_file)
            
            #Look for the variants_api substructure (general version for any number of substructures)and its "database" element to get variants_table
            api_substructure = [a for substructure in global_config['substructures'] if substructure['name']=='variants_api']
            if len(api_substructure) > 0:
                variants_table= api_substructure[0].get("database",None)
            else:
                result['status'] = 0
                result['error'] = "The config file does not have a database for variants_api. Please check your installation."
                return HttpResponse(json.dumps(result), mimetype="application/json")
            if not variants_table:
                result['status'] = 0
                result['error'] = "The config file does not have a database for variants_api. Please check your installation."
                return HttpResponse(json.dumps(result), mimetype="application/json")
            global_config_file.close()
            
            fprint(variants_table)
            
            #Look for the variant_api.yml config file to load the list of fields in the SELECT statement of the query
            api_config_path = os.path.join(os.path.dirname(global_config_path),'sourceFiles/variants_api.yml')
            if not os.path.exists(api_config_path):
                result['status'] = 0
                result['error'] = "The api config file could not be found. Please check your installation."
                return HttpResponse(json.dumps(result), mimetype="application/json")
            api_config_file = open(api_config_path)
            api_config = yaml.load(api_config_file)
            api_config_file.close()
            for key in api_config.keys():
                if key == 'variants': #TODO: Verify if others are necessary or if we only keep the variables associated with variants
                    for var in api_config[key]['columns'].keys():
                        listVars.append(key+'_'+var)
            listVars = [var.replace('.','_') for var in listVars]
            
            #Look for the fields.yml file for correspondences between criteria and fields in the database
            fields_config_path = os.path.join(os.path.dirname(global_config_path),'sourceFiles/fields.yml')
            if not os.path.exists(api_config_path):
                result['status'] = 0
                result['error'] = "The fields config file could not be found. Please check your installation."
                return HttpResponse(json.dumps(result), mimetype="application/json")
            fields_config_file = open(fields_config_path)
            fields_config = yaml.load(fields_config_file)
            fields_config_file.close()
            
            #fprint(str(criteria.keys()[0]))
            searchCriteriaList = list()
            for k in criteria.keys():
                fprint(criteria[k])
                reffield = fields_config.get(str(k),None)
                if reffield:
                    refvar = reffield["substructures"].get("variants_api",None)
                else:
                    result['status'] = -1
                    result['error'] = "Criterion field "+ k + " not found."
                    return HttpResponse(json.dumps(result), mimetype="application/json")
                if refvar:
                    refvar = refvar.replace('.','_')
                else:
                    result['status'] = -1
                    result['error'] = "Criterion field "+ k + " is not an api field."
                    return HttpResponse(json.dumps(result), mimetype="application/json")
                fprint(refvar + " in ('" + "','".join([str(s) for s in criteria[k]]) + "')")
                searchCriteriaList.append(refvar + " in ('" + "','".join([str(s) for s in criteria[k]]) + "')")

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
            var_result = []
            #Reverse dictionary of the fields config for the variants_api substructure 
            api_reverse_fields = {}
            for field in fields_config.keys():
                if fields_config[field]["substructures"].get("variants_api",None):
                    api_reverse_fields[fields_config[field]["substructures"]["variants_api"].replace(".","_")]= field 
            columns = [columns.append(api_reverse_fields['col']) for col in data.columns()]
            for row in data.rows():
                row_dict = {}
                for index in range(len(row)):
                    row_dict[columns[index]] = row[index]
                var_result.append(row_dict)
            
            result['variants'] = json.dumps(var_result)
            result['status'] = 1
            db.close(handle)

        else:
            result['error'] = 'No result found.'
            return HttpResponse(json.dumps(result), mimetype="application/json")

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
