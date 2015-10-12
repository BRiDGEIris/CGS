import os,sys
import json, ast
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../cgsdatatools'))
if not path in sys.path:
    sys.path.insert(1, path)
del path
import string
import collections
import shutil
import vcf
import os
import avro
from avro.io import DatumReader, DatumWriter
from avro.datafile import DataFileReader, DataFileWriter

from beeswax.design import hql_query
from beeswax.server import dbms
from beeswax.server.dbms import get_query_server_config
from subprocess import *
import json
import time
from hbase.api import HbaseApi
from django.db import connections
from django.db import connection
import settings

class formatConverters(object):
    previous_gonl = {}
    previous_dbn = {}
    previous_dbsnv = {}
    previous_chr = {}

    """
    Format converters

    Possible formats:
        * input: vcf, vcf.gz (gzipped), json, jsonflat
        * output: json, jsonflat, avro, parquet
        * additional file: avsc (avro schema)  
    """
    def __init__(self,
                 input_file,
                 output_file,
                 input_type = "",
                 output_type = "",
                 converting_method = "default"):
        
        self.input_file = input_file
        self.output_file = output_file
        if input_type == "":
            sp = input_file.split('.')
            self.input_type = sp[len(sp)-1]
            if self.input_type == 'gz':
                self.input_type = sp[len(sp)-2] + sp[len(sp)-1]
        else:
            self.input_type = input_type
            
        if output_type == "":
            sp = output_file.split('.')
            self.output_type = sp[len(sp)-1]
        else:
            self.output_type = output_type
    
        self.converting_method = converting_method

    def show(self):
        print("""
        Input file: %s
        Output file: %s
        Converting method""" % (self.input_type, self.output_type, self.converting_method))

    def convertVcfToFlatJson(self, request, organization="ulb", analysis="0", initial_file="no-file.vcf"):
        """
            Convert a vcf file to a flat json file
            Check the doc: https://pyvcf.readthedocs.org/en/latest/API.html#vcf-model-call
            Be careful: we do not respect exactly the google genomics structure in our database (but the api respects it)
        """
        if self.input_type not in ['vcf','vcf.gz'] or self.output_type != 'jsonflat':
            msg = "Error: vcf files (possibly gzipped) must be given as input files, and a jsonflat file should be given as output file."
            status = "failed"
            raise ValueError(msg)

        try:
            db_cursor = self.connect_to_db(request)
        except Exception as e:
            db_cursor = None

            infos = []
            for dbname in connections:
                infos.append(dbname)

            tmpf = open('/tmp/cgs_errors.txt','a')
            tmpf.write("Error to access the cgs_annotations database: "+str(e)+" ("+str(infos)+")\n")
            tmpf.close()

        mapping = self.getMappingPyvcfToJson()

        f = open(self.input_file, 'r')
        o = open(self.output_file, 'w')

        list_of_rowkeys = []
        list_of_columns = []
        list_of_samples = []
        vcf_reader = vcf.Reader(f)

        for record in vcf_reader:
            linedics = {}

            previous_alt = ""
            for s in record.samples:

                # The most important thing to start with: check if the current sample has an alternate or not!
                # This is almost mandatory, otherwise you create many samples with no alternate (for 1000genomes
                # you create an enormous amount of data if you don't care about that -_-)
                if not s.is_variant:
                    # We are not fetching a variant, so we can skip the information...
                    continue

                if len(record.ALT) > 2:
                    # For now the code only works with maximum 2 different ALTs (maybe it would be
                    # easy to modify for more, but not tested)
                    continue

                for alt_iteration in xrange(0,2):
                    """
                        Complicate to take care of variants structured like "1/2", so we do it like Highlander:
                        1   10177   rs367896724 A   C,T 100 PASS    AC=3,2  GT 0/1 1/2 1/2
                        Becomes:
                        1   10177   rs367896724 A   C 100 PASS    AC=1  GT 0/1 (sample 1)
                        1   10177   rs367896724 A   C 100 PASS    AC=1  GT 0/1 (sample 2)
                        1   10177   rs367896724 A   T 100 PASS    AC=1  GT 0/1 (sample 2)
                        1   10177   rs367896724 A   C 100 PASS    AC=1  GT 0/1 (sample 3)
                        1   10177   rs367896724 A   T 100 PASS    AC=1  GT 0/1 (sample 3)
                        So, sometimes, one raw line of variant might generate multiple lines in the db.
                    """
                    tmp = s.gt_bases.split('|')

                    if tmp[alt_iteration] == record.REF or (alt_iteration == 1 and tmp[0] == tmp[1]):
                        # Nothing to do in that case
                        continue
                    current_alt = tmp[alt_iteration]

                    # We may need to modify some information for the variant if we 'cut' one variant in two
                    if tmp[0] != tmp[1] and tmp[0] != record.REF and tmp[1] != record.REF:
                        # TODO
                        # Modify AC & AF (not needed in fact, well not sure) maybe other things
                        pass

                    rk = current_alt
                    linedics[rk] = {}

                    linedics[rk]['variants.filters[]'] = record.FILTER
                    linedics[rk]['variants.calls[]'] = {'info{}':{},'genotypeLikelihood[]':[],'genotype[]':[]}

                    linedics[rk]['variants.alternateBases[]'] = str(current_alt)
                    linedics[rk]['variants.calls[]']['genotype[]'] = str(current_alt)

                    # We get the common data for all the samples
                    if hasattr(s.data,'DP'):
                        linedics[rk]['variants.calls[]']['info{}']['read_depth'] = s.data.DP
                    else:
                        linedics[rk]['variants.calls[]']['info{}']['read_depth'] = "NA"
                    linedics[rk]['variants.variantSetId'] = analysis+'|'+initial_file

                    """ Start annotations """
                    gonl = {}
                    dbn = {}
                    dbsnv = {}
                    if db_cursor is not None:
                        chromosome = record.CHROM
                        position = record.POS
                        reference = record.REF
                        alternate = current_alt

                        gonl = self.annotate_with_gonl(db_cursor, chromosome, position, reference, alternate)
                        dbn = self.annotate_with_dbn(db_cursor, chromosome, position, reference, alternate)
                        dbsnv = self.annotate_with_dbsnv(db_cursor, chromosome, position, reference, alternate)
                        #chr = self.annotate_with_chr(db_cursor, linedic['variants.referenceName'], linedic['variants.start'], linedic['variants.referenceBases'], linedic['variants.calls[]']['genotype[]'])
                    """ End annotations """

                    # Now we map each additional data depending on the configuration
                    for pyvcf_parameter in mapping:

                        if mapping[pyvcf_parameter] == 'variants.calls[]' or mapping[pyvcf_parameter] == 'variants.calls[].info{}':
                            continue

                        # We detect how to take the information from PyVCF, then we take it
                        if pyvcf_parameter == 'Record.ALT':
                            value = '|'.join([str(a) for a in record.ALT])
                        elif pyvcf_parameter.startswith('Record.INFO'):
                            field = pyvcf_parameter.split('.')
                            try:
                                value = record.INFO[field.pop()]
                            except:
                                value = ""
                        elif pyvcf_parameter.startswith('Record'):
                            field = pyvcf_parameter.split('.')
                            try:
                                value = str(getattr(record, field.pop()))
                            except:
                                value = ""

                            if value is None:
                                value = ""
                        elif pyvcf_parameter.startswith('Call'):
                            field = pyvcf_parameter.split('.')
                            try:
                                value = str(getattr(s, field.pop()))
                            except:
                                value = ""

                            if value is None:
                                value = ""
                        elif pyvcf_parameter.startswith('gonl.'):
                            field = pyvcf_parameter.split('.').pop()

                            if field in gonl:
                                value = gonl[field]
                            else:
                                value = ""
                        elif pyvcf_parameter.startswith('dbnsfp.'):
                            field = pyvcf_parameter.split('.').pop()

                            if field in dbn:
                                value = dbn[field]
                            else:
                                value = ""
                        elif pyvcf_parameter.startswith('dbsnv.'):
                            field = pyvcf_parameter.split('.').pop()

                            if field in dbsnv:
                                value = dbsnv[field]
                            else:
                                value = ""
                        else:
                            value = ""
                            print("Parameter '"+pyvcf_parameter+"' not supported.")

                        # Now we decide how to store the information in json
                        if mapping[pyvcf_parameter] == 'variants.alternateBases[]':
                            pass
                        elif mapping[pyvcf_parameter] == 'variants.calls[].genotype[]':
                            linedics[rk]['variants.calls[]']['genotype[]'] = current_alt
                        elif mapping[pyvcf_parameter].startswith('variants.calls[].info{}'):
                            tmp = mapping[pyvcf_parameter].split('variants.calls[].info{}.')
                            linedics[rk]['variants.calls[]']['info{}'][tmp[1]] = value
                        elif mapping[pyvcf_parameter].startswith('variants.calls[].'):
                            tmp = mapping[pyvcf_parameter].split('variants.calls[].')
                            if tmp[1] != 'info{}':
                                linedics[rk]['variants.calls[]'][tmp[1]] = value
                        else:
                            linedics[rk][mapping[pyvcf_parameter]] = value

                    # Some information we need to compute ourselves
                    linedics[rk]['variants.calls[]']['callSetId'] = s.sample
                    linedics[rk]['variants.calls[]']['callSetName'] = s.sample

                    # We have to add the sample id for the current sample
                    linedics[rk]['variants.calls[]']['info{}']['sampleId'] = s.sample

                    if linedics[rk]['variants.calls[]']['info{}']['sampleId'] not in list_of_samples:
                        list_of_samples.append(linedics[rk]['variants.calls[]']['info{}']['sampleId'])

                    # Before writing the data to the json flat, we need to format them according to the avsc file
                    # and the current sample id
                    rowkey = organization + '|' + analysis + '|' + linedics[rk]['variants.referenceName'] + '|' + linedics[rk]['variants.start'] + '|' + linedics[rk]['variants.referenceBases'] + '|' + linedics[rk]['variants.calls[]']['genotype[]']
                    linedics[rk]['variants.id'] = rowkey
                    linedics[rk]['variants.calls[].'+s.sample] = json.dumps(linedics[rk]['variants.calls[]'])
                    del linedics[rk]['variants.calls[]']
                    if rowkey not in list_of_rowkeys:
                        list_of_rowkeys.append(rowkey)

            # We do not do a json.dumps for other columns than variants.calls[], except for variants.info{}
            for rk in linedics:
                linedic = linedics[rk]
                for jsonkey in linedic:
                    if type(linedic[jsonkey]) is list:
                        if len(linedic[jsonkey]) > 0 :
                            linedic[jsonkey] = '|'.join(linedic[jsonkey])
                    elif type(linedic[jsonkey]) is dict:
                        linedic[jsonkey] = json.dumps(linedic[jsonkey])

                    if jsonkey not in list_of_columns:
                        list_of_columns.append(jsonkey)

                o.write(json.dumps(linedic, ensure_ascii=False) + "\n")
        o.close()
        f.close()

        status = "succeeded"
        return status, list_of_columns, list_of_samples, list_of_rowkeys

    def annotate_with_gonl(self, cursor, chromosome, position, reference, alternate):
        if len(self.previous_gonl) > 0 and self.previous_gonl['chr'] == chromosome and self.previous_gonl['position'] == position and self.previous_gonl['reference'] == reference and self.previous_gonl['alternative'] == alternate:
            return self.previous_gonl

        try:
            if chromosome == 'X' or chromosome == 'Y':
                cursor.execute('SELECT * FROM gonl_chr'+str(chromosome)+' WHERE pos='+str(position)+' AND ref="'+reference+'" AND alt="'+alternate+'"')
            else:
                cursor.execute('SELECT * FROM gonl_chr'+str(chromosome)+' WHERE pos='+str(position)+' AND reference="'+reference+'" AND alternative="'+alternate+'"')
            self.previous_gonl = self.dictfetchall(cursor)
            self.previous_gonl = self.previous_gonl[0]
        except Exception as e:
            tmpf = open('/tmp/cgs_errors.txt','a')
            tmpf.write("Error to get annotations from gonl: "+str(e)+"\n")
            tmpf.close()
            self.previous_gonl = {}

        return self.previous_gonl

    def annotate_with_dbn(self, cursor, chromosome, position, reference, alternate):
        if len(self.previous_dbn) > 0 and self.previous_dbn['chr'] == chromosome and self.previous_dbn['position'] == position and self.previous_dbn['alternative'] == alternate:
            return self.previous_dbn

        try:
            cursor.execute('SELECT * FROM dbnsfp_chr'+str(chromosome)+' WHERE pos='+str(position)+' AND alt="'+alternate+'"')
            self.previous_dbn = self.dictfetchall(cursor)
            self.previous_dbn = self.previous_dbn[0]
        except Exception as e:
            tmpf = open('/tmp/cgs_errors.txt','a')
            tmpf.write("Error to get annotations from dbnsfp: "+str(e)+"\n")
            tmpf.close()
            self.previous_dbn = {}

        return self.previous_dbn

    def annotate_with_dbsnv(self, cursor, chromosome, position, reference, alternate):
        if len(self.previous_dbsnv) > 0 and self.previous_dbsnv['chr'] == chromosome and self.previous_dbsnv['position'] == position and self.previous_dbsnv['alternative'] == alternate:
            return self.previous_dbsnv
        try:
            cursor.execute('SELECT * FROM dbsnv_chr'+str(chromosome)+' WHERE pos='+str(position)+' AND alt="'+alternate+'"')
            self.previous_dbsnv = self.dictfetchall(cursor)
            self.previous_dbsnv = self.previous_dbsnv[0]
        except Exception as e:
            tmpf = open('/tmp/cgs_errors.txt','a')
            tmpf.write("Error to get annotations from dbsnv: "+str(e)+"\n")
            tmpf.close()
            self.previous_dbsnv = {}

        return self.previous_dbsnv

    def annotate_with_chr(self, cursor, chromosome, position, reference, alternate):
        return self.dictfetchall(cursor.execute('SELECT * FROM chr_chr'+str(chromosome)+' WHERE pos='+str(position)+' AND alternative="'+alternate+'"'))

    def convertFlatJsonToHbase(self):
        """
            Convert a flat json file to an hbase json file. It's mostly a key mapping so nothing big
        """

        # 1st: we take the json to hbase information
        mapping = self.getMapping()

        json_to_hbase = {}
        types = {}
        for key in mapping:
            json_to_hbase[mapping[key]['json']] = mapping[key]['hbase'].replace('.',':')
            types[mapping[key]['json']] = mapping[key]['type']

        # 2nd: we create a temporary file in which we will save each future line for HBase
        f = open(self.input_file, 'r')
        o = open(self.output_file, 'w')

        for json_line in f:
            variant = json.loads(json_line)

            output_line = {}
            output_line['pk'] = variant['variants.id']
            output_line['rowkey'] = variant['variants.id']
            for attribute in variant:

                if attribute.startswith('variants.calls[]'):
                    # We generate the table name based on the 'sampleId' and the 'id' field (containing the information on the current analysis)
                    call_info = json.loads(variant[attribute])
                    hbase_key = 'I:CALL_'+call_info['info{}']['sampleId']
                else:
                    hbase_key = json_to_hbase[attribute]

                if attribute in types:
                    if types[attribute] == 'int':
                        try:
                            output_line[hbase_key] = int(variant[attribute])
                        except:
                            output_line[hbase_key] = 0
                    elif types[attribute] == 'float' or types[attribute] == 'double':
                        try:
                            output_line[hbase_key] = float(variant[attribute])
                        except:
                            output_line[hbase_key] = 0.0
                    elif types[attribute] == 'boolean':
                        output_line[hbase_key] = (variant[attribute] == "1" or variant[attribute] == 1 or variant[attribute] == True or variant[attribute] == "true" or variant[attribute] == "True")
                    else:
                        output_line[hbase_key] = str(variant[attribute])
                else:
                    output_line[hbase_key] = str(variant[attribute])

            # We generate the line
            o.write(json.dumps(output_line)+'\n')
        f.close()
        o.close()

        status = "succeeded"
        return status

    def convertHbaseToAvro(self,avscFile = "", add_default=True, modify=True):
        """
            Convert an hbase json file to an avro file using AVSC for making the conversion
            http://avro.apache.org/docs/1.7.6/gettingstartedpython.html
        """

        with open(avscFile,'r') as content_file:
            avro_schema = json.loads(content_file.read())
        columns_lookup = {}
        for field in avro_schema['fields']:
            if 'default' in field:
                columns_lookup[field['name']] = field['default']
            else:
                columns_lookup[field['name']] = 'null'

        status = "failed"
        if avscFile == "":
            msg = "This feature is not yet implemented. Please provide an AVRO schema file (.avsc)."
            raise ValueError(msg)
        else:
            schema = avro.schema.parse(open(avscFile).read())
            writer = DataFileWriter(open(self.output_file, "w"), DatumWriter(), schema)
            h = open(self.input_file)
            i = 0
            st = time.time()
            lines = []
            while 1: ## reading line per line in the flat json file and write them in the AVRO format
                line = h.readline()
                if not line:
                    break
                ls = line.strip()
                data = json.loads(ls)

                if modify is True:
                    # We need to replace the ';' in the file to an '_'
                    modified_data = {}
                    for key in data:
                        modified_data[key.replace(':','_')] = data[key]
                    data = modified_data

                i += 1
                if i % 100 == 0:
                    tmpf = open('/tmp/cgs_superhello.txt','a')
                    tmpf.write('Converter for line '+str(i)+': '+str(time.time()-st)+' > len dict: '+str(len(data))+'\n')
                    tmpf.close()
                # We finally write the avro file
                writer.append(data)
                #supertmp.write(json.dumps(data)+'\n')
            h.close()
            writer.close()
            status = "succeeded"
        return(status)

    def getMappingJsonToText(self):
        # Return the mapping 'json_parameter' > 'order_in_text_file'

        mapping = self.getMapping()

        new_mapping = {}
        i = 0
        for key in mapping:
            new_mapping[mapping[key]['json']] = i
            i += 1

        return new_mapping

    def getMappingPyvcfToText(self):
        # Return the mapping 'pyvcf_parameter' > 'order_in_text_file'

        mapping = self.getMapping()

        new_mapping = {}
        i = 0
        for key in mapping:
            new_mapping[key] = i
            i += 1

        return new_mapping

    def getMappingPyvcfToJson(self):
        # Return the mapping PyVCF to JSON
        mapping = self.getMapping()

        new_mapping = {}
        for key in mapping:
            new_mapping[key] = mapping[key]['json']

        return new_mapping

    def getMappingJsonToHBase(self):
        # Return the mapping Json to HBase
        mapping = self.getMapping()

        new_mapping = {}
        for key in mapping:
            new_mapping[mapping[key]['json']] = mapping[key]['hbase']

        return new_mapping

    def getMappingJsonToParquet(self):
        # Return the mapping Json to Parquet field (we don't want to have the order for parquet, just the column names)
        mapping = self.getMapping()

        new_mapping = {}
        for key in mapping:
            new_mapping[mapping[key]['json']] = str(mapping[key]['hbase'].replace('.','_')).lower()

        return new_mapping

    def getMappingHighlanderToParquet(self):
        # Return the mapping Highlander to Parquet field (we don't want to have the order for parquet, just the column names)
        mapping = self.getMapping()

        new_mapping = {}
        for key in mapping:
            if len(mapping[key]['highlander']) > 0:
                new_mapping[mapping[key]['highlander']] = str(mapping[key]['hbase'].replace('.','_')).lower()

        return new_mapping

    def getMapping(self):
        # Return the mapping between PyVCF (or alternate db), JSON, HBase and Parquet (parquet position only)
        # Sometimes there is nothing in PyVCF to give information for a specific file created by ourselves.
        # DO NOT change the 'json' fields...

        mapping = {
            'Record.CHROM':{'json':'variants.referenceName','hbase':'R.C','highlander':'chr','type':'string'},
            'Record.POS':{'json':'variants.start','hbase':'R.P','highlander':'pos','type':'int'},
            'Record.REF':{'json':'variants.referenceBases','hbase':'R.REF','highlander':'reference','type':'string'},
            'Record.ALT':{'json':'variants.alternateBases[]','hbase':'R.ALT','highlander':'alternative','type':'list'},
            'Record.FILTER':{'json':'variants.filters[]','hbase':'R.FILTER','highlander':'','type':'list'},
            'Record.QUAL':{'json':'variants.quality','hbase':'R.QUAL','highlander':'confidence','type':'float'},
            'Record.INFO.QD':{'json':'variants.info.confidence_by_depth','hbase':'I.QD','highlander':'variant_confidence_by_depth','type':'float'},
            'Record.INFO.SB':{'json':'variants.strand_bias','hbase':'I.SB','highlander':'strand_bias','type':'float'},
            'Record.INFO.AD':{'json':'variants.calls[].info.confidence_by_depth','hbase':'F.AD','highlander':'','type':'string'},
            'Call.sample':{'json':'readGroupSets.readGroups.sampleID','hbase':'R.SI','highlander':'','type':'string'},

            'manual1':{'json':'variants.variantSetId','hbase':'R.VSI','highlander':'','type':'string'},
            'todefine2':{'json':'variants.id','hbase':'R.ID','highlander':'id','type':'string'}, # Ok
            'Call.sample2':{'json':'variants.names[]','hbase':'R.NAMES','highlander':'','type':'list'},
            'todefine4':{'json':'variants.created','hbase':'R.CREATED','highlander':'','type':'int'},
            'todefine5':{'json':'variants.end','hbase':'R.PEND','highlander':'','type':'int'},
            'todefine6':{'json':'variants.info{}','hbase':'R.INFO','highlander':'','type':'dict'},
            'todefine7':{'json':'variants.calls[]','hbase':'R.CALLS','highlander':'','type':'list'},
            'manual2':{'json':'variants.calls[].callSetId','hbase':'R.CALLS_ID','highlander':'','type':'string'},
            'manual3':{'json':'variants.calls[].callSetName','hbase':'R.CALLS_NAME','highlander':'','type':'string'},
            'Call.gt_bases':{'json':'variants.calls[].genotype[]','hbase':'R.CALLS_GT','highlander':'','type':'list'},
            'Call.phased':{'json':'variants.calls[].phaseset','hbase':'R.CALLS_PS','highlander':'','type':'string'},
            'todefine12':{'json':'variants.calls[].genotypeLikelihood[]','hbase':'R.CALLS_LHOOD','highlander':'','type':'list'},
            'todefine13':{'json':'variants.calls[].info{}','hbase':'R.CALLS_INFO','highlander':'','type':'dict'},

            'o1':{'json':'variants.info{}.change_type','hbase':'I.CT','highlander':'change_type','type':'string'},
            'o2':{'json':'variants.info{}.cdna_length','hbase':'I.CDNAL','highlander':'cdna_length','type':'int'},
            'o3':{'json':'variants.info{}.gene_symbol','hbase':'R.GS','highlander':'gene_symbol','type':'string'},
            'o4':{'json':'variants.info{}.gene_ensembl','hbase':'R.GIE','highlander':'gene_ensembl','type':'string'},
            'o5':{'json':'variants.info{}.number_genes','hbase':'I.NG','highlander':'num_genes','type':'int'},
            'o6':{'json':'variants.info{}.biotype','hbase':'I.BIOT','highlander':'biotype','type':'string'},
            'o7':{'json':'variants.info{}.transcript_ensembl','hbase':'I.TRE','highlander':'transcript_ensembl','type':'string'},
            'o7b':{'json':'variants.info{}.transcript_uniprot_id','hbase':'I.TRUID','highlander':'transcript_uniprot_id','type':'string'},
            'o8':{'json':'variants.info{}.transcript_uniprot_acc','hbase':'I.TRUAC','highlander':'transcript_uniprot_acc','type':'string'},
            'o9':{'json':'variants.info{}.transcript_refseq_prot','hbase':'I.TRRP','highlander':'transcript_refseq_prot','type':'string'},
            'o10':{'json':'variants.info{}.transcript_refseq_mrna','hbase':'I.TRRM','highlander':'transcript_refseq_mrna','type':'string'},
            'Record.ID':{'json':'variants.info{}.dbnsp_id','hbase':'I.DBSNP137','highlander':'dbsnp_id_137','type':'string'},
            'o12':{'json':'variants.info{}.unisnp_id','hbase':'I.UNID','highlander':'unisnp_ids','type':'string'},
            'o13':{'json':'variants.info{}.exon_intron_total','hbase':'I.EIT','highlander':'exon_intron_total','type':'int'},
            'Record.INFO.HRun':{'json':'variants.info{}.largest_homopolymer','hbase':'I.HR','highlander':'largest_homopolymer','type':'int'},
            'Record.INFO.DP':{'json':'variants.calls[].info{}.read_depth','hbase':'F.DPF','highlander':'read_depth','type':'int'},
            'Record.INFO.MQ0':{'json':'variants.info{}.mapping_quality_zero_read','hbase':'I.MQ0','highlander':'mapping_quality_zero_reads','type':'float'},
            'Record.INFO.DS':{'json':'variants.info{}.downsampled','hbase':'I.DS','highlander':'downsampled','type':'boolean'},
            'Record.INFO.AN':{'json':'variants.info{}.allele_num','hbase':'I.AN','highlander':'allele_num','type':'int'},
            'o21':{'json':'variants.calls[].info{}.confidence_by_depth_ref','hbase':'F.ADREF','highlander':'allelic_depth_ref','type':'int'},
            'o22':{'json':'variants.calls[].info{}.confidence_by_depth_alt','hbase':'F.ADALT','highlander':'allelic_depth_alt','type':'int'},
            'o23':{'json':'variants.calls[].info{}.allelic_depth_proportion_ref','hbase':'F.ADPR','highlander':'allelic_depth_proportion_ref','type':'float'},
            'o24':{'json':'variants.calls[].info{}.allelic_depth_proportion_alt','hbase':'F.ADPA','highlander':'allelic_depth_proportion_alt','type':'float'},
            'o25':{'json':'variants.calls[].info{}.zygosity','hbase':'F.ZYG','highlander':'zygosity','type':'string'},
            'o26':{'json':'variants.calls[].info{}.genotype_quality','hbase':'F.GQ','highlander':'genotype_quality','type':'float'},
            'o27':{'json':'variants.calls[].info{}.genotype_likelihood_hom_ref','hbase':'F.GLHR','highlander':'genotype_likelihood_hom_ref','type':'float'},
            'o28':{'json':'variants.calls[].info{}.genotype_likelihood_het','hbase':'F.GLH','highlander':'genotype_likelihood_het','type':'float'},
            'o29':{'json':'variants.calls[].info{}.genotype_likelihood_hom_alt','hbase':'F.GLHA','highlander':'genotype_likelihood_hom_alt','type':'float'},
            'o30':{'json':'variants.info{}.snpeff_effect','hbase':'I.SNPE','highlander':'snpeff_effect','type':'string'},
            'o31':{'json':'variants.info{}.snpeff_impact','hbase':'I.SNPI','highlander':'snpeff_impact','type':'string'},
            'dbnsfp.SIFT_score':{'json':'variants.info{}.sift_score','hbase':'I.SIFTS','highlander':'sift_score','type':'float'},
            'dbnsfp.SIFT_pred':{'json':'variants.info{}.sift_pred','hbase':'I.SIFTP','highlander':'sift_pred','type':'string'},
            'dbnsfp.Polyphen2_HDIV_score':{'json':'variants.info{}.pph2_hdiv_score','hbase':'I.PHS','highlander':'pph2_hdiv_score','type':'double'},
            'dbnsfp.Polyphen2_HDIV_pred':{'json':'variants.info{}.pph2_hdiv_pred','hbase':'I.PHP','highlander':'pph2_hdiv_pred','type':'string'},
            'dbnsfp.Polyphen2_HVAR_score':{'json':'variants.info{}.pph2_hvar_score','hbase':'I.PVS','highlander':'pph2_hvar_score','type':'double'},
            'dbnsfp.Polyphen2_HVAR_pred':{'json':'variants.info{}.pph2_hvar_pred','hbase':'I.PVP','highlander':'pph2_hvar_pred','type':'string'},
            'dbnsfp.LRT_score':{'json':'variants.info{}.lrt_score','hbase':'I.LRTS','highlander':'lrt_score','type':'double'},
            'dbnsfp.LRT_pred':{'json':'variants.info{}.lrt_pred','hbase':'I.LRTP','highlander':'lrt_pred','type':'string'},
            'dbnsfp.MutationTaster_score':{'json':'variants.info{}.mutation_taster_score','hbase':'I.MTS','highlander':'mutation_taster_score','type':'double'},
            'dbnsfp.MutationTaster_pred':{'json':'variants.info{}.mutation_taster_pred','hbase':'I.MTP','highlander':'mutation_taster_pred','type':'string'},
            'dbnsfp.MutationAssessor_score':{'json':'variants.info{}.mutation_assessor_score','hbase':'I.MAS','highlander':'mutation_assessor_score','type':'double'},
            'dbnsfp.MutationAssessor_pred':{'json':'variants.info{}.mutation_assessor_pred','hbase':'I.MAP','highlander':'mutation_assessor_pred','type':'string'},
            'o43':{'json':'variants.info{}.consensus_prediction','hbase':'I.CP','highlander':'consensus_prediction','type':'int'},
            'o44':{'json':'variants.info{}.other_effects','hbase':'I.ANN','highlander':'other_prediction','type':'boolean'},
            'dbnsfp.GERP++_NR':{'json':'variants.info{}.gerp_nr','hbase':'I.GENR','highlander':'gerp_nr','type':'double'},
            'dbnsfp.GERP++_RS':{'json':'variants.info{}.gerp_rs','hbase':'I.GERS','highlander':'gerp_rs','type':'double'},
            'dbnsfp.SiPhy_29way_pi':{'json':'variants.info{}.siphy_29way_pi','hbase':'I.S2PI','highlander':'siphy_29way_pi','type':'string'},
            'dbnsfp.SiPhy_29way_logOdds':{'json':'variants.info{}.siphy_29way_log_odds','hbase':'I.S2LO','highlander':'siphy_29way_log_odds','type':'double'},
            'dbnsfp.1000Gp1_AC':{'json':'variants.info{}.1000G_AC','hbase':'I.AC1000G','highlander':'1000G_AC','type':'int'},
            'dbnsfp.1000Gp1_AF':{'json':'variants.info{}.1000G_AF','hbase':'I.AF1000G','highlander':'1000G_AF','type':'double'},
            'dbnsfp.ESP6500_AA_AF':{'json':'variants.info{}.ESP6500_AA_AF','hbase':'I.EAAF','highlander':'ESP6500_AA_AF','type':'double'},
            'dbnsfp.ESP6500_EA_AF':{'json':'variants.info{}.ESP6500_EA_AF','hbase':'I.EEAF','highlander':'ESP6500_EA_AF','type':'double'},
            'o53':{'json':'variants.info{}.lof_tolerant_or_recessive_gene','hbase':'I.LOF','highlander':'lof_tolerant_or_recessive_gene','type':'string'},
            'o54':{'json':'variants.info{}.rank_sum_test_base_qual','hbase':'I.RBQ','highlander':'rank_sum_test_base_qual','type':'double'},
            'o55':{'json':'variants.info{}.rank_sum_test_read_mapping_qual','hbase':'I.RRMQ','highlander':'rank_sum_test_read_mapping_qual','type':'double'},
            'o56':{'json':'variants.info{}.rank_sum_test_read_pos_bias','hbase':'I.RPB','highlander':'rank_sum_test_read_pos_bias','type':'double'},
            'o57':{'json':'variants.info{}.haplotype_score','hbase':'I.HS','highlander':'haplotype_score','type':'double'},
            'o58':{'json':'variants.info{}.found_in_exomes_haplotype_caller','hbase':'I.FEHC','highlander':'found_in_exomes_haplotype_caller','type':'boolean'},
            'o59':{'json':'variants.info{}.found_in_exomes_lifescope','hbase':'I.FEL','highlander':'found_in_exomes_lifescope','type':'boolean'},
            'o60':{'json':'variants.info{}.found_in_genomes_haplotype_caller','hbase':'I.FGHC','highlander':'found_in_genomes_haplotype_caller','type':'boolean'},
            'o61':{'json':'variants.info{}.found_in_panels_haplotype_caller','hbase':'I.FPHC','highlander':'found_in_panels_haplotype_caller','type':'boolean'},
            'o62':{'json':'variants.info{}.check_insilico','hbase':'I.CI','highlander':'check_insilico','type':'int'},
            'o63':{'json':'variants.info{}.check_insilico_username','hbase':'I.CIU','highlander':'check_insilico_username','type':'string'},
            'o64':{'json':'variants.info{}.check_validated_change','hbase':'I.CVC','highlander':'check_validated_change','type':'int'},
            'o65':{'json':'variants.info{}.check_validated_change_username','hbase':'I.CVCU','highlander':'check_validated_change_username','type':'string'},
            'o66':{'json':'variants.info{}.check_somatic_change','hbase':'I.CSC','highlander':'check_somatic_change','type':'int'},
            'o67':{'json':'variants.info{}.check_somatic_change_username','hbase':'I.CSCU','highlander':'check_somatic_change_username','type':'string'},
            'o68':{'json':'variants.info{}.public_comments','hbase':'I.PC','highlander':'public_comments','type':'string'},
            'o70':{'json':'variants.info{}.insert_date','hbase':'I.ID','highlander':'insert_date','type':'int'},
            'o71':{'json':'variants.info{}.fisher_strand_bias','hbase':'I.FS','highlander':'fisher_strand_bias','type':'float'},
            'o72':{'json':'variants.info{}.mapping_quality','hbase':'I.MQ','highlander':'mapping_quality','type':'float'},
            'o73':{'json':'variants.info{}.mle_allele_count','hbase':'I.MLC','highlander':'mle_allele_count','type':'int'},
            'o74':{'json':'variants.info{}.mle_allele_frequency','hbase':'I.MLF','highlander':'mle_allele_frequency','type':'float'},
            'o75':{'json':'variants.info{}.short_tandem_repeat','hbase':'I.STR','highlander':'short_tandem_repeat','type':'boolean'},
            'o76':{'json':'variants.info{}.repeat_unit','hbase':'I.RU','highlander':'repeat_unit','type':'string'},
            'o77':{'json':'variants.info{}.repeat_number_ref','hbase':'I.RPAR','highlander':'repeat_number_ref','type':'int'},
            'o78':{'json':'variants.info{}.repeat_number_alt','hbase':'I.RPAA','highlander':'repeat_number_alt','type':'int'},
            'dbnsfp.FATHMM_score':{'json':'variants.info{}.fathmm_score','hbase':'I.FAS','highlander':'fathmm_score','type':'float'},
            'dbnsfp.FATHMM_pred':{'json':'variants.info{}.fathmm_pred','hbase':'I.FAP','highlander':'fathmm_pred','type':'string'},
            'dbnsfp.MetaSVM_rankscore':{'json':'variants.info{}.aggregation_score_radial_svm','hbase':'I.ASRS','highlander':'aggregation_score_radial_svm','type':'double'},
            'dbnsfp.MetaSVM_pred':{'json':'variants.info{}.aggregation_pred_radial_svm','hbase':'I.APRS','highlander':'aggregation_pred_radial_svm','type':'string'},
            'dbnsfp.MetaLR_score':{'json':'variants.info{}.aggregation_score_lr','hbase':'I.ASL','highlander':'aggregation_score_lr','type':'double'},
            'dbnsfp.MetaLR_pred':{'json':'variants.info{}.aggregation_pred_lr','hbase':'I.APL','highlander':'aggregation_pred_lr','type':'string'},
            'dbnsfp.Reliability_index':{'json':'variants.info{}.reliability_index','hbase':'I.RIN','highlander':'reliability_index','type':'int'},
            'gonl.gonl_ac':{'json':'variants.info{}.gonl_ac','hbase':'I.GONLAC','highlander':'gonl_ac','type':'int'},
            'gonl.gonl_af':{'json':'variants.info{}.gonl_af','hbase':'I.GONLAF','highlander':'gonl_af','type':'double'},
            'o88':{'json':'variants.info{}.found_in_crap','hbase':'I.FIC','highlander':'found_in_crap','type':'boolean'},
            'o89':{'json':'variants.info{}.hgvs_dna','hbase':'I.HGD','highlander':'hgvs_dna','type':'string'},
            'o90':{'json':'variants.info{}.hgvs_protein','hbase':'I.HGP','highlander':'hgvs_protein','type':'string'},
            'o91':{'json':'variants.info{}.cds_length','hbase':'I.CDL','highlander':'cds_length','type':'int'},
            'o92':{'json':'variants.info{}.cds_pos','hbase':'I.CDP','highlander':'cds_pos','type':'int'},
            'o93':{'json':'variants.info{}.cdna_pos','hbase':'I.CDAP','highlander':'cdna_pos','type':'int'},
            'o94':{'json':'variants.info{}.exon_intron_rank','hbase':'I.EXIR','highlander':'exon_intron_rank','type':'int'},
            'dbnsfp.phyloP46way_primate':{'json':'variants.info{}.phyloP46way_primate','hbase':'I.PHPR','highlander':'phyloP46way_primate','type':'double'},
            'o96':{'json':'variants.info{}.evaluation','hbase':'I.EV','highlander':'evaluation','type':'int'},
            'o97':{'json':'variants.info{}.evaluation_username','hbase':'I.EVU','highlander':'evaluation_username','type':'string'},
            'o98':{'json':'variants.info{}.evaluation_comments','hbase':'I.EVC','highlander':'evaluation_comments','type':'string'},
            'o99':{'json':'variants.info{}.history','hbase':'I.HIST','highlander':'history','type':'string'},
            'o100':{'json':'variants.info{}.check_segregation','hbase':'I.CS','highlander':'check_segregation','type':'string'},
            'o101':{'json':'variants.info{}.check_segregation_username','hbase':'I.CSU','highlander':'check_segregation_username','type':'string'},
            'o102':{'json':'variants.info{}.found_in_panels_torrent_caller','hbase':'I.FPTC','highlander':'found_in_panels_torrent_caller','type':'boolean'},
            'dbnsfp.COSMIC_ID':{'json':'variants.info{}.cosmic_id','hbase':'I.COID','highlander':'cosmic_id','type':'string'},
            'dbnsfp.COSMIC_CNT':{'json':'variants.info{}.cosmic_count','hbase':'I.COCO','highlander':'cosmic_count','type':'int'},
            'dbsnv.is_scSNV_RefSeq':{'json':'variants.info{}.is_scSNV_RefSeq','hbase':'I.ISREF','highlander':'is_scSNV_RefSeq','type':'boolean'},
            'dbsnv.is_scSNV_Ensembl':{'json':'variants.info{}.is_scSNV_Ensembl','hbase':'I.ISEN','highlander':'is_scSNV_Ensembl','type':'boolean'},
            'dbsnv.ada_score':{'json':'variants.info{}.splicing_ada_score','hbase':'I.SPAS','highlander':'splicing_ada_score','type':'double'},
            'dbnsfp.ARIC5606_EA_AC':{'json':'variants.info{}.ARIC5606_EA_AC','hbase':'I.AEAC','highlander':'ARIC5606_EA_AC','type':'int'},
            'dbnsfp.ARIC5606_EA_AF':{'json':'variants.info{}.ARIC5606_EA_AF','hbase':'I.AEAF','highlander':'ARIC5606_EA_AF','type':'double'},
            'dbnsfp.clinvar_rs':{'json':'variants.info{}.clinvar_rs','hbase':'I.CLRS','highlander':'clinvar_rs','type':'string'},
            'dbnsfp.clinvar_clnsig':{'json':'variants.info{}.clinvar_clnsig','hbase':'I.CLCL','highlander':'clinvar_clnsig','type':'string'},
            'dbnsfp.clinvar_clnsig':{'json':'variants.info{}.clinvar_trait','hbase':'I.CLTR','highlander':'clinvar_trait','type':'string'},
            'dbnsfp.phastCons46way_primate':{'json':'variants.info{}.phastCons46way_primate','hbase':'I.PHAPR','highlander':'phastCons46way_primate','type':'double'},
            'dbnsfp.phastCons46way_placental':{'json':'variants.info{}.phastCons46way_placental','hbase':'I.PHAPL','highlander':'phastCons46way_placental','type':'double'},
            'dbnsfp.phastCons100way_vertebrate':{'json':'variants.info{}.phastCons100way_vertebrate','hbase':'I.PHAV','highlander':'phastCons100way_vertebrate','type':'double'},
            'dbnsfp.ARIC5606_AA_AC':{'json':'variants.info{}.ARIC5606_AA_AC','hbase':'I.ARAC','highlander':'ARIC5606_AA_AC','type':'int'},
            'dbnsfp.ARIC5606_AA_AF':{'json':'variants.info{}.ARIC5606_AA_AF','hbase':'I.ARAF','highlander':'ARIC5606_AA_AF','type':'double'},
            'dbnsfp.phyloP100way_vertebreate':{'json':'variants.info{}.phyloP100way_vertebreate','hbase':'I.PHYV','highlander':'phyloP100way_vertebrate','type':'double'},
            'dbnsfp.phyloP46way_placental':{'json':'variants.info{}.phyloP46way_placental','hbase':'I.PHYP','highlander':'phyloP46way_placental','type':'double'},
            'dbnsfp.CADD_phred':{'json':'variants.info{}.cadd_phred','hbase':'I.CAPH','highlander':'cadd_phred','type':'double'},
            'dbnsfp.CADD_raw':{'json':'variants.info{}.cadd_raw','hbase':'I.CARA','highlander':'cadd_raw','type':'double'},
            'dbnsfp.VEST3_score':{'json':'variants.info{}.vest_score','hbase':'I.VES','highlander':'vest_score','type':'double'},
            'o123':{'json':'variants.info{}.dbsnp_id_141','hbase':'I.DB141','highlander':'dbsnp_id_141','type':'string'},
            'o124':{'json':'variants.info{}.splicing_ada_pred','hbase':'I.SPAP','highlander':'splicing_ada_pred','type':'string'},
            'dbsnv.rf_score':{'json':'variants.info{}.splicing_rf_score','hbase':'I.SPRS','highlander':'splicing_rf_score','type':'double'},
            'o126':{'json':'variants.info{}.splicing_rf_pred','hbase':'I.SPRP','highlander':'splicing_rf_pred','type':'string'},
            'o127':{'json':'variants.info{}.protein_pos','hbase':'I.PROPO','highlander':'protein_pos','type':'int'},
            'o128':{'json':'variants.info{}.protein_length','hbase':'I.PROLE','highlander':'protein_length','type':'int'},

        }

        return mapping

    def connect_to_db(self, request):

        #return connection.cursor() # > returns the default db of hue, not the one we configured in the settings.py
        #return connections['cgs_annotations'].cursor()

        database_id = "cgs_annotations"
        new_database = {}
        new_database["id"] = database_id
        new_database["NAME"] = database_id
        new_database['ENGINE'] = 'django.db.backends.mysql'
        new_database['USER'] = 'root'
        new_database['PASSWORD'] = 'cloudera'
        new_database['HOST'] = ''
        new_database['PORT'] = ''
        settings.DATABASES[database_id] = new_database
        connections.databases[database_id] = new_database
        return connections['cgs_annotations'].cursor()


    def dictfetchall(self, cursor):
        "Return all rows from a cursor as a dict"
        columns = [col[0] for col in cursor.description]
        return [
            dict(zip(columns, row))
            for row in cursor.fetchall()
        ]


def hbaseTableName(variantId, sampleId):
    # Return the hbase table name for a given variantId (generated by us, already containing information about the analysis)
    # and a sampleId
    return 'I:CALL_'+sampleId

def getHbaseColumns():
    # Return a list of the different columns for HBase
    fc = formatConverters(input_file='stuff.vcf',output_file='stuff.json')
    mapping = fc.getMapping()

    result = []
    for pyvcf in mapping:
        result.append(mapping[pyvcf]['hbase'].replace('.',':'))

    return result


def dbmap(json_term, database="impala", order=False):
    # Return the mapping between a given json name and a specific field name (for Impala typically, but it should be
    # the same for HBase, but we need to give the column family too). Returns None if nothing found.
    fc = formatConverters(input_file='stuff.vcf',output_file='stuff.json')
    mapping = fc.getMapping()

    value = None
    for pyvcf in mapping:
        if mapping[pyvcf]['json'] == json_term:
            if order is False: # We want the field name
                if database == 'impala':
                    value = mapping[pyvcf]['hbase']
                else: #if hbase
                    value = mapping[pyvcf]['hbase'].replace('.',':')
            else: # We want the field number
                value = mapping[pyvcf]['parquet']

    return value

def dbmap_length():
    # Return the number of fields inside parquet/hbase
    fc = formatConverters(input_file='stuff.vcf',output_file='stuff.json')
    mapping = fc.getMapping()

    max_number = 0
    for pyvcf in mapping:
        if mapping[pyvcf]['parquet'] > max_number:
            max_number = mapping[pyvcf]['parquet']

    return max_number

def dbmapToJson(data, database="impala"):
    # Map the data from a database line to a json object
    # The 'data' is received from impala, and we get something like ['NA06986-4-101620184-TAAC-T', '4', '101620184', 'TAAC', '[T]', 'None', '[]', '19', '', '', '', '3279', '', '', '2', '', 'NA06986']
    # so we cannot rely on the column name, only on the order of the fields
    # TODO: manage multiple objects
    # TODO: manage HBase data

    mapped = {}
    fc = formatConverters(input_file='stuff.vcf',output_file='stuff.json')
    mapping = fc.getMapping()

    for pyvcf in mapping:

        json_field = mapping[pyvcf]['json']
        order = mapping[pyvcf]['parquet']
        type = mapping[pyvcf]['type']

        try:
            if type == 'int':
                mapped[json_field] = int(data[order])
            elif type == 'float':
                mapped[json_field] = float(data[order])
            elif type == 'dict':
                mapped[json_field] = json.loads(data[order])
            elif type == 'list':
                mapped[json_field] = data[order].split(';')
            else:
                mapped[json_field] = data[order]
        except:
            if type == 'int':
                value = 0
            elif type == 'float':
                value = 0.0
            elif type == 'dict':
                value = {}
            elif type == 'list':
                value = []
            else:
                value = ''
            mapped[json_field] = value

    return mapped

def hbaseToJson(raw_data):
    # Map the data received from multiple entries (result.columns) of hbase with multiple columns to a JSON object
    # This function need to merge similar variants (=same chromosome, reference, ... but different alternates)
    # into one object, to return data like google genomics
    # The list of data received should belong to one variant at the end, we will exclude data with a rowkey containing
    # different information than the first one (we only accept different alternates)
    mapped = {}
    fc = formatConverters(input_file='stuff.vcf',output_file='stuff.json')
    mapping = fc.getMapping()

    # We remove the variants we will not use
    try:
        first_rowkey = raw_data[0].row
    except:
        first_rowkey = ''
    interesting_rowkey = first_rowkey.split('|')
    interesting_rowkey.pop()
    interesting_rowkey = '|'.join(interesting_rowkey)+'|'
    good_variants = []
    for hbase_variant in raw_data:
        if hbase_variant.row.startswith(interesting_rowkey):
            good_variants.append(hbase_variant)

    # We use a 'specific_variant' where we will take the data
    try:
        specific_variant = raw_data[0].columns
    except:
        specific_variant = {}

    # Basic data to map
    for pyvcf in mapping:

        json_field = mapping[pyvcf]['json']
        hbaseColumn = mapping[pyvcf]['hbase'].replace('.',':')
        type = mapping[pyvcf]['type']

        if json_field == 'variants.alternateBases[]':
            alts = []
            for good_variant in good_variants:
                alternatives = good_variant.columns[hbaseColumn].value.split('|')
                for alternative in alternatives:
                    if alternative not in alts:
                        alts.append(alternative)

            mapped[json_field] = alts
        else:
            try:
                if type == 'int':
                    mapped[json_field] = int(specific_variant[hbaseColumn].value)
                elif type == 'float':
                    mapped[json_field] = float(specific_variant[hbaseColumn].value)
                elif type == 'dict':
                    mapped[json_field] = json.loads(specific_variant[hbaseColumn].value)
                elif type == 'list':
                    mapped[json_field] = specific_variant[hbaseColumn].value.split(';')
                    if len(mapped[json_field]) == 1:
                        mapped[json_field] = specific_variant[hbaseColumn].value.split('|')
                else:
                    mapped[json_field] = specific_variant[hbaseColumn].value
            except:
                if type == 'int':
                    value = 0
                elif type == 'float':
                    value = 0.0
                elif type == 'dict':
                    value = {}
                elif type == 'list':
                    value = []
                else:
                    value = ''
                mapped[json_field] = value

    # Some modifications regarding the data we will display
    tmp = mapped['variants.id'].split('|')
    tmp.pop()
    mapped['variants.id'] = '|'.join(tmp)

    # Now we need to take care of calls (we cannot simply take information from specific_variant, we need to take
    # the data from all good_variants too)
    mapped['variants.calls[]'] = []
    for current_variant in good_variants:
        for hbase_field in current_variant.columns:
            if not hbase_field.startswith('I:CALL_'):
                continue
            try:
                call = json.loads(current_variant.columns[hbase_field].value)

                # We need to set the genotype[] value for the call, based on the different alts we generated above
                genotype_call = call['genotype[]']
                if genotype_call in alts:
                    genotype_id = 0
                    for alt in alts:
                        genotype_id += 1
                        if alt == genotype_call:
                            call['genotype[]'] = [genotype_id]
                            break
                else:
                    call['genotype[]'] = 'ERROR ('+genotype_call+')'

                mapped['variants.calls[]'].append(call)
            except:
                pass
    return mapped


def parquetToJson(raw_data):
    # Map the data received from multiples entries of parquet with multiple columns (we already have the name of columns in the keys) to a JSON object
    # This function need to merge similar variants (=same chromosome, reference, ... but different alternates)
    # into one object, to return data like google genomics
    # The list of data received should belong to one variant at the end, we will exclude data with a rowkey containing
    # different information than the first one (we only accept different alternates)

    mapped = {}
    fc = formatConverters(input_file='stuff.vcf',output_file='stuff.json')
    mapping = fc.getMapping()

    # We remove the variants we will not use
    first_rowkey = raw_data[0]['pk']
    interesting_rowkey = first_rowkey.split('|')
    interesting_rowkey.pop()
    interesting_rowkey = '|'.join(interesting_rowkey)+'|'
    good_variants = []
    for impala_variant in raw_data:
        if impala_variant['pk'].startswith(interesting_rowkey):
            good_variants.append(impala_variant)

    # Basic data to map
    specific_variant = good_variants[0]
    for pyvcf in mapping:

        json_field = mapping[pyvcf]['json']
        parquetColumn = str(mapping[pyvcf]['hbase'].replace('.','_')).lower()
        type = mapping[pyvcf]['type']

        if json_field == 'variants.alternateBases[]':
            alts = []
            for good_variant in good_variants:
                alternatives = good_variant[parquetColumn].split('|')
                for alternative in alternatives:
                    if alternative not in alts:
                        alts.append(alternative)

            mapped[json_field] = alts
        else:
            try:
                if type == 'int':
                    mapped[json_field] = int(specific_variant[parquetColumn])
                elif type == 'float':
                    mapped[json_field] = float(specific_variant[parquetColumn])
                elif type == 'dict':
                    mapped[json_field] = json.loads(specific_variant[parquetColumn])
                elif type == 'list':
                    mapped[json_field] = specific_variant[parquetColumn].split(';')
                    if len(mapped[json_field]) == 1:
                        mapped[json_field] = specific_variant[parquetColumn].split('|')
                else:
                    mapped[json_field] = specific_variant[parquetColumn]
            except:
                if type == 'int':
                    value = 0
                elif type == 'float':
                    value = 0.0
                elif type == 'dict':
                    value = {}
                elif type == 'list':
                    value = []
                else:
                    value = ''
                mapped[json_field] = value

    # Some modifications regarding the data we will display
    tmp = mapped['variants.id'].split('|')
    tmp.pop()
    mapped['variants.id'] = '|'.join(tmp)

    # Now we need to take care of calls
    mapped['variants.calls[]'] = []
    for current_variant in good_variants:
        for parquet_field in current_variant:
            if not parquet_field.startswith('i_call_'):
                continue
            if current_variant[parquet_field] != 'NA':
                try:
                    call = json.loads(current_variant[parquet_field])

                    # We need to set the genotype[] value for the call, based on the different alts we generated above
                    genotype_call = call['genotype[]']
                    if genotype_call in alts:
                        genotype_id = 0
                        for alt in alts:
                            genotype_id += 1
                            if alt == genotype_call:
                                call['genotype[]'] = [genotype_id]
                                break
                    elif genotype_call == mapped['variants.referenceBases']:
                        call['genotype[]'] = [0]
                    else:
                        call['genotype[]'] = 'ERROR ('+genotype_call+')'

                    mapped['variants.calls[]'].append(call)
                except:
                    pass

    return mapped

def jsonToSerializerData(json_data, fields, prefix):
    # Convert the json data from dbmapToJson to a data dict used by a DRF Serializer to initialize an object
    # The 'fields' come from the given Serializer. The 'prefix' comes also from the Serializer, it is based
    # on the hierarchy of the Serializer regarding the other Serializers (see google documentation)

    d = {}
    for field in fields:
        if prefix+'.'+field+'[]' in json_data:
            type = '[]'
        elif prefix+'.'+field+'{}' in json_data:
            type = '{}'
        else:
            type = ''

        try:
            d[field] = json_data[prefix+'.'+field+type]
        except:
            pass
    return d

def convertJSONdir2AVROfile(jsonDir, avroFile, avscFile):
    """ Convert all JSON files to one AVRO file
    """
    ## check if the input directory exists
    if not os.path.isdir(jsonDir):
        msg = "The directory %s does not exist" % jsonDir 
        raise ValueError(msg)
    
    ## check if the avsc file exists
    if not os.path.isfile(avscFile): 
        msg = "The file %s does not exist" % avscFile 
        raise ValueError(msg)
    
    ## convert JSON files to flat JSON files
    tmpJSONFLATDir = id_generator()
    os.makedirs(tmpJSONFLATDir)
    nbrJSONfiles = 0
    for f in os.listdir(jsonDir):
        if f.endswith(".json"):
            ft = f.replace(".json", "flat.json")
            converter = formatConverters(input_file = os.path.join(jsonDir,f) , output_file = os.path.join(tmpJSONFLATDir,ft))
            status = converter.convertJSON2FLATJSON()
            nbrJSONfiles += 1
            
    ## concat the flat JSON files into 1 flat JSON file 
    flatJSONFile = id_generator()
    o = open(flatJSONFile,"w")
    for f in os.listdir(tmpJSONFLATDir):
        h = open(os.path.join(tmpJSONFLATDir,f))
        while 1:
            line = h.readline()
            if not line:
                break
            o.write(line)
        h.close()
    o.close()
    
    ## reading the concatenated flat JSON file and write to AVRO file  
    converter = formatConverters(input_file = flatJSONFile, output_file = avroFile)
    status = converter.convertFLATJSON2AVRO(avscFile)
        
    ## cleaning up
    shutil.rmtree(tmpJSONFLATDir)
    os.remove(flatJSONFile)
    
    return(status)

def database_create_variants(request, temporary=False, specific_columns=None):
    # Create the variant table. If temporary is True, it means we need to create a temporary structure as Text to be imported
    # to another variants table (that won't be temporary). specific_columns eventually contain
    # the name of sample columns, like I.CALL_NA0787, we will verify if they are available, if not
    # we will alter the table
    if specific_columns is None:
        specific_columns = []

    result = {'value':True,'text':'Everything is alright'}

    # We install the tables for impala, based on the configuration
    fc = formatConverters(input_file='stuff.vcf',output_file='stuff.json',output_type='json')
    mapping = fc.getMapping()
    fields = fc.getMappingPyvcfToText()
    pyvcf_fields = fc.getMappingPyvcfToJson()
    hbase_fields = fc.getMappingJsonToHBase()
    inversed_fields = {}
    type_fields = {}
    max_value = 0
    for field in fields:
        if fields[field] > max_value:
            max_value = fields[field]
        future_field = hbase_fields[pyvcf_fields[field]].split('.')
        #inversed_fields[fields[field]] = future_field.pop()
        inversed_fields[fields[field]] = hbase_fields[pyvcf_fields[field]]

        try:
            type = mapping[field]['type']
        except:
            type = 'string'

        type_fields[fields[field]] = type

    # We add the specific fields for each variant
    for specific_column in specific_columns:
        max_value += 1
        inversed_fields[max_value] = specific_column
        type_fields[max_value] = 'string'

    variants_table = ["" for i in xrange(max_value+1)]
    for i in range(1, max_value + 1):
        if type_fields[i] == 'list' or type_fields[i] == 'dict':
            variants_table[i] = inversed_fields[i].replace('.','_')+" STRING"
        else:
            variants_table[i] = inversed_fields[i].replace('.','_')+" "+type_fields[i].upper()

        if i < max_value:
            variants_table[i] += ","
    variants_table[0] = "pk STRING, "

    # Deleting the old table and creating the new one
    if temporary is True:
        query_server = get_query_server_config(name='hive')
        db = dbms.get(request.user, query_server=query_server)

        avro_schema = {"name": "variants","type": "record","fields": []}
        for field in variants_table:
            tmp = field.split(' ')
            name = tmp[0]
            type = tmp[1].split(',').pop(0).lower()

            if type == 'int':
                default_value = 0
            elif type == 'float' or type == 'double':
                default_value = 0.0
            elif type == 'boolean':
                default_value = False
            else:
                default_value = 'NA'

            avro_schema['fields'].append({'name':name,'type':type,'default':default_value})
        request.fs.create('/user/cgs/cgs_variants_'+request.user.username+'.avsc.json', overwrite=True, data=json.dumps(avro_schema))

        handle = db.execute_and_wait(hql_query("DROP TABLE IF EXISTS variants_tmp_"+request.user.username+";"), timeout_sec=30.0)
        query = hql_query("CREATE TABLE variants_tmp_"+request.user.username+"("+"".join(variants_table)+") stored as avro TBLPROPERTIES ('avro.schema.url'='hdfs://localhost:8020/user/cgs/cgs_variants_"+request.user.username+".avsc.json');")
        handle = db.execute_and_wait(query, timeout_sec=30.0)
    else:
        query_server = get_query_server_config(name='impala')
        db = dbms.get(request.user, query_server=query_server)

        handle = db.execute_and_wait(hql_query("DROP TABLE IF EXISTS variants;"), timeout_sec=30.0)
        query = hql_query("CREATE TABLE variants("+"".join(variants_table)+") stored as parquet;")
        handle = db.execute_and_wait(query, timeout_sec=30.0)

    # We install the variant table for HBase
    if temporary is False:
        try:
            hbaseApi = HbaseApi(user=request.user)
            currentCluster = hbaseApi.getClusters().pop()
            hbaseApi.createTable(cluster=currentCluster['name'],tableName='variants',columns=[{'properties':{'name':'I'}},{'properties':{'name':'R'}},{'properties':{'name':'F'}}])
        except:
            result['value'] = False
            result['text'] = 'A problem occured when connecting to HBase and creating a table. Check if HBase is activated. Note that this message will also appear if the \'variants\' table in HBase already exists. In that case you need to manually delete it.'

    return result, variants_table

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    import random
    return ''.join(random.choice(chars) for x in range(size))

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass

    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass

    return False

def flatten(d, parent_key='', sep='.'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def uniqueInList(seq):
    seen = set()
    seen_add = seen.add
    return [ x for x in seq if not (x in seen or seen_add(x))]
