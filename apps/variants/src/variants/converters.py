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

from subprocess import *
import json

class formatConverters(object):
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

    def convertVCF2FLATJSON(self):
        """ Convert a VCF file to a FLAT JSON file
        Note: this function is a temporary function that should be replaced in future versions.
        Check the doc: https://pyvcf.readthedocs.org/en/latest/API.html#vcf-model-call
        """
        if self.input_type not in ['vcf','vcf.gz'] or self.output_type != 'jsonflat':
            msg = "Error: vcf files (possibly gzipped) must be given as input files, and a jsonflat file should be given as output file."
            status = "failed"
            raise ValueError(msg)

        mapping = self.getMappingPyvcfToJson()

        f = open(self.input_file, 'r')
        o = open(self.output_file, 'w')

        vcf_reader = vcf.Reader(f)
        for record in vcf_reader:
            call_i = 0
            linedic = {}
            linedic['variants.calls[]'] = [{} for i in xrange(0, len(record.samples))]
            for s in record.samples:
                linedic['variants.calls[]'][call_i] = {'info{}':{},'genotypeLikelihood[]':[],'genotype[]':[]}

                if hasattr(s.data,'DP'):
                    linedic['variants.calls[]'][call_i]['DP'] = s.data.DP
                else:
                    call_DP = "NA"

                if hasattr(s.data,'GT') and s.data.GT is not None:
                    current_gt = s.data.GT
                else:
                    current_gt = ""

                if len(uniqueInList(current_gt.split('|'))) > 1:
                    call_het = "Heterozygous"
                else:
                    call_het = "Homozygous"
                if isinstance(record.ALT, list):
                    ALT = '|'.join([str(a) for a in record.ALT])
                else:
                    ALT = record.ALT
                if isinstance(record.FILTER, list):
                    FILTER = '|'.join([str(a) for a in record.FILTER])
                else:
                    FILTER = str(record.FILTER)

                linedic['variants.calls[]'][call_i]['genotype[]'].append(current_gt)

                for pyvcf_parameter in mapping:

                    if mapping[pyvcf_parameter] == 'variants.calls[]' or mapping[pyvcf_parameter] == 'variants.calls[].info{}':
                        continue

                    # We detect how to take the information from PyVCF, then we take it
                    if pyvcf_parameter == 'Record.ALT':
                        value = ALT
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
                    else:
                        value = ""
                        print("Parameter '"+pyvcf_parameter+"' not supported.")

                    # Now we decide how to store the information in json
                    if mapping[pyvcf_parameter].startswith('variants.calls[].'):
                        tmp = mapping[pyvcf_parameter].split('variants.calls[].')
                        if tmp[1] != 'info{}':
                            linedic['variants.calls[]'][call_i][tmp[1]] = value
                    else:
                        linedic[mapping[pyvcf_parameter]] = value

                # We have to add the sample id for the current sample
                linedic['variants.calls[]'][call_i]['info{}']['sampleId'] = s.sample

                o.write(json.dumps(linedic, ensure_ascii=False) + "\n")
                call_i += 1
        o.close()
        f.close()

        status = "succeeded"
        return(status)

    def convertJsonToText(self, request):
        # The json received should be created previously by 'convertPyvcfToJson' as we will want a json object/line

        # 1st: we take the json to text information
        mapping = self.getMappingJsonToText()
        max_items = 0
        for key in mapping:
            if mapping[key] > max_items:
                max_items = mapping[key]

        # 2nd: we create the tsv file
        f = open(self.input_file, 'r')
        o = open(self.output_file, 'w')

        for json_line in f:
            variant = json.loads(json_line)

            # We take the different alternates
            # TODO: for some reasons the json.loads() doesn't like the value it received...
            try:
                alternates = json.loads(variant['variants.alternateBases[]'])
            except:
                alternates = [variant['variants.alternateBases[]'].replace('[','').replace(']','')]

            for alternate in alternates:

                # We associate a json value to a position in the output
                output_line = ["" for i in range(max_items+1)]
                for json_key in mapping:
                    if json_key in variant:
                        output_line[mapping[json_key]] = str(variant[json_key])

                # We generate the rowkey
                output_line[0] = variant['variants.referenceName'] + '-' + variant['variants.start'] + '-' + variant['variants.referenceBases'] + '-' + alternate

                # We generate the line
                o.write(','.join(output_line).replace('"','')+'\n')

        f.close()
        o.close()

        status = "succeeded"
        return(status)

    def convertJsonToHBase(self, request, analysis, organization):
        # The json received should be created previously by 'convertPyvcfToJson' as we will want a json object/line
        # We will create a json as output too, but it will be adapted to the one used in HBase

        # 1st: we take the json to text information
        mapping = self.getMapping()

        json_to_hbase = {}
        for key in mapping:
            json_to_hbase[mapping[key]['json']] = mapping[key]['hbase'].replace('.',':')

        # 2nd: we create a temporary file in which we will save each future line for HBase
        f = open(self.input_file, 'r')
        o = open(self.output_file, 'w')

        for json_line in f:
            variant = json.loads(json_line)

            output_line = {}
            rowkey = organization + '-' + analysis + '-' + variant['variants.referenceName'] + '-' + variant['variants.start'] + '-' + variant['variants.referenceBases'] + '-' + variant['variants.alternateBases[]'][0]
            output_line['rowkey'] = rowkey
            variant['variants.id'] = rowkey
            for attribute in variant:

                if attribute == 'variants.calls[]':
                    # Specific case for the variants.calls[] (in fact, it will be variants.calls[0], variants.calls[1], ...

                    for call in variant[attribute]:
                        # We take the sample id associated to this call
                        if not 'info{}' in call:
                            continue
                        sampleId = call['info{}']['sampleId']
                        variantId = variant['variants.id']

                        # We generate the table name based on the 'sampleId' and the 'id' field (containing the information on the current analysis)
                        table_name_for_call = hbaseTableName(variantId, sampleId)

                        # We got through the different fields for this object
                        subline = {}
                        for subattribute in call:
                            if subattribute == 'info{}':
                                for infokey in call[subattribute]:
                                    if subattribute in subline: # each dict info is separated through '|'
                                        subline[subattribute] += '|'
                                    else:
                                        subline[subattribute] = ''

                                    if type(call[subattribute][infokey]) is list:
                                        # The first element of multiple values separated by ';' is the info key.
                                        subline[subattribute] += infokey+';'+';'.join(str(value) for value in call[subattribute][infokey])
                                    else:
                                        subline[subattribute] += infokey+';'+str(call[subattribute][infokey])
                            else:
                                if type(call[subattribute]) is list:
                                    subline[subattribute] = ';'.join(str(value) for value in call[subattribute])
                                else:
                                    subline[subattribute] = str(call[subattribute])

                            if subline[subattribute] == "None":
                                subline[subattribute] = ""

                        # We merge the information for the given call.
                        output_line[table_name_for_call] = '|'.join(key+'|'+value for key, value in subline.iteritems())

                elif attribute == 'info{}':
                    for infokey in variant[attribute]:
                        if attribute in output_line: # each dict info is separated through '|'
                            output_line[attribute] += '|'

                        if type(variant[attribute][infokey]) is list:
                            # The first element of multiple values separated by ';' is the info key.
                            output_line[attribute] += infokey+';'+';'.join(str(value) for value in variant[attribute][infokey])
                        else:
                            output_line[attribute] += infokey+';'+str(variant[attribute][infokey])

                elif type(attribute) is list:
                    output_line[json_to_hbase[attribute]] = ';'.join(str(value) for value in variant[attribute])
                else:
                    output_line[json_to_hbase[attribute]] = str(variant[attribute])

            # We generate the line
            o.write(json.dumps(output_line)+'\n')
        f.close()
        o.close()

        status = "succeeded"
        return(status)

    def convertJSON2FLATJSON(self):
        """ Convert a JSON file (for the format, see the documentation) to a flat JSON file or more accurately a series of JSON lines  
        """
        if self.input_type != 'json' or self.output_type != 'json':
            msg = "Error: json files must be given as input files."
            status = "failed"
            raise ValueError(msg)
        
        f = open(self.input_file)
        h = open(self.output_file,'w')
        line = f.readline()
        jsl = json.loads(line)
        try:
            for i in jsl.keys():
                flatJSON = flatten(jsl[i])
                flatJSONLiteral = ast.literal_eval(json.dumps(flatJSON))
                h.write(str(flatJSONLiteral).replace("'",'"').replace(".","_") + '\n')
            status = "succeeded"
        except:
            msg = "Error: the json does not follow the right syntax."
            status = "failed"
            raise ValueError(msg)
        return(status)
        f.close()
        h.close()
         
    def convertFLATJSON2AVRO(self,avscFile = ""):
        """ Convert a JSON file (for the format, see the documentation) to an AVRO file using AVSC for making the conversion
        """
        status = "failed"
        if avscFile == "":
            msg = "This feature is not yet implemented. Please provide an AVRO schema file (.avsc)."
            raise ValueError(msg)
        else:
            pass
            """
            schema = avro.schema.parse(open(avscFile).read())
            writer = DataFileWriter(open(self.output_file, "w"), DatumWriter(), schema)
            h = open(self.input_file)
            while 1: ## reading line per line in the flat json file and write them in the AVRO format
                line = h.readline()
                if not line:
                    break
                ls = line.strip()
                writer.append(ast.literal_eval(ls))

            h.close()
            writer.close()
            status = "succeeded"
            """
        return(status)

        ## cmd = "java -jar ../avro-tools-1.7.7.jar fromjson --schema-file" + avscFile + " " + self.input_file > self.output_file 

    def getMappingJsonToText(self):
        # Return the mapping 'json_parameter' > 'order_in_text_file'

        mapping = self.getMapping()

        new_mapping = {}
        for key in mapping:
            new_mapping[mapping[key]['json']] = mapping[key]['parquet']

        return new_mapping

    def getMappingPyvcfToText(self):
        # Return the mapping 'pyvcf_parameter' > 'order_in_text_file'

        mapping = self.getMapping()

        new_mapping = {}
        for key in mapping:
            new_mapping[key] = mapping[key]['parquet']

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


    def getMapping(self):
        # Return the mapping between PyVCF, JSON, HBase and Parquet (parquet position only)
        # Sometimes there is nothing in PyVCF to give information for a specific file created by ourselves.

        mapping = {
        'Record.CHROM':{'json':'variants.referenceName','hbase':'R.C','parquet':1,'type':'string'},
           'Record.POS':{'json':'variants.start','hbase':'R.P','parquet':2,'type':'int'},
           'Record.REF':{'json':'variants.referenceBases','hbase':'R.REF','parquet':3,'type':'string'},
           'Record.ALT':{'json':'variants.alternateBases[]','hbase':'R.ALT','parquet':4,'type':'list'},
           'Record.ID':{'json':'variants.info.dbsnp_id','hbase':'I.DBSNP137','parquet':5,'type':'string'},
           'Record.FILTER':{'json':'variants.filters[]','hbase':'R.FILTER','parquet':6,'type':'list'},
           'Record.QUAL':{'json':'variants.quality','hbase':'R.QUAL','parquet':7,'type':'float'},
           'Record.INFO.QD':{'json':'variants.info.confidence_by_depth','hbase':'I.QD','parquet':8,'type':'string'},
           'Record.INFO.HRun':{'json':'variants.info.largest_homopolymer','hbase':'I.HR','parquet':9,'type':'string'},
           'Record.INFO.SB':{'json':'variants.strand_bias','hbase':'I.SB','parquet':10,'type':'string'},
           'Record.INFO.DP':{'json':'variants.calls[].info.read_depth','hbase':'F.DPF','parquet':11,'type':'string'},
           'Record.INFO.MQ0':{'json':'variants.info.mapping_quality_zero_read','hbase':'I.MQ0','parquet':12,'type':'string'},
           'Record.INFO.DS':{'json':'variants.info.downsampled','hbase':'I.DS','parquet':13,'type':'string'},
           'Record.INFO.AN':{'json':'variants.info.allele_num','hbase':'I.AN','parquet':14,'type':'string'},
           'Record.INFO.AD':{'json':'variants.calls[].info.confidence_by_depth','hbase':'F.AD','parquet':15,'type':'string'},
           'Call.sample':{'json':'readGroupSets.readGroups.sampleID','hbase':'R.SI','parquet':16,'type':'string'},

            # The following terms should be correctly defined
           'todefine1':{'json':'variants.variantSetId','hbase':'R.VSI','parquet':17,'type':'string'},
           'todefine2':{'json':'variants.id','hbase':'R.ID','parquet':18,'type':'string'}, # Ok
           'Call.sample':{'json':'variants.names[]','hbase':'R.NAMES','parquet':19,'type':'list'},
           'todefine4':{'json':'variants.created','hbase':'R.CREATED','parquet':20,'type':'int'},
           'todefine5':{'json':'variants.end','hbase':'R.PEND','parquet':21,'type':'int'},
           'todefine6':{'json':'variants.info{}','hbase':'R.INFO','parquet':22,'type':'dict'},
           'todefine7':{'json':'variants.calls[]','hbase':'R.CALLS','parquet':23,'type':'list'},
           'todefine8':{'json':'variants.calls[].callSetId','hbase':'R.CALLS_ID','parquet':24,'type':'string'},
           'todefine9':{'json':'variants.calls[].callSetName','hbase':'R.CALLS_NAME','parquet':25,'type':'string'},
           'Call.gt_bases':{'json':'variants.calls[].genotype[]','hbase':'R.CALLS_GT','parquet':26,'type':'list'},
           'Call.phased':{'json':'variants.calls[].phaseset','hbase':'R.CALLS_PS','parquet':27,'type':'string'},
           'todefine12':{'json':'variants.calls[].genotypeLikelihood[]','hbase':'R.CALLS_LHOOD','parquet':28,'type':'list'},
           'todefine13':{'json':'variants.calls[].info{}','hbase':'R.CALLS_INFO','parquet':29,'type':'dict'},
        }

        return mapping

def hbaseTableName(variantId, sampleId):
    # Return the hbase table name for a given variantId (generated by us, already containing information about the analysis)
    # and a sampleId

    # TODO: to improve, for now it is way too long
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

def hbaseVariantCallToJson(raw_call):
    # Map a given call from HBase to a json object
    mapped = {}

    raw_info = raw_call.split('info{}')
    if len(raw_info) > 1:
        subinfo = raw_info[1].split('|')
        for i in xrange(0, len(subinfo), 2):
            subsubinfo = subinfo[i+1].split(';')
            if len(subsubinfo) > 1 or '[]' in subinfo[i]:
                mapped['variants.calls[].info{}.'+subinfo[i]] = subsubinfo
            else:
                mapped['variants.calls[].info{}.'+subinfo[i]] = subinfo[i+1]

    info = raw_info[0].split('|')
    for i in xrange(0, len(info), 2):
        if i+1 < len(info):
            subinfo = info[i+1].split(';')
            if len(subinfo) > 1 or '[]' in info[i]:
                if len(subinfo) == 1 and subinfo[0] == "":
                    mapped['variants.calls[].'+info[i]] = []
                else:
                    mapped['variants.calls[].'+info[i]] = subinfo
            else:
                mapped['variants.calls[].'+info[i]] = info[i+1]

    return mapped

def hbaseToJson(data):
    # Map the data received from ONE entry (result.columns) of hbase with multiple columns to a JSON object

    mapped = {}
    fc = formatConverters(input_file='stuff.vcf',output_file='stuff.json')
    mapping = fc.getMapping()

    # Basic data to map
    for pyvcf in mapping:

        json_field = mapping[pyvcf]['json']
        hbaseColumn = mapping[pyvcf]['hbase'].replace('.',':')
        type = mapping[pyvcf]['type']

        try:
            if type == 'int':
                mapped[json_field] = int(data[hbaseColumn].value)
            elif type == 'float':
                mapped[json_field] = float(data[hbaseColumn].value)
            elif type == 'dict':
                mapped[json_field] = json.loads(data[hbaseColumn].value)
            elif type == 'list':
                mapped[json_field] = data[hbaseColumn].value.split(';')
                if len(mapped[json_field]) == 1:
                    mapped[json_field] = data[hbaseColumn].value.split('|')
            else:
                mapped[json_field] = data[hbaseColumn].value
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

    # Now we need to take care of calls
    mapped['variants.calls[]'] = []
    for hbase_field in data:
        if not hbase_field.startswith('I:CALL_'):
            continue
        mapped['variants.calls[]'].append(data[hbase_field])

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
