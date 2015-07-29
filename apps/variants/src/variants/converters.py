import os,sys
import json, ast
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../cgsdatatools'))
if not path in sys.path:
    sys.path.insert(1, path)
del path
import string
import collections
from .exception import *
import shutil
import vcf

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
            record = vcf_reader.next()
            for s in record.samples:
                if hasattr(s.data,'DP'):
                    call_DP = s.data.DP
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

                linedic = {}

                for pyvcf_parameter in mapping:

                    if pyvcf_parameter.startswith('Record.INFO'):
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

                    linedic[mapping[pyvcf_parameter]] = value

                o.write(json.dumps(linedic, ensure_ascii=False) + "\n")

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
                output_line[0] = variant['readGroupSets.readGroups.sampleID'] + '-' + variant['variants.referenceName'] + '-' + variant['variants.start'] + '-' + variant['variants.referenceBases'] + '-' + alternate

                # We generate the line
                o.write(','.join(output_line).replace('"','')+'\n')

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
           'Record.INFO.DP':{'json':'variants.calls.info.read_depth','hbase':'F.DPF','parquet':11,'type':'string'},
           'Record.INFO.MQ0':{'json':'variants.info.mapping_quality_zero_read','hbase':'I.MQ0','parquet':12,'type':'string'},
           'Record.INFO.DS':{'json':'variants.info.downsampled','hbase':'I.DS','parquet':13,'type':'string'},
           'Record.INFO.AN':{'json':'variants.info.allele_num','hbase':'I.AN','parquet':14,'type':'string'},
           'Record.INFO.AD':{'json':'variants.calls.info.confidence_by_depth','hbase':'F.AD','parquet':15,'type':'string'},
           'Call.sample':{'json':'readGroupSets.readGroups.sampleID','hbase':'R.SI','parquet':16,'type':'string'},

            # The following terms should be correctly defined
           'todefine1':{'json':'variants.variantSetId','hbase':'R.VSI','parquet':17,'type':'string'},
           'todefine2':{'json':'variants.id','hbase':'R.ID','parquet':18,'type':'string'}, # Ok
           'todefine3':{'json':'variants.names[]','hbase':'R.NAMES','parquet':19,'type':'list'},
           'todefine4':{'json':'variants.created','hbase':'R.CREATED','parquet':20,'type':'int'},
           'todefine5':{'json':'variants.end','hbase':'R.PEND','parquet':21,'type':'int'},
           'todefine6':{'json':'variants.info{}','hbase':'R.INFO','parquet':22,'type':'dict'},
           'todefine7':{'json':'variants.calls[]','hbase':'R.CALLS','parquet':23,'type':'list'},
           'todefine8':{'json':'variants.calls[].callSetId','hbase':'R.CALLS_ID','parquet':24,'type':'string'},
           'todefine9':{'json':'variants.calls[].callSetName','hbase':'R.CALLS_NAME','parquet':25,'type':'string'},
           'todefine10':{'json':'variants.calls[].genotype[]','hbase':'R.CALLS_GT','parquet':26,'type':'list'},
           'todefine11':{'json':'variants.calls[].phaseset','hbase':'R.CALLS_PS','parquet':27,'type':'string'},
           'todefine12':{'json':'variants.calls[].genotypeLikelihood[]','hbase':'R.CALLS_LHOOD','parquet':28,'type':'list'},
           'todefine13':{'json':'variants.calls[].info{}','hbase':'R.CALLS_INFO','parquet':29,'type':'dict'},
        }

        return mapping

def dbmap(json_term, database="impala", order=False):
    # Return the mapping between a given json name and a specific field name (for Impala typically, but it should be
    # the same for HBase, but we need to give the column family too). Returns None if nothing found.
    fc = formatConverters(input_file='stuff.vcf',output_file='stuff.json')
    mapping = fc.getMapping()

    value = None
    for pyvcf in mapping:
        if mapping[pyvcf]['json'] == json_term:
            if order is False: # We want the field name
                value = mapping[pyvcf]['hbase']
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

def dbmapToJson(data, database="impala", subdata=False):
    # Map the data from a database line to a json object
    # The 'data' is received from impala, and we get something like ['NA06986-4-101620184-TAAC-T', '4', '101620184', 'TAAC', '[T]', 'None', '[]', '19', '', '', '', '3279', '', '', '2', '', 'NA06986']
    # so we cannot rely on the column name, only on the order of the fields
    # If subdata is True, it means the 'data' received is not a entire data line, but a subpart of it, in that case
    # we may need to return a list of json objects and not a simple object.
    # So: subdata is False > simple object. subdata is True > list of objects
    # TODO: manage multiple objects (> 'subdata is True')
    # TODO: manage HBase data

    mapped = []
    fc = formatConverters(input_file='stuff.vcf',output_file='stuff.json')
    mapping = fc.getMapping()

    iter = 0
    mapped.append({})
    for pyvcf in mapping:

        json_field = mapping[pyvcf]['json']
        order = mapping[pyvcf]['parquet']
        type = mapping[pyvcf]['type']

        try:
            if type == 'int':
                mapped[iter][json_field] = int(data[order])
            elif type == 'float':
                mapped[iter][json_field] = float(data[order])
            elif type == 'dict':
                mapped[iter][json_field] = json.loads(data[order])
            elif type == 'list':
                mapped[iter][json_field] = data[order].split(';')
            else:
                mapped[iter][json_field] = data[order]
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
            mapped[iter][json_field] = value


    if subdata is False: # We surely only have one object
        return mapped[0]
    else: # We may have multiple objects, so we need to send back a list
        return mapped

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