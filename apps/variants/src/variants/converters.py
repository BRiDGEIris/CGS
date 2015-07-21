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
        * output: json, jsonflat, avro
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
                self.input_type = 'sp[len(sp)-2]' + 'sp[len(sp)-1]'
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

        f = open(self.input_file)
        o = open(self.output_file,'w')
        vcf_reader = vcf.Reader(f)
        #cc = 1
        for record in vcf_reader:
        #for i in [1]:
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
                    
                linedic = {
                    "variants_info_num_genes" : "NA", 
                    "variants_quality" : str(record.QUAL),
                    "variants_info_allele_num": "NA",
                    "variants_calls_info_zygosity": call_het,
                    "variants_info_short_tandem_repeat": "NA",
                    "readGroupSets_readGroups_experiment_sequencingCenter": "NA",
                    "readGroupSets_readGroups_info_patient": s.sample,
                    "variants_info_change_type": record.var_type,
                    "variants_calls_info_read_depth": str(call_DP),
                    "variants_info_other_effects": "NA",
                    "variants_referenceBases": record.REF,
                    "variants_info_is_scSNV_Ensembl": "NA",
                    "readGroupSets_readGroups_experiment_libraryId": "NA",
                    "variants_info_dbsnp_id_137": "NA",
                    "variants_info_lof_tolerant_or_recessive_gene": "NA",
                    "variants_info_is_scSNV_RefSeq": "NA",
                    "variants_filters": FILTER,
                    "readGroupSets_readGroups_sampleID": s.sample,
                    "variants_start": str(record.POS),
                    "variants_info_downsampled": "NA",
                    "variants_referenceName": record.CHROM,
                    "variants_alternateBases": ALT,
                    "variants_calls_genotype" : current_gt
                    }
                o.write(json.dumps(linedic, ensure_ascii=False) + "\n")

        o.close()
        f.close()

        status = "succeeded"
        return(status)
            # #sampleIdList =  
            # varDic = {{"Callset": {"id" : , "sampleId" : , "variantSetIds" : [] }},
            #           # {"ReadGroupSets" :
            #           #  {"ReadGroups" : {"sampleId" : }, {"sampleId" : }}
            #           # },
            #           {"Variants" :
            #            {"variantSetId" : "",
            #             "referenceName" : "",
            #             "start" : "",
            #             "end" : "",
            #             "referenceBases" :
            #             "alternateBases" :
            #             "quality" :
            #             "filter" :
            #             },
            #             "calls" :
            #             { "callSetId": ,
            #               "genotype" : []
            #               }
            #         },
            #         { "Variantsets" { "id" : }}
                      
                        
            
            # jsonline = json.dumps(varDic, ensure_ascii=False)
            # cc += 1
        
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