from rest_framework import serializers
from variants.models import *
from django.conf import settings
from beeswax.design import hql_query
from beeswax.server import dbms
from beeswax.server.dbms import get_query_server_config
from hbase.api import HbaseApi
from converters import *

# The fields of the following serializers directly come from https://cloud.google.com/genomics/v1beta2/reference/

class VCFSerializer(serializers.Serializer):
    pk = serializers.IntegerField(read_only=True)
    filename = serializers.CharField(max_length=100)
    patients = serializers.CharField(max_length=1000) # Ids of the different patients inside the vcf, separated by a comma
    analyzed = serializers.BooleanField(default=False)

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
        # TODO: dynamic loading (to not have to rewrite the fields one-by-one)
        d = {}

        # We load the data inside a 'data' dict, based on the current field above
        json_data = dbmapToJson(variantcall_data)
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


    def __init__(self, request=None, pk=None, *args, **kwargs):
        # TODO: for now we simply load the data inside the 'data' field, we should load
        # the data directly inside the current object
        if request is None and pk is None:
            return super(VariantSerializer, self).__init__(*args, **kwargs)

        # We take the information in the database. As we are interested in one variant, we use HBase
        hbaseApi = HbaseApi(user=request.user)
        currentCluster = hbaseApi.getClusters().pop()

        variant = hbaseApi.getRows(cluster=currentCluster['name'], tableName='variants', columns=getHbaseColumns(), startRowKey=pk, numRows=1, prefix=False)
        if len(variant) > 0:
            variant = variant.pop()
        else:
            variant = None

        if variant is not None:
            # We load it in the current object
            json_data = hbaseToJson(variant.columns)
            d = jsonToSerializerData(json_data, self.fields, 'variants')

            d['calls'] = []
            for variants_call in json_data['variants.calls[]']:
                call = VariantCallSerializer(variantcall_data=variants_call)
                d['calls'].append(call.data)

            # Load a specific variant
            kwargs['data'] = d
            super(VariantSerializer, self).__init__(*args, **kwargs)

            # TODO: we should remove that method call when we resolve the TODO above.
            self.is_valid()

    def post(self, request):
        # Insert a new variant inside the database (Impala - HBase)
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
