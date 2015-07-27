from rest_framework import serializers
from variants.models import *
from django.conf import settings
from beeswax.design import hql_query
from beeswax.server import dbms
from beeswax.server.dbms import get_query_server_config
from converters import *

# The fields of the following serializers directly come from https://cloud.google.com/genomics/v1beta2/

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
    callSetId = serializers.CharField()
    callSetName = serializers.CharField()
    genotype = serializers.ListField()
    phaseset = serializers.CharField()
    genotypeLikelihood = serializers.ListField()
    info = serializers.DictField()

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
    filter = serializers.ListField()
    info = serializers.DictField()
    calls = VariantCallSerializer(many=True)

    def load(self, pk):
        # Load a specific variant

        # We take the information in the database
        query_server = get_query_server_config(name='impala')
        db = dbms.get(request.user, query_server=query_server)
        query = hql_query("SELECT * FROM variants WHERE pk='"+pk+"'")
        handle = db.execute_and_wait(query, timeout_sec=5.0)
        if not handle:
            raise Exception("Impossible to load the variant...")

        # We load it in the current object
        data = db.fetch(handle, rows=1)
        json_data = dbmapToJson(data)
        self.variantSetId = json_data['variants.variantSetId']
        self.id = json_data['variants.id']
        self.names = json_data['variants.names'].split(';')
        self.created = int(json_data['variants.created'])
        self.referenceName = json_data['variants.referenceName']
        self.start = json_data['variants.start']
        self.end = json_data['variants.end']
        self.referenceBases = json_data['variants.referenceBases']
        self.alternateBases = json_data['variants.alternatesBases[]'].split(';')
        self.quality = json_data['variants.quality']
        self.filters = json_data['variants.filters[]'].split(';')
        self.info = json.loads(json_data['variants.filters[]'])
        self.calls = json_data['variants.calls[]'] # TODO

        # We close the database connection
        db.close(handle)

    def post(self, request):
        # Insert a new variant inside the database

        # We create the query to put the data
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
        query_data[dbmap('variants.alternateBases[]', order=True)] = ";".join(self.alternateBases)
        query_data[dbmap('variants.quality', order=True)] = self.quality
        query_data[dbmap('variants.filters[]', order=True)] = ";".join(self.filter)
        query_data[dbmap('variants.info', order=True)] = json.dumps(self.info)
        query_data[dbmap('variants.calls[]', order=True)] = "TODO" # TODO

        # We make the query
        query_server = get_query_server_config(name='impala')
        db = dbms.get(request.user, query_server=query_server)
        query = hql_query("INSERT INTO variant("+",".join(query_data)+")")
        handle = db.execute_and_wait(query, timeout_sec=5.0)
        if handle:
            db.close(handle)
        else:
            raise Exception("Impossible to create the variant...")

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







