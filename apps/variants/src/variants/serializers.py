from rest_framework import serializers
from variants.models import *
from django.conf import settings

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
    Variant
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







