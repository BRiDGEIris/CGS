from rest_framework import serializers
from variants.models import *
from django.conf import settings

class VCFSerializer(serializers.Serializer):
    pk = serializers.IntegerField(read_only=True)
    filename = serializers.CharField(max_length=100)
    patients = serializers.CharField(max_length=1000) # Ids of the different patients inside the vcf, separated by a comma
    analyzed = serializers.BooleanField(default=False)

class SampleSerializer(serializers.Serializer):
    pass

class DatasetSerializer(serializers.Serializer):
    pass

class ReadGroupSetSerializer(serializers.Serializer):
    pass

class VariantSetSerializer(serializers.Serializer):
    pass

class VariantSerializer(serializers.Serializer):
    pass

class CallSetSerializer(serializers.Serializer):
    pass