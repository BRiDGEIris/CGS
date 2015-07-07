from rest_framework import serializers
from variants.models import *
from django.conf import settings

class UserSerializer(serializers.Serializer):
    pk = serializers.IntegerField(read_only=True)
    filename = serializers.CharField("Filename", max_length=100)
    content = serializers.TextField()
    analyzed = serializers.BooleanField(default=False)
