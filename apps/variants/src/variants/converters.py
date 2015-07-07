#!/usr/bin/env python

import json
import logging

from django.core.urlresolvers import reverse
from django.utils.translation import ugettext as _
from django.http import HttpResponse

from beeswax.design import hql_query
from beeswax.server import dbms
from beeswax.server.dbms import get_query_server_config
from impala.models import Dashboard, Controller

#from desktop.lib.django_util import JsonResponse
from desktop.lib.rest.http_client import RestException
from exception import handle_rest_exception

from variants.decorators import api_error_handler


# This file allows a conversion from vcf file to json, but also other formats
# TODO This file should be moved to cgs-data at the end
# TODO We should use a yml structure instead of the current hardcoded data structure

