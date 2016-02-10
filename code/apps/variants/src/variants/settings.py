# Licensed to Cloudera, Inc. under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  Cloudera, Inc. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
DJANGO_APPS = [ "variants" ]
NICE_NAME = "CGS"
REQUIRES_HADOOP = False
MENU_INDEX = 100
#ICON = "variants/static/art/icon_genomicAPI_48.png"
ICON = "css/variants/art/icon_genomicAPI_48.png"
DEBUG = True

import os
variants_dir = os.path.dirname(__file__)
src_dir = os.path.dirname(variants_dir)
ROOT_PATH = os.path.abspath(os.path.dirname(src_dir))
#Gives access to the root path from other files

REST_FRAMEWORK = {
    'DEFAULT_PERMISSION_CLASSES': [],
    'DEFAULT_RENDERER_CLASSES': (
        'rest_framework.renderers.JSONRenderer',
        'rest_framework.renderers.BrowsableAPIRenderer',
    )
}

DATABASES = {
    'default': {},
    'cgs_annotations': {
        'NAME': 'cgs_annotations',
        'ENGINE': 'django.db.backends.mysql',
        'USER': 'root',
        'PASSWORD': 'cloudera'
    },
}