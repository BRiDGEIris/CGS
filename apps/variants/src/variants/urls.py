#!/usr/bin/env python

try: # Django < 1.5
    from django.conf.urls.defaults import patterns, include, url
except: # Django >= 1.6
    from django.conf.urls import patterns, include, url

from rest_framework.urlpatterns import format_suffix_patterns
from variants import api


"""
    Some rules:
     <PART>/<OPERATION>/ -> <PART>_<OPERATION> -> For API
     <PART>/<OPERATION>/interface/ -> <PART>_<OPERATION>_interface -> For the interface
        The file related to the interface has to be <PART>.<OPERATION>.interface.mako
    The <PART> is always singular.

    Each function specific to a page is always formated like that:
        <PART>_<OPERATION>_<INFORMATION-ABOUT-FUNCTION> where <INFORMATION-ABOUT-FUNCTION> does not contain underscore if possible.

    Exceptions: the main index, the database initialization.
"""

urlpatterns = patterns('variants',
    url(r'^$', 'views.index'),

    #url(r'^docs/', include('rest_framework_swagger.urls')),  
    url(r'^database/initialize/$', 'views.database_initialize'),

    # Importing the data
    url(r'^api/vcf/$', api.VCFDetail.as_view(),name='vcf-detail'),
    url(r'^api/variant/(?P<pk>[a-zA-Z0-9-]{1,30})/$', api.VariantDetail.as_view(),name='variant-detail'),
    url(r'^api/variant/$', api.VariantDetail.as_view(),name='variant-detail'),
    url(r'^api/sample/$', api.SampleDetail.as_view(),name='sample-detail'),
    url(r'^api/samples/$', api.SampleList.as_view(),name='sample-list'),
    url(r'^api/dataset/$', api.DatasetDetail.as_view(),name='dataset-detail'),
    url(r'^api/datasets/$', api.DatasetList.as_view(),name='dataset-list'),
    url(r'^api/readgroupset/$', api.ReadGroupSetDetail.as_view(),name='readgroupset-detail'),
    url(r'^api/variantset/$', api.VariantSetDetail.as_view(),name='variantset-detail'),
    url(r'^api/callset/$', api.CallSetDetail.as_view(),name='callset-detail'),

    # Interface
    url(r'^sample/insert/interface/$', 'views.sample_insert_interface'),
    url(r'^sample/insert/$', 'views.sample_insert'),
    url(r'^sample/index/interface/$', 'views.sample_index_interface'),
    url(r'^query/index/interface/$', 'views.query_index_interface'),
)
