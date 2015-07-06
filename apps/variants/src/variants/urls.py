#!/usr/bin/env python

try: # Django < 1.5
    from django.conf.urls.defaults import patterns, url
except: # Django >= 1.6
    from django.conf.urls import patterns, url

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
    #url(r'^database/initialize/$', 'views.database_initialize'),

    url(r'^variants/search$', 'api.variants_search'),

    url(r'^sample/insert/interface/$', 'views.sample_insert_interface'),
    url(r'^sample/insert/$', 'views.sample_insert'),
    url(r'^sample/index/interface/$', 'views.sample_index_interface'),
    url(r'^query/index/interface/$', 'views.query_index_interface'),

    url(r'^files/search/$', 'views.sample_search'), #Post request
    url(r'^variant/get/(?P<variant_id>[a-zA-Z0-9_-]*)/$', 'views.variant_get'),
    url(r'^general/insert/$', 'views.api_insert_general'), #Related to query_insert/
    url(r'^variant/search/$', 'views.variant_search'),
    url(r'^variant/import/$', 'views.variant_import'),

  
    url(r'^documentation/$', 'views.documentation'),
)
