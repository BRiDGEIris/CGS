#!/usr/bin/env python

from django.conf.urls.defaults import patterns, url

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

urlpatterns = patterns('patients',
    url(r'^$', 'views.index'),
  
    url(r'^database/initialize/$', 'views.database_initialize'),


    url(r'^patient/import/interface/$', 'views.patient_import_interface'),
    url(r'^patient/import/$', 'views.patient_import'),
    url(r'^patient/search/$', 'views.patient_search'), #Post request
    
      
    url(r'^documentation/$', 'views.documentation'),
)
