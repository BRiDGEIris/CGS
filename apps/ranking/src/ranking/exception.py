#!/usr/bin/env python

import logging
import socket

LOG = logging.getLogger(__name__)

from django.utils.translation import ugettext as _
from django.utils.encoding import smart_str


LOG = logging.getLogger(__name__)


def handle_rest_exception(e, msg):
    parent_ex = e.get_parent_ex()
    reason = None
    if hasattr(parent_ex, 'reason'):
        reason = parent_ex.reason
    if isinstance(reason, socket.error):
        LOG.error(smart_str('Could not connect to server: %s (%s)' % (reason[0], reason[1])))
        return {
            'status': -1,
            'errors': [_('Could not connect to server. %s (%s)') % (reason[0], reason[1])]
            }
    else:
        LOG.error(smart_str(msg))
        LOG.error(smart_str(e.message))
        return {
        'status': 1,
        'errors': [msg],
        'exception': str(e)
    }
    
    
