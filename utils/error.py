#!/usr/bin/python
#-*- coding: UTF-8 -*-
#
# error.py
#   user defined error
#
# init created: 2016-05-12
# last updated: 2016-05-12
#######################################################################

class Error(Exception):
    """Base class for exceptions in this module."""
    pass


class HttpError(Error):
    HTTP_OK = 200

    def __init__(self, **kwargs):
        self.statuscode = kwargs.get('statuscode')
        self.message = kwargs.get('message')
        self.requestid = kwargs.get('requestid')

        if self.statuscode is None:
            self.statuscode = HTTP_OK
        pass


    def __str__(self):
        ret = "HttpError"

        if self.message:
            if self.requestid:
                ret = "HttpError#%d - (%s): %s" % (self.statuscode, self.requestid, self.message)
            else:
                ret = "HttpError#%d - : %s" % (self.statuscode, self.message)
        else:
            if requestid:
                ret = "HttpError#%d - (%s)" % (self.statuscode, self.requestid)
            else:
                ret = "HttpError#%d" % (self.statuscode)

        return repr(ret)
