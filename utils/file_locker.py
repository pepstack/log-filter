#!/usr/bin/python
#-*- coding: utf-8 -*-
# file_locker.py
#
# Author: zhangliang@shinezone.com
#
# Refer:
#   https://github.com/HiSPARC/station-software/blob/master/user/pythonshared/plock.py
#
# init created: 2016-05-25
# last updated: 2016-07-13
#
#######################################################################
import portalocker as plock


def close_file_nothrow(fd):
    if not fd is None:
        try:
            fd.close()
        except:
            pass


def remove_file_nothrow(fname):
    import os
    if file_exists(fname):
        try:
            os.remove(fname)
        except OSError:
            pass
        except:
            pass


class FileLocker(object):
    def __init__(self, filename):
        self.locked = False
        self.stream_lock = None

        try:
            # Prevent multiple extensions on the lock file (Only handles the normal "*.log" case.)
            if filename.endswith(".log"):
                self.lock_file = filename[:-4] + ".lock"
            else:
                self.lock_file = filename + ".lock"

            self.stream_lock = open(self.lock_file, "w")
            self.filename = filename
        except:
            stream_lock = self.stream_lock
            self.stream_lock = None
            close_file_nothrow(stream_lock)
        finally:
            if self.stream_lock:
                self.locked = False
        pass


    def __del__(self):
        self.cleanup()
        pass


    def cleanup(self):
        self.unlock()

        stream_lock = self.stream_lock
        self.stream_lock = None

        if stream_lock:
            close_file_nothrow(stream_lock)
            remove_file_nothrow(self.lock_file)
        pass


    def lock(self, nonblock = True):
        if self.stream_lock:
            try:
                if not self.locked:
                    if nonblock:
                        plock.lock(self.stream_lock, plock.LOCK_EX | plock.LOCK_NB)
                    else:
                        plock.lock(self.stream_lock, plock.LOCK_EX)

                    self.locked = True
                return self.locked
            except Exception as ex:
                # failed to lock file
                return False
        else:
            return False


    def unlock(self):
        if self.stream_lock:
            try:
                if self.locked:
                    plock.unlock(self.stream_lock)
                    self.locked = False
            except:
                pass

#######################################################################

