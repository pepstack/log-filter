#!/usr/bin/python
#-*- coding: UTF-8 -*-
#
# @file: utility.py
#   Python utility functions
#
# @create: 2015-12-02
#
# @update: 2018-06-27 10:40:43
#
#######################################################################
import os, errno, sys, shutil, inspect, select, commands
import signal, threading
import codecs, tempfile, fileinput

import hashlib
import itertools
import binascii

import time, datetime
from datetime import datetime, timedelta

import optparse, ConfigParser


exit_event = threading.Event()

reload(sys)
sys.setdefaultencoding('utf-8')

#######################################################################

def error(s):
    print '\033[31m[ERROR] %s\033[0m' % s

def info(s):
    print '\033[32m[INFO] %s\033[0m' % s

def info2(s):
    print '\033[34m[INFO] %s\033[0m' % s

def warn(s):
    print '\033[33m[WARNING] %s\033[0m' % s

#######################################################################

# returns current datetime as string
def nowtime(dtfmt = '%Y-%m-%d %H:%M:%S'):
    return time.strftime(dtfmt, time.localtime(time.time()))


def datetime_to_string(dt = None, ms_width = 0, setdefault = datetime.now()):
    if dt is None:
        dt = setdefault
    dtfmt = '%Y-%m-%d %H:%M:%S.%f'
    y,m,d,H,M,S = dt.timetuple()[:6]
    ms = timedelta(microseconds = round(dt.microsecond/1000.0)*1000)
    ms_date = datetime(y, m, d, H, M, S) + ms
    return ms_date.strftime(dtfmt)[: (ms_width - 6)].strip('.')


def string_to_datetime(dtstr = None, setdefault = '9999-12-31 23:59:59.999999'):
    if dtstr is None:
        dtstr = setdefault
    if dtstr.rfind('.') == -1:
        dtfmt = '%Y-%m-%d %H:%M:%S'
    else:
        dtfmt = '%Y-%m-%d %H:%M:%S.%f'
    return datetime.strptime(dtstr, dtfmt)


# 分析路径字符串, 得到分组路径: [[group1],[group2],[group3]]
#  "/tmp/logstash/a.log"
#  "/tmp/logstash/{a.log,b.log}"
#  "/tmp/logstash/{a.log,b.log}:/tmp/logstash2/{c.log,d.log}"
#  "./stashcsv::/tmp/stash::/opt{a,b,c}"
#
def parse_path_groups(pathstr):
    pathgrps = pathstr.split('::')
    parsed_path_groups = []
    for grpstr in pathgrps:
        arrpaths = []
        for pl in grpstr.split(':'):
            s1, s2 = pl.find("{"), pl.find("}")
            if s1 > 1 and s2 > s1 + 1:
                path = pl[0 : s1].strip()

                for sub in pl[s1 + 1 : s2].split(','):
                    relp = os.path.join(path, sub.strip().rstrip('/'))
                    arrpaths.append(os.path.realpath(relp))
            elif s1 < 0 and s2 < 0:
                relp = pl.strip().rstrip('/')
                arrpaths.append(os.path.realpath(relp))
        parsed_path_groups.append(arrpaths)
    return parsed_path_groups


# get filename by minutes
#   t = "2018-12-31 23:59:59"
#
def name_by_split_minutes(dtstr, splitMinutes, prefixName = None, suffixName = None):
    if splitMinutes >= 60:
        # by hour
        SPLIT_HOURS = (splitMinutes + 59)/60
        dtstr = dtstr[:dtstr.find(':')]
        d = dtstr[:dtstr.find(' ')].replace('-', '').strip()
        h = int(dtstr[dtstr.find(' '):].strip())
        h = (h/SPLIT_HOURS) * SPLIT_HOURS

        if prefixName:
            if suffixName:
                return "%s%s%02d%s" % (prefixName, d, h, suffixName)
            else:
                return "%s%s%02d" % (prefixName, d, h)
        else:
            if suffixName:
                return "%s%02d%s" % (d, h, suffixName)
            else:
                return "%s%02d" % (d, h)
    else:
        m = int(dtstr[dtstr.find(':') + 1 : dtstr.rfind(':')])
        m = (m / splitMinutes) * splitMinutes
        dh = dtstr[ : dtstr.find(':')].replace(' ', '').replace('-', '').strip(' ')

        if prefixName:
            if suffixName:
                return "%s%s%02d%s" % (prefixName, dh, m, suffixName)
            else:
                return "%s%s%02d" % (prefixName, dh, m)
        else:
            if suffixName:
                return "%s%02d%s" % (dh, m, suffixName)
            else:
                return "%s%02d" % (dh, m)


#######################################################################

def sig_chld(signo, frame):
    pid, status = os.waitpid(-1, os.WNOHANG)
    if pid:
        error("child(%d) on signal(SIGCHLD)." % pid)
    pass


def sig_int(signo, frame):
    error("process(%d) on int signal(SIGINT)." % os.getpid())
    os.kill(os.getpid(), 9)
    pass


def sig_term(signo, frame):
    error("process(%d) on term signal(SIGTERM)." % os.getpid())
    os.kill(os.getpid(), 9)
    pass


def select_sleep(timeout_ms):
    select.select([], [], [], timeout_ms*0.001)
    pass


def is_exit_process(exit_queue, timeout_ms, exit_flag = 'EXIT'):
    from multiprocessing import Queue
    from Queue import Empty

    is_exit, arg = False, None

    try:
        if timeout_ms == 0:
            flag, arg = exit_queue.get_nowait()
        else:
            flag, arg = exit_queue.get(block=True, timeout=timeout_ms*0.001)

        if flag == exit_flag:
            is_exit = True
    except Empty:
        pass
    finally:
        return (is_exit, arg)


#######################################################################
# for case in switch(var):
#   if case("a"):
#       print "is a"
#       break
#   if case("b"):
#       print "is b"
#       break
#   else:
#       print "other"
#
class switch(object):
    def __init__ (self, value):
        self.value=value
        self.fall=False
    def __iter__ (self):
        yield self.match
        raise StopIteration
    def match (self, *args):
        if self.fall or not args:
            return True
        elif self.value in args:
            self.fall=True
            return True
        else:
            return False


#######################################################################

def use_parser_group(appname, appver, apphelp, usage='%prog [options] --arg1=VALUE1 --arg2=VALUE2'):
    print "\033[32m****************************************************************\033[32;m"
    print "\033[32m* %-60s *\033[32;m" % (appname + " version: " + appver)

    helps = apphelp.split('\n')
    for helpstr in helps:
        print "\033[32m* %-60s *\033[32;m" % helpstr

    print "\033[32m****************************************************************\033[32;m"

    parser = optparse.OptionParser(usage=usage,version="%prog " + appver)
    parser.add_option("-v", "--verbose",
                action="store_true", dest="verbose", default=True,
                help="be verbose (this is the default).")
    parser.add_option("-q", "--quiet",
                action="store_false", dest="verbose",
                help="quiet (no output).")
    group = optparse.OptionGroup(parser, appname, apphelp)
    parser.add_option_group(group)
    return (parser, group, optparse)

#######################################################################

def open_file(fname, mode='w+b', encoding='utf-8'):
    fd = codecs.open(fname, mode, encoding)
    return fd


def file_exists(pathfile):
    ret = False
    try:
        if pathfile and os.path.exists(pathfile) and os.path.isfile(pathfile):
            ret = True
    except:
        pass
    finally:
        return ret


def dir_exists(path):
    ret = False
    try:
        if path and os.path.isdir(path) and os.path.exists(path):
            ret = True
    except:
        pass
    finally:
        return ret


def close_file_nothrow(fd):
    if not fd is None:
        try:
            fd.close()
        except:
            pass


def remove_file_nothrow(fname):
    if file_exists(fname):
        try:
            os.remove(fname)
        except OSError:
            pass
        except:
            pass


def make_dirs_nothrow(path, mode=0755):
    try:
        os.makedirs(path, mode)
    except OSError as e:
        if e.errno == errno.EEXIST:
            # [Errno 17] File exists
            os.chmod(path, mode)
        else:
            # re-raise the exception
            raise


def file_size_nothrow(fname):
    size = -1
    try:
        size = os.stat(fname).st_size
    except:
        pass
    finally:
        return size


def file_mtime_nothrow(fname):
    mtime = 0
    try:
        mtime = os.stat(fname).st_mtime
    except:
        pass
    finally:
        return mtime


def read_first_line_nothrow(fname):
    fd, line = None, None
    try:
        fd = open(fname, "r")
        line = fd.readline().strip('\n')
    except:
        pass
    finally:
        close_file_nothrow(fd)
        return line


def write_first_line_nothrow(fname, line):
    fd, ret = None, False
    try:
        fd = open(fname, "w", 0)
        fd.write(line)
        ret = True
    except:
        pass
    finally:
        close_file_nothrow(fd)
        return ret


def relay_read_messages(pathfile, posfile, stopfile, chunk_size = 65536, read_maxsize = 16777216):
    infd, messages, last_position = None, [], 0

    if not file_exists(posfile):
        os.mknod(posfile)
        write_first_line_nothrow(posfile, str(last_position))

    if file_exists(posfile):
        last_position = int(read_first_line_nothrow(posfile))

    try:
        infd = open(pathfile, 'rb')

        position = last_position

        while position - last_position < read_maxsize:

            # 移动infd文件第position个字节处, 绝对位置
            infd.seek(position, 0)

            chunk = infd.read(chunk_size)

            if not chunk:
                # read EOF of source
                break
            else:
                start = 0
                end = start
                cbsize = len(chunk)

                for i in xrange(cbsize):
                    end = end + 1
                    if chunk[i] == '\n':
                        line = chunk[start : end]
                        position = position + end - start
                        start = end

                        msg = line.strip(" \r\n")
                        if msg and len(msg):
                            messages.append(msg)

                # 成功保存当前位置点
                if position > last_position:
                    write_first_line_nothrow(posfile, str(position))

            if file_exists(stopfile):
                break
    finally:
        close_file_nothrow(infd)
        pass

    return (messages, last_position)


def write_file(fd, encoding, format, *arg):
    fd.write(unicode((format % arg), encoding))
    pass


def write_file_utf8(fd, format, *arg):
    content = format % arg
    fd.write(unicode(content, 'utf-8'))
    pass


def remove_bom_header(filename):
    BOM = b'\xef\xbb\xbf'
    try:
        f = open(filename, 'rb')
        if f.read(3) == BOM:
            fbody = f.read()
            f.close()

            with open(filename, 'wb') as f:
                f.write(fbody)
    finally:
        f.close()


def add_bom_header(filename):
    BOM = b'\xef\xbb\xbf'
    try:
        f = open(filename, 'rb')
        if f.read(3) != BOM:
            f.close()
            f = open(filename, 'rb')
            fbody = f.read()
            f.close()
            with open(filename, 'wb') as f:
                f.write(BOM)
                f.write(fbody)
    finally:
        f.close()


# create file and write to it
#
def create_output_file(outfile, output_callback, param = None, fileExistedAsError=True):
    import os, tempfile, codecs, shutil
    # create temp file
    tmpfd = tempfile.NamedTemporaryFile(delete = False)
    tmpfname = tmpfd.name
    tmpfd.close()

    try:
        if fileExistedAsError:
            # failed if file existed
            if os.path.isfile(outfile):
                raise Exception(-1001, "File already existed: %s" % outfile)

        # create path if not exists
        path = os.path.dirname(outfile);
        if not os.path.exists(path):
            os.makedirs(path)

        fd = open_file(tmpfname)
        try:
            if output_callback:
                output_callback(fd, param)
        finally:
            fd.close();

        # copy tmpfile to destfile
        if os.path.basename(outfile) != '~':
            shutil.copy2(tmpfname, outfile)
            info("create file: %s" % outfile)
        else:
            info("create dir: %s" % os.path.dirname(outfile))
    except Exception as e:
        error("create_output_file() error(%d): %s" % (e.args[0], e.args[1]))
    finally:
        os.unlink(tmpfname)

#######################################################################

def script_abspath(frame=inspect.currentframe()):
    p = os.path.split(inspect.getfile( frame ))[0]
    absdir = os.path.realpath(os.path.abspath(p))
    return absdir


def script_abspath_parent(frame=inspect.currentframe()):
    return os.path.dirname(script_abspath(frame))


def include_dir(subdir=None, frame=inspect.currentframe()):
    # NOTES:
    # DO NOT USE __file__ !!!
    # dir = os.path.dirname(os.path.abspath(__file__))
    # __file__ fails if script is called in different ways on Windows
    # __file__ fails if someone does os.chdir() before
    # sys.argv[0] also fails because it doesn't not always contains the path
    #
    # realpath() will make your script run, even if you symlink it
    p = os.path.split(inspect.getfile( frame ))[0]
    incdir = os.path.realpath(os.path.abspath(p))
    if incdir not in sys.path:
        sys.path.insert(0, incdir)
    if subdir:
        # use this if you want to include modules from a subfolder
        incdir = os.path.realpath(os.path.abspath(os.path.join(p, subdir)))
        if incdir not in sys.path:
            sys.path.insert(0, incdir)

# cfgfile: full path for config file
# cfgitem: a config item in cfgfile might use abspath or relative path
def source_abspath(cfgfile, cfgitem=None, p=script_abspath()):
    dp = os.path.realpath(os.path.join(p, cfgfile))
    if cfgitem:
        return os.path.realpath(os.path.join(os.path.dirname(dp), cfgitem))
    else:
        return dp

#######################################################################
#enc = rc4('1234abcd', str(123))
#hstr = binascii.b2a_hex(enc)
#plain = rc4(binascii.a2b_hex(hstr), str(123))
#print plain, hstr
def rc4(data, key):
    x = 0
    box = range(256)
    for i in range(256):
        x = (x + box[i] + ord(key[i % len(key)])) % 256
        box[i], box[x] = box[x], box[i]
    x = 0
    y = 0
    out = []
    for char in data:
        x = (x + 1) % 256
        y = (y + box[x]) % 256
        box[x], box[y] = box[y], box[x]
        out.append(chr(ord(char) ^ box[(box[x] + box[y]) % 256]))
    return ''.join(out)


def md5sum(pathfile):
    with open(pathfile, 'rb') as fh:
        m = hashlib.md5()
        while True:
            chunk = fh.read(8192)
            if not chunk:
                break
            m.update(chunk)
        return m.hexdigest()

