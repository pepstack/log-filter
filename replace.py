#!/usr/bin/python
#-*- coding: UTF-8 -*-
# replace.py
#
#    replace strings in all source files
#
# init created: 2016-05-13
# @modify: 2016-06-15 08:46:01
#
#######################################################################
import os, sys, signal, shutil, inspect, commands

import time, datetime

import optparse, ConfigParser

#######################################################################
# application specific
APPFILE = os.path.realpath(sys.argv[0])
APPPATH = os.path.dirname(APPFILE)
APPNAME,_ = os.path.splitext(os.path.basename(APPFILE))
APPVER = "1.0.2"
APPHELP = "control script for repl"

# import your local modules
import utils.utility as util
import utils.evntlog as elog

import logging
from logging.handlers import RotatingFileHandler

#######################################################################
only_exts = ['.java', '.h', '.c', '.cpp', '.cxx', '.txt']

ignore_dirs = ['utils', '.svn', '.git', '.mvn']

ignore_exts = ['.dtsx', '.doc', '.docx', '.log', '.xls', '.vsd', '.pptx',
    '.class', '.jar', '.war',
    '.tar', '.gz', '.tgz', '.bz2',
    '.egg', '.pyc',
    '.exe', '.obj', '.lib', '.suo',
    '.so', '.svn', '.git', '.gitignore']

ignore_files = ['repl.py']


def file_times(filename):
    try:
        fstat = os.stat(filename)
        ct= time.localtime(fstat.st_ctime)
        mt= time.localtime(fstat.st_mtime)
        cts = time.strftime('%Y-%m-%d %H:%M:%S', ct)
        mts = time.strftime('%Y-%m-%d %H:%M:%S', mt)
        return cts, mts
    except:
        return None


def sweep_dir(path, srcs, results):
    dname = os.path.basename(path)

    if dname not in ignore_dirs:
        filelist = os.listdir(path)
        filelist.sort(key=lambda x:x[0:20])

        for f in filelist:
            pf = os.path.join(path, f)

            if util.dir_exists(pf):
                sweep_dir(pf, srcs, results)
            elif util.file_exists(pf):
                _, ext = os.path.splitext(f)

                passed_filters = False

                if f not in ignore_files and ext not in ignore_exts:
                    passed_filters = True

                if len(only_exts) > 0:
                    if ext not in only_exts:
                        passed_filters = False

                if passed_filters:
                    fd = None
                    try:
                        fd = open(pf, 'r')
                        lines = fd.readlines()
                        lineno = 0
                        for line in lines:
                            lineno += 1

                            for src in srcs:
                                if line.find(src) != -1:
                                    elog.info("found '%s': [%s:%d]", src, os.path.relpath(pf, APPPATH), lineno)
                                    elog.force_clean("%s", line)
                                    if pf not in results:
                                        results.append(pf)
                    except:
                        elog.error("%r: %s", sys.exc_info(), pf)
                    finally:
                        util.close_file_nothrow(fd)
                else:
                    #elog.warn("ignore file: %s", pf)
                    pass
    else:
        elog.warn("ignore path: %s", path)
        pass


def parse_strarr(instrs):
    strs = []
    if not instrs:
        return strs
    if instrs.find('[') != -1 and instrs.find(']') != -1:
        srcarr = instrs.split(',')
        for src in srcarr:
            s = src.strip(" []'")
            strs.append(s)
    else:
        strs.append(instrs.strip(" []'"))
    return strs


def main(parser):
    (options, args) = parser.parse_args(args=None, values=None)

    # 子进程退出后向父进程发送的信号
    ## signal.signal(signal.SIGCHLD, util.sig_chld)

    # 进程退出信号
    signal.signal(signal.SIGINT, util.sig_int)
    signal.signal(signal.SIGTERM, util.sig_term)

    # 当前脚本绝对路径
    abspath = util.script_abspath(inspect.currentframe())

    if not options.path:
        options.path = os.getcwd()
        elog.warn("No path specified. using current working dir or using: --path='PATH'")

    if not options.srcs:
        elog.error("No source strings specified. using: --srcs='STRINGS'")
        sys.exit(1)

    if not options.dsts:
        elog.warn("No destigation strings specified. using: --dsts='STRINGS'")

    # 取得配置项options.path的绝对路径
    root_path = util.source_abspath(APPFILE, options.path, abspath)
    srcs = parse_strarr(options.srcs)
    dsts = parse_strarr(options.dsts)

    elog.force("path: %s", root_path)
    elog.force("sour = %r", srcs)
    elog.force("dest = %r", dsts)

    founds = []
    sweep_dir(root_path, srcs, founds)
    elog.force("Total %d files found", len(founds))

    if len(founds) > 0:
        if options.replace:
            if len(srcs) == len(dsts):
                for pf in founds:
                    ctime, mtime = None, None
                    fts = file_times(pf)
                    if fts:
                        ctime, mtime = fts
                    else:
                        elog.warn("missing file: %s", pf)
                        continue

                    for i in range(0, len(srcs)):
                        srcstr = srcs[i]
                        dststr = None
                        if i < len(dsts):
                            dststr = dsts[i]

                        if dststr:
                            ds = dststr.replace('$(mtime)', mtime).replace('$(ctime)', ctime)

                            if options.whole_line:
                                cmd = "sed -i 's/%s.*/%s/g' '%s'" % (srcstr, ds, pf)
                            else:
                                cmd = "sed -i 's/%s/%s/g' '%s'" % (srcstr, ds, pf)

                            elog.debug(cmd)
                            (status, output) = commands.getstatusoutput(cmd)
                            if status != 0:
                                elog.error("failed to command: \"%s\", output: %r", sed, output)

                elog.force("Total %d files replaced", len(founds))
            else:
                elog.error("Failed to replace for srcs(%r) mismatched with dsts(%r)", srcs, dsts)
                pass
        else:
            elog.warn("No files to be replaced. Using: --replace")
            pass
    pass


########################################################################
# Usage:
#
#   $ ./repl.py -P './' -S "__cleanup" -D "___cleanup"
#
# 替换文件的创建时间
#   $ ./scripts/repl.py -S "@create:" -D "@create: $(ctime)" -L -R
#
# 替换文件的修改时间
#   $ ./scripts/repl.py -S "@modify:" -D "@modify: $(mtime)" -L -R
#
if __name__ == "__main__":
    parser, group, optparse = util.use_parser_group(APPNAME, APPVER, APPHELP,
        '%prog [Options]')

    group.add_option("-P", "--path",
        action="store", dest="path", type="string", default=None,
        help="Specifies root path to replace",
        metavar="PATH")

    group.add_option("-S", "--srcs",
        action="store", dest="srcs", type="string", default=None,
        help="Specifies source strings to be replaces",
        metavar="SRCS")

    group.add_option("-D", "--dsts",
        action="store", dest="dsts", type="string", default=None,
        help="Specifies dest strings as replacement",
        metavar="DSTS")

    group.add_option("-L", "--whole-line",
        action="store_true", dest="whole_line", default=False,
        help="replace source whole line by dst")

    group.add_option("-R", "--replace",
        action="store_true", dest="replace", default=False,
        help="force to replace !")

    main(parser)
    sys.exit(0)
