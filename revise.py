#!/usr/bin/python
#-*- coding: UTF-8 -*-
#
# @file: source_revise.py
#
#   update time and version for source files
#
#   源代码文件修改之后，运行本脚本会自动更新代码的版本和时间
#
# @version: 1.0.0
# @create: 2018-05-18 14:00:00
# @update: 2018-06-22
#
#######################################################################
import os, sys, stat, signal, shutil, inspect, commands, time, datetime

import optparse, ConfigParser

#######################################################################
# application specific
APPFILE = os.path.realpath(sys.argv[0])
APPPATH = os.path.dirname(APPFILE)
APPNAME,_ = os.path.splitext(os.path.basename(APPFILE))
APPVER = "1.0.0"
APPHELP = "update time and version for source files"

# import your local modules
import utils.utility as util
import utils.evntlog as elog
#######################################################################
global_counter = 0


ignored_dirs_list = [ ".git" ]

ignored_files_list = [ ".gitignore" ]


source_filters_dict = {
    'java': ['.java', '.properties', '.xml', '.jsp', '.cresql'],
    'c': ['.h', '.c'],
    'cpp': ['.h', '.cpp', '.cxx', '.hpp'],
    'python': ['.py'],
    'php': ['.php'],
    'html': ['.html', '.htm', '.js', '.css'],
    'sh': ['.sh'],
    'shell': ['.sh'],
    'bash': ['.sh']
}


#######################################################################

def parse_strarr(str):
    strs = []
    if not str:
        return strs
    arr = str.split(',')
    for src in arr:
        s = src.strip(" '")
        if len(s) > 0:
            strs.append(s)
    return strs


def update_file(pathfile, filename, fstat, author, curtime):
    global global_counter
    #
    # @file: updt.py
    # @version: 2018-05-18 15:54:46
    # @update: 2018-05-18 15:54:46
    #
    ct = time.localtime(fstat.st_ctime)
    mt = time.localtime(fstat.st_mtime)
    at = time.localtime(fstat.st_atime)

    cts = time.strftime('%Y-%m-%d %H:%M:%S', ct)
    mts = time.strftime('%Y-%m-%d %H:%M:%S', mt)
    ats = time.strftime('%Y-%m-%d %H:%M:%S', at)

    cmds = [
        "sed -i 's/@file:.*/@file: %s/' %s" % (filename, pathfile),
        "sed -i 's/@create: $create$.*/@create: %s/' %s" % (cts, pathfile),
        "sed -i 's/@version:.*/@version: %s/' %s" % (mts, pathfile),
        "sed -i 's/@update:.*/@update: %s/' %s" % (mts, pathfile)
    ]

    if author:
        cmds.append("sed -i 's/@author: $author$.*/@author: %s/' %s" % (author, pathfile))

    cmds.append("sed -i 's/ *$//' %s" % pathfile)

    for cmd in cmds:
        (retcode, retstring) = commands.getstatusoutput(cmd)

    os.utime(pathfile, (fstat.st_atime, fstat.st_mtime))

    global_counter = global_counter + 1
    print "[%d] updated: %s" % (global_counter, pathfile)
    pass


def sweep_path(path, file_exts, recursive, author, curtime):
    filelist = os.listdir(path)
    filelist.sort(key=lambda x:x[0:20])

    for f in filelist:
        try:
            pf = os.path.join(path, f)

            fs = os.stat(pf)
            
            mod = fs.st_mode

            if stat.S_ISDIR(mod):
                # is dir
                if f not in ignored_dirs_list:
                    if util.dir_exists(pf):
                        if recursive:
                            sweep_path(pf, file_exts, recursive, author, curtime)
                            pass
                pass
            elif stat.S_ISREG(mod):
                # is file
                if f not in ignored_files_list:
                    ignored = False

                    if not util.file_exists(pf) or pf == APPFILE or f == "__init__.py":
                        ignored = True

                    if not ignored:
                        _, ext = os.path.splitext(f)
                        if ext in file_exts:
                            update_file(pf, f, fs, author, curtime)
                            pass
        except OSError as e:
            elog.warn("%r: %s" % (e, pf))
        
    pass


#######################################################################
# main entry function
#
def main(parser):
    (options, args) = parser.parse_args(args=None, values=None)

    # 当前脚本绝对路径
    abspath = util.script_abspath(inspect.currentframe())

    if not options.path:
        elog.warn("No path specified. using: -P, --path=PATH")
        exit(-1)

    # 取得配置项 options.path 的绝对路径
    root_path = util.source_abspath(APPFILE, options.path, abspath)

    # 取得文件扩展名数组
    file_exts = []
    filters = parse_strarr(options.filter)
    for filter in filters:
        if filter.startswith('.'):
            if filter not in file_exts:
                file_exts.append(filter)

        if filter in source_filters_dict.keys():
            for ext in source_filters_dict[filter]:
                if ext not in file_exts:
                    file_exts.append(ext)

    curtime = time.time()

    elog.force("path:      %r", root_path)
    elog.force("exts:      %r", file_exts)
    elog.force("recursive: %r", options.recursive)
    elog.force("timestamp: %r", curtime)

    if options.author:
        elog.force("author:    %r", options.author)

    sweep_path(root_path, file_exts, options.recursive, options.author, curtime)

    pass


#######################################################################
# Usage:
#
#   $ ./updt.py -P /path/to/file -R
#   $ ./updt.py -P /path/to/file -F "java"
#
if __name__ == "__main__":
    parser, group, optparse = util.use_parser_group(APPNAME, APPVER, APPHELP,
        '%prog [Options]')

    group.add_option("-P", "--path",
        action="store", dest="path", type="string", default=None,
        help="specify path to update.",
        metavar="PATH")

    group.add_option("-F", "--filter",
        action="store", dest="filter", type="string", default=".md,java,c,cpp,python,php,shell,bash,sh",
        help="update file filter.",
        metavar="FILTER")

    group.add_option("-U", "--updver",
        action="store", dest="updver", type="string", default=None,
        help="update file version.",
        metavar="VERSION")

    group.add_option("-R", "--recursive",
        action="store_true", dest="recursive", default=False,
        help="if update files in child paths.",
        metavar="recursive")

    group.add_option("-A", "--author",
        action="store", dest="author", type="string", default=None,
        help="update author for file.",
        metavar="PATH")

    main(parser)

    sys.exit(0)
