#!/usr/bin/python
#-*- coding: UTF-8 -*-
#
# @file: process_stat.py
#    statistics by process
#
# @author: ZhangLiang, 350137278@qq.com
#
#
# @create: 2018-06-25
#
# @update: 2018-06-25 14:50:03
#
########################################################################
import os, sys, time, yaml, io, random, inspect

from time import strftime, localtime
from datetime import timedelta, date
import calendar

import multiprocessing
from multiprocessing import Process, Manager

########################################################################
import common
import utils.utility as util
import utils.evntlog as elog

# ! DO NOT CHANGE BELOW !
logger_module_name, _ = os.path.splitext(os.path.basename(inspect.getfile(inspect.currentframe())))

########################################################################

class ProcessStat(object):

    def __init__(self, **kwargs):
        self.reportInterval = int(kwargs['reportInterval'])
        self.statDict = Manager().dict()
        self.startTime = time.time()
        pass


    def __del__(self):
        self.final()
        pass


    def final(self):
        pass


    def start(self, pname):
        elog.info("start %s on %s", pname, util.nowtime())
        self.statDict[ pname + ":start-time" ] = time.time()
        pass


    def stop(self, pname):
        elog.info("stop %s on %s", pname, util.nowtime())
        self.statDict[pname + ':stop-time'] = time.time()
        pass


    def statistic(self, pname, key, value):
        statKey = "%s:stats:%s" % (pname, key)
        newValue = self.statDict.get(statKey, 0) + value
        self.statDict[statKey] = newValue
        pass


    def elapsedSeconds(self, pname = None):
        if pname:
            t0 = self.statDict.get(pname + ":start-time", self.startTime)
            return int(time.time() - t0)
        else:
            return int(time.time() - self.startTime)


    def printReport(self, procNameList, title):
        totalSeconds = self.elapsedSeconds()

        elog.force_clean("[%s Report by Process (%d seconds)]", title, totalSeconds)
        elog.force_clean("---------------------------------------")
        elog.force_clean("process        key             value")
        elog.force_clean("---------------------------------------")

        keyStatsTotal = {}

        for pname in procNameList:
            for statKey, statValue in self.statDict.items():
                keys = statKey.split(':')

                if keys[0] == pname and keys[1] == 'stats':
                    key = keys[2]

                    elog.force_clean("%s      %s      %d", pname, key, statValue)

                    totalValue = keyStatsTotal.get(key, 0) + statValue
                    keyStatsTotal[key] = totalValue

        elog.force_clean("=======================================")

        if len(procNameList) > 0:
            for (key, total) in keyStatsTotal.items():
                elog.force_clean("%s=%d (avg %d per second)", key, total, int(total/totalSeconds))
        pass
