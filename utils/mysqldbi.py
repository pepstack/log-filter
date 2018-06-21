#!/usr/bin/python
#-*- coding: UTF-8 -*-
#
# @filename: mysqldbi.py
#
# @author: zhangliang 张亮
# @since: 2013-11-15
# @version: 2018-06-21 13:35:41
#
# -- Install MySQLdb for python on ubuntu:
#   $ sudo apt-get install python-mysqldb
################################################################################
import os, sys, time, datetime, codecs
import MySQLdb
import MySQLdb.cursors


def error(s):
    print '\033[31m[MySQLdb ERROR] %s\033[31;m' % s

def warn(s):
    print '\033[33m[MySQLdb WARN] %s\033[33;m' % s


class MySQLDBInstance:
    VERSION = "1.0.1"

    CHARSET_DEFAULT = "utf8"
    HOST_DEFAULT = "127.0.0.1"
    PORT_DEFAULT = 3306
    MODE_DEFAULT = 0
    BATCH_DEFAULT = 20

    CURSOR_MODE_DEFAULT = 0       # used for inserting data into dest db
    CURSOR_MODE_SSCURSOR = 1      # used for fetching big data from source db
    CURSOR_MODE_DICTCURSOR = 2
    CURSOR_MODE_SSDICTCURSOR = 3

    def __init__(self, **kwargs):
        self.conn = None

        self.db = kwargs.get('db')
        self.user = kwargs.get('user')
        self.passwd = kwargs.get('passwd')

        # host
        if kwargs.has_key('host'):
            self.host = kwargs['host']
        else:
            self.host = MySQLDBInstance.DEFAULT_HOST

        # port
        if kwargs.has_key('port'):
            self.port = int(kwargs['port'])
        else:
            self.port = MySQLDBInstance.PORT_DEFAULT

        # charset
        if kwargs.has_key('charset'):
            self.charset = kwargs['charset']
        else:
            self.charset = MySQLDBInstance.CHARSET_DEFAULT

        # mode
        if kwargs.has_key('mode'):
            self.curmode = int(kwargs['mode'])
        else:
            self.curmode = MySQLDBInstance.MODE_DEFAULT

        # batchrows
        if kwargs.has_key('batch'):
            self.batchrows = kwargs['batch']
        else:
            self.batchrows = MySQLDBInstance.BATCH_DEFAULT

        self.cursors = {}


    def __del__(self):
        self.disconnect()


    def connect(self):
        dbconn = None

        if not self.conn:
            if self.curmode == MySQLDBInstance.CURSOR_MODE_DEFAULT:
                try:
                    dbconn = MySQLdb.connect(
                        host = self.host,
                        port = self.port,
                        db = self.db,
                        user = self.user,
                        passwd = self.passwd,
                        charset = self.charset)
                except MySQLdb.Error, e:
                    error('%d: %s' % (e.args[0], e.args[1]))
                finally:
                    self.conn = dbconn
            elif self.curmode == MySQLDBInstance.CURSOR_MODE_SSCURSOR:
                try:
                    dbconn = MySQLdb.connect(
                        host = self.host,
                        port = self.port,
                        db = self.db,
                        user = self.user,
                        passwd = self.passwd,
                        charset = self.charset,
                        cursorclass = MySQLdb.cursors.SSCursor)
                except MySQLdb.Error, e:
                    error('%d: %s' % (e.args[0], e.args[1]))
                finally:
                    self.conn = dbconn
        else:
            warn('MySQLdb Connection is already connected')

        return self.conn


    def checkConnection(self):
        if not self.conn:
            raise Exception('MySQLdb Connection not available')


    def disconnect(self):
        if self.conn:
            try:
                for cursorName in self.cursors:
                    cursor = self.cursors[cursorName]
                    self.cursors[cursorName] = None
                    if cursor:
                        cursor.close()
                self.cursors.clear()
                self.conn.close()
            except MySQLdb.Error, e:
                error('%d: %s' % (e.args[0], e.args[1]))
            finally:
                self.conn = None


    def getCursor(self, cursorName = "DEFAULT", cursorMode = CURSOR_MODE_DEFAULT):
        cur = self.cursors.get(cursorName)
        if not cur:
            if not cursorMode:
                cur = self.conn.cursor()
            elif cursorMode == CURSOR_MODE_DEFAULT:
                cur = self.conn.cursor(MySQLdb.cursors.Cursor)
            elif cursorMode == CURSOR_MODE_SSCURSOR:
                cur = self.conn.cursor(MySQLdb.cursors.SSCursor)
            elif cursorMode == CURSOR_MODE_DICTCURSOR:
                cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
            elif cursorMode == CURSOR_MODE_SSDICTCURSOR:
                cur = self.conn.cursor(MySQLdb.cursors.SSDictCursor)

            if cur:
                self.cursors[cursorName] = cur
        return cur


    def showCreateTable(self, db, table):
        cur = self.getCursor()
        cur.execute("SHOW CREATE TABLE `%s`.`%s`;" % (db, table))
        rs = cur.fetchall()
        return rs[0][1]


    def tableComment(self, db, table):
        cur = self.getCursor()
        cur.execute("SELECT TABLE_COMMENT FROM information_schema.TABLES " + \
            "WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s';" % (db, table))
        rs = cur.fetchall()
        return rs[0][0]


    def descTable(self, db, table):
        cur = self.getCursor()
        cur.execute("DESC `%s`.`%s`;" % (db, table))
        rs = cur.fetchall()
        cols = []
        for row in rs:
            col_dic = {
                'field': row[0],
                'type': row[1],
                'null': row[2],
                'key': row[3],
                'default': row[4],
                'extra': row[5]
            }
            cols.append(col_dic)
        return cols


    def descColumns(self, db, table):
        cur = self.getCursor()
        descSql = '''SELECT
        TABLE_CATALOG,
        TABLE_SCHEMA,
        TABLE_NAME,
        COLUMN_NAME,
        ORDINAL_POSITION,
        COLUMN_DEFAULT,
        IS_NULLABLE,
        DATA_TYPE,
        CHARACTER_MAXIMUM_LENGTH,
        CHARACTER_OCTET_LENGTH,
        NUMERIC_PRECISION,
        NUMERIC_SCALE,
        CHARACTER_SET_NAME,
        COLLATION_NAME,
        COLUMN_TYPE,
        COLUMN_KEY,
        EXTRA,
        PRIVILEGES,
        COLUMN_COMMENT
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s';''' % (db, table)
        cur.execute(descSql)
        rs = cur.fetchall()
        coldefs = []
        for row in rs:
            coldef_dic = {
                'table_catalog': row[0],
                'table_schema': row[1],
                'table_name': row[2],
                'column_name': row[3],
                'ordinal_position': row[4],
                'column_default': row[5],
                'is_nullable': row[6],
                'data_type': row[7],
                'character_maximum_length': row[8],
                'character_octet_length': row[9],
                'numeric_precision': row[10],
                'numeric_scale': row[11],
                'character_set_name': row[12],
                'collation_name': row[13],
                'column_type': row[14],
                'column_key': row[15],
                'extra': row[16],
                'privileges': row[17],
                'column_comment': row[18]
            }

            # since mysql is absent of boolean type, here fix it:
            if coldef_dic['column_type'] == 'tinyint(1)':
                coldef_dic['data_type'] = 'boolean'

            coldefs.append(coldef_dic)
        return coldefs


    def tableConstraints(self, db, table):
        cur = self.getCursor()
        cur.execute("SELECT * FROM information_schema.TABLE_CONSTRAINTS " + \
            "WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s';" % (db, table))
        rs = cur.fetchall()
        cons = []
        for row in rs:
            con_dic = {
                'constraint_catalog': row[0],
                'constraint_schema': row[1],
                'constraint_name': row[2],
                'table_schema': row[3],
                'table_name': row[4],
                'constraint_type': row[5]
            }
            cons.append(con_dic)
        return cons


    # get all tables of given db
    def dbTables(self, db):
        cur = self.getCursor()
        count = cur.execute("SELECT `TABLE_NAME` FROM `information_schema`.`TABLES` " + \
            "WHERE `TABLE_SCHEMA`=%s", (db))
        tables = []
        while count > 0:
            count = 0
            rs = cur.fetchmany(self.batchrows)
            for row in rs:
                tables.append(row[0])
                count = count + 1
        return tables

    # test if table exists in given db
    def tableExists(self, db, table):
        cur = self.getCursor()
        count = cur.execute("SELECT `TABLE_NAME` FROM `information_schema`.`TABLES` " + \
            "WHERE `TABLE_SCHEMA`=%s AND `TABLE_NAME`=%s", (db, table))
        return count > 0


    def commit(self):
        self.conn.commit()
        pass
