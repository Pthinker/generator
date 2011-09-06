#!/usr/bin/env python2.5
"""
Requirements: apache, mod_python, python2.5, MySQLdb

Sample of virtual host file:
<Directory /var/www/generator/>
    Options MultiViews
    Order allow,deny
    allow from all
    AddHandler mod_python .py
    PythonHandler webservice
    PythonAuthenHandler webservice
    AuthType Basic
    AuthName "Restricted Area"
    require valid-user
    AuthBasicAuthoritative Off
    PythonDebug On
    PythonOption mod_python.legacy.importer *
</Directory>
<VirtualHost *:80>
    DocumentRoot /var/www/generator/
    ServerName generator
    ServerAlias www.generator
</VirtualHost>


Needed modules: apache, util (from mod_python)
Local modules: simplejson
    


"""
import os
import sys
import datetime
import MySQLdb
path = os.path.abspath(os.path.dirname(__file__))
sys.path.append(path)
import simplejson
import logging
import logging.handlers
import os, tempfile
from datetime import date

class MLogger:
    def __init__(self, name):
        self._logger = logging.getLogger(name)
        self._logger.setLevel(logging.INFO)

        log_name = 'log-%s-.txt' % date.today()

        full_log_dir = '/var/www/generator/log/'#os.path.join(os.path.split(os.path.split(os.path.split(os.path.abspath( __file__ ))[0])[0])[0], 'log')
        full_log_name = os.path.join(full_log_dir, log_name)

        try:
            os.chmod(full_log_name, 0777)
        except OSError:
            pass
        try:
            self._ch = logging.FileHandler(full_log_name)
        except IOError:
            tmp = tempfile.mkstemp(prefix='log_', dir = full_log_dir)
            self._ch = logging.FileHandler(tmp[1])

        self._formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s","%Y-%m-%d %H:%M:%S")
        self._ch.setFormatter(self._formatter)
        self._logger.addHandler(self._ch)
    def get_logger(self):
        return self._logger

"""
local testing data
"""


#main date format
datetime_format = '%Y-%m-%d %H:%M:%S'
date_format = '%Y-%m-%d'

def unformat_date(var):
    """
    unformat string to datetime
    """
    date = None
    if var:
        try:
            date = datetime.datetime.strptime(var, datetime_format)
        except:
            try:
                date = datetime.datetime.strptime(var, date_format)
            except:
                # cannot format it
                pass
    return date

def format_date(var):
    """
    unformat datetime to string
    """
    date = None
    if var:
        try:
            date = datetime.datetime.strftime(var, datetime_format)
        except:
            # cannot format it
            pass
    return date


"""
Login and password to access this script
"""
web_service_credentials = {'username': 'user',
                      'password': ''
                       #'password': 'U2FsdGVkX19z/09S2MlKaiqCS3YmkwcCnOPqnFkX1Yc='
                    }
reports = {27: {'data_fetch_command_sql':
                    """
                        SELECT calendar_date 'Order Date',
                            sum(if( country = 'United States', total_amount, 0)) 'US order Volume (US$)',
                            sum(if( country = 'United States', 0, total_amount)) 'Intl order Volume (US$)',
                            sum(total_amount) 'Total order Volume (US$)'
                        FROM daily_order_summary
                        WHERE calendar_date > date(%(measurement_time)s) - INTERVAL 60 DAY
                            AND channel = %(channel)s
                        GROUP BY 1
                        ORDER BY 1 DESC
                    """,
                'measurement_time_fetch_command_sql':
                    """
                    SELECT DISTINCT calendar_date
                        FROM demo.daily_order_summary
                    WHERE calendar_date < date(now())
                        AND calendar_date > date(%(last_measurement_time)s)
                    """
                }
            }


field_type = {
    0: 'DECIMAL',
    1: 'TINY',
    2: 'SHORT',
    3: 'LONG',
    4: 'FLOAT',
    5: 'DOUBLE',
    6: 'NULL',
    7: 'TIMESTAMP',
    8: 'LONGLONG',
    9: 'INT24',
    10: 'DATE',
    11: 'TIME',
    12: 'DATETIME',
    13: 'YEAR',
    14: 'NEWDATE',
    15: 'VARCHAR',
    16: 'BIT',
    246: 'NEWDECIMAL',
    247: 'INTERVAL',
    248: 'SET',
    249: 'TINY_BLOB',
    250: 'MEDIUM_BLOB',
    251: 'LONG_BLOB',
    252: 'BLOB',
    253: 'VAR_STRING',
    254: 'STRING',
    255: 'GEOMETRY' }

simple_field_type = {
    0: 'DECIMAL',
    1: 'INTEGER',
    2: 'INTEGER',
    3: 'INTEGER',
    4: 'DECIMAL',
    5: 'DECIMAL',
    6: 'TEXT',
    7: 'DATE',
    8: 'INTEGER',
    9: 'INTEGER',
    10: 'DATE',
    11: 'DATE',
    12: 'DATE',
    13: 'DATE',
    14: 'DATE',
    15: 'NVARCHAR',
    16: 'INTEGER',
    246: 'DECIMAL',
    247: 'TEXT',
    248: 'TEXT',
    249: 'TEXT',
    250: 'TEXT',
    251: 'TEXT',
    252: 'TEXT',
    253: 'NVARCHAR',
    254: 'NVARCHAR',
    255: 'TEXT' }

SEGMENT_NAME = 'channel'

class MysqlConnect(object):
    error = ''
    connection = None
    headers = []
    #headers_types = []
    result = None
    rows = []
    def __init__(self, *args, **kargs):
        self.info = {
                     'host': 'localhost',
                     'user': 'generators',
                     'passwd': 'p0rtal',
                     'db': 'demo',
                     'port': 3306,
                     'use_unicode': True,
                     'charset': 'utf8'
                     }
        if kargs.has_key('host'):
            self.info['host'] = kargs['host']
        if kargs.has_key('user'):
            self.info['user'] = kargs['user']
        if kargs.has_key('passwd'):
            self.info['passwd'] = kargs['passwd']
        if kargs.has_key('db'):
            self.info['db'] = kargs['db']
        if kargs.has_key('port'):
            self.info['port'] = int(kargs['port'])

    def connect(self):
        try:
            self.connection = MySQLdb.connect(*[], **self.info)
            return True
        except MySQLdb.Error, e:
            self.error = "%d %s" % (e.args[0], e.args[1])
        except Exception, e:
            self.error = e
        return False

    def close(self):
        if self.connection is not None:
            try:
                self.connection.close()
            except Exception, e:
                pass

    def query(self, query, params):
        try:
            cursor = self.connection.cursor(MySQLdb.cursors.Cursor)
            cursor.execute(query, params)
        except MySQLdb.Error, e:
            self.error = "%d %s" % (e.args[0], e.args[1])
            return False
        try:
            self.result = {'header': [{'name': header[0], 'type': simple_field_type[header[1]]} for header in cursor.description],
                      'data': []}

            records = cursor.fetchall()

            for record in records:
                row = []
                for i, item in enumerate(record):
                    if self.result['header'][i]['type'] == 'DATE':
                        item = item.strftime(datetime_format)
                    else:
                        item = unicode(item)
                    row.append(item)
                self.result['data'].append(row)
            #self.json_result = simplejson.dumps(result)
            return True
        except Exception, e:
            self.error = e
        return False

def is_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

def authenhandler(req):
    pw = req.get_basic_auth_pw()
    user = req.user
    if user == web_service_credentials['username'] and (
            (web_service_credentials['password'] and pw == web_service_credentials['password']) or not web_service_credentials['password']):
       return apache.OK
    else:
       return apache.HTTP_UNAUTHORIZED

def handler(req):
    """
    Binds handler routing
    """
    req.log_error('handler')
    req.content_type = 'application/json'
    #req.content_type = 'text/html'
    req.send_http_header()

    form = util.FieldStorage(req, keep_blank_values=1)
    process(form, req, ret_answer)
    return apache.OK


def ret_answer(ret, req):
    """
    Formats answer to json answer and returns to apache
    """
    #req.write(simplejson.dumps(ret, indent=4))
    req.write(simplejson.dumps(ret))
    return apache.OK

def print_answer(ret, req):
    """
    Print answer to stdout. For test purposes.
    """
    print simplejson.dumps(ret,4)
    pass
    

def process(form, req, ret_answer):
    """
    Main routine
    """

    # empty answer dict
    #ret = {'error': ''}
    ret = {}

    # check for last_measurement_time field
    if 'measurement_time' in form:
        form['last_measurement_time'] = None

    log = MLogger('webservice')
    logger = log.get_logger()
    #logger.info('before elem id checks')

    # check for element_id field
    if 'element_id' not in form:
        ret['error'] = 'ERROR. element_id is not set'
        ret_answer(ret, req)
        return

    # check if element_id is correct
    if not is_int(form['element_id']) or int(form['element_id']) not in reports:
        ret['error'] = 'element_id is incorrect %s ' % form['element_id']
        ret_answer(ret, req)
        return

    element_id = int(form['element_id'])

    # get mysql connection
    outer_conn = MysqlConnect()
    if not outer_conn.connect():
        ret['error'] = "ERROR. Cannot connect to db: %s" % outer_conn.error
        ret_answer(ret, req)
        return

    if 'command' in form and form['command'] == 'get_measurement_times':
        if 'last_measurement_time' in form and form['last_measurement_time']:
            last_meas_time = unformat_date(form['last_measurement_time'])
        else:
            last_meas_time = datetime.datetime(1900, 1, 1, 0, 0, 0)

        if not last_meas_time:
            last_meas_time = datetime.datetime(1900, 1, 1, 0, 0, 0)

        query = reports[element_id]['measurement_time_fetch_command_sql']
        params = {'last_measurement_time': last_meas_time}
    else:
        # check for segment value substitution
        segment_value = ''
        if SEGMENT_NAME in form:
            segment_value = unicode(form[SEGMENT_NAME])

        if not segment_value:
            ret['error'] = "ERROR. segment_value is not specified"
            ret_answer(ret, req)
            return

#        # check for segment names substitution
#        segment_name = ''
#        if 'segment_name' in form:
#            segment_name = unicode(form['segment_name'])
#
#        if not segment_name:
#            ret['error'] = "ERROR. segment_name is not specified"
#            ret_answer(ret, req)
#            return
        
        # check for measurement time
        #if 'measurement_time' in form and form['measurement_time']:
        #    meas_time = unformat_date(form['measurement_time'])
        if 'last_measurement_time' in form and form['last_measurement_time']:
            meas_time = unformat_date(form['last_measurement_time'])
        else:
            meas_time = None

        if not meas_time:
            ret['error'] = "ERROR. Measurement time is required"
            ret_answer(ret, req)
            return
        query = reports[element_id]['data_fetch_command_sql']
        params = {'measurement_time': meas_time, SEGMENT_NAME: segment_value}

    if not outer_conn.query(query, params):
        ret['error'] = "ERROR. Cannot execute query: %s" % outer_conn.error
        ret_answer(ret, req)
        return

    result = outer_conn.result
    
    if not result:
        ret['error'] = "ERROR. Source db returned empty result"
        ret_answer(ret, req)
        return

    ret['header'] = result['header']
    ret['data'] = result['data']

    ret_answer(ret, req)
    return


if __name__ == "__main__":
    """
    for testing from bash
    """

    form = {'element_id': 27, 'username': u'user', 'meas_time': '', 'segment_value': u'corporate sales', 'segment_name': u'channel', 'last_measurement_time': None, 'password': ''}
    if len(sys.argv) >= 3:
        form['command'] = 'get_measurement_times'
        form['last_measurement_time'] = sys.argv[2]
    elif len(sys.argv) >= 2:
        form['measurement_time'] = sys.argv[1]
    process(form, sys.stdout, print_answer)
else:
    from mod_python import apache, util
    directory = os.path.dirname(__file__)
