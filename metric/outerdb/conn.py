#-*- coding: utf-8 -*-
from db.db_conn import DBManager
import simplejson
from metric.formatter import DateUnformat, FieldFormatter
from metric.conf import ConfigReader
import datetime
from simplejson.decoder import JSONDecodeError
from subprocess import Popen, PIPE
import re
#from metric.aes_coder import AESCoder
#import base64
#import urllib
#import urllib2

#import time
#import pprint

class JCollectorConn:
    _error = ''
    json_result = ''

    _returned_date_format = ''
    parsed_json_result = None
    _unformatter = None

    _segment = None
    _segment_id = None
    _data_fetch_method = None
    #_source_database_connection_id = None
    #_web_service_credentials_id = None
    #_plugin_connection_profile_id = None
    _db = None

    _connection_type = ''
    _connection_id = 0

    _returned_date_format = None
    _current_time_sql_stmt = None
    _logger = None

    def __init__(self, element_id, segment, data_fetch_method, source_database_connection_id, web_service_credentials_id, plugin_connection_profile_id):
        self._conf = ConfigReader()
        self._db = DBManager.get_query()

        self._element_id = element_id
        self._segment = segment
        if segment:
            self._segment_id = segment['segment_id']
        self._data_fetch_method = data_fetch_method

        #self._source_database_connection_id = source_database_connection_id
        #self._web_service_credentials_id = web_service_credentials_id
        #self._plugin_connection_profile_id = plugin_connection_profile_id

        self._formatter = FieldFormatter(0)
        self._returned_date_format = None

        self._current_time_sql_stmt = None

        if self._data_fetch_method in (u'sql', u'existing reports'):
            if not source_database_connection_id:
                raise Exception("No source_database_connection_id specified")

            self._connection_type = 'source_database_connection_id'
            self._connection_id = source_database_connection_id

            res = self._db.Query("""SELECT jdbc_to_mysql_date_format_string, current_time_sql_stmt
                            FROM source_database_connection
                                LEFT JOIN jdbc_driver ON jdbc_driver.jdbc_driver_id = source_database_connection.jdbc_driver_id
                        WHERE
                            `source_database_connection_id`=%s""", (source_database_connection_id, ))
            if not res:
                raise Exception("No source database record for source_database_connection_id=%s" % source_database_connection_id)
            preferences = self._db.record[0]

            if preferences['jdbc_to_mysql_date_format_string']:
                self._returned_date_format = preferences['jdbc_to_mysql_date_format_string']

            if preferences['current_time_sql_stmt']:
                self._current_time_sql_stmt = preferences['current_time_sql_stmt']

        elif self._data_fetch_method == u'web service':
            if not web_service_credentials_id:
                raise Exception("No web_service_credentials_id specified")

            self._connection_type = 'web_service_credentials_id'
            self._connection_id = web_service_credentials_id

            res = self._db.Query("""SELECT web_service_to_mysql_date_format_string, mysql_to_web_service_date_format_string
                            FROM web_service_credentials
                        WHERE
                            `web_service_credentials_id`=%s""", (web_service_credentials_id, ))
            if not res:
                raise Exception("No web service credentials record for web_service_credentials_id=%s" % web_service_credentials_id)
            preferences = self._db.record[0]

            if preferences['web_service_to_mysql_date_format_string']:
                self._returned_date_format = preferences['web_service_to_mysql_date_format_string']

            if preferences['mysql_to_web_service_date_format_string']:
                self._formatter.set_custom_date_format_rule(preferences['mysql_to_web_service_date_format_string'])

        elif self._data_fetch_method == u'plugin':
            if not plugin_connection_profile_id:
                raise Exception("No plugin_connection_profile_id specified")

            self._connection_type = 'plugin_connection_profile_id'
            self._connection_id = plugin_connection_profile_id
        else:
            raise Exception("No data_fetch_method is specified")

        self._unformatter = DateUnformat(self._returned_date_format)

    def _unformat_date(self, formatted_date):
        return self._unformatter.unformat(formatted_date)

    def get_current_time(self):
        data = {'header': [], 'data': []}

        if self._data_fetch_method in (u'sql', u'existing reports') and self._current_time_sql_stmt:
            params = {
                   #'bind':'',
                  'data_fetch_command': self._current_time_sql_stmt,
                  #'segment_value_id': segment_value_id,
                  'fetch_type': 'Measurement Time',
                  'element_type': 'other',
                  'element_id': ''
            }
            ret_data = self._make_query(params)

            if ret_data['header']:
                data['header'].append(unicode(ret_data['header'][0]['name']))

            if ret_data['data']:
                data['data'].append([self._unformat_date(ret_data['data'][0][0])])
        else:
            data['header'].append('current_time')
            data['data'].append([datetime.datetime.now()])
        return data

    def get_meas_times(self, segment_value_id, data_fetch_command, last_measurement_time):
        params = {'bind': self._formatter.format_date(last_measurement_time),
                  'data_fetch_command': data_fetch_command,
                  'segment_value_id': '',
                  'fetch_type': 'Measurement Time',
                  'element_type': 'other',
                  'element_id': ''
                  }
        if self._data_fetch_method == u'web service':
            params['element_id'] = self._element_id
            params['fetch_type'] = 'Measurement Time'

        if segment_value_id and self._segment:
            if self._data_fetch_method in [u'sql', u'existing reports']:
                if ':%s' % self._segment['data_fetch_command_bind_parameter'] in data_fetch_command:
                    params['segment_value_id'] = segment_value_id

        return self.parse_collected_data(self._make_query(params))

    def get_data(self, segment_value_id, data_fetch_command, measurement_time):
        params = {'bind': self._formatter.format_date(measurement_time),
                  'data_fetch_command': data_fetch_command,
                  'segment_value_id': segment_value_id,
                  'fetch_type': 'Element Data',
                  'element_type': 'report',
                  'element_id': self._element_id
                  }
        return self.parse_collected_data(self._make_query(params))


    def _make_query(self, params):
        params['segment_id'] = self._segment_id
        params['method'] = self._data_fetch_method
        params[self._connection_type] = self._connection_id
        params['batch_real_time_ind'] = 'RT'

        escaped_params = simplejson.dumps(params)
        #self._logger.info('%s "%s"' % (self._conf.fetch_broker, escaped_params.replace('"', '\\"')))
        #print self._conf.fetch_broker,'"%s"' % escaped_params.replace('"', '\\"')
        #print

        args = [self._conf.fetch_broker, escaped_params]

        p = Popen(args, stdin=PIPE, stdout=PIPE)
        self.json_result = p.communicate()[0]

        p.stdin.close()
        p.stdout.close()

        if not self.json_result:
            raise Exception("No data returned")

        try:
            self.parsed_json_result = simplejson.loads(self.json_result)
        except JSONDecodeError, exc:
            raise Exception("Error parsing data: %s" % exc)

        if 'error' in self.parsed_json_result and self.parsed_json_result['error']:
            raise Exception("ERROR: %s" % self.parsed_json_result['error'])

        return self.parsed_json_result

    def parse_collected_data(self, collected_data):
        data = {'header': [], 'data': [], 'type': []}

        formats = []
        may_be_date = []
        may_be_date_values = []
        format_functions = []

        if 'header' not in collected_data:
            raise Exception("ERROR: 'header' is missed")

        for header in collected_data['header']:
            data['header'].append(unicode(header['name']))
            formats.append(header['type'])
            may_be_date.append(False)
            may_be_date_values.append(False)

        for i, format in enumerate(formats):
            if format == 'DATE':
                data['type'].append('date')
                format_functions.append(self._unformat_date)
            elif format == 'INTEGER':
                data['type'].append('int')
                format_functions.append(long)
            elif format == 'NUMBER':
                data['type'].append('float')
                format_functions.append(float)
            elif format == 'DECIMAL':
                data['type'].append('float')
                format_functions.append(float)
            elif format == 'VARCHAR':
                data['type'].append('text')
                format_functions.append(self._try_unformat_date)
                may_be_date[i] = True
            elif format == 'NVARCHAR':
                data['type'].append('text')
                format_functions.append(self._try_unformat_date)
                may_be_date[i] = True
            elif format == 'STRING':
                data['type'].append('text')
                format_functions.append(self._try_unformat_date)
                may_be_date[i] = True
            else:
                data['type'].append('text')
                format_functions.append(self._try_unformat_date)
                may_be_date[i] = True

        header_len = len(collected_data['header'])

        for formatted_row in collected_data['data']:
            if len(formatted_row) != header_len:
                raise Exception("ERROR: 'header' length is not equal to row length")
            row = map(self._mapfunc, format_functions, formatted_row)
            data['data'].append(row)
            for i, value in enumerate(row):
                if may_be_date[i] and value is not None:
                    if isinstance(value, (datetime.datetime, datetime.date)):
                        may_be_date_values[i] = True
                    else:
                        may_be_date_values[i] = False
                        may_be_date[i] = False
        for i, may_be_date_column in enumerate(may_be_date):
            if may_be_date_column and may_be_date_values[i]:
                data['type'][i] = 'date'
        return data

    def _mapfunc(self, format_function, value):
        if value is None:
            return value
        return format_function(value)

    def _unformat_date(self, formatted_date):
        return self._unformatter.unformat(formatted_date)

    def _try_unformat_date(self, text):
        return self._unformatter.try_unformat(text)

    def get_json_result(self):
        return self.json_result
