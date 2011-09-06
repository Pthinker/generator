#-*- coding: utf-8 -*-

from db.db_conn import DBManager
from file_man.jfiles import JFile
from conf import ConfigReader
from libs import partial_dict_index
import MySQLdb
from mustaine.client import HessianProxy
#import simplejson
#import datetime
#from simplejson.ordered_dict import OrderedDict
#from operator import itemgetter

class ReportDataTableManager(object):
    _id = 0
    _logger = None
    _db_name = ''
    _db = None
    _data_db = None
    _table_name = ''
    _report_name = ''
    _fields = []
    _reserved_words = [u'ADD', u'ALL', u'ALTER', u'ANALYZE', u'AND', u'AS', u'ASC', u'ASENSITIVE', u'BEFORE', u'BETWEEN', u'BIGINT', u'BINARY', u'BLOB', u'BOTH', u'BY', u'CALL',
                       u'CASCADE', u'CASE', u'CHANGE', u'CHAR', u'CHARACTER', u'CHECK', u'COLLATE', u'COLUMN', u'COLUMNS', u'CONDITION', u'CONNECTION', u'CONSTRAINT', u'CONTINUE',
                       u'CONVERT', u'CREATE', u'CROSS', u'CURRENT_DATE', u'CURRENT_TIME', u'CURRENT_TIMESTAMP', u'CURRENT_USER', u'CURSOR', u'DATABASE', u'DATABASES', u'DAY_HOUR',
                       u'DAY_MICROSECOND', u'DAY_MINUTE', u'DAY_SECOND', u'DEC', u'DECIMAL', u'DECLARE', u'DEFAULT', u'DELAYED', u'DELETE', u'DESC', u'DESCRIBE', u'DETERMINISTIC',
                       u'DISTINCT', u'DISTINCTROW', u'DIV', u'DOUBLE', u'DROP', u'DUAL', u'EACH', u'ELSE', u'ELSEIF', u'ENCLOSED', u'ESCAPED', u'EXISTS', u'EXIT', u'EXPLAIN', u'FALSE',
                       u'FETCH', u'FIELDS', u'FLOAT', u'FLOAT4', u'FLOAT8', u'FOR', u'FORCE', u'FOREIGN', u'FROM', u'FULLTEXT', u'GOTO', u'GRANT', u'GROUP', u'HAVING', u'HIGH_PRIORITY',
                       u'HOUR_MICROSECOND', u'HOUR_MINUTE', u'HOUR_SECOND', u'IF', u'IGNORE', u'IN', u'INDEX', u'INFILE', u'INNER', u'INOUT', u'INSENSITIVE', u'INSERT', u'INT',
                       u'INT1', u'INT2', u'INT3', u'INT4', u'INT8', u'INTEGER', u'INTERVAL', u'INTO', u'IS', u'ITERATE', u'JOIN', u'KEY', u'KEYS', u'KILL', u'LABEL', u'LEADING',
                       u'LEAVE', u'LEFT', u'LIKE', u'LIMIT', u'LINES', u'LOAD', u'LOCALTIME', u'LOCALTIMESTAMP', u'LOCK', u'LONG', u'LONGBLOB', u'LONGTEXT', u'LOOP', u'LOW_PRIORITY',
                       u'MATCH', u'MEDIUMBLOB', u'MEDIUMINT', u'MEDIUMTEXT', u'MIDDLEINT', u'MINUTE_MICROSECOND', u'MINUTE_SECOND', u'MOD', u'MODIFIES', u'NATURAL', u'NOT',
                       u'NO_WRITE_TO_BINLOG', u'NULL', u'NUMERIC', u'ON', u'OPTIMIZE', u'OPTION', u'OPTIONALLY', u'OR', u'ORDER', u'OUT', u'OUTER', u'OUTFILE', u'PRECISION',
                       u'PRIMARY', u'PRIVILEGES', u'PROCEDURE', u'PURGE', u'READ', u'READS', u'REAL', u'REFERENCES', u'REGEXP', u'RELEASE', u'RENAME', u'REPEAT', u'REPLACE',
                       u'REQUIRE', u'RESTRICT', u'RETURN', u'REVOKE', u'RIGHT', u'RLIKE', u'SCHEMA', u'SCHEMAS', u'SECOND_MICROSECOND', u'SELECT', u'SENSITIVE', u'SEPARATOR', u'SET',
                       u'SHOW', u'SMALLINT', u'SONAME', u'SPATIAL', u'SPECIFIC', u'SQL', u'SQLEXCEPTION', u'SQLSTATE', u'SQLWARNING', u'SQL_BIG_RESULT', u'SQL_CALC_FOUND_ROWS',
                       u'SQL_SMALL_RESULT', u'SSL', u'STARTING', u'STRAIGHT_JOIN', u'TABLE', u'TABLES', u'TERMINATED', u'THEN', u'TINYBLOB', u'TINYINT', u'TINYTEXT', u'TO',
                       u'TRAILING', u'TRIGGER', u'TRUE', u'UNDO', u'UNION', u'UNIQUE', u'UNLOCK', u'UNSIGNED', u'UPDATE', u'UPGRADE', u'USAGE', u'USE', u'USING', u'UTC_DATE',
                       u'UTC_TIME', u'UTC_TIMESTAMP', u'VALUES', u'VARBINARY', u'VARCHAR', u'VARCHARACTER', u'VARYING', u'WHEN', u'WHERE', u'WHILE', u'WITH', u'WRITE', u'XOR',
                       u'YEAR_MONTH', u'ZEROFILL']

    _column_name_to_field_name = {}
    _structure_is_validated = False
    config = None
    
    def __init__(self, report_id, logger):
        self._id = report_id
        self._db = DBManager.get_query()
        self._data = self._get_element()
        self._logger = logger
        self._db_name = "report_data_segment_%s" % self._data['segment_id']
        self._data_db = DBManager().get_db_query(self._db_name)
        self._structure_is_validated = False

        self._conf = ConfigReader()

        self._path = self._conf.report_root
        self._jfile = JFile(self._path, report_id, self._data)

    def _run_aqb_manager(self):
        res = self._db.Query("""SELECT source_database_connection_id
                            FROM source_database_connection
                        WHERE
                            segment_id = %s
                            AND report_data_table_access_ind = 'Y'
                        """, (self._data['segment_id'], ))
        if res:
            source_db_conn = self._db.record[0]
            source_database_connection_id = source_db_conn['source_database_connection_id']
        else:
            if not self._data['segment_id']:
                raise Exception("Connection to segment '%s' data base is not found" % self._data['segment_id'])

            self._db.Query("""SELECT name
                        FROM segment
                    WHERE
                        segment_id = %s
                    """, (self._data['segment_id'], ))

            segment_data = self._db.record[0]

            connection_name = "%s Report Data Connection" % segment_data['name']


            res = self._db.Query("""INSERT INTO source_database_connection
                        (name,
                        report_data_table_access_ind,
                        segment_id,
                        allow_visual_editor_ind,
                        infer_foreign_keys_ind,
                        concurrent_threads_per_update_trigger_event,
                        jdbc_driver_id, username, password, host_name, port, database_name, jdbc_string, jdbc_options)
                SELECT
                        %s, 'Y', %s, 'Y', 'Y', 1,
                        jdbc_driver_id, username, password, host_name, port, database_name, jdbc_string, jdbc_options
                    FROM source_database_connection
                    WHERE segment_id = 0
                        """, (connection_name, self._data['segment_id']))
            if not res:
                raise Exception("Connection to segment '%s' data base is not found" % self._data['segment_id'])

            source_database_connection_id = self._db.lastInsertID

        try:
            aqb_service = HessianProxy(self._conf.aqb_metadata_manager_url)
            aqb_response = aqb_service.getMetadata({'connectionId': source_database_connection_id, 'refreshMetadata': True})
            #self._logger.info('First access to aqb: %s' % aqb_response.success)
        except Exception, e:
            # try once again. due to lost db connection it might bwe necessary to invoke once again.
            try:
                aqb_service = HessianProxy(self._conf.aqb_metadata_manager_url)
                aqb_response = aqb_service.getMetadata({'connectionId': source_database_connection_id, 'refreshMetadata': True})
                #self._logger.info('Second access to aqb: %s' % aqb_response.success)
            except Exception, e:
                self._logger.error('Cannot invoke AQB meta data manager: %s' % e)

    def _get_sql_formatted_string(self, sql_string):
        """
        make string convenient to be database name of field name
        """
        
        #if type(sql_string) != 'unicode':
        if not isinstance(sql_string, unicode):
            sql_string = unicode(sql_string)

        sql_string.strip()
        sql_string = ' '.join(sql_string.split())
        sql_string = sql_string.replace('\0',"")
        sql_string = sql_string.replace("'","_")
        sql_string = sql_string.replace('"',"_")
        sql_string = sql_string.replace("`","_")
        sql_string = sql_string.replace("\n","_")
        sql_string = sql_string.replace("\r","_")
        sql_string = sql_string.replace("\t","_")
        sql_string = sql_string.replace("\z","_")
        sql_string = sql_string.replace("\\","_")
        sql_string = sql_string.replace("\%","_")
        sql_string = sql_string.replace("\_","_")
        sql_string = sql_string.replace(" ","_")

        if sql_string.upper() in self._reserved_words:
            sql_string = None
        
        return sql_string

    def _get_segment_values(self):
        """
        Get segment values
        """
        segment_values = []
        if self._data['segment_id']:
            self._db.Query("""SELECT *
                            FROM segment_value
                        WHERE
                            `segment_id` = %s""",(self._data['segment_id']))

            segment_values = [segment for segment in self._db.record]
        return segment_values

    def _get_element(self):
        self._db.Query("""SELECT dashboard_element.*
                            FROM dashboard_element
                        WHERE
                            dashboard_element.`element_id` = %s""", (self._id, ))
        data = self._db.record[0]
        if not data['segment_id']:
            data['segment_id'] = 0
        return data

    def _get_table_fields(self):
        fields = []
        self._column_name_to_field_name = {}
        # get current titles from db
        self._db.Query("""SELECT report_data_set_column.*
                            FROM report_data_set_column
                        WHERE
                            report_data_set_column.`element_id`  =%s
                            AND column_dropped_from_sql_ind = 'N'
                        ORDER BY column_sequence""",(self._id, ))
        for field in self._db.record:
            if not field['reference_name']:
                field['reference_name'] = field['display_name']
                self._db.Query("""UPDATE report_data_set_column
                            SET `reference_name` = `display_name`
                        WHERE
                            `element_id`= %s
                        """,(self._id, ))
            fields.append(field)
            self._column_name_to_field_name[field['column_name']] = self._get_sql_formatted_string(field['reference_name'])
        return fields

    def check_table_exists(self, table_name):
        """
        Checks composite table existing
        """
        res = self._data_db.Query("""SHOW TABLES LIKE %s""", (table_name, ))
        if res:
            return True
        return False

#    def check_table(self):
#        """
#        check if composite report table exists
#        """
#        if self._data['segment_id']:
#            segment_values = self._get_segment_values()
#            segment_value = segment_values[0]
#        else:
#            segment_value = 0
#
#        self._jfile.set_segment_value(segment_value)
#
#        # get saved dataset it's needed to check float/int fields
#        saved_dataset = self._jfile.get_current_stored_dataset()
#        headers = {}
#        if saved_dataset:
#            dataset = simplejson.loads(saved_dataset['instance'])
#            for column in dataset['header']:
#                headers[column['name']] = column['type']
#
#        res = self._data_db.Query("""SHOW TABLES LIKE %s""", (self._table_name, ))
#
#        if not res:
#            # if table is not existed, create it
#            # default field measurement_time
#            fields = [{'name': u'measurement_time', 'type': 'DATETIME NOT NULL'}]
#            indexes =[u'`measurement_time`']
#            if self._data['segment_id']:
#                # default field segment_value_id for segmented report
#                fields.append({'name': u'segment_value_id', 'type': 'INT( 11 ) NOT NULL'})
#                indexes.append('`segment_value_id`')
#
#            # create list of fields and their types
#            for field in self._fields:
#                type = ''
#                if field['value_type'] == 'datetime':
#                    # datetime
#                    type = 'DATETIME NULL'
#                elif field['value_type'] == 'text':
#                    # text
#                    type = 'VARCHAR(400) CHARACTER SET utf8 COLLATE utf8_general_ci NULL'
#                elif field['value_type'] == 'numeric':
#                    # numeric
#                    if field['column_name'] in headers:
#                        if headers[field['column_name']] == 'DECIMAL':
#                            type = 'FLOAT'
#                        else:
#                            type = 'BIGINT(20)'
#                    else:
#                        type = 'FLOAT'
#                # get sql safe field name
#                name = self._get_sql_formatted_string(field['display_name'])
#
#                if name is None:
#                    raise Exception("Error: The following reserved word '%s' may not be used in column names." % field['display_name'])
#                elif not name:
#                    raise Exception("Error: The following name '%s' may not be used in column names." % field['display_name'])
#                fields.append({'name': name, 'type': type})
#
#            formatted_fields = []
#
#            for field in fields:
#                formatted_fields.append("`%s` %s" % (field['name'], field['type']))
#            sql_fields = ', '.join(formatted_fields)
#            sql_indexes = ', '.join(indexes)
#            sql = """CREATE TABLE `%s` (%s, INDEX (%s))""" % (self._table_name, sql_fields, sql_indexes)
#
#            self._data_db.query(sql)

    #def check_fields(self, columns, types):
    #def _validate_structure(self, columns, types):
    def _validate_structure(self):
        fields, indexes = self._get_table_structure()

        has_added_fields = False

        self._data_db.Query("""SHOW COLUMNS FROM %s""" % self._table_name)

        current_fields = []

        for column in self._data_db.record:
            simple_type = ''
            if column['Type'] == u'datetime':
                 simple_type = 'date'
            elif column['Type'] == u'float':
                simple_type = 'float'
            elif column['Type'] == u'bigint(20)':
                simple_type = 'int'
            elif column['Type'] == u'varchar(400)':
                simple_type = 'text'
            
            current_fields.append({'name': column['Field'], 'simple_type': simple_type})

        for field in fields:
            ind = partial_dict_index(current_fields, {'name': field['name']})
            if ind is None:
                has_added_fields = True
            elif current_fields[ind]['simple_type'] != field['simple_type']:
                  has_added_fields = True

        structure_is_changed = False

        if has_added_fields:
            self._run_recreate_table_sql()
            structure_is_changed = True
        else:
            for current_field in current_fields:
                ind = partial_dict_index(fields, {'name': current_field['name']})
                if ind is None:
                    self._data_db.query("""ALTER TABLE `%s` DROP `%s`""" % (self._table_name, current_field['name']))
                    structure_is_changed = True

        if structure_is_changed:
            self._run_aqb_manager()

        self._structure_is_validated = True

    def validate_structure(self):
        """
        Validates composite table structure
        """
        self._report_name = self._data['name']
        self._table_name = self._get_sql_formatted_string(self._report_name)
        self._validate_structure()
        
    def process_instance(self, instance, meas_time, segment_value_id):
        """
        Inserts data into composite table
        """

        self._report_name = self._data['name']
        self._table_name = self._get_sql_formatted_string(self._report_name)

        if not self.check_table_exists(self._table_name):
            raise Exception("Error: Composite table %s does not exist" % self._table_name)

        if not self._structure_is_validated:
            self._validate_structure()

        self._run_sql_insert_instance(instance, meas_time, segment_value_id)

    def _run_sql_insert_instance(self, instance, meas_time, segment_value_id):
        """
        Inserts data into composite table
        """
        # clear old data from composite table
        if segment_value_id:
            # for segmented report
            if self._data['report_save_historical_instances_ind'] == u'Y':
                # for report with historical instances
                sql = """DELETE FROM %s WHERE measurement_time = %%s and segment_value_id = %%s""" % self._table_name
                params = (meas_time, segment_value_id)
            else:
                # for report without historical instances
                sql = """DELETE FROM %s WHERE segment_value_id = %%s""" % self._table_name
                params = (segment_value_id, )
            fields = ['measurement_time', 'segment_value_id']
            default_values = [meas_time, segment_value_id]
        else:
            # for non-segmented report
            if self._data['report_save_historical_instances_ind'] == u'Y':
                # for report with historical instances
                sql = """DELETE FROM %s WHERE measurement_time = %%s""" % self._table_name
                params = (meas_time, )
            else:
                # for report without historical instances
                sql = """DELETE FROM %s WHERE 1""" % self._table_name
                params = ()
            fields = ['measurement_time']
            default_values = [meas_time]
        self._data_db.Query(sql, params)

        row_values = []

        # list of table fields
        for column_name in instance['header']:
            fields.append(self._column_name_to_field_name[column_name])

        # list of rows to insert
        for row in instance['data']:
            row_value = default_values[:]
            row_value.extend(row)
            row_values.append(tuple(row_value))

        sql_fields = u', '.join([u"`%s`" % field for field in fields])
        sql_value_row = ', '.join(["%s"] * len(fields))

        sql = """INSERT INTO %s (%s) VALUES (%s)""" % (self._table_name, sql_fields, sql_value_row)
        self._data_db.queryMany(sql, row_values)

    def _get_table_structure(self):
        """
        Returns composite table structure (field names with types and indexes) based on prent table columns info
        """
        self._fields = self._get_table_fields()
        used_fields = []
        fields = [{'name': 'measurement_time', 'sql_type': 'DATETIME NOT NULL', 'simple_type': 'date'}]
        used_fields.append('measurement_time')

        indexes =['`measurement_time`']
        if self._data['segment_id']:
            # default field segment_value_id for segmented report
            fields.append({'name': 'segment_value_id', 'sql_type': 'INT( 11 ) NOT NULL', 'simple_type': 'int'})
            indexes.append('`segment_value_id`')
            used_fields.append('segment_value_id')

        # create list of fields and their types
        for field in self._fields:
            if field['value_type'] == 'datetime':
                # datetime
                sql_type = 'DATETIME NULL'
                simple_type = 'date'
            elif field['value_type'] == 'text':
                # text
                sql_type = 'VARCHAR(400) CHARACTER SET utf8 COLLATE utf8_general_ci NULL'
                simple_type = 'text'
            elif field['value_type'] == 'numeric':
                # numeric
                if field['numeric_type'] == 'float':
                    sql_type = 'FLOAT'
                    simple_type = 'float'
                elif field['numeric_type'] == 'int':
                    sql_type = 'BIGINT(20)'
                    simple_type = 'int'
                else:
                    sql_type = 'FLOAT'
                    simple_type = 'float'
            else:
                raise Exception("Error: Cannot determine field '%s' type." % field['display_name'])
            # get sql safe field name
            name = self._get_sql_formatted_string(field['reference_name'])

            if name is None:
                raise Exception("Error: The following reserved word '%s' cannot be used as column name." % field['reference_name'])
            if name in used_fields:
                raise Exception("Error: The following field '%s' is duplicated." % name)
    
            used_fields.append(name)

            fields.append({'name': name, 'sql_type': sql_type, 'simple_type': simple_type})

        return fields, indexes

    def _run_create_table_sql(self):
        """
        Creates table
        """

        fields, table_indexes = self._get_table_structure()

        table_fields = []

        for field in fields:
            table_fields.append("`%s` %s" % (field['name'], field['sql_type']))

        sql_fields = ', '.join(table_fields)
        sql_indexes = ', '.join(table_indexes)

        try:
            self._data_db.query("""CREATE TABLE `%s` (%s, INDEX (%s))""" % (self._table_name, sql_fields, sql_indexes))
        except MySQLdb.Error, message:
            raise Exception("Error: Cannot create table `%s`: %s " % (self._table_name, message))

    def _run_recreate_table_sql(self):
        """
        Recreates table (drops and creates)
        """
        self._run_drop_table_sql()
        self._run_create_table_sql()

    def _run_rename_table_sql(self, old_table_name):
        """
        Renames table
        """
        try:
            self._data_db.query("""RENAME TABLE `%s` TO `%s`""" % (old_table_name, self._table_name))
        except MySQLdb.Error, message:
            raise Exception("Error: Cannot rename table `%s` to `%s`: %s " % (old_table_name, self._table_name, message))

    def _run_drop_table_sql(self):
        """
        Drops table
        """
        try:
            self._data_db.query("""DROP TABLE IF EXISTS `%s`""" % (self._table_name, ))
        except MySQLdb.Error, message:
            raise Exception("Error: Cannot drop table `%s`: %s " % (self._table_name, message))

    def create_table(self):
        """
        Creates table and inserts data starting from initial_measurement_time
        """
        self._report_name = self._data['name']

        if not self._report_name:
            raise Exception("Error: Report name is empty.")

        self._table_name = self._get_sql_formatted_string(self._report_name)

        if self._table_name is None:
            raise Exception("Error: The following reserved word '%s' cannot be used as table name." % self._report_name)
        elif not self._table_name:
            raise Exception("Error: Table name is empty.")

        if self.check_table_exists(self._table_name):
            raise Exception("Error: Table `%s` already exists" % self._table_name)

        self._run_create_table_sql()
        self._run_aqb_manager()

        return self._table_name

    def generate_data(self, initial_measurement_time):
        self._report_name = self._data['name']
        self._table_name = self._get_sql_formatted_string(self._report_name)

        if self._table_name is None:
            raise Exception("Error: The following reserved word '%s' cannot be used as table name." % self._report_name)
        elif not self._table_name:
            raise Exception("Error: Table name is empty.")

        if not self.check_table_exists(self._table_name):
            raise Exception("Error: Table `%s` does not exists" % self._table_name)

        from report import Report
        report = Report('composite', None, initial_measurement_time)

        report.set_logger(self._logger)
        report.init(self._id)
        report._composite_manager = self

        report.update(None)
        return self._table_name

    def test_table_name(self, report_name):
        """
        Renames table old_report_name into report name
        """
        
        if not report_name:
            raise Exception("Error: Report name is empty")

        table_name = self._get_sql_formatted_string(report_name)

        if table_name is None:
            raise Exception("Error: The following reserved word '%s' cannot be used as table name." % report_name)
        elif not table_name:
            raise Exception("Error: Table name is empty.")


        if self.check_table_exists(table_name):
            raise Exception("Error: Table `%s` already exists" % table_name)
        self._run_aqb_manager()
        return table_name

    def rename_table(self, old_report_name):
        """
        Renames table old_report_name into report name
        """
        self._report_name = self._data['name']

        self._table_name = self._get_sql_formatted_string(self._report_name)
        old_table_name = self._get_sql_formatted_string(old_report_name)

        if self._table_name is None:
            raise Exception("Error: The following reserved word '%s' cannot be used as table name." % self._report_name)
        elif not self._table_name:
            raise Exception("Error: Table name is empty.")

        if old_table_name is None:
            raise Exception("Error: The Following reserved word '%s' cannot be used as table name." % old_report_name)
        elif not old_table_name:
            raise Exception("Error: New table name is empty")

        if not self.check_table_exists(old_table_name):
            raise Exception("Error: Table `%s` does not exists" % old_table_name)

        if old_table_name != self._table_name:
            self._run_rename_table_sql(old_table_name)
            self._run_aqb_manager()

        return self._table_name, old_table_name

    def drop_table(self):
        """
        Drops table
        """
        self._report_name = self._data['name']

        self._table_name = self._get_sql_formatted_string(self._report_name)

        if self._table_name is None:
            raise Exception("Error: The following reserved word '%s' cannot be used as table name." % self._report_name)
        elif not self._table_name:
            raise Exception("Error: Table name is empty.")

        if not self.check_table_exists(self._table_name):
            raise Exception("Error: Table `%s` does not exists" % self._table_name)

        self._run_drop_table_sql()
        self._run_aqb_manager()
        return self._table_name