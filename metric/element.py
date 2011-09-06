#-*- coding: utf-8 -*-
from outerdb.conn import WebServiceConn, JCollectorConn
from file_man.jfiles import JFile
from file_man.flocker import FileLock
from conf import ConfigReader
from db.db_conn import DBManager
import datetime
from simplejson.ordered_dict import OrderedDict
from chart.chartGenerator import ChartGenerator
from dataset import DataSet
from formatter import FieldFormatter
import simplejson
import pprint
import time
import sys

class ReportElement:
    """
    Internal report dashboard element
    """
    # path to directory for data and meta data
    _path = None
    _web_service_data = None
    _segment_values = list()
    _segment = dict()
    _segment_value_id = None
    _charts = list()
    _filelocker = None
    _logger = None
    _process_type = 'normal'
    _process_dataset_ids = None

    def __init__(self, process_type, process_dataset_ids): #, segment_value_id = None
        # get db connect
        config = ConfigReader()
        self._path = config.report_root
        self._db = DBManager.get_query()
        #self._full_gen = full_gen
        self._process_type = process_type
        self._process_dataset_ids = process_dataset_ids
        #self.segment_value_id = segment_value_id

    def init(self, id):
        self._id = id

        #self._date_format_rule = None
        self._outer_conn = None
        self._data = None
        self._web_service = None

        # fetch all element data
        self._data = self._get_element()

        # fetch segments
        self._segment = self._get_segment()
        self._segment_values = self._get_segment_values()
        
        if not self._data['last_report_column_metadata_update_time']:
            self._data['last_report_column_metadata_update_time'] = datetime.datetime(1900, 1, 1, 0, 0, 0)
        if not self._data['last_data_fetch_command_update_time']:
            self._data['last_data_fetch_command_update_time'] = datetime.datetime(1900, 1, 1, 0, 0, 0)
        
        # get all pivots
        self._pivots = self._get_pivots()

        # get all charts
        self._charts = self._get_charts()

        # json file operation wrapper
        self._jfile = JFile(self._path, id, self._data)
        
        self._jfile.set_segment(self._segment)
        self._formatter = FieldFormatter(self._data['def_date_mask_id'])
        
        if self._process_type == 'soft_gen' and self._process_dataset_ids:
            self._parse_process_dataset_ids()
        

    def is_int(self, s):
        try:
            int(s)
            return True
        except ValueError:
            return False

    def _parse_process_dataset_ids(self):
        if self._data['report_save_historical_instances_ind'] != 'Y':
            self._process_dataset_ids = None
            return
        process_dataset_ids = list()
        items = self._process_dataset_ids.split(',')
        for item in items:
            interval = item.split('-')
            if type(interval) == list and len(interval) == 2:
                if self.is_int(interval[0]) and self.is_int(interval[1]):
                    process_dataset_ids.append([int(interval[0]), int(interval[1])])
            elif len(interval) == 1:
                if self.is_int(item):
                    process_dataset_ids.append(int(item))
        self._process_dataset_ids = process_dataset_ids

        
    def set_logger(self, logger):
        self._logger = logger

    def _clear_instances(self, segment_value_id):
        """
        remove all existing instances of charts, pivots, data sets from db
        needed for full report regeneration
        """
        #if self.segment_value_id is not None :
        # get dataset instances ids
        self._db.Query("""SELECT report_data_set_instance_id
                                FROM report_data_set_instance
                                    WHERE element_id = %s
                                        AND segment_value_id = %s""", (self._id, segment_value_id))
        
        report_data_set_instance_ids = [row['report_data_set_instance_id'] for row in self._db.record]
        
        # process charts
        for chart in self._charts:
            # remove annotation charts instance
            self._db.Query("""DELETE FROM report_data_set_chart_annotation_instance
                                WHERE report_data_set_chart_instance_id IN 
                                    (SELECT report_data_set_chart_instance_id 
                                        FROM report_data_set_chart_instance 
                                            WHERE report_data_set_chart_id = %s AND
                                                report_data_set_instance_id IN (
                                                    SELECT report_data_set_instance_id
                                                        FROM report_data_set_instance
                                                            WHERE element_id = %s
                                                                AND segment_value_id = %s 
                                                ))""", (chart['report_data_set_chart_id'], self._id, segment_value_id))
            
            # remove chart instances files
            self._jfile.purge_chart_files(chart['report_data_set_chart_id'], report_data_set_instance_ids, segment_value_id)
            # remove charts instances from DB 
            self._db.Query("""DELETE FROM report_data_set_chart_instance
                                WHERE report_data_set_chart_id = %s AND
                                    report_data_set_instance_id IN (
                                        SELECT report_data_set_instance_id
                                            FROM report_data_set_instance
                                                WHERE element_id = %s
                                                    AND segment_value_id = %s 
                                    )""", (chart['report_data_set_chart_id'], self._id, segment_value_id))
        # process pivots
        for pivot in self._pivots:
            # remove chart instances files
            self._jfile.purge_pivot_files(pivot['report_data_set_pivot_id'], report_data_set_instance_ids, segment_value_id)
            
            # remove pivots instances from DB
            self._db.Query("""DELETE FROM report_data_set_pivot_instance 
                                WHERE report_data_set_pivot_id = %s AND
                                    report_data_set_instance_id IN (
                                        SELECT report_data_set_instance_id
                                            FROM report_data_set_instance
                                                WHERE element_id = %s
                                                    AND segment_value_id = %s 
                                    )""", (pivot['report_data_set_pivot_id'], self._id, segment_value_id))
        
        # remove dataset instances from DB
        self._db.Query("""DELETE FROM report_data_set_instance
                                WHERE element_id = %s
                                    AND segment_value_id = %s""", (self._id, segment_value_id))            
        # remove dataset instances files
        self._jfile.purge_dataset_files(report_data_set_instance_ids, segment_value_id)
#        else:
#            # remove charts instance
#            for chart in self._charts:
#                # remove annotation charts instance
#                self._db.Query("""DELETE FROM report_data_set_chart_annotation_instance
#                                    WHERE report_data_set_chart_instance_id IN 
#                                        (SELECT report_data_set_chart_instance_id 
#                                            FROM report_data_set_chart_instance 
#                                                WHERE report_data_set_chart_id = %s)""",(chart['report_data_set_chart_id']))    
#                
#                # remove charts instance
#                self._db.Query("""DELETE FROM report_data_set_chart_instance
#                                    WHERE report_data_set_chart_id = %s""",(chart['report_data_set_chart_id']))
#            
#            # remove pivots instance
#            for pivot in self._pivots:
#                self._db.Query("""DELETE FROM report_data_set_pivot_instance
#                        WHERE report_data_set_pivot_id = %s""",(pivot['report_data_set_pivot_id']))
#            
#            # remove dataset instance
#            self._db.Query("""DELETE FROM report_data_set_instance
#                        WHERE element_id = %s""",(self._id))
#            
#            # remove all files from report directory
#            self._jfile.purge_files()
        
    def _get_element(self):
        self._db.Query("""SELECT dashboard_element.*,
                               dashboard_category.category, 
                               topic.name AS topic_name,
                               measurement_interval.measurement_interval_button_name,
                               measurement_interval.display_mask_id AS def_date_mask_id
                            FROM dashboard_element 
                            LEFT JOIN dashboard_category ON dashboard_category.category_id=dashboard_element.category_id 
                            LEFT JOIN topic ON topic.topic_id=dashboard_element.primary_topic_id
                            LEFT JOIN measurement_interval ON measurement_interval.measurement_interval_id=dashboard_element.measurement_interval_id
                        WHERE 
                            dashboard_element.`element_id`=%s""", (self._id, ))
        data = self._db.record[0]
        if not data['last_report_column_metadata_update_time']:
            data['last_report_column_metadata_update_time'] = datetime.datetime(1900, 1, 1, 0, 0, 0)
        if not data['last_data_fetch_command_update_time']:
            data['last_data_fetch_command_update_time'] = datetime.datetime(1900, 1, 1, 0, 0, 0)
        if not data['segment_id']:
            data['segment_id'] = 0
        if not data['report_used_for_drill_to_ind']:
            data['report_used_for_drill_to_ind'] = 'N'
        return data

    def _get_segment(self):
        """
        Get segment
        """
        segment = list()
        if self._data['segment_id']:
            res = self._db.Query("""SELECT *
                            FROM segment
                        WHERE 
                            `segment_id`=%s""",(self._data['segment_id']))
            if res:
                segment = self._db.record[0]
                if not segment['segment_value_prefix']:
                    segment['segment_value_prefix'] = ''
                if not segment['segment_value_suffix']:
                    segment['segment_value_suffix'] = ''
        return segment

    def _get_segment_values(self):
        """
        Get segment values
        """
        segment_values = list()
        if self._segment:
            self._db.Query("""SELECT *
                            FROM segment_value
                        WHERE
                            `segment_id`=%s""",(self._segment['segment_id']))

            segment_values = [segment for segment in self._db.record]
        return segment_values

    def _get_pivots(self, enabled_only = True):
        """
        Get pivots info
        """
        if enabled_only:
            self._db.Query("""SELECT *
                                FROM report_data_set_pivot 
                            WHERE 
                                `element_id`=%s AND enabled_ind='Y'""", (self._id, ))
        else:
            self._db.Query("""SELECT *
                                FROM report_data_set_pivot 
                            WHERE 
                                `element_id`=%s""", (self._id, ))
        pivots = [pivot for pivot in self._db.record]
        return pivots

    def _get_charts(self, enabled_only = True):
        """
        Get charts info
        """
        charts = list()
        sql = """SELECT report_data_set_chart.*,
                                    IFNULL(report_data_set_chart.report_data_set_pivot_id, 0),
                                    report_data_set_pivot.pivot_column_value_column_id,
                                    report_data_set_pivot.pivot_row_value_column_id,
                                    report_data_set_column.column_name
                                FROM report_data_set_chart
                                LEFT JOIN report_data_set_column
                                    ON report_data_set_chart.chart_by_report_data_set_column_id = report_data_set_column.report_data_set_column_id
                                LEFT JOIN report_data_set_pivot ON report_data_set_pivot.report_data_set_pivot_id = report_data_set_chart.report_data_set_pivot_id
                            WHERE
                                report_data_set_chart.`element_id`=%s"""
        if enabled_only:
            sql +=  """ AND report_data_set_chart.enabled_ind='Y'"""
        
        self._db.Query(sql, (self._id, ))
        
        for chart in self._db.record:
            if not chart['report_data_set_pivot_id']:
                chart['report_data_set_pivot_id'] = 0
            charts.append(chart)
        return charts
        
    
    def _get_outer_connection(self):
        """
        Create connection to outer db
        """
        if self._data['data_fetch_method'] == 'sql':
            if not self._data['source_database_connection_id']:
                raise Exception("No source_database_connection_id specified")
            res = self._db.Query("""SELECT source_database_connection.*, jdbc_driver.current_time_sql_stmt
                            FROM source_database_connection
                            LEFT JOIN jdbc_driver ON jdbc_driver.jdbc_driver_id = source_database_connection.jdbc_driver_id  
                        WHERE 
                            `source_database_connection_id`=%s""",(self._data['source_database_connection_id']))
            if not res:
                raise Exception("No source database record for source_database_connection_id=%s", (self._data['source_database_connection_id']))
            outer_connection = self._db.record[0]
            conn = JCollectorConn(outer_connection)
            return conn

        elif self._data['data_fetch_method'] == 'web service':
            res = self._db.Query("""SELECT web_service_credentials.*
                            FROM web_service_credentials
                        WHERE
                            `web_service_credentials_id`=%s""",(self._data['web_service_credentials_id']))
            if not res:
                raise Exception("no source web service record")
            self.web_service = self._db.record[0]
            self.web_service['url'] = self._data['data_fetch_command']
            conn = WebServiceConn(self.web_service)
            return conn

        return False



    def _get_meas_times(self, last_meas_time):
        """
        Get available measurement times
        """
        meas_times = list()
        data = None
        
        if self._process_type == 'soft_gen':
            meas_times = self._get_meas_times_from_db()
        else:
            if self._data['data_fetch_method'] == 'sql':
                # get from outer sql db
                data = self._get_meas_times_sql(last_meas_time)
            elif self._data['data_fetch_method'] == 'web service':
                # get from web service
                data = self._get_meas_times_web_service(last_meas_time)


            if data:
                clear_data = [row[0] for row in data['data']]
                # check if we have values in list of datetime type
                if clear_data:
                    if type(clear_data[0]) == datetime.datetime:
                        meas_times = clear_data
                    else:
                        # it's a date type
                        meas_times = [datetime.datetime.combine(d, datetime.time.min) for d in clear_data]

        


        # sort measurement times if they weren't sorted before
        meas_times.sort()
        # if do not save history, take only last element
        if self._data['report_save_historical_instances_ind'] != 'Y':
            if len(meas_times) > 1:
                del meas_times[:-1]
        
        return meas_times    

    def _get_meas_times_from_db(self):
        """
        get existing measurement times from db for soft regeneration
        """
        meas_times = []
        if self._data['report_save_historical_instances_ind'] != 'Y':
            # for non historical reports take measurement time from saved dataset
            dataset = self._jfile.get_current_stored_dataset()
            try:
                meas_time = datetime.datetime.strptime(dataset['meas_time'], '%Y-%m-%d %H:%M:%S')
            except ValueError:
                raise Exception("Cannot unformat string %s to datetime" % dataset['meas_time'])
            meas_times.append(meas_time)

        else:
            # for historical reports take measurement times from db datasets
            where_sql = ''
            where_sql_list = list()
            params = [self._id, self._segment_value_id]

            if self._process_dataset_ids:
                for dataset_id in self._process_dataset_ids:
                    if type(dataset_id) == list:
                        where_sql_list.append("(report_data_set_instance_id >= %s AND report_data_set_instance_id <= %s)")
                        if dataset_id[0] < dataset_id[1]:
                            params.append(dataset_id[0])
                            params.append(dataset_id[1])
                        else:
                            params.append(dataset_id[1])
                            params.append(dataset_id[0])
                    else:
                        where_sql_list.append("report_data_set_instance_id = %s")
                        params.append(dataset_id)
                where_sql = ' AND (%s)' % ' OR '.join(where_sql_list)

            self._db.Query("""SELECT measurement_time
                            FROM report_data_set_instance
                        WHERE
                            `element_id`= %%s
                            AND segment_value_id = %%s
                            %s
                        ORDER BY measurement_time ASC""" % where_sql, tuple(params))
            meas_times = [item['measurement_time'] for item in self._db.record]

        return meas_times

    def _get_meas_times_sql(self, last_meas_time):
        """
        Get available measurement times from sql db
        """
        #meas_times = dict()
        
        # check measurement time calc method
        if self._data['report_measurement_time_calc_method'] == 'sql':
            
            res = self._db.Query("""SELECT measurement_time_calc_command.*
                            FROM measurement_time_calc_command
                        WHERE
                            `measurement_time_calc_command_id`=%s""",(self._data['report_measurement_time_calc_command_id']))
            if not res:
                raise Exception("Measurement_time_calc_command not found")
            meas_time_calc_command = self._db.record[0]

            named_placeholders = list()
            last_meas_time_arg = dict()#{'name': '', 'value': '', 'type': ''}
            last_meas_time_arg['name'] = 'last_measurement_time'
            last_meas_time_arg['value'] = last_meas_time.strftime('%Y-%m-%d %H:%M:%S')
            last_meas_time_arg['type'] = 'DATE'
            named_placeholders.append(last_meas_time_arg)
            
            # process segment data
            if meas_time_calc_command['segment_id']:
                if self._segment and self._segment_value:
                    segment_arg = dict()
                    #segment_arg['value'] = ''
                    #segment_arg['type'] = ''
                    segment_arg['name'] = self._segment['data_fetch_command_bind_parameter']
                    if self._segment['partition_value_type'] == 'int':
                        segment_arg['value'] = self._segment_value['value_int']
                        segment_arg['type'] = 'INTEGER'
                    elif self._segment['partition_value_type'] == 'varchar':
                        segment_arg['value'] = self._segment_value['value_varchar']
                        segment_arg['type'] = 'NVARCHAR'
                        
                    named_placeholders.append(segment_arg)
                else:
                    raise Exception("Try to fetch segmented measurement times for non-segmented report")
            
            meas_times = self._outer_conn.query(meas_time_calc_command['select_statement'], named_placeholders)
        else:
            meas_times = self._outer_conn.get_current_time()

        return meas_times

    
    def _get_meas_times_web_service(self, last_meas_time):
        """
        Get available measurement times from web service.
        Actually web service returns list of measurement_time=>data, so this function emulate trivial process.
        So self._web_service_data populated with measurement_time=>data
        return dict  {'header':'meas_time', 'data': <list of collected measurement times>}
        """
        subst = ''
        if self._segment and self._segment_value:
            if self._segment['partition_value_type'] == 'int':
                subst = self._segment_value['value_int']
            elif self._segment['partition_value_type'] == 'varchar':
                subst = self._segment_value['value_varchar']
            data_fetch_command_bind_parameter = self._segment['data_fetch_command_bind_parameter']
        else:
            data_fetch_command_bind_parameter = ''
            subst = ''

        #meas_times = self._outer_conn.query(last_meas_time, data_fetch_command_bind_parameter, subst, 'get_meas_times', None)
        ret_data = self._outer_conn.query(last_meas_time, data_fetch_command_bind_parameter, subst)
        self._web_service_data = dict()
        meas_times = {'header':'meas_time', 'data': list()}
        for meas_time, meas_data in ret_data.iteritems():
            meas_times['data'].append([meas_time])
            self._web_service_data[meas_time] = meas_data 
 
        return meas_times

    

    def _get_instance(self, meas_time, segment_value, last_meas_time):
        """
        Get data from outer db for measurement time instance
        """
        data = list()

        if self._process_type == 'soft_gen':
            if self._data['report_save_historical_instances_ind'] != 'Y':
                # for non historical reports take measurement time from saved dataset
                saved_dataset = self._jfile.get_current_stored_dataset()
            else:
                self._db.Query("""SELECT report_data_set_instance_id
                            FROM report_data_set_instance
                        WHERE
                            `element_id`= %s
                            AND segment_value_id = %s
                            AND measurement_time = %s
                        LIMIT 0, 1""", (self._id, self._segment_value_id, meas_time))
                data_set_instance_id = self._db.record[0]['report_data_set_instance_id']
                saved_dataset = self._jfile.get_stored_dataset(data_set_instance_id)

            if saved_dataset:
                dataset = simplejson.loads(saved_dataset['instance'])
                data = self._outer_conn.parse_collected_data(dataset)
            
        else:
            if self._data['data_fetch_method'] == 'sql':
                sql = self._data['data_fetch_command']

                named_placeholders = list()
                last_meas_time_arg = dict()
                last_meas_time_arg['name'] = 'last_measurement_time'
                last_meas_time_arg['value'] = last_meas_time.strftime('%Y-%m-%d %H:%M:%S')
                last_meas_time_arg['type'] = 'DATE'
                named_placeholders.append(last_meas_time_arg)

                meas_time_arg = dict()
                meas_time_arg['name'] = 'measurement_time'
                meas_time_arg['value'] = meas_time.strftime('%Y-%m-%d %H:%M:%S')
                meas_time_arg['type'] = 'DATE'
                named_placeholders.append(meas_time_arg)

                # process segment data
                if segment_value:
                    segment_arg = dict()
                    segment_arg['value'] = ''
                    segment_arg['type'] = ''
                    segment_arg['name'] = self._segment['data_fetch_command_bind_parameter']
                    if self._segment['partition_value_type'] == 'int':
                        segment_arg['value'] = self._segment_value['value_int']
                        segment_arg['type'] = 'INTEGER'
                    elif self._segment['partition_value_type'] == 'varchar':
                        segment_arg['value'] = self._segment_value['value_varchar']
                        segment_arg['type'] = 'NVARCHAR'

                    named_placeholders.append(segment_arg)

                data = self._outer_conn.query(sql, named_placeholders)
                self._json_fetched_data = self._outer_conn.get_json_result()

            elif self._data['data_fetch_method'] == 'web service':
                self._json_fetched_data = ''
    #            if self._segment and self._segment_value:
    #                if self._segment['partition_value_type'] == 'int':
    #                    subst = self._segment_value['value_int']
    #                elif self._segment['partition_value_type'] == 'varchar':
    #                    subst = self._segment_value['value_varchar']
    #                data_fetch_command_bind_parameter = self._segment['data_fetch_command_bind_parameter']
    #            else:
    #                data_fetch_command_bind_parameter = ''
    #                subst = ''

                #data = self._outer_conn.query(last_meas_time, data_fetch_command_bind_parameter, subst, 'get_data', meas_time)

                self._json_fetched_data = self._outer_conn.get_json_result(meas_time)
                data = self._web_service_data[meas_time] 
             
            
        return data

    def _process_charts(self, data_set_instance_id):
        """
        Process charts - write to report_data_set_chart_instance table
        """
        for chart in self._charts:
            self._db.Query("""INSERT INTO report_data_set_chart_instance
                    (report_data_set_chart_id, report_data_set_instance_id, chart_generation_time)
                    VALUES(%s, %s, NOW())
                    ON DUPLICATE KEY UPDATE chart_generation_time = NOW()""",(chart['report_data_set_chart_id'], data_set_instance_id))
            
    def _process_instance(self, instance, meas_time, update_columns, write_clear_headers, segment_value):
        """
        Start update workflow
        """
        data_set_instance = DataSet(self._id, self._data, meas_time, self._formatter, segment_value)
        data_set_instance.process_data(instance, update_columns)
        if self._data['report_save_historical_instances_ind'] == 'Y':
            data_set_instance.insert_to_db()
        self._jfile.create_data_set(data_set_instance.instance_id, data_set_instance.prepare_json(write_clear_headers))
        return data_set_instance

    #def _process_pivot(self, pivot, data_set_instance, segment_value):
    def _process_pivot(self, pivot, data_set_instance):
        """
        Process pivot
        """
        data_set_pivot = data_set_instance.create_pivot(pivot)
        #data_set_pivot.prepare_pivot()
        if self._data['report_save_historical_instances_ind'] == 'Y':
            data_set_pivot.insert_to_db()

        data_set_pivot.process_pivot()
        # make a json file
        self._jfile.create_pivot(pivot, data_set_instance.instance_id, data_set_pivot.prepare_json())
        return data_set_pivot
        

    #def _make_current_jfiles(self, segment_value):
    def _make_current_jfiles(self):
        """
        Create *_current json files
        """
        res = self._db.Query("""SELECT *
                            FROM report_data_set_instance
                        WHERE
                            `element_id`=%s
                            AND segment_value_id = %s
                        ORDER BY measurement_time DESC
                        LIMIT 0, 1""",(self._id, self._segment_value_id))
        if res:
            last_data_set_instance = self._db.record[0]
            last_data_set_instance_id = last_data_set_instance['report_data_set_instance_id']
            
            self._jfile.make_current_data_set(last_data_set_instance_id)
            self._jfile.make_current_saved_data_set(last_data_set_instance_id)

            for pivot in self._pivots:
                self._jfile.make_current_pivot_set(pivot['report_data_set_pivot_id'], last_data_set_instance_id)

            for chart in self._charts:
                self._jfile.make_current_chart_set(chart['report_data_set_chart_id'], last_data_set_instance_id)


    def _make_meta(self):
        """
        Create/Update report metadata file
        """
        available_meas_times = list()
        available_intervals = list()
        drill_by = list()
        related = list()
        last_data_set_instance = dict()

        if self._data['report_save_historical_instances_ind'] == 'Y':
            # last measurement instance
            res = self._db.Query("""SELECT *
                                FROM report_data_set_instance
                            WHERE
                                `element_id`=%s
                                AND `segment_value_id` = %s
                            ORDER BY measurement_time DESC
                            LIMIT 0, 1""",(self._id, self._segment_value_id))
            if res:
                last_data_set_instance = self._db.record[0]
                last_data_set_instance['measurement_time'] = self._formatter.format_date(last_data_set_instance['measurement_time'])

            # available measurement instances
            res = self._db.Query("""SELECT *
                                FROM report_data_set_instance
                            WHERE
                                `element_id`=%s
                                AND `segment_value_id` = %s
                            ORDER BY measurement_time DESC""",(self._id, self._segment_value_id))
            if res:
                for data_set_instance in self._db.record:
                    data_set_instance['measurement_time'] = self._formatter.format_date(data_set_instance['measurement_time'])
                    available_meas_times.append(data_set_instance)
            

        # get drill by. not for this version

        # available measurement intervals
        if self._data['report_primary_shared_dimension_id'] is None:
            self._data['report_primary_shared_dimension_id'] = 0

        self._db.Query("""
                        SELECT measurement_interval.*,
                                 dashboard_element.element_id
                            FROM dashboard_element
                            LEFT JOIN measurement_interval
                                ON measurement_interval.measurement_interval_id = dashboard_element.measurement_interval_id
                        WHERE
                            (dashboard_element.`element_id`<>%s
                            AND dashboard_element.measurement_interval_id <> %s
                            AND dashboard_element.shared_measure_id = %s
                            AND dashboard_element.`type` = 'internal report'
                            AND ifnull(dashboard_element.report_used_for_drill_to_ind,'N') = %s
                            AND ifnull(dashboard_element.report_primary_shared_dimension_id,0) = %s
                            AND ifnull(dashboard_element.segment_id,0) = %s)
                            OR
                                dashboard_element.`element_id`=%s
                            AND 3=4
                            
                        GROUP BY measurement_interval.measurement_interval_id
                        ORDER BY
                                measurement_interval.display_sequence,
                                dashboard_element.name ASC
                        """,
                        (self._id,
                        self._data['measurement_interval_id'],
                        self._data['shared_measure_id'],
                        self._data['report_used_for_drill_to_ind'],
                        self._data['report_primary_shared_dimension_id'],
                        self._data['segment_id'],
                        self._id))


        for interval in self._db.record:
            interval['report_data_set_instance_id'] = 0
            available_intervals.append(interval)

        # see related
        self._db.Query("""SELECT e.*
                                    FROM dashboard_element_topic det, dashboard_element e
                                WHERE e.element_id = det.dashboard_element_id
                                    AND dashboard_element_id <> %s
                                    AND e.enabled_ind = 'Y'
                                    AND topic_id IN (select topic_id from dashboard_element_topic where dashboard_element_id = %s)
                                UNION SELECT e.*
                                    FROM dashboard_element e, metric_drill_to_report m
                                WHERE m.metric_element_id = e.element_id
                                    AND m.report_element_id = %s
                                    AND e.enabled_ind = 'Y'
                                    AND ifnull(e.segment_id,0) = %s
                            """, (self._id, self._id, self._id, self._data['segment_id']))
        

        for related_element in self._db.record:
            if not related_element['segment_id']:
                related_element['segment_id'] = 0
            if related_element['segment_id'] == self._data['segment_id']:
                related_element['segment_value_id'] = self._segment_value_id
            else:
                related_element['segment_value_id'] = 0
            related.append(related_element)

        # elements displayed on the page
        before_dataset = list()
        after_dataset = list()
        
        charts_before_dataset = list()
        charts_after_dataset = list()
        
        
        # dataset table
        dataset_el = OrderedDict()
        dataset_el['element_id'] = ''
        dataset_el['element_type'] = 'dataset'
        dataset_el['element_name'] = ''
        dataset_el['element_desc'] = ''
        dataset_el['placement'] = ''
        dataset_el['sequence'] = 0
        dataset_el['show_ind'] = self._data['show_data_set_table_in_report_ind']
        
        
        # charts
        self._db.Query("""SELECT *
                        FROM report_data_set_chart 
                    WHERE 
                        `element_id`= %s
                        AND 
                            (ISNULL(report_data_set_pivot_id)
                            OR report_data_set_pivot_id = 0) 
                    ORDER BY display_sequence ASC""", (self._id, ))
        for chart in self._db.record:
            chart_el = OrderedDict()
            chart_el['element_id'] = chart['report_data_set_chart_id']
            chart_el['element_type'] = 'chart'
            chart_el['pivot_id'] = 0
            if chart['report_data_set_pivot_id']:
                chart_el['pivot_id'] = chart['report_data_set_pivot_id']
            chart_el['element_name'] = chart['name']
            chart_el['element_desc'] = chart['description']
            chart_el['placement'] = chart['chart_placement']
            chart_el['sequence'] = chart['display_sequence']
            chart_el['show_ind'] = chart['enabled_ind']
            if chart_el['placement'] == 'before table': 
                charts_before_dataset.append(chart_el)
            else:
                charts_after_dataset.append(chart_el)
        
        # pivots
        self._db.Query("""SELECT *
                            FROM report_data_set_pivot
                        WHERE
                            `element_id`= %s
                        ORDER BY display_sequence ASC""", (self._id, ))
        for pivot in self._db.record:
            before_pivot = list()
            after_pivot = list()
            #pivot_element = list()
            
            pivot_el = OrderedDict()
            pivot_el['element_id'] = pivot['report_data_set_pivot_id']
            pivot_el['element_type'] = 'pivot'
            pivot_el['element_name'] = pivot['name']
            pivot_el['element_desc'] = ''
            pivot_el['placement'] = pivot['pivot_table_report_placement']
            pivot_el['sequence'] = pivot['display_sequence']
            pivot_el['show_ind'] = pivot['enabled_ind']
            
            # charts
            self._db.Query("""SELECT *
                            FROM report_data_set_chart 
                        WHERE 
                            `element_id`= %s
                            AND report_data_set_pivot_id = %s  
                        ORDER BY display_sequence ASC""",
                        (self._id, pivot_el['element_id']))
            for chart in self._db.record:
                chart_el = OrderedDict()
                chart_el['element_id'] = chart['report_data_set_chart_id']
                chart_el['element_type'] = 'chart'
                chart_el['pivot_id'] = 0
                if chart['report_data_set_pivot_id']:
                    chart_el['pivot_id'] = chart['report_data_set_pivot_id']
                chart_el['element_name'] = chart['name']
                chart_el['element_desc'] = chart['description']
                chart_el['placement'] = chart['chart_placement']
                chart_el['sequence'] = chart['display_sequence']
                chart_el['show_ind'] = chart['enabled_ind']
                if chart_el['placement'] == 'before table': 
                    before_pivot.append(chart_el)
                else:
                    after_pivot.append(chart_el)
            pivot_element = before_pivot + [pivot_el] + after_pivot   
            
            if pivot_el['placement'] == 'before data set':
                before_dataset += pivot_element
            else:
                after_dataset += pivot_element
        elements = charts_before_dataset + before_dataset + [dataset_el] + after_dataset + charts_after_dataset
      
        
        self._jfile.make_current_meta(last_data_set_instance,
                                      available_meas_times,
                                      available_intervals,
                                      drill_by,
                                      related,
                                      elements,
                                      self._segment_values)
    def _get_last_meas_time(self):
        """
        Get last measurement time for segment
        """

        #if flag for whole data regeneration is set
        if self._process_type == 'full_gen':
            return datetime.datetime(1900, 1, 1, 0, 0, 0)
        
         
        res = self._db.Query("""SELECT last_measurement_time
                            FROM last_dashboard_element_segment_value
                        WHERE
                            element_id = %s
                            AND segment_value_id = %s
                        """,(self._id, self._segment_value_id))
        if not res:
            return datetime.datetime(1900, 1, 1, 0, 0, 0)
        item = self._db.record[0]
        if item['last_measurement_time']:
            return item['last_measurement_time']
        return datetime.datetime(1900, 1, 1, 0, 0, 0)
        
    
    def _update_run_time(self):
        """
        Update last_display_generation_time
        """
        self._db.Query("""INSERT INTO last_dashboard_element_segment_value
                        SET last_display_generation_time = NOW(),
                            element_id = %s,
                            segment_value_id = %s
                        ON DUPLICATE KEY UPDATE 
                            last_display_generation_time = NOW()
                            """,(self._id, self._segment_value_id))
        self._db.Query("""UPDATE dashboard_element
                        SET last_run_time = NOW()
                        WHERE
                            `element_id` = %s""", (self._id, ))
    
    
    def _update_last_meas_time(self, meas_time):
        """
        Update last measurement time
        """
        self._db.Query("""INSERT INTO last_dashboard_element_segment_value
                        SET last_measurement_time = %s,
                            element_id = %s,
                            segment_value_id = %s
                        ON DUPLICATE KEY UPDATE 
                            last_measurement_time = %s""",(meas_time, self._id, self._segment_value_id, meas_time))
    
    def _populate_row_values(self, charts, all_data):
        """
        Update/insert row values for charts plotted by selected row values
        """
        # get needed charts
        charts = filter(lambda chart: chart['chart_type'] == 'line/bar' and chart['chart_include_method'] == 'selected values', charts)

        
        for chart in charts:
            populated_values = list()
            report_column = None
            # get data for chart
            if chart['report_data_set_pivot_id']:
                # it's pivot chart 
                data = all_data[chart['report_data_set_pivot_id']]
                pivot = filter(lambda pivot: pivot['report_data_set_pivot_id'] == chart['report_data_set_pivot_id'], self._pivots)[0]
                # get column id from pivot settings
                if chart['bars_or_lines_created_for'] == 'column headers':
                    column_id = pivot['pivot_column_value_column_id']
                else:
                    column_id = pivot['pivot_row_value_column_id']
                
                    
                # get column name 
                res = self._db.Query("""
                                SELECT column_name,
                                    report_data_set_column_id,
                                    value_type AS `type` 
                                    FROM report_data_set_column
                                WHERE element_id = %s
                                    AND report_data_set_column_id = %s""", (self._id, column_id))
                if res:
                    report_column = self._db.record[0]
                    if report_column['type'] == 'text':
                        if chart['bars_or_lines_created_for'] == 'column headers':
                            for header in data['header'][1:-1]:
                                if header['original_val'] != 'TOTAL':
                                    populated_values.append(header['original_val'])
                            
                        elif chart['bars_or_lines_created_for'] == 'row values':
                            # get charting column
                            orig_headers = [header['original_val'] for header in data['header']]
                            if report_column['column_name'] in orig_headers:
                                col_index = orig_headers.index(report_column['column_name'])
                                #column = data['header'][col_index]
                                # run all rows
                                for row in data['rows']:
                                    if row[col_index]['original_val'] != 'TOTAL':
                                        populated_values.append(row[col_index]['original_val'])
            else:
                if chart['bars_or_lines_created_for'] == 'row values':
                    # it's non-pivot chart
                    data = all_data[0]
                    # get charting column. it's first visible column
                    col_index = 0
                    for header in data['header']:
                        if header['show_column_in_table_display_ind'] == 'Y':
                            break
                        col_index += 1
                    column = data['header'][col_index]
                    if column['type'] == 'text':
                        # get column id 
                        res = self._db.Query("""
                                        SELECT report_data_set_column.report_data_set_column_id 
                                            FROM report_data_set_column
                                        WHERE report_data_set_column.element_id = %s
                                            AND report_data_set_column.column_name = %s""", (self._id, column['original_val']))
                            
                        if res:
                            report_column = self._db.record[0]
                            # run all rows
                            for row in data['rows']:
                                if row[col_index]['original_val'] != 'TOTAL':
                                    populated_values.append(row[col_index]['original_val']) 
             
            if report_column and populated_values:
                format_strings = ','.join(['%s'] * len(populated_values))
                
                param = list(populated_values)
                param.append(report_column['report_data_set_column_id'])
                self._db.Query("""SELECT report_data_set_row_value_id, row_value
                                                FROM report_data_set_row_value
                                            WHERE row_value IN(%s) AND
                                                report_data_set_column_id = %%s
                                                """% format_strings,tuple(param))
                existed_values = []
                existed_values_ids = []
                for row in self._db.record:
                    existed_values.append(row['row_value'])
                    existed_values_ids.append(row['report_data_set_row_value_id'])
                # insert new values
                for value in populated_values:
                    if not (value in existed_values):
                        self._db.Query("""INSERT INTO report_data_set_row_value
                                            SET row_value = %s,
                                            report_data_set_column_id = %s,
                                            last_updated_time = NOW()
                                            """,(value, report_column['report_data_set_column_id']))
                # update existed values
                if existed_values:
                    format_strings = ','.join(['%s'] * len(existed_values))
                    param = list(existed_values_ids)
                    param.append(report_column['report_data_set_column_id'])
                    self._db.Query("""UPDATE report_data_set_row_value
                                        SET last_updated_time = NOW()
                                            WHERE report_data_set_row_value_id IN(%s) AND
                                                report_data_set_column_id = %%s
                                                """% format_strings,tuple(param))
                    
    def unlock(self):
        if self._filelocker:
            self._filelocker.release()
        self._filelocker = None

    def _is_last_dataset_id(self, instance_id):
        """
        check if current instance id is the last instance id
        """
        res = self._db.Query("""SELECT report_data_set_instance_id
                                FROM report_data_set_instance
                            WHERE
                                `element_id`=%s
                                AND `segment_value_id` = %s
                            ORDER BY measurement_time DESC
                            LIMIT 0, 1""",(self._id, self._segment_value_id))
        if not res:
            return False
        last_data_set_instance = self._db.record[0]
        if last_data_set_instance['report_data_set_instance_id'] == instance_id:
            return True

        return False

    def update(self, segment_value_id):
        """
        Start update work flow
        """
        segments = list()
        if segment_value_id and self._segment and any(segment['segment_value_id'] == segment_value_id for segment in self._segment_values):
            segments = [segment for segment in self._segment_values if segment['segment_value_id'] == segment_value_id]
        elif self._segment_values:
            segments = self._segment_values
            self._process_dataset_ids = None
        else:
            segments.append(0)
            #self._process_dataset_ids = None

        self._outer_conn = self._get_outer_connection()

        any_data_fetched = False
        chart_gen = ChartGenerator()
        if self._outer_conn:
            self._jfile.save_fetch_settings({'sql': self._data['data_fetch_command'],
                                            'segment_id': self._data['segment_id'],
                                            'source_database_connection_id': self._data['source_database_connection_id'],
                                            })

            # check if index chart is set
            index_chart = 0
            if self._charts and self._data['report_index_report_data_set_chart_id'] \
                    and any(chart['report_data_set_chart_id']==self._data['report_index_report_data_set_chart_id'] for chart in self._charts):
                index_chart = filter(lambda chart: chart['report_data_set_chart_id'] == self._data['report_index_report_data_set_chart_id'], self._charts)[0]
            # no index chart is set, use first chart
            if self._charts and not index_chart:
                index_chart = self._charts[0]
            

            for segment_value in segments:
                
                self._jfile.set_segment_value(segment_value)
                if segment_value:
                    self._segment_value_id = segment_value['segment_value_id']
                    self._segment_value = segment_value
                else:
                    self._segment_value_id = 0
                    self._segment_value = None
                
                self._filelocker = FileLock("%s%s/run_segment_%s" % (self._path, self._id, self._segment_value_id), 0, 0)
                
                # try to lock run segment file lock
                if not self._filelocker.acquire():
                    # if segment file is lock continue for next segment
                    if self._logger:
                        self._logger.info("Segment %s is locked. Skip it." % self._segment_value_id)
                    continue
                
                
                if self._process_type == 'full_gen':
                    self._clear_instances(self._segment_value_id)
                    
                last_meas_time = self._get_last_meas_time()
                
                meas_times = self._get_meas_times(last_meas_time)
                
                update_columns = True
                

                if self._process_type == 'soft_gen':
                    update_columns = False

                
                #any_segment_data_fetched = False
                last_instance = None
                last_generation_time = None
                #instance = None

                if meas_times:
                    last_instance_id = None
                    meas_time = None
                    for meas_time in meas_times:
                        #start = time.time()
                        self._jfile.set_meas_time(meas_time)
                        instance = self._get_instance(meas_time, segment_value, last_meas_time)
                        all_data = dict()

                        last_instance_id = None
                        if instance:
                            if self._process_type != 'soft_gen':
                                last_instance = self._json_fetched_data
                                last_meas_time = meas_time
                                last_generation_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                #any_segment_data_fetched = True
                            any_data_fetched = True

                            # process data set
                            data_set_instance = self._process_instance(instance, meas_time, update_columns, False, segment_value)
                            last_instance_id = data_set_instance.instance_id

                            # save raw data
                            if self._process_type != 'soft_gen':
                                if self._data['report_save_historical_instances_ind'] == 'Y':
                                    self._jfile.save_data_fetch_instance(
                                                    {'instance': last_instance,
                                                     'meas_time': last_meas_time.strftime('%Y-%m-%d %H:%M:%S'),
                                                     'generation_time': last_generation_time
                                                     }, last_instance_id)
                                else:
                                    self._jfile.save_data_fetch(
                                                        {'instance': last_instance,
                                                         'meas_time': last_meas_time.strftime('%Y-%m-%d %H:%M:%S'),
                                                         'generation_time': last_generation_time
                                                         })

                            if data_set_instance:
                                # process new row values for charts drawn by selected row values
                                update_columns = False
                                all_data[0] = data_set_instance.get_formatted_header_rows()

                                # run all pivots
                                for pivot in self._pivots:
                                    #data_set_pivot_instance = self._process_pivot(pivot, data_set_instance, segment_value)
                                    data_set_pivot_instance = self._process_pivot(pivot, data_set_instance)
                                    all_data[pivot['report_data_set_pivot_id']] = data_set_pivot_instance.get_formatted_header_rows()

                                if self._process_type != 'soft_gen':
                                    self._populate_row_values(self._charts, all_data)

                                #insert chart instances to db. no not save instances if no historical instances or processing soft get
                                if self._data['report_save_historical_instances_ind'] == 'Y' and self._process_type != 'soft_gen':
                                    self._process_charts(last_instance_id)

                                chart_gen.report_charts(self._id, self._segment_value_id, meas_time, last_instance_id, all_data, self._jfile, chart_id=0)
                                    
#                                #create preview and thumbnail
#                                index_chart = None
#
#                                # check if index chart is set
#                                if self._charts and self._data['report_index_report_data_set_chart_id'] \
#                                        and any(chart['report_data_set_chart_id']==self._data['report_index_report_data_set_chart_id'] for chart in self._charts):
#                                    index_chart = filter(lambda chart: chart['report_data_set_chart_id'] == self._data['report_index_report_data_set_chart_id'], self._charts)[0]
#
#                                # no index chart is set, use first chart
#                                if self._charts and not index_chart:
#                                    index_chart = self._charts[0]

#                                create_thumb_preview = True
#                                print index_chart

#                                if not index_chart:
#                                    # do not create thumbnail/preview if no charts available
#                                    create_thumb_preview = False
#                                elif self._process_type == 'soft_gen':
#                                    # do not create thumbnail/preview if processing soft regeneration and current dataset instance id is not the last
#                                    if self._data['report_save_historical_instances_ind'] == 'Y' and not self._is_last_dataset_id(last_instance_id):
#                                        create_thumb_preview = False
#
#
#                                if create_thumb_preview:
#                                    chart_gen.report_thumbnail(self._id, self._segment_value_id, meas_time, 0, all_data, self._jfile, chart_id=index_chart['report_data_set_chart_id'])
#                                    chart_gen.report_preview(self._id, self._segment_value_id, meas_time, 0, all_data, self._jfile, chart_id=index_chart['report_data_set_chart_id'])

                                if self._process_type != 'soft_gen':
                                    self._update_last_meas_time(meas_time)

                        #print "it took", time.time() - start, "seconds."


                    create_thumb_preview = True

                    if last_instance_id:
                        if not index_chart:
                            # do not create thumbnail/preview if no charts available
                            create_thumb_preview = False
                        elif self._process_type == 'soft_gen':
                            # do not create thumbnail/preview if processing soft regeneration and current dataset instance id is not the last
                            if self._data['report_save_historical_instances_ind'] == 'Y' and not self._is_last_dataset_id(last_instance_id):
                                create_thumb_preview = False


                        if create_thumb_preview:
                            chart_gen.report_thumbnail(self._id, self._segment_value_id, meas_time, 0, all_data, self._jfile, chart_id=index_chart['report_data_set_chart_id'])
                            chart_gen.report_preview(self._id, self._segment_value_id, meas_time, 0, all_data, self._jfile, chart_id=index_chart['report_data_set_chart_id'])

                    
                    # create current json files for historical instances
                    if self._data['report_save_historical_instances_ind'] == 'Y' and last_instance_id and \
                            (self._process_type != 'soft_gen' or (self._process_type == 'soft_gen' and self._is_last_dataset_id(last_instance_id))):
                        self._make_current_jfiles()

                    self._make_meta()

                self._update_run_time()

#                if any_segment_data_fetched and self._process_type != 'soft_gen':
#                    self._jfile.save_data_fetch({'instance': last_instance,
#                                                 'meas_time': last_meas_time.strftime('%Y-%m-%d %H:%M:%S'),
#                                                 'generation_time': last_generation_time
#                                                 })

                # release run segment file lock
                self.unlock()
                
                
            if not any_data_fetched:
                return "None of data was fetched"
        else:
            raise Exception("No external db connection")
        return ''

        

