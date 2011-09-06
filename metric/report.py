#-*- coding: utf-8 -*-

from outerdb.conn import JCollectorConn
from file_man.jfiles import JFile
from file_man.flocker import FileLock
from conf import ConfigReader
from db.db_conn import DBManager
import datetime
from simplejson.ordered_dict import OrderedDict
from chart.report_chart import ReportChart
from dataset import DataSet
from formatter import FieldFormatter, DateUnformat
import simplejson
import os
from operator import itemgetter
from libs import is_int
from report_data_manager import ReportDataTableManager
import pprint
import time
import sys

class Report(object):
    """
    Internal Report Dashboard Element
    """

    _id = None
    # path to directory for data and meta data
    _path = None
    _web_service_data = None
    _segment = {}
    _segment_id = 0
    _segment_values = []
    _segment_value_id = None


    _charts = []
    _pivots = []
    _filelocker = None
    _logger = None
    _process_type = 'normal'
    _process_dataset_ids = None
    _initial_measurement_time = None
    _final_measurement_time = None
    _composite_manager = None
    _outer_conn = None
    _outer_conn_meas_time = None

    _index_chart = None

    _is_fetched = False
    
    def __init__(self, process_type, process_dataset_ids, initial_measurement_time, final_measurement_time = None):
        config = ConfigReader()
        self._path = config.report_root
        self._db = DBManager.get_query()
        # process type: normal, soft, full or composite
        self._process_type = process_type
        # dataset ids for 'soft' regeneration
        self._process_dataset_ids = process_dataset_ids

        if self._process_type in ('full_gen', 'delete_data'):
            self._initial_measurement_time = self._parse_datetime(initial_measurement_time, datetime.datetime(1900, 1, 1, 0, 0, 0))
            self._final_measurement_time = self._parse_datetime(final_measurement_time, datetime.datetime(datetime.MAXYEAR, 1, 1, 0, 0, 0))
            if self._final_measurement_time < self._initial_measurement_time:
                raise Exception("Final measurement time '%s' is less than initial measurement time '%s'" % (self._final_measurement_time, self._initial_measurement_time))
        else:
            self._initial_measurement_time = datetime.datetime(1900, 1, 1, 0, 0, 0)
            self._final_measurement_time = datetime.datetime(datetime.MAXYEAR, 1, 1, 0, 0, 0)


    def init(self, id):
        """
        Initiates report settings
        """
        self._id = id

        self._outer_conn = None
        self._outer_conn_meas_time = None
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
        if self._data['source_for_composite_reporting_ind'] == u'Y':
            self._composite_manager = ReportDataTableManager(self._id, self._logger)

    def _parse_datetime(self, raw_measurement_time, value_if_raw_fails):
        """
        Parses and returns initial measurement time for full regeneration
        """

        measurement_time = None
        if raw_measurement_time:
            unformatter = DateUnformat()
            measurement_time = unformatter.unformat(raw_measurement_time)
        if not measurement_time:
            measurement_time = value_if_raw_fails
            #initial_measurement_time = None
        return measurement_time

    def _parse_process_dataset_ids(self):
        """
        Parses string with dataset ids into list on numbers and returns it
        """
        
        if self._data['report_save_historical_instances_ind'] != u'Y':
            process_dataset_ids = None
        else:
            process_dataset_ids = []
            items = self._process_dataset_ids.split(',')
            for item in items:
                interval = item.split('-')
                #if type(interval) == list and len(interval) == 2:
                if isinstance(interval, list) and len(interval) == 2:
                    if is_int(interval[0]) and is_int(interval[1]):
                        process_dataset_ids.append([int(interval[0]), int(interval[1])])
                elif len(interval) == 1:
                    if is_int(item):
                        process_dataset_ids.append(int(item))
        self._process_dataset_ids = process_dataset_ids

    def set_logger(self, logger):
        """
        Just sets logger. Get rid of it.
        """
        self._logger = logger

    def _clear_instances(self, segment_value_id):
        """
        Removes all existing instances of charts, pivots, data sets from db
        It is needed for full report regeneration
        """
        
        # get dataset instances ids
        self._db.Query("""SELECT report_data_set_instance_id
                                FROM report_data_set_instance
                                    WHERE element_id = %s
                                        AND segment_value_id = %s
                                        AND measurement_time >= %s
                                        AND measurement_time <= %s""", (self._id, segment_value_id, self._initial_measurement_time, self._final_measurement_time))

        report_data_set_instance_ids = [row['report_data_set_instance_id'] for row in self._db.record]

        # process charts
        for chart in self._charts:
            # remove annotation charts instance
            self._db.Query("""
                            DELETE
                                FROM report_data_set_chart_annotation_instance
                            WHERE report_data_set_chart_instance_id IN (
                                SELECT report_data_set_chart_instance_id
                                    FROM report_data_set_chart_instance
                                WHERE
                                    report_data_set_chart_id = %s
                                    AND report_data_set_instance_id IN (
                                        SELECT report_data_set_instance_id
                                            FROM report_data_set_instance
                                        WHERE
                                            element_id = %s
                                            AND segment_value_id = %s
                                            AND measurement_time >= %s
                                            AND measurement_time <= %s                                            
                                    )
                            )""", (chart['report_data_set_chart_id'], self._id, segment_value_id, self._initial_measurement_time, self._final_measurement_time))

            # remove chart instances files
            self._jfile.purge_chart_files(chart['report_data_set_chart_id'], report_data_set_instance_ids, segment_value_id)
            # remove charts instances from DB
            self._db.Query("""
                            DELETE
                                FROM report_data_set_chart_instance
                            WHERE
                                report_data_set_chart_id = %s
                                AND report_data_set_instance_id IN (
                                    SELECT report_data_set_instance_id
                                        FROM report_data_set_instance
                                    WHERE
                                        element_id = %s
                                        AND segment_value_id = %s
                                        AND measurement_time >= %s
                                        AND measurement_time <= %s
                                )""", (chart['report_data_set_chart_id'], self._id, segment_value_id, self._initial_measurement_time, self._final_measurement_time))
        # process pivots
        for pivot in self._pivots:
            # remove chart instances files
            self._jfile.purge_pivot_files(pivot['report_data_set_pivot_id'], report_data_set_instance_ids, segment_value_id)
            # remove pivots instances from DB
            self._db.Query("""
                            DELETE
                                FROM report_data_set_pivot_instance
                            WHERE
                                report_data_set_pivot_id = %s
                                AND report_data_set_instance_id IN (
                                    SELECT report_data_set_instance_id
                                        FROM report_data_set_instance
                                    WHERE
                                        element_id = %s
                                        AND segment_value_id = %s
                                        AND measurement_time >= %s
                                        AND measurement_time <= %s                                        
                                )""", (pivot['report_data_set_pivot_id'], self._id, segment_value_id, self._initial_measurement_time, self._final_measurement_time))

        # remove dataset instances from DB
        self._db.Query("""
                    DELETE
                        FROM report_data_set_instance
                    WHERE
                        element_id = %s
                        AND segment_value_id = %s
                        AND measurement_time >= %s
                        AND measurement_time <= %s""", (self._id, segment_value_id, self._initial_measurement_time, self._final_measurement_time))
        # remove dataset instances files
        self._jfile.purge_dataset_files(report_data_set_instance_ids, segment_value_id)

    def _get_element(self):
        """
        Returns main report data
        """
        self._db.Query("""SELECT dashboard_element.*,
                               dashboard_category.category, 
                               topic.name AS topic_name,
                               measurement_interval.measurement_interval_button_name,
                               measurement_interval.display_mask_id AS def_date_mask_id,
                               measurement_interval.preview_display_format_string
                            FROM dashboard_element 
                            LEFT JOIN dashboard_category ON dashboard_category.category_id = dashboard_element.category_id
                            LEFT JOIN topic ON topic.topic_id = dashboard_element.primary_topic_id
                            LEFT JOIN measurement_interval ON measurement_interval.measurement_interval_id = dashboard_element.measurement_interval_id
                        WHERE 
                            dashboard_element.`element_id` = %s""", (self._id, ))
        data = self._db.record[0]
        if not data['last_report_column_metadata_update_time']:
            data['last_report_column_metadata_update_time'] = datetime.datetime(1900, 1, 1, 0, 0, 0)
        if not data['last_data_fetch_command_update_time']:
            data['last_data_fetch_command_update_time'] = datetime.datetime(1900, 1, 1, 0, 0, 0)
        if not data['segment_id']:
            data['segment_id'] = 0
        if not data['report_used_for_drill_to_ind']:
            data['report_used_for_drill_to_ind'] = u'N'
        return data

    def _get_segment(self):
        """
        Returns report segment data
        """
        segment = []
        if self._data['segment_id']:
            res = self._db.Query("""SELECT *
                            FROM segment
                        WHERE 
                            `segment_id` = %s""",(self._data['segment_id']))
            if res:
                segment = self._db.record[0]
                if not segment['segment_value_prefix']:
                    segment['segment_value_prefix'] = ''
                if not segment['segment_value_suffix']:
                    segment['segment_value_suffix'] = ''
        return segment

    def _get_segment_values(self):
        """
        Returns report segment values (if any)
        """
        segment_values = []
        if self._segment:
            self._db.Query("""SELECT *
                            FROM segment_value
                        WHERE
                            `segment_id` = %s""",(self._segment['segment_id']))

            segment_values = [segment for segment in self._db.record]
        return segment_values

    def _get_pivots(self, enabled_only = True):
        """
        Gets pivots info
        """
        if enabled_only:
            self._db.Query("""SELECT *
                                FROM report_data_set_pivot 
                            WHERE 
                                `element_id` = %s AND enabled_ind = 'Y'""", (self._id, ))
        else:
            self._db.Query("""SELECT *
                                FROM report_data_set_pivot 
                            WHERE 
                                `element_id` = %s""", (self._id, ))
        pivots = [pivot for pivot in self._db.record]
        return pivots

    def _get_charts(self, enabled_only = True):
        """
        Get charts info
        """
        charts = []
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
            sql +=  """ AND report_data_set_chart.enabled_ind = 'Y'"""
        
        self._db.Query(sql, (self._id, ))
        
        for chart in self._db.record:
            if not chart['report_data_set_pivot_id']:
                chart['report_data_set_pivot_id'] = 0
            charts.append(chart)

        self._index_chart = None

        # get index chart to be used fore preview/thumbnail
        if (charts and self._data['report_index_report_data_set_chart_id']
                and any(chart['report_data_set_chart_id'] == self._data['report_index_report_data_set_chart_id'] for chart in charts)):
            self._index_chart = filter(lambda chart: chart['report_data_set_chart_id'] == self._data['report_index_report_data_set_chart_id'], charts)[0]

        # no index chart is set, use first chart
        if charts and not self._index_chart:
            self._index_chart = charts[0]

        return charts
        
    #def _get_outer_connection(self, data_fetch_method, source_database_connection_id, web_service_credentials_id):
    def _get_outer_connection(self, get_meas_time_conn=True):
        self._outer_conn = self._create_outer_connection(self._data['data_fetch_method'],
                                                      self._data['source_database_connection_id'],
                                                      self._data['web_service_credentials_id'],
                                                      self._data['plugin_connection_profile_id'])
        if get_meas_time_conn:
            if self._data['report_measurement_time_calc_method'] == u'execution time':
                self._outer_conn_meas_time = self._outer_conn
            else:
                res = self._db.Query("""SELECT measurement_time_calc_command.*
                                FROM measurement_time_calc_command
                            WHERE
                                `measurement_time_calc_command_id`=%s""",(self._data['report_measurement_time_calc_command_id']))
                if not res:
                    raise Exception("Measurement_time_calc_command not found")
                meas_time_calc_command = self._db.record[0]

                if not meas_time_calc_command['source_database_connection_id']:
                    meas_time_calc_command['source_database_connection_id'] = self._data['source_database_connection_id']

                if not meas_time_calc_command['web_service_credentials_id']:
                    meas_time_calc_command['web_service_credentials_id'] = self._data['web_service_credentials_id']

                self._outer_conn_meas_time = self._create_outer_connection(meas_time_calc_command['data_fetch_method'],
                                                          meas_time_calc_command['source_database_connection_id'],
                                                          meas_time_calc_command['web_service_credentials_id'],
                                                          meas_time_calc_command['plugin_connection_profile_id'])


    def _create_outer_connection(self, data_fetch_method, source_database_connection_id, web_service_credentials_id, plugin_connection_profile_id):
        """
        Creates and returns  connection to outer db
        """
        conn = JCollectorConn(
                    self._id,
                    self._segment,
                    data_fetch_method,
                    source_database_connection_id,
                    web_service_credentials_id,
                    plugin_connection_profile_id)
        conn._logger = self._logger


        if not conn:
            raise Exception("No source database connection specified")

        return conn

    def _get_meas_times(self, from_time, to_time):
        """
        Gets and returns available measurement times starting from last measurement time
        """

        meas_times = []

        if self._process_type == 'soft_gen':# or self._process_type == 'composite':
            meas_times = self._get_meas_times_from_db()

        if not meas_times:
            data = self._get_meas_times_from_external(from_time)
            if data:
                clear_data = [row[0] for row in data['data']]
                # check if we have values in list of datetime type
                if clear_data:
                    if isinstance(clear_data[0], datetime.datetime):
                        meas_times = clear_data
                    else:
                        # it's a date type
                        meas_times = [datetime.datetime.combine(d, datetime.time.min) for d in clear_data]

        # sort measurement times if they weren't sorted before
        meas_times.sort()
        # remove all times which are greater than "to_time"
        if to_time:
            if meas_times and meas_times[0] > to_time:
                del meas_times[:]            

            for i, meas_time in enumerate(reversed(meas_times)):
                if meas_time > to_time:
                    del meas_times[-i:]
                    break

        # if do not save history, take only last element
        if self._data['report_save_historical_instances_ind'] != u'Y':
            if meas_times:
                del meas_times[:-1]
        
        return meas_times

    def _get_all_meas_times_from_db(self):
        """
        Fetches all existing measurement times from db for composite regeneration
        """
        meas_times = []
        if self._data['report_save_historical_instances_ind'] != u'Y':
            # for non historical reports take measurement time from saved dataset
            dataset = self._jfile.get_current_stored_dataset()
            if dataset:
                try:
                    meas_time = datetime.datetime.strptime(dataset['meas_time'], '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    raise Exception("Cannot unformat string %s to datetime" % dataset['meas_time'])
                meas_times.append(meas_time)
        else:
            # for historical reports take measurement times from db datasets
            where_sql = ''
            where_sql_list = []
            params = [self._id, self._segment_value_id]

            if self._process_dataset_ids:
                for dataset_id in self._process_dataset_ids:
                    #if type(dataset_id) == list:
                    if isinstance(dataset_id, list):
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

    def _get_meas_times_from_db(self):
        """
        Fetches existing measurement times from db for soft regeneration
        """
        meas_times = []
        if self._data['report_save_historical_instances_ind'] != u'Y':
            # for non historical reports take measurement time from saved dataset
            dataset = self._jfile.get_current_stored_dataset()
            if dataset:
                try:
                    meas_time = datetime.datetime.strptime(dataset['meas_time'], '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    raise Exception("Cannot unformat string %s to datetime" % dataset['meas_time'])
                meas_times.append(meas_time)
        else:
            # for historical reports take measurement times from db datasets
            where_sql = ''
            where_sql_list = []
            params = [self._id, self._segment_value_id]

            if self._process_dataset_ids:
                for dataset_id in self._process_dataset_ids:
                    #if type(dataset_id) == list:
                    if isinstance(dataset_id, list):
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

    def _get_meas_times_from_external(self, last_meas_time):
        """
        Fetches available measurement times from external source
        """

        if self._data['report_measurement_time_calc_method'] == 'execution time':
            meas_times = self._outer_conn_meas_time.get_current_time()
        else:
            res = self._db.Query("""SELECT measurement_time_calc_command.*
                            FROM measurement_time_calc_command
                        WHERE
                            `measurement_time_calc_command_id`=%s""",(self._data['report_measurement_time_calc_command_id']))
            if not res:
                raise Exception("Measurement_time_calc_command not found")

            meas_time_calc_command = self._db.record[0]
            meas_times = self._outer_conn_meas_time.get_meas_times(self._segment_value_id, meas_time_calc_command['data_fetch_command'], last_meas_time)

        return meas_times

    #def _get_instance(self, meas_time, segment_value):
    def _get_instance(self, meas_time):
        """
        Fetches data from outer db for measurement time instance
        """
        
        if self._process_type == 'soft_gen' or self._process_type == 'composite':
            saved_dataset = None

            if self._data['report_save_historical_instances_ind'] != u'Y':
                # for non historical reports take measurement time from saved dataset
                saved_dataset = self._jfile.get_current_stored_dataset()
            else:
                res = self._db.Query("""SELECT report_data_set_instance_id
                            FROM report_data_set_instance
                        WHERE
                            `element_id`= %s
                            AND segment_value_id = %s
                            AND measurement_time = %s
                        LIMIT 0, 1""", (self._id, self._segment_value_id, meas_time))
                if res:
                    data_set_instance_id = self._db.record[0]['report_data_set_instance_id']
                    saved_dataset = self._jfile.get_stored_dataset(data_set_instance_id)

            if saved_dataset:
                dataset = simplejson.loads(saved_dataset['instance'])
                data = self._outer_conn.parse_collected_data(dataset)
                self._is_fetched = False
                return data

        self._is_fetched = True

        if not self._data['data_fetch_command']:
            raise Exception("Data fetch command is empty")

        data = self._outer_conn.get_data(self._segment_value_id, self._data['data_fetch_command'], meas_time)
        self._json_fetched_data = self._outer_conn.get_json_result()

        return data

    def _get_data_set(self, meas_time, segment_value, update_columns):
        """
        Fetches data set for specified measurement time and creates dataset class
        """
        # get instance
        #instance = self._get_instance(meas_time, segment_value)
        instance = self._get_instance(meas_time)

        # process data set
        if instance and instance['data']:
            if self._data['source_for_composite_reporting_ind'] == u'Y':
                self._load_composite_data(instance, meas_time, segment_value)

            # process data set
            data_set_instance = self._process_instance(instance, meas_time, update_columns, write_clear_headers=False, segment_value=segment_value)

            # save raw data
            if self._is_fetched:
                json_instance = self._json_fetched_data
                last_generation_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                instance_id = data_set_instance.instance_id

                if self._data['report_save_historical_instances_ind'] == u'Y':
                    self._jfile.save_data_fetch_instance({'instance': json_instance,
                                                             'meas_time': meas_time.strftime('%Y-%m-%d %H:%M:%S'),
                                                             'generation_time': last_generation_time
                                                             }, instance_id)
                else:
                    self._jfile.save_data_fetch({'instance': json_instance,
                                                 'meas_time': meas_time.strftime('%Y-%m-%d %H:%M:%S'),
                                                 'generation_time': last_generation_time})
            return data_set_instance
        return None

    def _process_charts(self, data_set_instance_id):
        """
        Processes charts - writes to report_data_set_chart_instance table
        """
        for chart in self._charts:
            self._db.Query("""INSERT INTO report_data_set_chart_instance
                    (report_data_set_chart_id, report_data_set_instance_id, chart_generation_time)
                    VALUES(%s, %s, NOW())
                    ON DUPLICATE KEY UPDATE chart_generation_time = NOW()""",(chart['report_data_set_chart_id'], data_set_instance_id))
            
    def _process_instance(self, instance, meas_time, update_columns, write_clear_headers, segment_value):
        """
        Processes fetched data set into class and creates data set json file
        """
        data_set_instance = DataSet(self._id, self._data, meas_time, self._formatter, segment_value)
        data_set_instance.process_data(instance, update_columns)
        if self._data['report_save_historical_instances_ind'] == u'Y':
            data_set_instance.insert_to_db()
        self._jfile.create_data_set(data_set_instance.instance_id, data_set_instance.prepare_json(write_clear_headers), meas_time)
        if update_columns and self._data['source_for_composite_reporting_ind'] == u'Y':
            self._composite_manager.validate_structure()

        return data_set_instance


    def _process_pivot(self, pivot, data_set_instance):
        """
        Processes pivot
        """
        data_set_pivot = data_set_instance.create_pivot(pivot)
        #data_set_pivot.prepare_pivot()
        if self._data['report_save_historical_instances_ind'] == u'Y':
            data_set_pivot.insert_to_db()

        data_set_pivot.process_pivot()
        # make a json file
        self._jfile.create_pivot(pivot, data_set_instance.instance_id, data_set_pivot.prepare_json())
        return data_set_pivot
        

    def _make_current_jfiles(self):
        """
        Creates "current" (*_current) versions of the last json files
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
        Creates report metadata file
        """
        available_meas_times = []
        available_intervals = []
        #drill_by = []
        related = []
        last_data_set_instance = {}

        if self._data['report_save_historical_instances_ind'] == u'Y':
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
                            (dashboard_element.`element_id` <> %s
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
        elements = []

        # dataset
        dataset_el = OrderedDict()
        dataset_el['element_id'] = ''
        dataset_el['element_type'] = 'dataset'
        dataset_el['element_name'] = ''
        dataset_el['element_desc'] = ''
        dataset_el['placement'] = ''
        dataset_el['sequence'] = 0
        dataset_el['show_ind'] = self._data['show_data_set_table_in_report_ind']
        
        root_elements_before_dataset = []
        root_elements_after_dataset = []

        # get non-pivot charts
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
                #charts_before_dataset.append(chart_el)
                root_elements_before_dataset.append(chart_el)
            else:
                #charts_after_dataset.append(chart_el)
                root_elements_after_dataset.append(chart_el)
        
        # get pivots
        self._db.Query("""SELECT *
                            FROM report_data_set_pivot
                        WHERE
                            `element_id`= %s
                        ORDER BY display_sequence ASC""", (self._id, ))
        for pivot in self._db.record:
            pivot_el = OrderedDict()
            pivot_el['element_id'] = pivot['report_data_set_pivot_id']
            pivot_el['element_type'] = 'pivot'
            pivot_el['element_name'] = pivot['name']
            pivot_el['element_desc'] = ''
            pivot_el['placement'] = pivot['pivot_table_report_placement']
            pivot_el['sequence'] = pivot['display_sequence']
            pivot_el['show_ind'] = pivot['enabled_ind']
            if pivot_el['placement'] == 'before data set':
                root_elements_before_dataset.append(pivot_el)
            else:
                root_elements_after_dataset.append(pivot_el)

        root_elements_before_dataset.sort(key=itemgetter('sequence'))
        root_elements_after_dataset.sort(key=itemgetter('sequence'))

        for element in root_elements_before_dataset:
            if element['element_type'] == 'chart':
                elements.append(element)
            elif element['element_type'] == 'pivot':
                # get pivot elements
                pivot_elements = self._get_pivot_charts(element)
                elements.extend(pivot_elements)

        elements.append(dataset_el)

        for element in root_elements_after_dataset:
            if element['element_type'] == 'chart':
                elements.append(element)
            elif element['element_type'] == 'pivot':
                # get pivot elements
                pivot_elements = self._get_pivot_charts(element)
                elements.extend(pivot_elements)

        available_segments = self._get_non_empty_segments()

        self._jfile.make_current_meta(last_data_set_instance,
                                      available_meas_times,
                                      available_intervals,
                                      related,
                                      elements,
                                      available_segments)

    def _get_non_empty_segments(self):
        """
        Filters segment values with some data only
        """
        segments = []
        for segment in self._segment_values:
            if self._fetch_last_meas_time(segment['segment_value_id']):
                segments.append(segment)
        return segments
                

    def _get_pivot_charts(self, pivot):
        """
        Returns ordered pivot charts separated into two groups: before and after pivot table
        """
        before_pivot = []
        after_pivot = []
        pivot_elements = []
        
        self._db.Query("""SELECT *
                        FROM report_data_set_chart
                    WHERE
                        `element_id`= %s
                        AND report_data_set_pivot_id = %s
                    ORDER BY display_sequence ASC""",
                    (self._id, pivot['element_id']))

        for chart in self._db.record:
            chart_el = OrderedDict()
            chart_el['element_id'] = chart['report_data_set_chart_id']
            chart_el['element_type'] = 'chart'
            chart_el['pivot_id'] = 0
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
        if before_pivot:
            pivot_elements.extend(before_pivot)
        pivot_elements.append(pivot)
        if after_pivot:
            pivot_elements.extend(after_pivot)
        return pivot_elements

    def _get_last_meas_time(self):
        """
        Returns last measurement time for segment
        """
        # if flag for whole data regeneration is set

        if self._process_type in ('full_gen', 'delete_data'):
            return self._initial_measurement_time

        last_measurement_time = self._fetch_last_meas_time(self._segment_value_id)
        if not last_measurement_time:
            return datetime.datetime(1900, 1, 1, 0, 0, 0)

        return last_measurement_time

    def _fetch_last_meas_time(self, segment_value_id):
        """
        Fetches last measurement time from db
        """
        last_measurement_time = None
        res = self._db.Query("""SELECT last_measurement_time
                            FROM last_dashboard_element_segment_value
                        WHERE
                            element_id = %s
                            AND segment_value_id = %s
                        """,(self._id, segment_value_id))
        if res:
            item = self._db.record[0]
            last_measurement_time = item['last_measurement_time']
        return last_measurement_time

    def _update_run_time(self):
        """
        Updates last_display_generation_time
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
        Updates last measurement time for segment
        """
        self._db.Query("""INSERT INTO last_dashboard_element_segment_value
                        SET last_measurement_time = %s,
                            element_id = %s,
                            segment_value_id = %s
                        ON DUPLICATE KEY UPDATE 
                            last_measurement_time = %s""",(meas_time, self._id, self._segment_value_id, meas_time))
    
    def _populate_row_values(self, charts, all_data):
        """
        Updates/inserts row values for charts plotted by selected row values
        """
        # get needed charts
        charts = filter(lambda chart: chart['chart_type'] == 'line/bar' and chart['chart_include_method'] == 'selected values', charts)

        for chart in charts:
            populated_values = []
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
                        if header['show_column_in_table_display_ind'] == u'Y':
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
        """
        Unlocks segment for other generator instances
        """
        if self._filelocker:
            self._filelocker.release()
        self._filelocker = None

    def _is_last_dataset_id(self, instance_id):
        """
        Check if specified instance id is the last instance id
        """
        last_instance_id = self._get_last_dataset_id()
        if last_instance_id and last_instance_id == instance_id:
            return True
        return False

    def _get_last_dataset_id(self):
        """
        Returns last instance id
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

        return last_data_set_instance['report_data_set_instance_id']

    def _load_composite_data(self, instance, meas_time, segment_value):
        if segment_value:
            segment_value_id = segment_value['segment_value_id']
        else:
            segment_value_id = 0
        self._composite_manager.process_instance(instance, meas_time, segment_value_id)

    def _get_working_segments(self, segment_value_id):
        """
        Returns list of available/specified segment values or list with zero in un-segmented
        """
        segments = []
        if segment_value_id and self._segment and any(segment['segment_value_id'] == segment_value_id for segment in self._segment_values):
            segments = [segment for segment in self._segment_values if segment['segment_value_id'] == segment_value_id]
        elif self._segment_values:
            segments = self._segment_values
            self._process_dataset_ids = None
        else:
            segments.append(0)
        return segments
        
    def update(self, segment_value_id):
        """
        Main method. Starts update work flow
        """
        if self._process_type == 'composite':
            return self._update_composite(segment_value_id)

        segments = self._get_working_segments(segment_value_id)

        self._get_outer_connection()
        any_data_fetched = False

        self._jfile.save_fetch_settings({'sql': self._data['data_fetch_command'],
                                        'segment_id': self._data['segment_id'],
                                        'source_database_connection_id': self._data['source_database_connection_id']})

        has_unlocked_segments = False
        process_type = self._process_type

        for segment_value in segments:
            self._process_type = process_type
            self._jfile.set_segment_value(segment_value)
            if segment_value:
                self._segment_value_id = segment_value['segment_value_id']
                self._segment_value = segment_value
            else:
                self._segment_value_id = 0
                self._segment_value = None

            # check if segment is not in run by other process
            if not self._available():
                # if segment file is lock continue for next segment
                self._logger.info("Report %s. Segment %s is locked. Skip it." % (self._id, self._segment_value_id))
                if len(segments) == 1:
                    raise Exception("You can not initiate live-update of this report at this time because report generation has already been initiated by a different process.")
                continue

            has_unlocked_segments = True

            if self._process_type in ('full_gen', 'delete_data'):
                self._clear_instances(self._segment_value_id)

            if self._process_type == 'delete_data':
                continue

            last_meas_time = self._get_last_meas_time()
            real_last_measurement_time = self._fetch_last_meas_time(self._segment_value_id)

            meas_times = self._get_meas_times(last_meas_time, self._final_measurement_time)

            if not meas_times and (self._process_type == 'normal' or self._process_type == 'soft_gen'):
                if last_meas_time:
                    self._process_type = 'soft_gen'
                    meas_times = [last_meas_time]

            update_columns = True

            if self._process_type == 'soft_gen' or self._process_type == 'composite':
                update_columns = False

            all_data = {}
            
            if meas_times:
                last_instance_id = None
                meas_time = None

                for meas_time in meas_times:
                    self._jfile.set_meas_time(meas_time)
                    data_set_instance = self._get_data_set(meas_time, segment_value, update_columns)

                    if data_set_instance:
                        last_instance_id = data_set_instance.instance_id
                        any_data_fetched = True
                        # process new row values for charts drawn by selected row values
                        update_columns = False

                        all_data[0] = data_set_instance.get_formatted_header_rows()

                        # run all pivots
                        for pivot in self._pivots:
                            data_set_pivot_instance = self._process_pivot(pivot, data_set_instance)
                            all_data[pivot['report_data_set_pivot_id']] = data_set_pivot_instance.get_formatted_header_rows()

                        if self._process_type != 'soft_gen':
                            self._populate_row_values(self._charts, all_data)

                        #insert chart instances to db. no not save instances if no historical instances or processing soft get
                        if self._data['report_save_historical_instances_ind'] == u'Y' and self._process_type != 'soft_gen':
                            self._process_charts(last_instance_id)

                        # run all charts
                        for chart in self._charts:
                            if chart['report_data_set_pivot_id']:
                                if not all_data.has_key(chart['report_data_set_pivot_id']):
                                    raise Exception("Source pivot %s for chart %s does not exists" % (chart['report_data_set_pivot_id'], chart['report_data_set_chart_id']))
                                data_chart = all_data[chart['report_data_set_pivot_id']]
                            else:
                                data_chart = all_data[0]
                            if self._index_chart and self._index_chart['report_data_set_chart_id'] == chart['report_data_set_chart_id']:
                                is_index = True
                                if (self._process_type == 'soft_gen' and
                                        self._data['report_save_historical_instances_ind'] == u'Y' and
                                        not self._is_last_dataset_id(last_instance_id)):
                                    is_index = False
                            else:
                                is_index = False
                            report_chart = ReportChart(chart['report_data_set_chart_id'],
                                                       self._id,
                                                       self._segment_value_id,
                                                       meas_time,
                                                       last_instance_id,
                                                       data_chart,
                                                       self._jfile,
                                                       'large',
                                                       is_index,
                                                       self._data['preview_display_format_string'],
                                                       self._formatter)
                            report_chart.generateChart()
                        if self._process_type != 'soft_gen':
                            self._update_last_meas_time(meas_time)

                if self._process_type != 'composite':
                    if self._index_chart and all_data:
                        create_thumb_preview = True
                        if last_instance_id:
                            if (self._process_type == 'soft_gen' and self._data['report_save_historical_instances_ind'] == u'Y'
                                    and not self._is_last_dataset_id(last_instance_id)):
                                # do not create thumbnail/preview if processing soft regeneration and current dataset instance id is not the last
                                create_thumb_preview = False

                        if create_thumb_preview:
                            if self._index_chart['report_data_set_pivot_id']:

                                data_chart = all_data[self._index_chart['report_data_set_pivot_id']]
                            else:
                                data_chart = all_data[0]
                            report_chart = ReportChart(self._index_chart['report_data_set_chart_id'],
                                                       self._id,
                                                       self._segment_value_id,
                                                       meas_time,
                                                       last_instance_id,
                                                       data_chart,
                                                       self._jfile,
                                                       'thumbnail',
                                                       False,
                                                       self._data['preview_display_format_string'],
                                                       self._formatter)
                            report_chart.generateChart()

                            report_chart = ReportChart(self._index_chart['report_data_set_chart_id'],
                                                       self._id,
                                                       self._segment_value_id,
                                                       meas_time,
                                                       last_instance_id,
                                                       data_chart,
                                                       self._jfile,
                                                       'preview',
                                                       False,
                                                       self._data['preview_display_format_string'],
                                                       self._formatter)
                            report_chart.generateChart()

                    # create current json files for historical instances
                    if (self._data['report_save_historical_instances_ind'] == u'Y' and last_instance_id and
                            (self._process_type != 'soft_gen' or (self._process_type == 'soft_gen' and self._is_last_dataset_id(last_instance_id)))):
                        self._make_current_jfiles()

            self._make_meta()
            if self._process_type != 'composite':
                self._update_run_time()
                if any_data_fetched and self._segment and not real_last_measurement_time:
                    for segment_value in self._segment_values:
                        if segment_value['segment_value_id'] != self._segment_value_id:
                            try:
                                report = Report('normal', None, None)
                                report.set_logger(self._logger)
                                report.init(self._id)
                                report._segment_value_id = segment_value['segment_value_id']
                                report._segment_value = segment_value
                                if report._available():
                                    report._jfile.set_segment_value(segment_value)
                                    report._make_meta()
                                    report.unlock()
                            except Exception:
                                pass

            # release run segment file lock
            self.unlock()

        if not has_unlocked_segments:
            raise Exception("You can not initiate live-update of this report at this time because report generation has already been initiated by a different process.")

        if self._process_type == 'delete_data':
            return "Instances from '%s' to '%s' were deleted" % (self._initial_measurement_time, self._final_measurement_time)
        else:
            if not any_data_fetched:
                return "None of data was fetched"

        return ''

    def _available(self):
        # get file locker instance
        self._filelocker = FileLock(os.path.join(self._path, str(self._id), "run_segment_%s" % self._segment_value_id), 0, 0)

        # try to lock run segment file lock
        return self._filelocker.acquire()

    def _update_composite(self, segment_value_id):
        """
        Main method. Starts update work flow
        """
        segments = self._get_working_segments(segment_value_id)

        self._get_outer_connection()

        for segment_value in segments:
            self._jfile.set_segment_value(segment_value)
            if segment_value:
                self._segment_value_id = segment_value['segment_value_id']
                self._segment_value = segment_value
            else:
                self._segment_value_id = 0
                self._segment_value = None
            data_set_instances = []
            if self._data['report_save_historical_instances_ind'] != u'Y':
                # for non historical reports take measurement time from saved dataset
                dataset = self._jfile.get_current_stored_dataset()
                if dataset:
                    data_set_instances.append(self._jfile.get_current_stored_dataset())
            else:
                if self._initial_measurement_time:
                    self._db.Query("""SELECT report_data_set_instance_id
                            FROM report_data_set_instance
                        WHERE
                            `element_id`= %s
                            AND segment_value_id = %s
                            AND measurement_time >= %s
                            AND measurement_time <= %s
                        ORDER BY measurement_time""", (self._id, self._segment_value_id, self._initial_measurement_time, self._final_measurement_time))
                else:
                    self._db.Query("""SELECT report_data_set_instance_id
                            FROM report_data_set_instance
                        WHERE
                            `element_id`= %s
                            AND segment_value_id = %s
                        ORDER BY measurement_time""", (self._id, self._segment_value_id))
                for instance in self._db.record:
                    dataset = self._jfile.get_stored_dataset(instance['report_data_set_instance_id'])
                    if dataset:
                        data_set_instances.append(dataset)

            for data_set_instance in data_set_instances:
                data = self._outer_conn.parse_collected_data(simplejson.loads(data_set_instance['instance']))
                meas_time = datetime.datetime.strptime(data_set_instance['meas_time'], '%Y-%m-%d %H:%M:%S')
                self._load_composite_data(data, meas_time, segment_value)
        
        return ''
