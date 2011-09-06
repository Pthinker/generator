#-*- coding: utf-8 -*-
from simplejson.ordered_dict import OrderedDict
from db.db_conn import DBManager
from operator import itemgetter
import copy
import pprint 
from datetime import datetime

class AbstractDataSet:
    headers = []
    headers_formats = {}

    formatted_headers = []
    formatted_data = []
    _id = 0
    _data = None
    _element = None
    _display_formats = {}

    instance_id = 0
    clear_data = []

    _segment_value = None

    def __init__(self, element_id, element, formatter, segment_value):
        self._id = element_id
        self._element = element
        self._formatter = formatter
        self._segment_value = segment_value
        self.clear_data = []
        self._db = DBManager.get_query()

    def _prepare_json(self, write_clear_headers):
        """
        Returns data formatted for json data
        """
        prepared_data = OrderedDict()
        prepared_data['header'] = []
        prepared_data['rows'] = map(list, len(self.formatted_data)*[[]])

        # add max_width to every header
        for i, header in enumerate(self.formatted_headers):
            if write_clear_headers:
                is_visible_column = True
            else:
                is_visible_column = 'show_column_in_table_display_ind' not in header or ('show_column_in_table_display_ind' in header and header['show_column_in_table_display_ind'] == u'Y')

            if is_visible_column:
                header['max_width'] = len(header['formatted_val'])

                is_date_val = header['type'] == 'datetime'

                for row_i, formatted_row in enumerate(self.formatted_data):
                    cur_val = formatted_row[i]
                    header['max_width'] = max(header['max_width'], len(cur_val['formatted_val']))
                    prepared_data['rows'][row_i].append({'value': cur_val['formatted_val'],
                                               'raw_value': str(cur_val['original_val']) if is_date_val else cur_val['original_val']
                                              })
                if write_clear_headers:
                    prepared_data['header'].append({'name': header['formatted_val'],
                                                    'column_name': header['original_val'],
                                                    'type': header['type'],
                                                    'max_width': header['max_width']
                     })
                else:
                    prepared_data['header'].append({'name': header['formatted_val'],
                                             'type': header['type'],
                                             'max_width': header['max_width']
                                             })
        return prepared_data

    def get_formatted_header_rows(self):
        """
        Returns list formatted for passing to chart generator
        """
        return {'header': self.formatted_headers, 'rows': self.formatted_data}


class DataSet(AbstractDataSet):
    headers = []
    headers_formats = {}
    formatted_headers = []
    formatted_data = []
    headers_db_types = []
    _id = 0
    _data = None
    _element = None
    meas_time = None
    instance_id = 0
    clear_data = []
    raw_data = None

    def __init__(self, element_id, element, meas_time, formatter, segment_value):
        self.meas_time = meas_time
        AbstractDataSet.__init__(self, element_id, element, formatter, segment_value)

    def _db_type_to_numeric_type(self, db_type):
        numeric_type = None
        if db_type == 'int':
            numeric_type = 'int'
        elif db_type == 'float':
            numeric_type = 'float'
        return numeric_type

    def _db_type_to_value_type(self, db_type):
        value_type = 'text'
        if db_type == 'int':
            value_type = 'numeric'
        elif db_type == 'float':
            value_type = 'numeric'
        elif db_type == 'date':
            value_type = 'datetime'
        return value_type

    def prepare_json(self, write_clear_headers):
        return self._prepare_json(write_clear_headers)

    def process_update_column_headers(self):
        """
        Process updating column headers
        """
        titles = self.headers

        #add_titles = []
        unchanged_titles = []
        deleted_titles = []
        dropped_titles = []

        # check if column names are duplicated
        unique_columns = set()
        for title in titles:
            if title in unique_columns:
                raise Exception("The column '%s' is included more than once in the fetch command result set. The name of each column in the result set must be distinct." % title)
            unique_columns.add(title)

        # get current titles from db
        self._db.Query("""SELECT report_data_set_column_id, column_name
                            FROM report_data_set_column
                        WHERE
                            report_data_set_column.`element_id`=%s
                        ORDER BY column_sequence""", (self._id, ))
        existing_titles = []
        for title in self._db.record:
            # mark all current titles as not actual
            title['actual'] = False
            is_used = False

            use_sql = [
                """SELECT report_data_set_column_id FROM report_data_set_chart_column WHERE report_data_set_column_id = %s""",
                """SELECT pivot_column_value_column_id FROM report_data_set_pivot WHERE pivot_column_value_column_id = %s""",
                """SELECT pivot_column_sort_column_id FROM report_data_set_pivot WHERE pivot_column_sort_column_id = %s""",
                """SELECT pivot_row_value_column_id FROM report_data_set_pivot WHERE pivot_row_value_column_id = %s""",
                """SELECT pivot_row_sort_column_id FROM report_data_set_pivot WHERE pivot_row_sort_column_id = %s""",
                """SELECT pivot_measure_column_id FROM report_data_set_pivot WHERE pivot_measure_column_id = %s""",
            ]
            for sql in use_sql:
                if self._db.Query(sql, (title['report_data_set_column_id'], )):
                    is_used = True
                    break

            if title['column_name'] in titles:
                # if column is still in data set
                existing_titles.append(title['column_name'])
                index = titles.index(title['column_name'])
                unchanged_titles.append({'id': title['report_data_set_column_id'],
                                         'column_name': title['column_name'],
                                         'order': index + 1,
                                         'db_type': self.headers_db_types[index]})
            else:
                # if column is not in data set
                if is_used:
                    # if column is used somewhere (pivot/chart)
                    dropped_titles.append({'id': title['report_data_set_column_id']})
                else:
                    # if column is not used anywhere
                    deleted_titles.append({'id': title['report_data_set_column_id']})

        # add new titles to db
        sql = """INSERT INTO report_data_set_column SET
                    `element_id` = %s,
                    `column_name` = %s,
                    `display_name` =  %s,
                    `column_sequence` = %s,
                    `display_mask_id` = %s,
                    `value_type` = %s,
                    `column_dropped_from_sql_ind` = 'N',
                    `show_column_in_table_display_ind` = 'Y',
                    `numeric_type` = %s
                """
        for index, title in enumerate(titles):
            if title not in existing_titles:
                display_name = self._get_default_display_name(title)
                db_type = self.headers_db_types[index]
                value_type = self._db_type_to_value_type(db_type)
                numeric_type = self._db_type_to_numeric_type(db_type)
                default_display_mask_id = self._get_default_display_mask_id(title, display_name, value_type, numeric_type)
                self._db.Query(sql, (self._id, title, display_name, index + 1, default_display_mask_id, value_type, numeric_type))

        # update existing titles to db
        sql = """UPDATE report_data_set_column SET
                `column_sequence` = %s,
                `column_dropped_from_sql_ind` = 'N',
                `value_type` = %s,
                `numeric_type` = %s
                WHERE report_data_set_column_id = %s
                """
        for title in unchanged_titles:
            value_type = self._db_type_to_value_type(title['db_type'])
            numeric_type = self._db_type_to_numeric_type(title['db_type'])
            self._db.Query(sql, (title['order'], value_type, numeric_type, title['id']))

        # delete old titles from db
        sql = """DELETE FROM report_data_set_column
                WHERE report_data_set_column_id = %s
              """
        for title in deleted_titles:
            self._db.Query(sql, (title['id'], ))

        # drop unused titles from db
        sql = """UPDATE report_data_set_column
                    SET
                        `column_sequence` = '0',
                        `column_dropped_from_sql_ind` = 'Y'
                WHERE report_data_set_column_id = %s"""
        for title in dropped_titles:
            self._db.Query(sql, (title['id'], ))

        # update dashboard element
        self._db.Query("""UPDATE dashboard_element
                        SET last_report_column_metadata_update_time = NOW()
                    WHERE element_id = %s""", (self._id, ))

    def _update_column_headers(self):
        """
        Start update column headers
        """
#        if self._element['last_report_column_metadata_update_time'] is None or self._element['last_data_fetch_command_update_time'] >= self._element['last_report_column_metadata_update_time']:
#            self.process_update_column_headers()
        self.process_update_column_headers()

    def process_data(self, data, update_titles):
        """
        main function. retrieves raw data and start processing it
        """
        self.raw_data = data

        #get clear header
        self.headers = []
        if self.raw_data:
            self.headers = self.raw_data['header']
            self.headers_db_types = self.raw_data['type']

        # update header titles in db
        if update_titles:
            self._update_column_headers()

        # create dict with headers and display_mask_id
        self.headers_formats = self._create_headers_formats()

        # get clear data list
        self.clear_data = []
        if self.raw_data['data']:
            self.clear_data = self.raw_data['data']

        self.formatted_headers = self._format_headers()

        self.formatted_data = self._format_data()

    def _format_headers(self):
        """
        returns list with formatted and original headers
        """
        return [{'original_val': title,
                 'formatted_val': self.headers_formats[title]['display_name'],
                 'show_column_in_table_display_ind': self.headers_formats[title]['show_column_in_table_display_ind'],
                 'display_mask_id': self.headers_formats[title]['display_mask_id'],
                 'type': self.headers_formats[title]['value_type']
                 } for title in self.headers]

    def _format_data(self):
        """
        Returns list with formatted and original data
        """
        formatter_list = []

        for i, header in enumerate(self.headers):
            def x(item, display_mask_id=self.headers_formats[header]['display_mask_id'], full_format=self._formatter.format_full):
                return {'original_val': item,
                        'formatted_val': full_format(item, display_mask_id),
                        'display_mask_id': display_mask_id}
            formatter_list.append(x)
        return [[formatter_list[i](item) for i, item in enumerate(row)] for row in self.clear_data]

    def _create_headers_formats(self):
        """
        returns dict with headers format
        """
        headers_formats = OrderedDict()

        for index, header in enumerate(self.headers):
            res = self._db.Query("""SELECT report_data_set_column.column_name,
                                    report_data_set_column.display_name,
                                    report_data_set_column.display_mask_id,
                                    report_data_set_column.show_column_in_table_display_ind,
                                    report_data_set_column.column_dropped_from_sql_ind,
                                    report_data_set_column.report_data_set_column_id,
                                    report_data_set_column.numeric_type,
                                    report_data_set_column.value_type
                                FROM report_data_set_column
                            WHERE
                                report_data_set_column.`element_id` = %s
                                AND column_name = %s
                            """,(self._id, header))
            if res:
                title = self._db.record[0]
                if not title['display_mask_id'] and title['value_type'] != u'text':
                    title['display_mask_id'] = self._get_default_display_mask_id(title['column_name'], title['display_name'], title['value_type'], title['numeric_type'])
            else:
                display_name = self._get_default_display_name(header)
                db_type = self.headers_db_types[index]
                value_type = self._db_type_to_value_type(db_type)
                numeric_type = self._db_type_to_numeric_type(db_type)
                display_mask_id = self._get_default_display_mask_id(header, display_name, value_type, numeric_type)

                title = {
                    'column_name': header,
                    'display_name': display_name,
                    'show_column_in_table_display_ind': u'Y',
                    'value_type': value_type,
                    'numeric_type': numeric_type,
                    'display_mask_id': display_mask_id,
                }
            title['index'] = index
            headers_formats[header] = title
        return headers_formats

    def _get_default_display_name(self, column_name):
        """
        case 1054. returns column display name of the report with the same shared measure. if doesn't exist, uses current
             column name with underscore replaced by spaces.
        """
        res = self._db.Query("""SELECT
                report_data_set_column.display_name
            FROM
                report_data_set_column, dashboard_element
            WHERE
                dashboard_element.type = 'internal report' AND
                dashboard_element.element_id <> %s AND
                dashboard_element.shared_measure_id = %s AND
                report_data_set_column.element_id = dashboard_element.element_id AND
                report_data_set_column.column_name = %s
            GROUP BY
                report_data_set_column.display_name
            ORDER BY
                COUNT(report_data_set_column.display_name) DESC
            """, (self._id, self._element['shared_measure_id'], column_name))
        if res:
            return self._db.record[0]['display_name']
        else:
            return column_name.replace('_', ' ')
            
    def _get_default_display_mask_id(self, column_name, display_name, value_type, numeric_type):
        """
        case 699. returns default display mask by trying to find display mask in columns of "similar" report.
        """
        if value_type == 'text':
            return None

        value_based_display_mask_id = self._formatter.get_default_display_mask_id(value_type, numeric_type)

        dict_sql_params = {'element_id' : self._id,
               'shared_measure_id' : self._element['shared_measure_id'],
               'measurement_interval_id' : self._element['measurement_interval_id'],
               'column_name': column_name,
               'display_name': display_name,
               'value_type': value_type,
               'numeric_type': numeric_type
        }

        sql = """SELECT
                report_data_set_column.display_mask_id
            FROM
                report_data_set_column, dashboard_element
            WHERE
                dashboard_element.`type` = 'internal report' AND
                dashboard_element.enabled_ind = 'Y' AND
                dashboard_element.element_id <> %%(element_id)s AND
                dashboard_element.shared_measure_id = %%(shared_measure_id)s AND
                report_data_set_column.value_type = %%(value_type)s AND
                report_data_set_column.numeric_type = %%(numeric_type)s AND
                report_data_set_column.display_mask_id > 0 AND
                report_data_set_column.display_mask_id IS NOT NULL
                %(rule_condition)s
            ORDER BY
                dashboard_element.element_id DESC
            LIMIT 1"""
        # 1.
        rule_condition = """AND
                dashboard_element.measurement_interval_id = %(measurement_interval_id)s AND
                (report_data_set_column.column_name = %(column_name)s OR report_data_set_column.display_name = %(display_name)s)"""
        res = self._db.Query(sql % {'rule_condition': rule_condition}, dict_sql_params)
        if res:
            return self._db.record[0]['display_mask_id']

        # 2.
        rule_condition = """AND (report_data_set_column.column_name = %(column_name)s OR report_data_set_column.display_name = %(display_name)s)"""
        res = self._db.Query(sql % {'rule_condition': rule_condition}, dict_sql_params)
        if res:
            return self._db.record[0]['display_mask_id']

        # 3.
        rule_condition = ''
        res = self._db.Query(sql % {'rule_condition': rule_condition}, dict_sql_params)
        if res:
            return self._db.record[0]['display_mask_id']

        # 4. Return value based (detected display mask id) if it was not possible to find the "similar" mask
        return value_based_display_mask_id

    def _get_clear_data(self):
        """
        returns raw data converted from dict to list
        """
        data = []
        if self.raw_data['data']:
            data = self.raw_data['data']

        return data

    def insert_to_db(self):
        """
        inserts data set instance to db and returns instance id
        """
        segment_value_id = 0
        if self._segment_value:
            segment_value_id = self._segment_value['segment_value_id']

        # insert data set instance to db
        sql = """INSERT INTO report_data_set_instance
            SET
                element_id = %s,
                measurement_time = %s,
                segment_value_id = %s,
                log_time = NOW()
            ON DUPLICATE KEY UPDATE log_time = NOW()"""
        self._db.Query(sql, (self._id, self.meas_time, segment_value_id))
        self.instance_id  = self._db.lastInsertID

    def create_pivot(self, pivot):
        """
        creates pivot class instance
        """
        pivot = PivotDataSet(self._id, self._element, self.instance_id, pivot, self._formatter, self._segment_value)
        pivot.original_formatted_data = list(self.formatted_data)
        pivot.original_headers_formats = self.headers_formats.copy()
        pivot.original_formatted_headers = list(self.formatted_headers)
        pivot.prepare_pivot()
        return pivot

class PivotDataSet(AbstractDataSet):
    original_clear_data = []
    original_raw_data = []
    original_headers_formats = {}
    original_formatted_headers = []
    original_formatted_data = []

    clear_data = []
    raw_data = []
    headers = []
    headers_formats = {}
    formatted_headers = []
    formatted_data = []

    _id = 0
    _data = None
    _element = None
    meas_time = None
    instance_id = 0
    pivot = None

    column_field = ''
    column_sort_field = ''
    row_value_field = ''
    row_sort_field = ''
    measure_column_field = ''
    pivot_instance = []
    columns = []
    measure_display_mask_id = 0

    def __init__(self, element_id, element, instance_id, pivot, formatter, segment_value):
        self.instance_id = instance_id
        self.pivot = pivot
        AbstractDataSet.__init__(self, element_id, element, formatter, segment_value)

    def prepare_json(self):
        return self._prepare_json(False)

    def process_pivot(self):
        """
        main function. retrieves raw data and start processing it to pivot
        """
        # now we have all meta info let's rock!
        # pivot - contains all data
        self.pivot_instance = []
        self.formatted_pivot_instance = []
        # column headers
        self.formatted_headers = []
        
        # it's first column
        if self.row_value_field:
            self.formatted_headers.append(self.original_formatted_headers[self.original_headers_formats[self.row_value_field]['index']])
        else:
            # pivot has no row_value_field so is has only TOTAL row
            self.formatted_headers.append({'original_val': '', 'formatted_val': '', 'show_column_in_table_display_ind': u'Y', 'display_mask_id': None, 'type': u'text'})

        #columns - all titles except first column
        self.formatted_columns = []
        
        # sort instance data by column sort, if specified sort field and it is not by measure field. if by measure field then sort by total row
        if self.column_sort_field and self.column_sort_field != self.measure_column_field:
            if self.column_sort_field in self.original_headers_formats:
                column_sort = self.original_headers_formats[self.column_sort_field]['index']
                column_sorted_reverse = False
                if self.pivot['column_value_sort_order'] == u'descending':
                    column_sorted_reverse = True
                formatted_column_sorted_instance = sorted(self.original_formatted_data, key=lambda x: x[column_sort]['original_val'], reverse=column_sorted_reverse)
            else:
                formatted_column_sorted_instance = self.original_formatted_data
        else:
            formatted_column_sorted_instance = self.original_formatted_data

        #a = self.formatted_columns[:]
        #b = self.formatted_headers[:]
        #s = time.time()

        # create pivot columns
        used_columns = []
        if self.column_field:
            if self.column_field not in self.original_headers_formats:
                raise Exception("'Column Values' column '%s' is not found in data set of pivot %s (%s)" % (self.column_field, self.pivot['name'], self._id))
            index_col = self.original_headers_formats[self.column_field]['index']
            for formatted_column in formatted_column_sorted_instance:
                formatted_column[index_col]['type'] = u'numeric'
                if formatted_column[index_col]['original_val'] not in used_columns:
                    self.formatted_columns.append(formatted_column[index_col])
                    self.formatted_headers.append(formatted_column[index_col])
                    used_columns.append(formatted_column[index_col]['original_val'])

#        if self.column_field:
#            for formatted_column in formatted_column_sorted_instance:
#                formatted_column[self.original_headers_formats[self.column_field]['index']]['type'] = u'numeric'
#                if not any(row['original_val'] == formatted_column[self.original_headers_formats[self.column_field]['index']]['original_val'] for row in self.formatted_columns):
#                    self.formatted_columns.append(formatted_column[self.original_headers_formats[self.column_field]['index']])
#                    self.formatted_headers.append(formatted_column[self.original_headers_formats[self.column_field]['index']])
#        if self.formatted_columns != a or  self.formatted_headers != b:

        # if there is TOTAL column
        self.formatted_headers.append({'original_val': u'TOTAL', 'formatted_val': u'TOTAL', 'show_column_in_table_display_ind': u'Y', 'display_mask_id': None, 'type': u'numeric'})
        
        # sort rows if it is not by measure field. if by measure field then sort by total column 
        if self.row_sort_field and self.row_sort_field != self.measure_column_field:
            row_sort = self.original_headers_formats[self.row_sort_field]['index']
            row_sorted_reverse = False
            if self.pivot['row_value_sort_order'] == u'descending':
                row_sorted_reverse = True
            formatted_row_sorted_instance = sorted(self.original_formatted_data, key=lambda x: x[row_sort]['original_val'], reverse = row_sorted_reverse)
        else:
            formatted_row_sorted_instance = self.original_formatted_data
        
        # pivot row with headers
        formatted_rows = []
        
        # create rows (for first column)
        if self.row_value_field:
            for formatted_row in formatted_row_sorted_instance:
                if not any(row['original_val']==formatted_row[self.original_headers_formats[self.row_value_field]['index']]['original_val'] for row in formatted_rows):
                    formatted_rows.append(formatted_row[self.original_headers_formats[self.row_value_field]['index']])
        
        # add TOTAL row
        formatted_row_total_sum = [{'original_val': u'TOTAL', 'formatted_val': u'TOTAL', 'display_mask_id': None}]
        # +2 means first column and TOTAL column
        num_of_columns = len(self.formatted_columns) + 2
        
        #add empty values for TOTAL row
        for i in range(1, num_of_columns):
            formatted_row_total_sum.append({'original_val': 0, 'formatted_val': '', 'display_mask_id': self.measure_display_mask_id})
        
        #---------------------------
        # let's fill pivot with data
        #---------------------------
        row_value_field_index = None
        column_field_index = None
        if self.row_value_field:
            row_value_field_index = self.original_headers_formats[self.row_value_field]['index']
        if self.column_field:
            column_field_index = self.original_headers_formats[self.column_field]['index']
        
        measure_column_index = self.original_headers_formats[self.measure_column_field]['index']

        # there will be no rows except TOTAL
        if row_value_field_index is None:
            #formatted_pivot_row = []
            #formatted_pivot_row.append({})
            formatted_pivot_row = [{}]
            for formatted_column in self.formatted_columns:
                # select all row values from data instance
                formatted_row_value = self.parse_original_formatted_data({}, formatted_column, row_value_field_index, column_field_index, measure_column_index)
                formatted_row_value['column'] =  formatted_column['original_val']
                
                # add element to pivot row
                formatted_pivot_row.append(formatted_row_value)

            sum_value = sum(item['original_val'] for item in formatted_pivot_row[1:])

            formatted_row_sum = {
                'display_mask_id': self.measure_display_mask_id,
                'original_val': sum_value,
                'formatted_val': self._formatter.format_full(sum_value, self.measure_display_mask_id)
            }

            formatted_pivot_row.append(formatted_row_sum)
            
            for k in xrange(1, len(formatted_pivot_row)):
                formatted_row_total_sum[k]['original_val'] = formatted_row_total_sum[k]['original_val'] + formatted_pivot_row[k]['original_val']
            
        else:
            # there will be some rows
            for formatted_row in formatted_rows:
                formatted_pivot_row = [formatted_row]
                #formatted_pivot_row.append(formatted_row)
                
                if column_field_index is None:
                    # there will be no columns except TOTAL
                    formatted_row_value = self.parse_original_formatted_data(formatted_row, {}, row_value_field_index, column_field_index, measure_column_index)
                    # add element to pivot row
                    formatted_pivot_row.append(formatted_row_value)
                else:
                    # pivot has columns and rows
                    formatted_row_sum = {'original_val': 0, 'formatted_val': '', 'display_mask_id': self.measure_display_mask_id}
                     
                    for formatted_column in self.formatted_columns:
                        # select all row values from data instance
                        formatted_row_value = self.parse_original_formatted_data(formatted_row, formatted_column, row_value_field_index, column_field_index, measure_column_index)
                        # add element to pivot row
                        formatted_pivot_row.append(formatted_row_value)
    
                    formatted_row_sum['original_val'] = sum(item['original_val'] for item in formatted_pivot_row[1:])
                    formatted_row_sum['formatted_val'] = self._formatter.format_full(formatted_row_sum['original_val'], self.measure_display_mask_id)
                    formatted_pivot_row.append(formatted_row_sum)
    
                for k in xrange(1, len(formatted_pivot_row)):
                    formatted_row_total_sum[k]['original_val'] = formatted_row_total_sum[k]['original_val'] + formatted_pivot_row[k]['original_val']
                
                self.formatted_pivot_instance.append(formatted_pivot_row)

        # add TOTAL row
        real_formatted_row_total_sum = [{'original_val': row['original_val'],
                                        'formatted_val': self._formatter.format_full(row['original_val'], row['display_mask_id']),
                                        'display_mask_id': row['display_mask_id']}
                                    for row in formatted_row_total_sum]
        self.formatted_pivot_instance.append(real_formatted_row_total_sum)
        
        # sort rows by total column
        if self.row_sort_field and self.row_sort_field == self.measure_column_field:
            # if there is TOTAL row then exclude it from sorting
            row_sorted_reverse = False
            if self.pivot['row_value_sort_order'] == u'descending':
                row_sorted_reverse = True
            # sort all
            self.formatted_pivot_instance[0:-1] = sorted(self.formatted_pivot_instance[0:-1], key=lambda row: row[-1]['original_val'], reverse=row_sorted_reverse)
        
        # sort columns by total row
        if self.column_sort_field and self.column_sort_field == self.measure_column_field:
            row_total = self.formatted_pivot_instance[-1]
            row_len = len(self.formatted_headers) - 1

            column_sorted_reverse = False
            if self.pivot['column_value_sort_order'] == u'descending':
                column_sorted_reverse = True
            
            # sort headers    
            for i in xrange(1, row_len):
                self.formatted_headers[i]['sort_total_val'] = row_total[i]['original_val']
            self.formatted_headers[1:row_len] = sorted(self.formatted_headers[1:row_len], key=itemgetter('sort_total_val'), reverse=column_sorted_reverse)
            
            # sort rows
            for i, row in enumerate(self.formatted_pivot_instance):
                for j in xrange(1, row_len):
                    row[j]['sort_total_val'] = row_total[j]['original_val']
                self.formatted_pivot_instance[i][1:row_len] = sorted(row[1:row_len], key=itemgetter('sort_total_val'), reverse=column_sorted_reverse)

            for i in xrange(1, row_len):
                del(self.formatted_headers[i]['sort_total_val'])
            
            for i, row in enumerate(self.formatted_pivot_instance):
                for j in xrange(1, row_len):
                    del(self.formatted_pivot_instance[i][j]['sort_total_val'])

        self._whole_formatted_data = copy.deepcopy(self.formatted_pivot_instance)
        self._whole_formatted_headers = copy.deepcopy(self.formatted_headers)

        # remove row TOTAL if it is not enabled
        if self.pivot['include_row_total_ind'] != 'Y':
            del(self.formatted_pivot_instance[-1])
        
        # remove column TOTAL if it is not enabled
        if self.pivot['include_column_total_ind'] != 'Y':
            del(self.formatted_headers[-1])
            for i, row in enumerate(self.formatted_pivot_instance):
                del(self.formatted_pivot_instance[i][-1])
        self.formatted_data = self.formatted_pivot_instance

    def parse_original_formatted_data(self, formatted_row, formatted_column, row_value_field_index, column_field_index, measure_column_index):
        """
        process whole data and fetch needed values
        """
        #create new empty formatted element
        formatted_row_value = {'original_val': 0, 'formatted_val': '', 'display_mask_id': self.measure_display_mask_id}

        values = []
        va = values.append
        
        for formatted_data in self.original_formatted_data:
            if row_value_field_index is None:
                if formatted_data[column_field_index]['original_val'] == formatted_column['original_val']:
                    va(formatted_data[measure_column_index]['original_val'])
            elif column_field_index is None:
                if formatted_data[row_value_field_index]['original_val'] == formatted_row['original_val']:
                    va(formatted_data[measure_column_index]['original_val'])
            else:
                if (formatted_data[row_value_field_index]['original_val'] == formatted_row['original_val']
                        and formatted_data[column_field_index]['original_val'] == formatted_column['original_val']):
                    va(formatted_data[measure_column_index]['original_val'])

        if self.pivot['pivot_aggregate_function'] == u'Sum':
            formatted_row_value['original_val'] = sum(values)
        else:
            # count only non empty values
            formatted_row_value['original_val'] = len([v for v in values if v])
            formatted_row_value['display_mask_id'] = None

        formatted_row_value['formatted_val'] = self._formatter.format_full(formatted_row_value['original_val'], self.measure_display_mask_id)
        return formatted_row_value          
    
    def insert_to_db(self):
        """
        inserts pivot data set instance to db
        """
        sql = """INSERT INTO report_data_set_pivot_instance
                    SET
                        report_data_set_pivot_id = %s,
                        report_data_set_instance_id = %s,
                        pivot_generation_time = NOW()
                    ON DUPLICATE KEY UPDATE pivot_generation_time = NOW()"""
        self._db.Query(sql, (self.pivot['report_data_set_pivot_id'], self.instance_id))

    def prepare_pivot(self):
        """
        gets all columns info for making pivot
        """

        # get column field
        if self.pivot['pivot_column_value_column_id'] and int(self.pivot['pivot_column_value_column_id']):
            res = self._db.Query("""SELECT *
                                FROM report_data_set_column
                            WHERE
                                `report_data_set_column_id`=%s""",(self.pivot['pivot_column_value_column_id']))
            if not res:
                raise Exception("Cannot find column 'pivot_column_value_column_id' in pivot  %s (%s)" % (self.pivot['name'], self._id))
            column_field = self._db.record[0]
            self.column_field = column_field['column_name'].strip()

            # get column sort field
            res = self._db.Query("""SELECT *
                                FROM report_data_set_column
                            WHERE
                                `report_data_set_column_id`=%s""",(self.pivot['pivot_column_sort_column_id']))
            if not res:
                raise Exception("Cannot find column 'pivot_column_sort_column_id' in pivot  %s (%s)" % (self.pivot['name'], self._id))
    
            column_sort_field = self._db.record[0]
            self.column_sort_field = column_sort_field['column_name'].strip()

        # get row value field
        if self.pivot['pivot_row_value_column_id'] and int(self.pivot['pivot_row_value_column_id']):
            res = self._db.Query("""SELECT *
                                FROM report_data_set_column
                            WHERE
                                `report_data_set_column_id`=%s""",(self.pivot['pivot_row_value_column_id']))
            if not res:
                raise Exception("Cannot find column 'pivot_row_value_column_id' in pivot  %s (%s)" % (self.pivot['name'], self._id))
    
            row_value_field = self._db.record[0]
            self.row_value_field = row_value_field['column_name'].strip()

            # get row sort field
            res = self._db.Query("""SELECT *
                                FROM report_data_set_column
                            WHERE
                                `report_data_set_column_id`=%s""",(self.pivot['pivot_row_sort_column_id']))
            if not res:
                raise Exception("Cannot find column 'pivot_row_sort_column_id' in pivot  %s (%s)" % (self.pivot['name'], self._id))

            row_sort_field = self._db.record[0]
            self.row_sort_field = row_sort_field['column_name'].strip()
        
        # force include row TOTAL if row_value_field is not specified  
        if not self.row_value_field:
            self.pivot['include_row_total_ind'] = u'Y'
        
        # force include column TOTAL if column_field is not specified  
        if not self.column_field:
            self.pivot['include_column_total_ind'] = u'Y'
        
        if not self.row_value_field and not self.column_field:
            raise Exception("pivot_row_value_column_id and pivot_column_value_column_id both not specified in pivot %s (%s)" % (self.pivot['name'], self._id))
        
        # get measure column field is aggregate function is Sum
        if self.pivot['pivot_measure_column_id']:
            res = self._db.Query("""SELECT *
                            FROM report_data_set_column
                        WHERE
                            `report_data_set_column_id`=%s""",(self.pivot['pivot_measure_column_id']))
            if not res:
                raise Exception("Column 'Count column' is not specified in report columns %s (%s)" % (self.pivot['name'], self._id))
            measure_column_field = self._db.record[0]
        else:
            raise Exception("Column pivot_measure_column_id is not specified in pivot %s (%s)" % (self.pivot['name'], self._id))
        
        self.measure_column_field = measure_column_field['column_name'].strip()
        if self.pivot['pivot_aggregate_function'] == u'Sum':
            self.measure_display_mask_id = self.original_headers_formats[self.measure_column_field]['display_mask_id']

    def get_formatted_header_rows(self):
        """
        Returns list formatted for passing to chart generator
        """
        return {'header': self._whole_formatted_headers, 'rows': self._whole_formatted_data}
