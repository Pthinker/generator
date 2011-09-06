#-*- coding: utf-8 -*-

from db.db_conn import DBManager
from simplejson.ordered_dict import OrderedDict
from metric.formatter import FieldFormatter
from math import floor
from fontManager import FontManager
from decimal import Decimal
from heapq import nsmallest, nlargest
from operator import itemgetter
from itertools import cycle
import random
from datetime import date
from sys import exit
import pprint
import time

class ReportChart:
    chart_display_mode = ''
    bar_elements_count = 0
    
    settings = None 
    
    chart_id = 0
    element_id = 0
    data_set_instance_id = 0
    type = ''
    data = None
    formatted_data = None
    pivot_id = 0
    meas_time = None
    segment_value_id = 0
    is_x_axis_date = False

    display_mask_ids = None
    x_axis_display_mask_id = None

    def __init__(self, chart_id, element_id, segment_value_id, meas_time, data_set_instance_id, data, jfile, type):
        self._db = DBManager.get_query()
        self.chart_id = chart_id
        self.segment_value_id = segment_value_id  
        self.element_id = element_id
        self.meas_time = meas_time
        self.data_set_instance_id = data_set_instance_id
        self.type = type
        self.settings, self.pivot_id = self._get_chart_settings()
        
        self.formatter = FieldFormatter(self.settings['def_date_mask_id'])
        
        self.orig_data = data[self.pivot_id]

        self._jfile = jfile
        
        # split original data into clean data and formatted data 
        self.get_data_from_orig()
        
        
        # for non pivot charts by column header - contains avg value for each column
        self.avg_column_value = dict()
        
        #associative array to get formatted element name by clean element name
        self.clean_to_formatted = dict()
        self.formatted_to_clean = dict()
        
        # columns or rows to be used for charting 
        self.included_elements = list()
        
        self.combine_excluded_elements_as_other_ind = 'N'

    def generateChart(self):
        data = self.get_data()
        
        chart_type = data['layout_data']['chart_type']

        settings = {'type': self.type, 'chart_display_mode': self.chart_display_mode}

        if chart_type == 'line/bar':
            from baseChart import BaseChart

            settings['is_x_axis_date'] = self.is_x_axis_date
            if settings['is_x_axis_date']:
                data['layout_data']['flip_x_and_y_axis'] = 'No'
            data['layout_data']['y_axis_title_left'] = data['layout_data']['y_axis_title']
            data['layout_data']['y_axis_title_right'] = data['layout_data']['secondary_y_axis_title']
            

            #chart = BaseChart(self.element_id, self.chart_id, self.data_set_instance_id, data, settings, self._jfile)
            chart = BaseChart(data, self._jfile, settings)
            chart.set_chart_ids( self.chart_id, self.data_set_instance_id )
            chart.create_chart()
        else:
            from basePieChart import BasePieChart
            chart = BasePieChart(self.element_id, self.chart_id, self.data_set_instance_id, data, settings, self._jfile)
        
        if self.type == 'large' and chart.annotations_map and self.data_set_instance_id:
            res = self._db.Query("""SELECT report_data_set_chart_instance_id
                                FROM report_data_set_chart_instance
                            WHERE report_data_set_chart_id = %s
                                AND report_data_set_instance_id = %s
                                """,(self.chart_id, self.data_set_instance_id))
            if res:
                report_data_set_chart_instance_id = self._db.record[0]
                self._update_annotations_instances(report_data_set_chart_instance_id['report_data_set_chart_instance_id'], chart.annotations_map)
        del chart
    def _get_chart_settings(self):
        """
        getting data for layout
        """
        self._db.Query("""SELECT
                                dashboard_element.description,
                                report_data_set_chart.reverse_row_value_sort_order_ind,
                                report_data_set_chart.use_stacked_bars_ind,
                                report_data_set_chart.bars_or_lines_created_for,
                                report_data_set_chart.chart_by_report_data_set_column_id,
                                report_data_set_chart.chart_include_method,
                                report_data_set_column.column_name,
                                report_data_set_chart.combine_excluded_elements_as_other_ind,
                                report_data_set_chart.data_thinning_method,
                                report_data_set_chart.flip_x_and_y_axis,
                                report_data_set_chart.layout_id,
                                report_data_set_chart.name,
                                
                                report_data_set_chart.other_chart_value_display_type,
                                
                                report_data_set_chart.pivot_total_chart_value_display_type,
                                report_data_set_chart.primary_chart_value_display_type,
                                report_data_set_chart.primary_charting_line_style,
                                report_data_set_chart.report_data_set_pivot_id,
                                report_data_set_chart.secondary_y_axis_title,
                                report_data_set_chart.x_axis_title,
                                report_data_set_chart.y_axis_count,
                                report_data_set_chart.y_axis_title,
                                report_data_set_chart.pivot_total_axis_number,

                                report_data_set_chart.start_y_axis_from_zero_ind,
                                report_data_set_chart.chart_pivot_total_ind,

                                report_data_set_chart.chart_type,

                                report_data_set_chart.start_y_axis_from_zero_ind,

                                chart_display_format.second_y_axis_padding_allowance,
                                
                                chart_display_format.chart_object_size_x,
                                chart_display_format.chart_object_size_y,
                                chart_display_format.plot_area_x_coord,
                                chart_display_format.plot_area_y_coord,
                                chart_display_format.plot_area_width,
                                chart_display_format.plot_area_height,
                                chart_display_format.title_font_id,
                                chart_display_format.title_font_size,
                                chart_display_format.legend_x_coord,
                                chart_display_format.legend_y_coord,
                                chart_display_format.max_bar_data_points_to_chart,
                                chart_display_format.max_line_data_points_to_chart,
                                chart_display_format.max_x_axis_labels,

                                chart_display_format.include_title_ind,

                                chart_layout.background_color,
                                chart_layout.bar_gap,
                                chart_layout.bar_group_gap,
                                chart_layout.border_color,
                                chart_layout.title_font_color,
                                chart_layout.line_width,
                                chart_layout.metric_line_color,
                                chart_layout.metric_bar_color,
                                chart_layout.bar_shape,
                                chart_layout.include_x_axis_label_ind,
                                chart_layout.x_axis_label_font_id,
                                chart_layout.x_axis_label_font_color,
                                chart_layout.x_axis_label_font_size,
                                chart_layout.x_axis_tick_mark_position,
                                chart_layout.include_y_axis_label_ind,
                                chart_layout.y_axis_font_id,
                                chart_layout.y_axis_font_color,
                                chart_layout.y_axis_font_size,
                                chart_layout.include_legend_ind,
                                chart_layout.legend_font_id,
                                chart_layout.legend_font_size,
                                chart_layout.legend_background_color,
                                chart_layout.plot_area_background_color_ind,
                                chart_layout.plot_area_background_color,
                                chart_layout.show_plot_area_grid_ind,
                                chart_layout.plot_area_horizontal_grid_color,
                                chart_layout.plot_area_vertical_grid_color,
                                chart_layout.highlight_current_value_ind,
                                chart_layout.current_value_dot_color,
                                chart_layout.current_value_dot_size,
                                chart_layout.current_value_font_id,
                                chart_layout.current_value_font_size,
                                chart_layout.show_line_data_points_ind,
                                chart_layout.line_data_point_dot_size,
                                chart_layout.bar_soft_lighting_direction,
                                chart_layout.month_display_format,
                                chart_layout.line_data_point_dot_size,
                                chart_layout.data_gap_line_type,
                                measurement_interval.display_mask_id AS def_date_mask_id,
                                
                                
                                
                                chart_layout.pie_chart_sector_label_font_id,
                                chart_layout.pie_chart_sector_label_font_size,
                                chart_layout.pie_chart_sector_label_font_color,
                                chart_layout.pie_chart_sector_value_font_id,
                                chart_layout.pie_chart_sector_value_font_size,
                                chart_layout.pie_chart_sector_value_font_color,
                                chart_layout.use_sector_color_for_Label_fill_ind,
                                chart_layout.sector_label_corner_style,
                                chart_layout.sector_label_border_color,
                                chart_layout.sector_label_join_line_color,
                                chart_layout.pie_chart_shading,
                                chart_layout.sector_edge_line_color,
                                chart_layout.pie_chart_display_angle,
                                chart_layout.pie_chart_depth,
                                chart_layout.pie_chart_start_angle,
                                chart_layout.pie_chart_sector_border_width,
                                chart_layout.explode_all_pie_sectors,
                                chart_layout.pie_sectors_explode_width,
                                
                                report_data_set_chart.pie_chart_type,
                                report_data_set_chart.sector_value_pct_precision_digits,
                                report_data_set_chart.show_sector_value_ind,
                                
                                report_data_set_chart.chart_by_report_data_set_column_id,
                                report_data_set_chart.sector_value_data_set_column_id,
                                report_data_set_chart.max_elements_to_chart,
                                
                                chart_display_format.pie_chart_center_x_coord,
                                chart_display_format.pie_chart_center_y_coord,
                                chart_display_format.pie_chart_radius,
                                
                                chart_display_format.max_pie_sectors_to_chart,
                                
                                report_data_set_chart.limit_x_axis_values_ind,
                                report_data_set_chart.top_x_axis_values_to_chart,
                                report_data_set_chart.include_dropped_x_axis_values_as_other_ind
                                                                
                            FROM report_data_set_chart 
                                LEFT JOIN dashboard_element
                                    ON report_data_set_chart.element_id = dashboard_element.element_id
                                LEFT JOIN measurement_interval 
                                    ON measurement_interval.measurement_interval_id=dashboard_element.measurement_interval_id
                                LEFT JOIN chart_layout
                                    ON chart_layout.layout_id = report_data_set_chart.layout_id
                                LEFT JOIN report_data_set_column
                                    ON report_data_set_chart.chart_by_report_data_set_column_id = report_data_set_column.report_data_set_column_id
                                LEFT JOIN chart_display_format
                                    ON chart_display_format.chart_display_format_id = chart_layout.chart_display_format_id
                                LEFT JOIN display_mask
                                    ON display_mask.display_mask_id = dashboard_element.display_mask_id
                        WHERE 
                            report_data_set_chart.`report_data_set_chart_id`=%s""",(self.chart_id, ))
        settings = self._db.record[0]


        # special settings for preview and thumbnail
        settings['include_annotations_ind'] = 'Y'
        if self.type != 'large':
            settings['include_annotations_ind'] = 'N'
            
            if self.type == 'thumbnail':
                settings['show_line_data_points_ind'] = 'N'
                settings['include_title_ind'] = 'N'
                settings['include_legend_ind'] = 'N'
                settings['include_x_axis_label_ind'] = 'N'
                settings['include_y_axis_label_ind'] = 'N'
                settings['show_plot_area_grid_ind'] = 'N'
                self._db.Query("""    SELECT *
                                    FROM chart_display_format
                                    WHERE report_thumbnail_display_format_ind='Y'""")
            elif self.type == 'preview':
                self._db.Query("""    SELECT *
                                    FROM chart_display_format
                                    WHERE preview_display_format_ind='Y'""")
            chart_display_format = self._db.record[0]
            for k, v in chart_display_format.iteritems():
                settings[k] = v
        
        # getting pivot id
        pivot_id = settings['report_data_set_pivot_id']
        
        # set key for fetching data
        if not pivot_id:
            pivot_id = 0
        return settings, pivot_id
        
    def get_specific_settings(self, data):
        """
        common settings for line/bar and pie charts
        """
        self.combine_excluded_elements_as_other_ind = data['combine_excluded_elements_as_other_ind']

        # getting font for title 
        if data['include_title_ind'] == 'Y':
            self._db.Query("""SELECT * FROM font
                                WHERE font_id=%s""",(data['title_font_id']))
            data['title_font'] = self._db.record[0]
            if data['title_font_color']:
                data['title_font_color'] = FontManager.get_db_color(data['title_font_color'])
            else:
                data['title_font_color'] = 0xffffff
                  
        if data['chart_type'] == 'pie':
            #pie chart specific setting

            # getting font for pie chart sector
            self._db.Query("""SELECT * FROM font
                            WHERE font_id=%s""",(data['pie_chart_sector_label_font_id']))
            data['pie_chart_sector_label_font'] = self._db.record[0]
            
            # getting font for pie sector value
            self._db.Query("""SELECT * FROM font
                            WHERE font_id=%s""",(data['pie_chart_sector_value_font_id']))
            data['pie_chart_sector_value_font'] = self._db.record[0]
            
            # get sector name field
            if data['chart_by_report_data_set_column_id']:
                res = self._db.Query("""SELECT column_name FROM report_data_set_column 
                                WHERE report_data_set_column_id=%s AND element_id = %s""",(data['chart_by_report_data_set_column_id'], self.element_id))
                if not res:
                    raise Exception("chart_by_report_data_set_column_id for %s %s is incorrect" % (data['name'], self.chart_id))
                data['chart_by_report_data_set_column'] = self._db.record[0]['column_name']
            else:
                data['chart_by_report_data_set_column'] = ''
                #raise Exception("chart_by_report_data_set_column_id for %s %s is missing"  % (data['name'], self.chart_id))
            
            # get sector value field
            if data['sector_value_data_set_column_id']:
                res = self._db.Query("""SELECT column_name FROM report_data_set_column 
                                WHERE report_data_set_column_id=%s AND element_id = %s""",(data['sector_value_data_set_column_id'], self.element_id))
                if not res:
                    raise Exception("sector_value_data_set_column_id for %s %s is incorrect" % (data['name'], self.chart_id))
                data['sector_value_data_set_column'] = self._db.record[0]['column_name']
            else:
                data['sector_value_data_set_column'] = ''
                #raise Exception("sector_value_data_set_column_id for %s %s is missing"  % (data['name'], self.chart_id))
            
            # get colors for charting data from layout data set
            res = self._db.Query("""SELECT sector_color, chart_layout_dataset_id FROM chart_layout_dataset
                                WHERE 
                                    layout_id=%s 
                                    AND total_ind = 'N'
                                    AND other_ind = 'N'
                                ORDER BY charting_order""",(data['layout_id']))
            data['sector_colors'] = []
            if res:
                #data['sector_colors'] = list(FontManager.get_db_color(item['sector_color']) for item in self._db.record if item['sector_color'])
                for el in self._db.record:
                    data['sector_colors'].append({'color': el['sector_color'], 'chart_layout_dataset_id': el['chart_layout_dataset_id']})
            
            # getting colors for charting data of 'Other' type
            res = self._db.Query("""SELECT sector_color 
                                        FROM chart_layout_dataset
                                    WHERE layout_id=%s 
                                        AND other_ind = 'Y'
                                    ORDER BY charting_order""",(data['layout_id']))
            data['sector_other_color'] = None
            if res:
                el = self._db.record[0]
                data['sector_other_color'] = el['sector_color']
            
            self.custom_chart_data = dict()
            
            if data['chart_by_report_data_set_column'] and data['sector_value_data_set_column']:
                if data['chart_include_method'] == 'selected values':
                    self._db.Query("""SELECT report_data_set_row_value.row_value AS display_name,
                                            report_data_set_chart_row_value.color
                                    FROM report_data_set_chart_row_value
                                        LEFT JOIN report_data_set_row_value
                                            ON report_data_set_row_value.report_data_set_row_value_id = report_data_set_chart_row_value.report_data_set_row_value_id
                                    WHERE report_data_set_chart_row_value.report_data_set_chart_id=%s
                                    ORDER BY charting_sequence_order""",(self.chart_id, ))
                    for el in self._db.record:
                        self.included_elements.append(el['display_name'])
                        self.custom_chart_data[el['display_name']] = el
                
                """                
                elif data['chart_include_method'] == 'top N values':
                    self._db.Query('SELECT report_data_set_column.column_name, 
                                        report_data_set_column.display_name,
                                        report_data_set_column.avg_column_value  
                                FROM report_data_set_column
                                    WHERE report_data_set_column.element_id=%s',(self.element_id))
                    for el in self._db.record:
                        self.avg_column_value[el['column_name']] = el['avg_column_value']
                """
            else:
                if data['chart_include_method'] == 'selected values':
                    self._db.Query("""SELECT report_data_set_column.column_name as name, 
                                        report_data_set_column.display_name,
                                        report_data_set_column.avg_column_value,
                                        report_data_set_chart_column.color
                                          
                                FROM report_data_set_chart_column
                                LEFT JOIN report_data_set_column
                                    ON report_data_set_chart_column.report_data_set_column_id = report_data_set_column.report_data_set_column_id
                                WHERE report_data_set_chart_column.report_data_set_chart_id=%s
                                ORDER BY charting_sequence_order
                                """,(self.chart_id, ))
                    for el in self._db.record:
                        self.included_elements.append(el['name'])
                        self.avg_column_value[el['name']] = el['avg_column_value']
                        self.custom_chart_data[el['display_name']] = el 
                elif data['chart_include_method'] == 'top N values':
                    self._db.Query("""SELECT report_data_set_column.column_name as name, 
                                        report_data_set_column.display_name,
                                        report_data_set_column.avg_column_value  
                                FROM report_data_set_column
                                    WHERE report_data_set_column.element_id=%s""",(self.element_id, ))
                    for el in self._db.record:
                        self.avg_column_value[el['name']] = el['avg_column_value']
        else:
            #line/bar chart specific settings

            if data['bar_gap'] is None:
                data['bar_gap'] = 0
            if data['bar_group_gap'] is None:
                data['bar_group_gap'] = 0
    
            # getting font for x axis label
            if data['include_x_axis_label_ind'] == 'Y':
                self._db.Query("""SELECT * FROM font
                                    WHERE font_id=%s""",(data['x_axis_label_font_id']))
                data['x_axis_label_font'] = self._db.record[0]
    
            # getting font for y axis label
            if data['include_y_axis_label_ind'] == 'Y':
                self._db.Query("""SELECT * FROM font
                                    WHERE font_id=%s""",(data['y_axis_font_id']))
                data['y_axis_label_font'] = self._db.record[0]
    
    
            # getting font for legend
            if data['include_legend_ind'] == 'Y':
                self._db.Query("""SELECT * FROM font
                                    WHERE font_id=%s""",(data['legend_font_id']))
                data['legend_font'] = self._db.record[0]
    
            # getting font for values
            self._db.Query("""SELECT * FROM font
                                WHERE font_id=%s""",(data['current_value_font_id']))
            data['current_value_font'] = self._db.record[0]
            
            
            self.custom_chart_data = dict()
            
            # getting field that will be x or y axis

            if data['bars_or_lines_created_for'] == 'column headers':
                self.x_axis_field = ''
                if data['chart_by_report_data_set_column_id']:
                    res = self._db.Query("""SELECT column_name 
                                FROM report_data_set_column
                                WHERE report_data_set_column_id=%s""",(data['chart_by_report_data_set_column_id']))
                    if res:
                        tmp = self._db.record[0]
                        self.x_axis_field = tmp['column_name'] 
                
                if data['chart_include_method'] == 'selected values':
                    # include only selected values
                    if self.pivot_id:
                        # this is pivot chart. Take selected row values
                        self._db.Query("""SELECT
                                                report_data_set_row_value.row_value as display_name,
                                                report_data_set_chart_row_value.row_chart_value_display_type as chart_value_display_type,
                                                report_data_set_chart_row_value.row_chart_line_type as chart_line_type,
                                                report_data_set_chart_row_value.row_chart_line_style as chart_line_style,
                                                report_data_set_chart_row_value.chart_data_point_shape_id,
                                                report_data_set_chart_row_value.color
                                            FROM report_data_set_chart_row_value
                                                LEFT JOIN report_data_set_row_value 
                                                    ON report_data_set_row_value.report_data_set_row_value_id = report_data_set_chart_row_value.report_data_set_row_value_id
                                            WHERE report_data_set_chart_id = %s
                                            ORDER BY charting_sequence_order""",(self.chart_id, ))
                        for el in self._db.record:
                            self.included_elements.append(el['display_name'])
                            self.avg_column_value[el['display_name']] = None
                            # getting colors for every custom chart line/bar
                            self.custom_chart_data[el['display_name']] = el
                    else:
                        # this is non-pivot chart. Take selected columns
                        self._db.Query("""SELECT  
                                                report_data_set_column.avg_column_value,
                                                report_data_set_column.column_name as name,
                                                report_data_set_chart_column.column_chart_value_display_type as chart_value_display_type,
                                                report_data_set_chart_column.column_chart_line_type as chart_line_type,
                                                report_data_set_chart_column.column_chart_line_style as chart_line_style,
                                                report_data_set_chart_column.color,
                                                report_data_set_chart_column.chart_data_point_shape_id,
                                                report_data_set_column.display_name
                                            FROM report_data_set_chart_column
                                                LEFT JOIN report_data_set_column
                                                    ON report_data_set_chart_column.report_data_set_column_id = report_data_set_column.report_data_set_column_id
                                            WHERE report_data_set_chart_column.report_data_set_chart_id=%s
                                            ORDER BY charting_sequence_order""",(self.chart_id, ))
                        for el in self._db.record:
                            self.included_elements.append(el['name'])
                            self.avg_column_value[el['name']] = el['avg_column_value']
                            # getting colors for every custom chart line/bar
                            self.custom_chart_data[el['name']] = el
                elif data['chart_include_method'] == 'top N values':
                    # include only top N values
                    self._db.Query("""SELECT 
                                            report_data_set_column.column_name, 
                                            report_data_set_column.display_name,
                                            report_data_set_column.avg_column_value  
                                        FROM report_data_set_column
                                            WHERE report_data_set_column.element_id=%s""",(self.element_id, ))
                    for el in self._db.record:
                        self.avg_column_value[el['column_name']] = el['avg_column_value']
            elif data['bars_or_lines_created_for'] == 'row values':
                if data['column_name'] in self.data['header']:
                    self.x_axis_field = data['column_name']
                    #ind = self.data['header'].index(data['column_name'])
                    #if ind == 0:
                    #    col = 1
                    #else:
                    #    col = 0
                    #column = self.data['header'][col]
                else:
                    self.x_axis_field = self.data['header'][0]
                    #column = self.data['header'][0]

                if data['chart_include_method'] == 'selected values':
                    # include only selected values
                    self._db.Query("""SELECT 
                                            report_data_set_row_value.row_value as display_name,
                                            report_data_set_chart_row_value.row_chart_value_display_type as chart_value_display_type,
                                            report_data_set_chart_row_value.row_chart_line_type as chart_line_type,
                                            report_data_set_chart_row_value.row_chart_line_style as chart_line_style,
                                            report_data_set_chart_row_value.chart_data_point_shape_id,
                                            report_data_set_chart_row_value.color
                                        FROM report_data_set_chart_row_value
                                            LEFT JOIN report_data_set_row_value 
                                                ON report_data_set_row_value.report_data_set_row_value_id = report_data_set_chart_row_value.report_data_set_row_value_id
                                        WHERE report_data_set_chart_row_value.report_data_set_chart_id = %s""",(self.chart_id, ))
                    for el in self._db.record:
                        self.included_elements.append(el['display_name'])
                        self.avg_column_value[el['display_name']] = None
                        # getting colors for every custom chart line/bar
                        self.custom_chart_data[el['display_name']] = el
                    

                    # get column_id by name of column from report_data_set_column
                    #res = self._db.Query("""
                    #            SELECT report_data_set_column.report_data_set_column_id 
                    #                FROM report_data_set_column
                    #            WHERE report_data_set_column.element_id = %s
                    #                AND report_data_set_column.column_name = %s""", (self.element_id, column)) #self.data['header'][0]
                    
                    #if res:
                    #    report_column = self._db.record[0]
                    #    self._db.Query("""
                    #                SELECT report_data_set_row_value.row_value
                    #                    FROM report_data_set_chart_row_value
                    #                        LEFT JOIN report_data_set_row_value
                    #                            ON report_data_set_row_value.report_data_set_row_value_id = report_data_set_chart_row_value.report_data_set_row_value_id
                    #                WHERE report_data_set_row_value.report_data_set_column_id = %s""",(report_column['report_data_set_column_id']))
                    #    for el in self._db.record:
                    #        self.included_elements.append(el['row_value'])

            if data['chart_include_method'] != 'selected values':
                self.included_elements = []
            
            # getting colors for charting data from layout data set
            res = self._db.Query("""SELECT bar_color, line_color, chart_layout_dataset_id 
                                        FROM chart_layout_dataset
                                    WHERE layout_id=%s 
                                        AND total_ind = 'N'
                                        AND other_ind = 'N'
                                    ORDER BY charting_order """,(data['layout_id']))
            data['line_colors'] = []
            data['bar_colors'] = []
            if res:
                for el in self._db.record:
                    if el['line_color']:
                        data['line_colors'].append({'color': el['line_color'], 'chart_layout_dataset_id': el['chart_layout_dataset_id']})
                    if el['bar_color']:
                        data['bar_colors'].append({'color': el['bar_color'], 'chart_layout_dataset_id': el['chart_layout_dataset_id']})

            
            # getting colors for charting data of 'Other' type
            res = self._db.Query("""SELECT bar_color, line_color 
                                        FROM chart_layout_dataset
                                    WHERE layout_id=%s 
                                        AND other_ind = 'Y'
                                    ORDER BY charting_order """,(data['layout_id']))
            data['line_other_color'] = None
            data['bar_other_color'] = None
            if res:
                el = self._db.record[0]
                data['line_other_color'] = el['line_color']
                data['bar_other_color'] = el['bar_color']
            
            # getting colors for charting data of 'TOTAL' type
            res = self._db.Query("""SELECT bar_color, line_color 
                                        FROM chart_layout_dataset
                                    WHERE layout_id=%s 
                                        AND total_ind = 'Y'
                                    ORDER BY charting_order """,(data['layout_id']))
            data['line_total_color'] = None
            data['bar_total_color'] = None
            if res:
                el = self._db.record[0]
                data['line_total_color'] = el['line_color']
                data['bar_total_color'] = el['bar_color']
        
            
            self._db.Query("""SELECT * FROM chart_data_point_shape ORDER BY charting_order """)
            data['line_point_shapes'] = [shape for shape in self._db.record]

            # getting colors for every custom chart line/bar
            #===================================================================
            # if data['bars_or_lines_created_for'] == 'column headers':
            #    self._db.Query("""SELECT report_data_set_column.column_name as name,
            #                        report_data_set_chart_column.column_chart_value_display_type as chart_value_display_type, 
            #                        report_data_set_chart_column.column_chart_line_type as chart_line_type,
            #                        report_data_set_chart_column.column_chart_line_style as chart_line_style,
            #                        report_data_set_chart_column.color,
            #                        report_data_set_column.display_name
            #                    FROM report_data_set_chart_column
            #                        LEFT JOIN report_data_set_column
            #                            ON report_data_set_chart_column.report_data_set_column_id = report_data_set_column.report_data_set_column_id
            #                    WHERE report_data_set_chart_column.report_data_set_chart_id=%s""",(self.chart_id))
            # elif data['bars_or_lines_created_for'] == 'row values':
            #    self._db.Query("""SELECT report_data_set_row_value.row_value as name,
            #                        report_data_set_chart_row_value.row_chart_value_display_type as chart_value_display_type,
            #                        report_data_set_chart_row_value.row_chart_line_type as chart_line_type,
            #                        report_data_set_chart_row_value.row_chart_line_style as chart_line_style,
            #                        report_data_set_chart_row_value.color
            #                    FROM report_data_set_chart_row_value
            #                        LEFT JOIN report_data_set_row_value
            #                            ON report_data_set_chart_row_value.report_data_set_row_value_id = report_data_set_row_value.report_data_set_row_value_id
            #                    WHERE report_data_set_chart_row_value.report_data_set_chart_id=%s""",(self.chart_id))
            # self.custom_chart_data = dict()
            # for r in self._db.record:
            #    self.custom_chart_data[r['name']] = r
            #===================================================================
            
            # getting secondary y axis values
            if data['bars_or_lines_created_for'] == 'column headers':
                if self.pivot_id:
                    self._db.Query("""SELECT report_data_set_row_value.row_value as name FROM report_data_set_chart_row_value
                                    LEFT JOIN report_data_set_row_value
                                    ON report_data_set_chart_row_value.report_data_set_row_value_id = report_data_set_row_value.report_data_set_row_value_id
                                    WHERE report_data_set_chart_row_value.report_data_set_chart_id=%s AND report_data_set_chart_row_value.axis_number = 2 """,(self.chart_id, ))
                else:
                    self._db.Query("""SELECT report_data_set_column.column_name as name FROM report_data_set_chart_column
                                    LEFT JOIN report_data_set_column
                                    ON report_data_set_chart_column.report_data_set_column_id = report_data_set_column.report_data_set_column_id
                                    WHERE report_data_set_chart_column.report_data_set_chart_id=%s AND report_data_set_chart_column.axis_number = 2 """,(self.chart_id, ))
            elif data['bars_or_lines_created_for'] == 'row values':
                self._db.Query("""SELECT report_data_set_row_value.row_value as name FROM report_data_set_chart_row_value
                                    LEFT JOIN report_data_set_row_value
                                    ON report_data_set_chart_row_value.report_data_set_row_value_id = report_data_set_row_value.report_data_set_row_value_id
                                    WHERE report_data_set_chart_row_value.report_data_set_chart_id=%s AND report_data_set_chart_row_value.axis_number = 2 """,(self.chart_id, ))
            yaxis2_elements = []
            for el in self._db.record:
                yaxis2_elements.append(el['name'])
    
            # add TOTAL to secondary y-axis if necessary
            if data['report_data_set_pivot_id'] and data['pivot_total_axis_number'] and str(data['pivot_total_axis_number']) == '2':
                yaxis2_elements.append('TOTAL')

            data['secondary_y_axis_values'] = set(yaxis2_elements)

        
            # getting primary axis mask id
            self.primary_y_axis_display_mask_id = 0
            
            if data['bars_or_lines_created_for'] == 'row values':
                x_axis_field_index = 0
        
                # get index of x axis column 
                if self.x_axis_field:
                    for k, v in enumerate(self.data['header']):
                        if self.x_axis_field == v:
                            x_axis_field_index = k
                for _k, _v in enumerate(self.data['rows']):
                    # check it
                    if not _v[x_axis_field_index] in data['secondary_y_axis_values']:
                        if not self.primary_y_axis_display_mask_id:
                            for _k2, _v2 in enumerate(_v):
                                if _k2 > 0 and self.orig_data['rows'][_k][_k2]['display_mask_id']:
                                    self._db.Query("""SELECT * FROM display_mask
                                        WHERE display_mask_id=%s""",(self.orig_data['rows'][_k][_k2]['display_mask_id']))
                                    mask = self._db.record[0]
                                    if mask['mask_type'] == 'numeric':
                                        self.primary_y_axis_display_mask_id = self.orig_data['rows'][_k][_k2]['display_mask_id']
                                        break
            else:
                if self.orig_data['rows']:
                    for _k, _v in enumerate(self.data['header']):
                        if not _v in data['secondary_y_axis_values']:
                            if self.included_elements and _v not in self.included_elements:
                                continue  
                            if self.primary_y_axis_display_mask_id == 0 and _v != self.x_axis_field:
                                if self.orig_data['rows'][0][_k]['display_mask_id']:
                                    self._db.Query("""SELECT * FROM display_mask
                                            WHERE display_mask_id=%s""",(self.orig_data['rows'][0][_k]['display_mask_id']))
                                            
                                    mask = self._db.record[0]
                                    if mask['mask_type'] == 'numeric':
                                        self.primary_y_axis_display_mask_id = self.orig_data['rows'][0][_k]['display_mask_id']

            #data['primary_axis_display_mask'] = self.primary_axis_display_mask_id
            
            if self.primary_y_axis_display_mask_id:
                self._db.Query("""SELECT * FROM display_mask
                                WHERE display_mask_id=%s""",(self.primary_y_axis_display_mask_id, ))

                format = self._db.record[0]
                if not format['display_precision_digits']:
                    format['display_precision_digits'] = 0
                if not format['thousands_delimiter']:
                    format['thousands_delimiter'] = ''
                if not format['decimal_point_delimiter']:
                    format['decimal_point_delimiter'] = ''
                if not format['prefix']:
                    format['prefix'] = ''
                if not format['suffix']:
                    format['suffix'] = ''
                data['primary_y_axis_display_mask'] = format
            else:
                data['primary_y_axis_display_mask'] = None

    
            # getting secondary axis mask id
            self.secondary_y_axis_display_mask_id = None

            if data['bars_or_lines_created_for'] == 'row values':
                for _k, _v in enumerate(self.data['rows']):
                    if _v[0] in data['secondary_y_axis_values']:
                        if not self.secondary_y_axis_display_mask_id:
                            for _k2, _v2 in enumerate(_v):
                                if _k2 > 0 and self.orig_data['rows'][_k][_k2]['display_mask_id']:
                                    self._db.Query("""SELECT * FROM display_mask
                                        WHERE display_mask_id=%s""",(self.orig_data['rows'][_k][_k2]['display_mask_id']))
                                    mask = self._db.record[0]
                                    if mask['mask_type'] == 'numeric':
                                        self.secondary_y_axis_display_mask_id = self.orig_data['rows'][_k][_k2]['display_mask_id']
                                        break
            else:
                if self.orig_data['rows']:
                    for _k, _v in enumerate(self.data['header']):
                        if _v in data['secondary_y_axis_values']:
                            if self.included_elements and _v not in self.included_elements:
                                continue
                            if not self.secondary_y_axis_display_mask_id:
                                if self.orig_data['rows'][0][_k]['display_mask_id']:
                                    self._db.Query("""SELECT * FROM display_mask
                                            WHERE display_mask_id=%s""",(self.orig_data['rows'][0][_k]['display_mask_id']))
                                    mask = self._db.record[0]
                                    if mask['mask_type'] == 'numeric':
                                        self.secondary_y_axis_display_mask_id = self.orig_data['rows'][0][_k]['display_mask_id']


            #data['secondary_y_axis_display_mask_id'] = self.secondary_y_axis_display_mask_id
            if self.secondary_y_axis_display_mask_id:
                self._db.Query("""SELECT * FROM display_mask
                                WHERE display_mask_id=%s""",(self.secondary_y_axis_display_mask_id, ))

                format = self._db.record[0]
                if not format['display_precision_digits']:
                    format['display_precision_digits'] = 0
                if not format['thousands_delimiter']:
                    format['thousands_delimiter'] = ''
                if not format['decimal_point_delimiter']:
                    format['decimal_point_delimiter'] = ''
                if not format['prefix']:
                    format['prefix'] = ''
                if not format['suffix']:
                    format['suffix'] = ''
                data['secondary_y_axis_display_mask'] = format
            else:
                data['secondary_y_axis_display_mask'] = None

#            if self.secondary_y_axis_display_mask_id:
#                self._db.Query("""SELECT * FROM display_mask
#                                WHERE display_mask_id=%s""",(self.secondary_y_axis_display_mask_id, ))
#                data['secondary_display_precision_digits'] = self._db.record[0]['display_precision_digits']
#                data['secondary_thousands_delimiter'] = self._db.record[0]['thousands_delimiter']
#                data['secondary_decimal_point_delimiter'] = self._db.record[0]['decimal_point_delimiter']
#                data['secondary_suffix'] = self._db.record[0]['suffix']
#                data['secondary_prefix'] = self._db.record[0]['prefix']
#                data['secondary_show_amount_as'] = self._db.record[0]['show_amount_as']
#                if not data['secondary_display_precision_digits']:
#                    data['secondary_display_precision_digits'] = 0
#                if not data['secondary_thousands_delimiter']:
#                    data['secondary_thousands_delimiter'] = ''
#                if not data['secondary_decimal_point_delimiter']:
#                    data['secondary_decimal_point_delimiter'] = ''
#                if not data['secondary_prefix']:
#                    data['secondary_prefix'] = ''
#                if not data['secondary_suffix']:
#                    data['secondary_suffix'] = ''


        return data

    def get_data(self):
        self.settings = self.get_specific_settings(self.settings)
        chart_type = self.settings['chart_type']

        if not chart_type:
            raise Exception("Chart type is not set")
        
        result_data = {}
        
        #self.strip_non_shown_columns()
        
        if chart_type == 'pie':
            #this is pie chart

            self.chart_display_mode = 'pie'
            
             
            if not self.settings['chart_by_report_data_set_column'] or not self.settings['sector_value_data_set_column']:
                self.prepare_pie_row_fetched_data()
                
            modified_data = self.get_pie_modified_data(self.settings['chart_by_report_data_set_column'], 
                                                    self.settings['sector_value_data_set_column'])
            
            
            result_data = modified_data
            
            result_data['colors'] = list()
            
            colors = self.settings['sector_colors'][:]
            used_sector_colors = list()
            try:
                result_data['formatted_header'].index('Other')
                sector_other_color = FontManager.get_db_color(self.settings['sector_other_color'])
                used_sector_colors.append(sector_other_color)
            except ValueError:
                pass
            
            for key in result_data['formatted_header']:
                property_id = None
                color_dataset_id = None
                color = None
                if key == 'Other':
                    result_data['colors'].append(sector_other_color)
                else:
                    #if self.custom_chart_data.has_key(key) and self.custom_chart_data[key]['color']:
                    if key in self.custom_chart_data and self.custom_chart_data[key]['color']:
                        # Color step 1.specify custom color if it is set by user
                        color = FontManager.get_db_color(self.custom_chart_data[key]['color'])
                        used_sector_colors.append(color)
                        result_data['colors'].append(color)
                    else:
                        # Color step 2. Try to find color from report_data_set_chart_element_property table
                        res = self._db.Query("""SELECT * FROM report_data_set_chart_element_property
                                                WHERE report_data_set_chart_id = %s 
                                                    AND chart_element_name = %s
                                                    AND chart_element_type = %s
                                                """, (self.chart_id, key, 'sector'))
                        
                        if res:
                            # if property is set for current charting element, use it
                            element_property = self._db.record[0]
                            property_id = element_property['report_data_set_chart_element_property_id']
                            if element_property['chart_layout_dataset_id']:
                                # if color is set in property table
                                res = self._db.Query("""SELECT chart_layout_dataset.*  
                                            FROM chart_layout_dataset    
                                            WHERE chart_layout_dataset_id = %s
                                                """, (element_property['chart_layout_dataset_id']))
                                if res:
                                    element_color = self._db.record[0]
                                    color_dataset_id = element_property['chart_layout_dataset_id']
                                    color = FontManager.get_db_color(element_color['sector_color'])

                        if color and color not in used_sector_colors:
                            result_data['colors'].append(color)
                            used_sector_colors.append(color)
                        else:
                            color = None
                            color_dataset_id = None
                            # Color step 3. Try to find color from report_data_set_chart_element_property table for similar element
                            res = self._db.Query("""SELECT * FROM report_data_set_chart_element_property
                                                    WHERE chart_element_name = %s
                                                        AND chart_element_type = %s
                                                    ORDER BY created_time DESC
                                                    """, (key, 'sector'))
                            
                            if res:
                                element_property = self._db.record[0]
                                if element_property['chart_layout_dataset_id']:
                                    # if color is set in property table
                                    res = self._db.Query("""SELECT chart_layout_dataset.*  
                                                FROM chart_layout_dataset    
                                                WHERE chart_layout_dataset_id = %s
                                                    """, (element_property['chart_layout_dataset_id']))
                                    if res:
                                        element_color = self._db.record[0]
                                        color_dataset_id = element_property['chart_layout_dataset_id']
                                        color = FontManager.get_db_color(element_color['sector_color'])
                                        
                            if color and color not in used_sector_colors:
                                result_data['colors'].append(color)
                                used_sector_colors.append(color)
                                self._store_charting_property(property_id, color_dataset_id, key, 'sector', None)
                            else:
                                while colors:
                                    # loop all possible sector colors
                                    color_row = colors.pop(0)
                                    try_color = FontManager.get_db_color(color_row['color'])
                                    if try_color not in used_sector_colors:
                                        # found color not used before
                                        color = try_color
                                        result_data['colors'].append(color)
                                        color_dataset_id = color_row['chart_layout_dataset_id']
                                        self._store_charting_property(property_id, color_dataset_id, key, 'sector', None)
                                        break
                                if not color:
                                    # nothing is found, lets randomly create it
                                    color = random.randrange(0x0, 0xaaaaaa)
                                    while color in used_sector_colors:
                                        color = random.randrange(0x0, 0xaaaaaa)
                                    used_sector_colors.append(color)
                                    result_data['colors'].append(color)

            
            # get annotations for chart
            result_data['annotations'] = OrderedDict()

            for header in modified_data['formatted_header']:
                result_data['annotations'][header] = list()
                
                
 
                        
            if self.type == 'large':
                annotations = self.get_non_date_chart_annotations()
                for annot in annotations:
                    if annot['chart_by_column_value'] in modified_data['formatted_header']:
                        result_data['annotations'][annot['chart_by_column_value']].append(annot)
        else:
            self.chart_display_mode = self.settings['primary_chart_value_display_type']
             
            # strip first element of each rows. only needed for 'column headers'
            strip_first = True

            # reverse order of data used for x-axis
            reverse = False

            if self.settings['bars_or_lines_created_for'] == 'column headers':
                if not self.pivot_id:
                    self.prepare_fetched_data()
                
                x_axis_field_index = 0
        
                for k, v in enumerate(self.data['header']):
                    if self.x_axis_field == v:
                        x_axis_field_index = k

                self.x_axis_display_mask_id = self.display_mask_ids['header'][x_axis_field_index]
                
                self.data = self.transform_columns_into_rows(self.data, x_axis_field_index, str_header = False)
                self.formatted_data = self.transform_columns_into_rows(self.formatted_data, x_axis_field_index, str_header = True)

                self.display_mask_ids = self.transform_columns_into_rows(self.display_mask_ids, x_axis_field_index, str_header = False)
                if self.settings['reverse_row_value_sort_order_ind'] == 'Y':
                    reverse = True
                
                strip_first = False
            else:
                if not self.pivot_id:
                    self.prepare_row_fetched_data()
            

            # check if this is pivot with missed pivot_column_value_column_id or pivot_row_value_column_id
            # in this case do not strip TOTAl column/row

            if self.pivot_id:
                self.settings['chart_pivot_total_ind'] = self.check_for_pivot_total_settings(self.settings['chart_pivot_total_ind'])
            modified_data = self.get_modified_data(strip_first, self.settings['chart_pivot_total_ind'], reverse)

            
            has_other = False
            other_is_bar = False
            has_total = False
            total_is_bar = False 
            
            #if modified_data['data'].has_key('Other'):
            if 'Other' in modified_data['data']:
                has_other = True
                if self.settings['other_chart_value_display_type'] == 'bar':
                    self.bar_elements_count += 1
                    other_is_bar = True 
            #if modified_data['data'].has_key('TOTAL'):
            if 'TOTAL' in modified_data['data']:
                has_total = True
                if self.settings['pivot_total_chart_value_display_type'] == 'bar':
                    self.bar_elements_count += 1
                    total_is_bar = True
            
            # get max data points
            if self.custom_chart_data:
                for k in self.custom_chart_data:
                    if self.settings['primary_chart_value_display_type'] != self.custom_chart_data[k]['chart_value_display_type']:
                        self.chart_display_mode = 'both'
                    
                    if self.custom_chart_data[k]['chart_value_display_type'] == 'bar':
                        self.bar_elements_count += 1
            else:
                if self.settings['primary_chart_value_display_type'] == 'bar':
                    self.bar_elements_count = len(modified_data['data'].keys())
                    
                    if has_other and not other_is_bar:
                        self.bar_elements_count -= 1
                    
                    if has_total and not total_is_bar:
                        self.bar_elements_count -= 1

            self.is_x_axis_date = self._is_x_axis_date(modified_data['header'])

#            if self.is_x_axis_date:
#                if self.x_axis_display_mask_id:
#                    self.formatter = FieldFormatter(self.x_axis_display_mask_id)
#                result_data['x_scale_values'] = modified_data['header'][:]
#                result_data['x_scale_labels'] = modified_data['formatted_header'][:]
            if self.settings['limit_x_axis_values_ind'] == 'Y':
                modified_data = self.get_thinned_x_axis(modified_data)
            else:
                # thinning data according to maximum data points number
                max_data_points = self.settings['max_line_data_points_to_chart']

                if self.chart_display_mode == 'line':
                    max_data_points = self.settings['max_line_data_points_to_chart']
                elif self.chart_display_mode == 'bar':
                    max_data_points = self.settings['max_bar_data_points_to_chart']
                elif self.chart_display_mode == 'both':
                    max_data_points = min(self.settings['max_line_data_points_to_chart'], self.settings['max_bar_data_points_to_chart'])
                modified_data = self.get_thinned_data(modified_data, self.settings['data_thinning_method'], max_data_points, None)

            result_data['labels'] = modified_data['formatted_header']
            result_data['orig_header'] = modified_data['header']
            result_data['settings'] = OrderedDict()

            if self.is_x_axis_date:
                if self.x_axis_display_mask_id:
                    self.formatter = FieldFormatter(self.x_axis_display_mask_id)
                result_data['x_scale_values'] = modified_data['header'][:]
                result_data['x_scale_labels'] = modified_data['formatted_header'][:]

            if not self.is_x_axis_date:
                result_data['x_scale_values'] = range(len(modified_data['header']))
                result_data['x_scale_labels'] = modified_data['formatted_header'][:]
             

            
            line_point_shapes = cycle(self.settings['line_point_shapes'])
            
            #bar_key = 0
            bar_colors = self.settings['bar_colors']
            #bar_colors_len = len(self.settings['bar_colors'])
            
            line_colors = cycle(self.settings['line_colors'])
            used_bar_colors = list()
            used_line_colors_shapes = list()
            used_line_shapes = list()
            used_line_colors = list()
            
            # specify color for Other element
            #if modified_data['data'].has_key('Other'):
            if 'Other' in modified_data['data']:
                if self.settings['other_chart_value_display_type'] == 'bar':
                    color = FontManager.get_db_color(self.settings['bar_other_color'])
                    used_bar_colors.append(color)
                else:
                    color = FontManager.get_db_color(self.settings['line_other_color'])
                    used_line_colors.append(color)
            
            # specify color for TOTAL element
            #if result_data['settings'].has_key('TOTAL'):
            if 'TOTAL' in result_data['settings']:
                if self.settings['pivot_total_chart_value_display_type'] == 'bar':
                    color = FontManager.get_db_color(self.settings['bar_total_color'])
                    used_bar_colors.append(color)
                else:
                    color = FontManager.get_db_color(self.settings['line_total_color'])
                    used_line_colors.append(color)
                
            #color = 0
            self.formatted_to_clean = dict()
            order = 0
            for key in modified_data['data']:
                result_data['settings'][key] = OrderedDict()
                result_data['settings'][key]['order'] = order
                order += 1
                result_data['settings'][key]['display_type'] = self.settings['primary_chart_value_display_type']
                result_data['settings'][key]['line_type'] = 'solid'
                result_data['settings'][key]['line_style'] = self.settings['primary_charting_line_style']
                result_data['settings'][key]['axis_number'] = 1
                result_data['settings'][key]['data'] = modified_data['data'][key]

                formatted_key = self.clean_to_formatted[key]

                self.formatted_to_clean[formatted_key] = key
                
                result_data['settings'][key]['label'] = formatted_key


                result_data['settings'][key]['formatted_data'] = modified_data['formatted_data'][formatted_key]
                result_data['settings'][key]['color'] = None
                result_data['settings'][key]['line_point_shape'] = None
                
                # set color and line point shape for not TOTAL/Other elements
                if key != 'Other' and key != 'TOTAL':
                    property_id = None
                    color_dataset_id = 0
                    shape_id = None
                    

                    #if self.custom_chart_data.has_key(key):
                    if key in self.custom_chart_data:
                        try:
                            #if self.settings['bars_or_lines_created_for'] == 'column headers':
                            result_data['settings'][key]['line_type'] = self.custom_chart_data[key]['chart_line_type']
                            result_data['settings'][key]['line_style'] = self.custom_chart_data[key]['chart_line_style']
                            result_data['settings'][key]['display_name'] = self.custom_chart_data[key]['display_name']
                            result_data['settings'][key]['display_type'] = self.custom_chart_data[key]['chart_value_display_type']
                            # Color step 1.specify custom color if it is set by user
                            if self.custom_chart_data[key]['color']:
                                result_data['settings'][key]['color'] = FontManager.get_db_color(self.custom_chart_data[key]['color'])
                            if result_data['settings'][key]['display_type'] == 'line' and self.custom_chart_data[key]['chart_data_point_shape_id']:
                                res = self._db.Query("""SELECT * FROM chart_data_point_shape WHERE chart_data_point_shape_id = %s""", (self.custom_chart_data[key]['chart_data_point_shape_id'], ))
                                if res:
                                    result_data['settings'][key]['line_point_shape'] = self._db.record[0]
                        except KeyError:
                            if self.pivot_id:
                                raise Exception("Required column '%s' is missing from charting data. Check chart settings and pivot data." % key)
                            else:
                                raise Exception("Required column '%s' is missing from charting data. Check chart settings." % key)
                    
                    if result_data['settings'][key]['display_type'] == 'bar' and result_data['settings'][key]['color']:
                        # if it is 'bar' element and color is set, store used color and continue
                        used_bar_colors.append(result_data['settings'][key]['color'])
                    elif result_data['settings'][key]['display_type'] == 'line' and result_data['settings'][key]['color'] and result_data['settings'][key]['line_point_shape']:
                        # if it is 'liner' element and color/shape are set, store used color/shape and continue
                        used_line_colors_shapes.append({'color': result_data['settings'][key]['color'], 'shape': result_data['settings'][key]['line_point_shape']['chart_data_point_shape_id']})
                        used_line_colors.append(result_data['settings'][key]['color'])
                        used_line_shapes.append(result_data['settings'][key]['line_point_shape']['chart_data_point_shape_id'])

                    else:
                        # something is not set
                        if not result_data['settings'][key]['color'] or (result_data['settings'][key]['display_type'] == 'line' and not result_data['settings'][key]['line_point_shape']):
                            # Color step 2. Try to find color/shape from report_data_set_chart_element_property table
                            res = self._db.Query("""SELECT * FROM report_data_set_chart_element_property
                                                    WHERE report_data_set_chart_id = %s 
                                                        AND chart_element_name = %s
                                                        AND chart_element_type = %s
                                                    """, (self.chart_id, 
                                                         result_data['settings'][key]['label'],
                                                         result_data['settings'][key]['display_type']))
                            
                            if res:
                                # if property is set for current charting element, use it
                                element_property = self._db.record[0]
                                property_id = element_property['report_data_set_chart_element_property_id']
                                
                                if not result_data['settings'][key]['color']:
                                    # if color is not preset for element
                                    if element_property['chart_layout_dataset_id']:
                                        # if color is set in property table
                                        res = self._db.Query("""SELECT chart_layout_dataset.*  
                                                    FROM chart_layout_dataset    
                                                    WHERE chart_layout_dataset_id = %s
                                                        """, (element_property['chart_layout_dataset_id']))
                                        if res:
                                            element_color = self._db.record[0]
                                            color_dataset_id = element_property['chart_layout_dataset_id']
                                            if result_data['settings'][key]['display_type'] == 'bar':
                                                result_data['settings'][key]['color'] = FontManager.get_db_color(element_color['bar_color'])
                                            elif result_data['settings'][key]['display_type'] == 'line':
                                                result_data['settings'][key]['color'] = FontManager.get_db_color(element_color['line_color'])
                                                 
                                
                                if result_data['settings'][key]['display_type'] == 'line' and not result_data['settings'][key]['line_point_shape']:
                                    # if shape is not preset for line element 
                                    if element_property['chart_data_point_shape_id']:
                                        res = self._db.Query("""SELECT chart_data_point_shape.*  
                                                        FROM chart_data_point_shape
                                                        WHERE chart_data_point_shape_id = %s
                                                        """, (element_property['chart_data_point_shape_id']))
                                    
                                        if res:
                                            # if shape is set in property table
                                            element_shape = self._db.record[0]
                                            shape_id = element_property['chart_data_point_shape_id']
                                            result_data['settings'][key]['line_point_shape'] = element_shape
                                            
                        if result_data['settings'][key]['display_type'] == 'bar' and result_data['settings'][key]['color']:
                            # if it is 'bar' element and color is set and unique, store used color and continue
                            if result_data['settings'][key]['color'] not in used_bar_colors:
                                used_bar_colors.append(result_data['settings'][key]['color'])
                            else:
                                result_data['settings'][key]['color'] = None
                                color_dataset_id = None
                        elif result_data['settings'][key]['display_type'] == 'line' and result_data['settings'][key]['color'] and result_data['settings'][key]['line_point_shape']:
                            # if it is 'liner' element and color/shape are set and unique, store used color/shape and continue
                            if {'color': result_data['settings'][key]['color'], 'shape': result_data['settings'][key]['line_point_shape']['chart_data_point_shape_id']} not in used_line_colors_shapes and \
                                        result_data['settings'][key]['color'] not in used_line_colors and \
                                        result_data['settings'][key]['line_point_shape']['chart_data_point_shape_id'] not in used_line_shapes:
                                used_line_colors_shapes.append({'color': result_data['settings'][key]['color'], 'shape': result_data['settings'][key]['line_point_shape']['chart_data_point_shape_id']})
                                used_line_colors.append(result_data['settings'][key]['color'])
                                used_line_shapes.append(result_data['settings'][key]['line_point_shape']['chart_data_point_shape_id'])
                            else:
                                if result_data['settings'][key]['color'] in used_line_colors:
                                    result_data['settings'][key]['color'] = None
                                    color_dataset_id = None
                                if result_data['settings'][key]['line_point_shape']['chart_data_point_shape_id'] in used_line_shapes:
                                    result_data['settings'][key]['line_point_shape'] = None
                                    shape_id = None
                        
                        # something is not set
                        if not result_data['settings'][key]['color'] or (result_data['settings'][key]['display_type'] == 'line' and not result_data['settings'][key]['line_point_shape']):
                            # Color step 3. Try to find color from report_data_set_chart_element_property table for similar element
                            res = self._db.Query("""SELECT * FROM report_data_set_chart_element_property
                                                    WHERE chart_element_name = %s
                                                        AND chart_element_type = %s
                                                    ORDER BY created_time DESC
                                                    """, (result_data['settings'][key]['label'],
                                                         result_data['settings'][key]['display_type']))
        
                            if res:
                                element_property = self._db.record[0]
                                if not result_data['settings'][key]['color']:
                                    # if color is not preset for element
                                    if element_property['chart_layout_dataset_id']:
                                        # if color is set in property table
                                        res = self._db.Query("""SELECT chart_layout_dataset.*  
                                                    FROM chart_layout_dataset    
                                                    WHERE chart_layout_dataset_id = %s
                                                        """, (element_property['chart_layout_dataset_id']))
                                        if res:
                                            element_color = self._db.record[0]
                                            color_dataset_id = element_property['chart_layout_dataset_id']
                                            if result_data['settings'][key]['display_type'] == 'bar':
                                                result_data['settings'][key]['color'] = FontManager.get_db_color(element_color['bar_color'])
                                            elif result_data['settings'][key]['display_type'] == 'line':
                                                result_data['settings'][key]['color'] = FontManager.get_db_color(element_color['line_color'])
                                                 
                                
                                if result_data['settings'][key]['display_type'] == 'line' and not result_data['settings'][key]['line_point_shape']:
                                    # if shape is not preset for line element 
                                    if element_property['chart_data_point_shape_id']:
                                        res = self._db.Query("""SELECT chart_data_point_shape.*  
                                                        FROM chart_data_point_shape
                                                        WHERE chart_data_point_shape_id = %s
                                                        """, (element_property['chart_data_point_shape_id']))
                                    
                                        if res:
                                            # if shape is set in property table
                                            element_shape = self._db.record[0]
                                            shape_id = element_property['chart_data_point_shape_id']
                                            result_data['settings'][key]['line_point_shape'] = element_shape
                                    
                            if result_data['settings'][key]['display_type'] == 'bar' and result_data['settings'][key]['color']:
                                # if it is 'bar' element and color is set and unique, store used color and continue
                                if result_data['settings'][key]['color'] not in used_bar_colors:
                                    used_bar_colors.append(result_data['settings'][key]['color'])
                                    self._store_charting_property(property_id,
                                                                 color_dataset_id, 
                                                                 result_data['settings'][key]['label'],
                                                                 result_data['settings'][key]['display_type'],
                                                                 None)
                                else:
                                    result_data['settings'][key]['color'] = None
                                    color_dataset_id = None
                            elif result_data['settings'][key]['display_type'] == 'line' and result_data['settings'][key]['color'] and result_data['settings'][key]['line_point_shape']:
                                # if it is 'liner' element and color/shape are set and unique, store used color/shape and continue
                                if {'color': result_data['settings'][key]['color'], 'shape': result_data['settings'][key]['line_point_shape']['chart_data_point_shape_id']} not in used_line_colors_shapes and \
                                        result_data['settings'][key]['color'] not in used_line_colors and \
                                        result_data['settings'][key]['line_point_shape']['chart_data_point_shape_id'] not in used_line_shapes:
                                    used_line_colors_shapes.append({'color': result_data['settings'][key]['color'], 'shape': result_data['settings'][key]['line_point_shape']['chart_data_point_shape_id']})
                                    used_line_colors.append(result_data['settings'][key]['color'])
                                    used_line_shapes.append(result_data['settings'][key]['line_point_shape']['chart_data_point_shape_id'])
                                    
                                    self._store_charting_property(property_id,
                                                                 color_dataset_id, 
                                                                 result_data['settings'][key]['label'],
                                                                 result_data['settings'][key]['display_type'],
                                                                 shape_id)
                                else:
                                    if result_data['settings'][key]['color'] in used_line_colors:
                                        result_data['settings'][key]['color'] = None
                                        color_dataset_id = None
                                    if result_data['settings'][key]['line_point_shape']['chart_data_point_shape_id'] in used_line_shapes:
                                        result_data['settings'][key]['line_point_shape'] = None
                                        shape_id = None
                            
                             
                            if result_data['settings'][key]['display_type'] == 'bar' and not result_data['settings'][key]['color']:
                                #still has no color for bar element 
                                while bar_colors:
                                    # loop all possible bar colors
                                    color_row = bar_colors.pop(0)
                                    color = FontManager.get_db_color(color_row['color'])
                                    if color not in used_bar_colors:
                                        # found color not used before
                                        result_data['settings'][key]['color'] = color
                                        color_dataset_id = color_row['chart_layout_dataset_id']
                                        self._store_charting_property(property_id,
                                                                 color_dataset_id, 
                                                                 result_data['settings'][key]['label'],
                                                                 result_data['settings'][key]['display_type'],
                                                                 None)
                                        break
                                if not result_data['settings'][key]['color']:
                                    # nothing is found, lets randomly create it
                                    color = random.randrange(0x0, 0xaaaaaa)
                                    while color in used_bar_colors:
                                        color = random.randrange(0x0, 0xaaaaaa)
                                    used_bar_colors.append(color)
                                    result_data['settings'][key]['color'] = color
                                    
                                
                                used_bar_colors.append(result_data['settings'][key]['color'])
                                
                            elif result_data['settings'][key]['display_type'] == 'line' and result_data['settings'][key]['color'] and not result_data['settings'][key]['line_point_shape']:
                                #still has no shape for line element, but color is set
                                for i in range(len(self.settings['line_point_shapes'])):
                                    shape = line_point_shapes.next()
                                    if {'color': result_data['settings'][key]['color'], 'shape': shape['chart_data_point_shape_id']} not in used_line_colors_shapes and \
                                            result_data['settings'][key]['color'] not in used_line_colors and \
                                            shape['chart_data_point_shape_id'] not in used_line_shapes:
                                        shape_id = shape['chart_data_point_shape_id']
                                        result_data['settings'][key]['line_point_shape'] = shape
                                        used_line_colors_shapes.append({'color': result_data['settings'][key]['color'], 'shape': result_data['settings'][key]['line_point_shape']['chart_data_point_shape_id']})
                                        used_line_colors.append(result_data['settings'][key]['color'])
                                        used_line_shapes.append(result_data['settings'][key]['line_point_shape']['chart_data_point_shape_id'])
                                        break
                                
                                if not result_data['settings'][key]['line_point_shape']:
                                    for i in range(len(self.settings['line_colors'])):
                                        color_row = line_colors.next()
                                        color = FontManager.get_db_color(color_row['color'])
                                        for j in range(len(self.settings['line_point_shapes'])):
                                            shape = line_point_shapes.next()
                                            if {'color': color, 'shape': shape['chart_data_point_shape_id']} not in used_line_colors_shapes:
                                                result_data['settings'][key]['color'] = color
                                                color_dataset_id = color_row['chart_layout_dataset_id']
                                                shape_id = shape['chart_data_point_shape_id']
                                                result_data['settings'][key]['line_point_shape'] = shape
                                                used_line_colors_shapes.append({'color': result_data['settings'][key]['color'], 'shape': result_data['settings'][key]['line_point_shape']['chart_data_point_shape_id']})
                                                used_line_colors.append(result_data['settings'][key]['color'])
                                                used_line_shapes.append(result_data['settings'][key]['line_point_shape']['chart_data_point_shape_id'])
                                                break
                                        if result_data['settings'][key]['line_point_shape']:
                                            break
                                
                                if result_data['settings'][key]['line_point_shape']:
                                    self._store_charting_property(property_id,
                                                                 color_dataset_id, 
                                                                 result_data['settings'][key]['label'],
                                                                 result_data['settings'][key]['display_type'],
                                                                 shape_id)
                                else:
                                    color = random.randrange(0x0, 0xaaaaaa)
                                    while color in used_line_colors:
                                        color = random.randrange(0x0, 0xaaaaaa)
                                    result_data['settings'][key]['color'] = color
                                    
                                    used_line_colors_shapes.append({'color': result_data['settings'][key]['color'], 'shape': result_data['settings'][key]['line_point_shape']['chart_data_point_shape_id']})
                                    used_line_colors.append(result_data['settings'][key]['color'])
                                    used_line_shapes.append(result_data['settings'][key]['line_point_shape']['chart_data_point_shape_id'])
                            elif result_data['settings'][key]['display_type'] == 'line' and not result_data['settings'][key]['color'] and result_data['settings'][key]['line_point_shape']:
                                #still has no color for line element, but shape is set
                                for i in range(len(self.settings['line_colors'])):
                                    color_row = line_colors.next()
                                    color = FontManager.get_db_color(color_row['color'])
                                    if {'color': color, 'shape': result_data['settings'][key]['line_point_shape']['chart_data_point_shape_id']} not in used_line_colors_shapes:
                                        result_data['settings'][key]['color'] = color
                                        color_dataset_id = color_row['chart_layout_dataset_id']
                                        used_line_colors_shapes.append({'color': result_data['settings'][key]['color'], 'shape': result_data['settings'][key]['line_point_shape']['chart_data_point_shape_id']})
                                        used_line_colors.append(result_data['settings'][key]['color'])
                                        used_line_shapes.append(result_data['settings'][key]['line_point_shape']['chart_data_point_shape_id'])
                                        break
                                
                                if not result_data['settings'][key]['color']:
                                    for j in range(len(self.settings['line_point_shapes'])):
                                        shape = line_point_shapes.next()
                                        for i in range(len(self.settings['line_colors'])):
                                            color_row = line_colors.next()
                                            color = FontManager.get_db_color(color_row['color'])
                                            if {'color': color, 'shape': shape['chart_data_point_shape_id']} not in used_line_colors_shapes:
                                                result_data['settings'][key]['color'] = color
                                                color_dataset_id = color_row['chart_layout_dataset_id']
                                                shape_id = shape['chart_data_point_shape_id']
                                                result_data['settings'][key]['line_point_shape'] = shape
                                                used_line_colors_shapes.append({'color': result_data['settings'][key]['color'], 'shape': result_data['settings'][key]['line_point_shape']['chart_data_point_shape_id']})
                                                used_line_colors.append(result_data['settings'][key]['color'])
                                                used_line_shapes.append(result_data['settings'][key]['line_point_shape']['chart_data_point_shape_id'])
                                                break
                                        if result_data['settings'][key]['line_point_shape']:
                                            break
                                
                                
                                if result_data['settings'][key]['color']:
                                    self._store_charting_property(property_id,
                                                                 color_dataset_id, 
                                                                 result_data['settings'][key]['label'],
                                                                 result_data['settings'][key]['display_type'],
                                                                 shape_id)
                                else:
                                    color = random.randrange(0x0, 0xaaaaaa)
                                    while color in used_line_colors:
                                        color = random.randrange(0x0, 0xaaaaaa)
                                    result_data['settings'][key]['color'] = color
                                    
                                    used_line_colors_shapes.append({'color': result_data['settings'][key]['color'], 'shape': result_data['settings'][key]['line_point_shape']['chart_data_point_shape_id']})
                                    used_line_colors.append(result_data['settings'][key]['color'])
                            elif result_data['settings'][key]['display_type'] == 'line' and not result_data['settings'][key]['color'] and not result_data['settings'][key]['line_point_shape']:
                                #still has no color and no shape for line element
                                for i in range(len(self.settings['line_colors'])):
                                    color_row = line_colors.next()
                                    color = FontManager.get_db_color(color_row['color'])
                                    for j in range(len(self.settings['line_point_shapes'])):
                                        shape = line_point_shapes.next()
                                        if {'color': color, 'shape': shape['chart_data_point_shape_id']} not in used_line_colors_shapes:
                                            result_data['settings'][key]['color'] = color
                                            color_dataset_id = color_row['chart_layout_dataset_id']
                                            shape_id = shape['chart_data_point_shape_id']
                                            result_data['settings'][key]['line_point_shape'] = shape
                                            used_line_colors_shapes.append({'color': result_data['settings'][key]['color'], 'shape': result_data['settings'][key]['line_point_shape']['chart_data_point_shape_id']})
                                            used_line_colors.append(result_data['settings'][key]['color'])
                                            break
                                    if result_data['settings'][key]['line_point_shape'] and result_data['settings'][key]['color']:
                                        break
                                if result_data['settings'][key]['color'] and result_data['settings'][key]['line_point_shape']:
                                    self._store_charting_property(property_id,
                                                                 color_dataset_id, 
                                                                 result_data['settings'][key]['label'],
                                                                 result_data['settings'][key]['display_type'],
                                                                 shape_id)
                                else:
                                    color = random.randrange(0x0, 0xaaaaaa)
                                    while color in used_line_colors:
                                        color = random.randrange(0x0, 0xaaaaaa)
                                    result_data['settings'][key]['color'] = color
                                    used_line_colors_shapes.append({'color': result_data['settings'][key]['color'], 'shape': result_data['settings'][key]['line_point_shape']['chart_data_point_shape_id']})
                                    used_line_colors.append(result_data['settings'][key]['color'])

            # specify color for Other element
            #if result_data['settings'].has_key('Other'):
            if 'Other' in result_data['settings']:
                if self.settings['other_chart_value_display_type']:
                    result_data['settings']['Other']['display_type'] = self.settings['other_chart_value_display_type']

                if result_data['settings']['Other']['display_type'] == 'bar':
                    color = FontManager.get_db_color(self.settings['bar_other_color'])
                else:
                    color = FontManager.get_db_color(self.settings['line_other_color'])
                result_data['settings']['Other']['color'] = color
                
                # set line shape
                if result_data['settings']['Other']['display_type'] == 'line':
                    # for line
                    result_data['settings']['Other']['line_point_shape'] = line_point_shapes.next()
                else:
                    #for bar
                    result_data['settings']['Other']['line_point_shape'] = None
            
            # specify color for TOTAL element
            #if result_data['settings'].has_key('TOTAL'):
            if 'TOTAL' in result_data['settings']:
                if self.settings['pivot_total_chart_value_display_type']:
                    result_data['settings']['TOTAL']['display_type'] = self.settings['pivot_total_chart_value_display_type']

                if result_data['settings']['TOTAL']['display_type'] == 'bar':
                    color = FontManager.get_db_color(self.settings['bar_total_color'])
                else:
                    color = FontManager.get_db_color(self.settings['line_total_color'])
                result_data['settings']['TOTAL']['color'] = color
                
                # set line shape
                if result_data['settings']['TOTAL']['display_type'] == 'line':
                    # for line
                    result_data['settings']['TOTAL']['line_point_shape'] = line_point_shapes.next()
                else:
                    #for bar
                    result_data['settings']['TOTAL']['line_point_shape'] = None

            # get annotations for chart
            result_data['annotations'] = dict()
            result_data['range_annotations'] = dict()
            result_data['point_annotations'] = dict()

            for element in result_data['settings']:
                result_data['annotations'][element] = dict()
                result_data['point_annotations'][element] = list()
                result_data['range_annotations'][element] = list()

            if self.type == 'large':
                if self.is_x_axis_date:
                    all_annotations = list()
                    # get point annotations
                    annotations = self.get_date_chart_point_annotations(result_data['orig_header'][0], result_data['orig_header'][-1])
                    
                    # add to single list
                    for annot in annotations:
                        if (annot['chart_element_identifier'] in result_data['settings'] or annot['chart_element_identifier'] in self.formatted_to_clean) and \
                                annot['annotation_measurement_time'] in result_data['orig_header']:
                            index = result_data['orig_header'].index(annot['annotation_measurement_time'])
                            if annot['chart_element_identifier'] in self.formatted_to_clean:
                                element = self.formatted_to_clean[annot['chart_element_identifier']]
                            else:
                                element = annot['chart_element_identifier']
                                annot['chart_element_identifier'] = result_data['settings'][element]['label']
                            annot['value'] = result_data['settings'][element]['data'][index]
                            annot['formatted_value'] = result_data['settings'][element]['formatted_data'][index]
                            all_annotations.append({'time': result_data['orig_header'][index],
                                                    'is_range': 0,
                                                    'index': None,
                                                    'header_index': index,
                                                    'element': element,
                                                    'order': result_data['settings'][element]['order'],
                                                    'data': annot})
                    # get range annotations
                    annotations = self.get_date_chart_range_annotations(result_data['orig_header'][0], result_data['orig_header'][-1])

                    for annot in annotations:
                        if annot['chart_element_identifier'] in result_data['settings'] or annot['chart_element_identifier'] in self.formatted_to_clean:
                            if annot['chart_element_identifier'] in self.formatted_to_clean:
                                element = self.formatted_to_clean[annot['chart_element_identifier']]
                            else:
                                element = annot['chart_element_identifier']
                                annot['chart_element_identifier'] = result_data['settings'][element]['label']

                            from_ind = 0
                            for i, d in enumerate(result_data['orig_header']):
                                if d and annot['from_time'] <= d:
                                    from_ind = i
                                    break
                            to_ind = len(result_data['orig_header']) - 1
                            for i, d in enumerate(result_data['orig_header']):
                                to_ind = i
                                if d and annot['to_time'] < d:
                                    break
                            value = max(result_data['settings'][element]['data'][from_ind:to_ind])
                            if value is None:
                                value = max(values)
                            if value is not None:
                                annot['index'] = None
                                annot['value'] = value

                                annot['raw_start_time'] = annot['annotation_measurement_start_time']
                                annot['raw_finish_time'] = annot['annotation_measurement_finish_time']

                                annot['start_time'] = self.formatter.format_date(annot['annotation_measurement_start_time'])
                                annot['finish_time'] = self.formatter.format_date(annot['annotation_measurement_finish_time'])

                                all_annotations.append({'time': annot['annotation_measurement_start_time'],
                                    'is_range': 1,
                                    'index': None,
                                    'header_index': None,
                                    'element': element,
                                    'order': result_data['settings'][element]['order'],
                                    'data': annot})
                    # index all annotations
                    indexed_all_annotations = self.index_annotations(all_annotations)
                    header_len = len(result_data['orig_header'])

                    # split all annotations to charting elements and annotation type
                    for element in result_data['settings']:
                        annotations, point_annotations, range_annotations = self.parse_annotations(indexed_all_annotations, header_len, element)
                        result_data['point_annotations'][element] = point_annotations
                        result_data['range_annotations'][element] = range_annotations
                        result_data['annotations'][element] = annotations

                else:
                    # get point annotations
                    annotations = self.get_non_date_chart_annotations()
                    point_annotations = dict()
                    for annot in annotations:
                        if (annot['chart_element_identifier'] in result_data['settings'] or annot['chart_element_identifier'] in self.formatted_to_clean) and \
                                annot['chart_by_column_value'] in result_data['labels']:#modified_data['formatted_header']:
                            index = result_data['labels'].index(annot['chart_by_column_value'])
                            

                            if annot['chart_element_identifier'] in self.formatted_to_clean:
                                element = self.formatted_to_clean[annot['chart_element_identifier']]
                            else:
                                element = annot['chart_element_identifier']
                                annot['chart_element_identifier'] = result_data['settings'][element]['label']

                            #if not point_annotations.has_key(element):
                            if element not in point_annotations:
                                point_annotations[element] = [[] for i in range(len(result_data['labels']))]
                            annot['value'] = result_data['settings'][element]['formatted_data'][index]
                            annot['formatted_value'] = result_data['settings'][element]['formatted_data'][index]

                            point_annotations[element][index].append(annot)

                    annotation_index = 1
                    # index annotations
                    for element in result_data['settings']:
                        #if point_annotations.has_key(element):
                        if element in point_annotations:
                            for i, annotations in enumerate(point_annotations[element]):
                                if annotations:
                                    result_data['point_annotations'][element].append(annotation_index)
                                    result_data['annotations'][element][annotation_index] = annotations
                                    annotation_index += 1
                                else:
                                    result_data['point_annotations'][element].append(None)
                        
        
        
        result = {'chart_data': result_data, 'layout_data': self.settings}
        return result
         
        
    
    def get_pie_modified_data(self, chart_by_report_data_set_column, sector_value_data_set_column):
       
        end_result = {'header': list(), 'formatted_header': list(), 'data': list(), 'formatted_data': list()}
        label_index = 0
        value_index = 0 
        display_mask_id = 0
        
        # get index for label and value values
        for key, header in enumerate(self.orig_data['header']):
            if header['original_val'] == chart_by_report_data_set_column:
                label_index = key
            if header['original_val'] == sector_value_data_set_column:
                value_index = key
        
        #let's create data for charting
        for row in self.orig_data['rows']:
            if not display_mask_id:
                display_mask_id = row[value_index]['display_mask_id']
            
            if row[label_index]['original_val'] in end_result['header']:
                # if label is already in charting list, sum it's values
                index = end_result['header'].index(row[label_index]['original_val'])
                end_result['data'][index] = end_result['data'][index] + row[value_index]['original_val']
                #end_result['formatted_data'][index] = self.formatter.format_half(end_result['data'][index], display_mask_id)
                end_result['formatted_data'][index] = self.formatter.format_full(end_result['data'][index], display_mask_id)
            else:
                # if label is not charting list, add it
                end_result['header'].append(row[label_index]['original_val'])
                end_result['formatted_header'].append(row[label_index]['formatted_val'])
                end_result['data'].append(row[value_index]['original_val'])
                end_result['formatted_data'].append(row[value_index]['formatted_val'])
        if display_mask_id:
            self.primary_y_axis_display_mask_id = display_mask_id
        else:
            self.primary_y_axis_display_mask_id = 0
        
        if self.settings['chart_include_method'] == 'top N values':
            return self.limit_top_N_pie_chart_sectors(end_result)
        
        elif self.settings['chart_include_method'] == 'selected values':
            return self.limit_selected_pie_chart_sectors(end_result)
        return end_result
            
        
        
        
    def get_modified_data(self, strip_first, chart_pivot_total_ind, reverse):
        x_axis_field_index = 0
        #excluded_data = OrderedDict()
        
        for k, v in enumerate(self.data['header']):
            if self.x_axis_field == v:
                x_axis_field_index = k

        self.x_axis_display_mask_id = self.display_mask_ids['header'][x_axis_field_index]

        #remove x axis column name from headers
        if strip_first:
            del(self.data['header'][x_axis_field_index])
            del(self.formatted_data['header'][x_axis_field_index])
        
        # delete row TOTAL if it is should not be shown
        if self.pivot_id and chart_pivot_total_ind == 'N':
            for k, v in enumerate(self.data['rows']):
                if v[0] == 'TOTAL':
                    del(self.data['rows'][k])
                    del(self.formatted_data['rows'][k])

        end_result = {'header': self.data['header'], 'formatted_header': self.formatted_data['header'], 'data': OrderedDict(), 'formatted_data': OrderedDict()}
        
        
        result = OrderedDict()
        formatted_result = OrderedDict()
        
        # prepare array with structure <element_to_chart>: <list of values to chart>
        for k, v in enumerate(self.data['rows']):
            # this is <element_to_chart>
            element_to_chart = unicode(v[x_axis_field_index])
            del(v[x_axis_field_index])
            
            #get the same(current) row of formatted rows list 
            formatted_v = self.formatted_data['rows'][k]
            
            # this is formatted <element_to_chart>
            formatted_element_to_chart = unicode(formatted_v[x_axis_field_index])
            del(formatted_v[x_axis_field_index])
            
            # make next association [element name] -> [formatted element name]
            self.clean_to_formatted[element_to_chart] = formatted_element_to_chart 
            
            # create <element_to_chart>: empty <list of values to chart>  
            result[element_to_chart] = list()
            formatted_result[formatted_element_to_chart] = list()
            
            #fill <list of values to chart> 
            for k1, v1 in enumerate(v):
                result[element_to_chart].append(v1)
                formatted_result[formatted_element_to_chart].append(formatted_v[k1])
            
        if self.settings['chart_include_method'] == 'selected values' and len(self.included_elements) > 0:
            result, formatted_result, excluded_data = self.limit_selected_bar_line_elements(result, formatted_result)
        elif self.settings['chart_include_method'] == 'top N values':
            result, formatted_result, excluded_data = self.limit_top_N_bar_line_elements(result, formatted_result)
        else:
            excluded_data = OrderedDict()


        # if excluded data have to be sum and charted as other
        if self.combine_excluded_elements_as_other_ind == 'Y' and excluded_data:
            row_len = len(end_result['header'])
            #create empty <list of values to chart>
            other = [0.0 for i in range(row_len)]
            
            #sum all excluded values into Other row
            for k, v in excluded_data.iteritems():
                for k1, v1 in enumerate(v):
                    other[k1] = other[k1] + v1
            
            result['Other'] = other
            
            if 'Other' in self.settings['secondary_y_axis_values']:
                other_display_mask_id = self.secondary_y_axis_display_mask_id
            else:
                other_display_mask_id = self.primary_y_axis_display_mask_id
                
            self.clean_to_formatted['Other'] = 'Other'
            
            #formatted_result['Other'] = [self.formatter.format_half(value, other_display_mask_id) for value in other]
            formatted_result['Other'] = [self.formatter.format_full(value, other_display_mask_id) for value in other]
             
        
        end_result['data'] = result
        end_result['formatted_data'] = formatted_result
        
        
        # reverse data if specified
        if reverse:
            # reverse header
            end_result['header'].reverse()
            end_result['formatted_header'].reverse()
            # reverse each data row
            for k in end_result['data']:
                end_result['data'][k].reverse()
            
            for k in end_result['formatted_data']:
                end_result['formatted_data'][k].reverse()
        
        #transfer TOTAL to the last place
        #if end_result['data'].has_key('Other') and end_result['data'].has_key('TOTAL'):
        if 'Other' in end_result['data'] and 'TOTAL' in end_result['data']:
            tmp_total = end_result['data']['TOTAL']
            del end_result['data']['TOTAL']
            end_result['data']['TOTAL'] = tmp_total
            
            tmp_total = end_result['formatted_data']['TOTAL']
            del end_result['formatted_data']['TOTAL']
            end_result['formatted_data']['TOTAL'] = tmp_total

        return end_result
    
    
    def limit_selected_bar_line_elements(self, result, formatted_result):
        """
        chart by selected rows/columns
        """
        clean_result = OrderedDict()
        clean_formatted_result = OrderedDict()
        excluded_data = OrderedDict()
        for k, v in result.iteritems():
            if k in self.included_elements or (self.pivot_id and k == 'TOTAL'):
                clean_result[k] = v
                formatted_k = self.clean_to_formatted[k]
                clean_formatted_result[formatted_k] = formatted_result[formatted_k]
            else:
                excluded_data[k] = v
        return clean_result, clean_formatted_result, excluded_data
    

    def limit_top_N_bar_line_elements(self, result, formatted_result):
        """
        chart by top N elements
        """
        header_len = len(result)
        excluded_data = OrderedDict()
        if self.settings['max_elements_to_chart'] and header_len > self.settings['max_elements_to_chart']:

            # remove one more element if excluded into other
            #if self.combine_excluded_elements_as_other_ind == 'Y':
            #    self.settings['max_elements_to_chart'] -= 1
            
            clean_result = OrderedDict()
            clean_formatted_result = OrderedDict()
            
            has_total = False 
            
            # create dict <element>: <value> for determine top N elements
            data = OrderedDict()
            if self.pivot_id:
                # for pivots use sum all element values
                for key in result:
                    if key != 'TOTAL':
                        data[key] = sum(result[key])
                    else:
                        has_total = True 
            else:
                # for non pivots use average of all element values
                for key in result:
                    if self.avg_column_value[key] is not None:
                        data[key] = self.avg_column_value[key]
                    else:
                        data[key] = (sum([number for number in result[key] if number is not None]) + 0.0) / len([number for number in result[key] if number is not None])
            
            if has_total:
                self.settings['max_elements_to_chart'] += 1

            to_reduce = header_len - self.settings['max_elements_to_chart']        
            
            excluded = nsmallest(to_reduce, data.iteritems(), itemgetter(1))
            excluded_elements = [item[0] for item in excluded]
            for key in result:
                if key in excluded_elements:
                    excluded_data[key] = result[key]
                else:
                    clean_result[key] = result[key]
                    clean_formatted_result[self.clean_to_formatted[key]] = formatted_result[self.clean_to_formatted[key]] 
            return clean_result, clean_formatted_result, excluded_data 
        else:
            return result, formatted_result, excluded_data
    
    def prepare_fetched_data(self):
        """
        prepare fetched data for column header chart - remove non numeric columns, sum rows with duplicated x axis value
        """
        data = {'header': [], 'rows': []}
        
        x_axis_field_index = 0
        
        # get index of x axis column 
        if self.x_axis_field:
            for k, v in enumerate(self.orig_data['header']):
                if self.x_axis_field == v['original_val']:
                    x_axis_field_index = k
        
        # get columns with numeric values
        # add column with x axis value first 
        data['header'].append(self.orig_data['header'][x_axis_field_index])
        # add column with x axis value first to each row 
        for row in self.orig_data['rows']:
            data['rows'].append([row[x_axis_field_index]])
        
        #for i in range(len(self.orig_data['header'])):
        for i, itm in enumerate(self.orig_data['header']):
            include_column = False
            # if this is not a x axis column
            if i != x_axis_field_index:
                # check all rows for specified column to find out does it contain numeric value  
                for row in self.orig_data['rows']:
                    if row[i]['original_val'] is None:
                        # this is None, cannot decide. continue checking 
                        continue
                    elif isinstance(row[i]['original_val'], int) or \
                            isinstance(row[i]['original_val'], float) or \
                            isinstance(row[i]['original_val'], long) or \
                            isinstance(row[i]['original_val'], Decimal):
                        # this is numeric value. column should be included. stop checking
                        include_column = True
                        break
                    else:
                        # this is NOT numeric value. column should not be included. stop checking
                        break
            
            if include_column:
                # add column to header 
                data['header'].append(self.orig_data['header'][i])
                # add column to each row
                #for j in range(len(self.orig_data['rows'])):
                for j, itmj in enumerate(self.orig_data['rows']):
                    data['rows'][j].append(self.orig_data['rows'][j][i])
        
        clean_rows = list() 
        for row in data['rows']:
            duplicated = None
            # check if there is any row with the same x axis value 
            for i, clean_row in enumerate(clean_rows):
                # take the first elements(z axis value) of the rows and compare
                if clean_row[0]['original_val'] == row[0]['original_val']:
                    duplicated = i
                    break
            if duplicated is not None:
                # this is duplicated x axis value. sum all elements of row except first
                for i in range(1, len(data['header'])):
                    clean_rows[duplicated][i]['original_val'] += row[i]['original_val']
                    #clean_rows[duplicated][i]['formatted_val'] = self.formatter.format_half(clean_rows[duplicated][i]['original_val'], clean_rows[duplicated][i]['display_mask_id'])
                    clean_rows[duplicated][i]['formatted_val'] = self.formatter.format_full(clean_rows[duplicated][i]['original_val'], clean_rows[duplicated][i]['display_mask_id'])
            else:
                # this is original x axis value. just add this row
                clean_rows.append(row)          
        
        self.orig_data['rows'] = clean_rows
        self.orig_data['header'] = data['header']
        self.get_data_from_orig()
         
    def transform_columns_into_rows(self, data, x_axis_field_index, str_header):
        """
        transform all columns into rows
        """
        result = {'header': [], 'rows': []}
        header = data['header']
        
        del(header[x_axis_field_index])
        
        #result['header'] = [str(v[0]) for k, v in enumerate(data['rows'])]
        #result['header'] = [unicode(v[x_axis_field_index]) for k, v in enumerate(data['rows'])]
        if str_header:
            result['header'] = [unicode(v[x_axis_field_index]) for k, v in enumerate(data['rows'])]
        else:
            result['header'] = [v[x_axis_field_index] for k, v in enumerate(data['rows'])]

        for k, v in enumerate(header):
            l = [v]
            #for k1, v1 in enumerate(data['rows']):
            for v1 in data['rows']:
                l.append(v1[k + 1])
            result['rows'].append(l)
        return result
    

    def get_thinned_data(self, data, method, max_data_points, primary_field):
        """
        Thin data according to method: sample, drop oldest, drop smallest
        """
        if not max_data_points:
            return data
        
        real_data_points = data_points = len(data['header'])
        by_bars = False
        if self.bar_elements_count and self.settings['use_stacked_bars_ind'] != 'Y':
            bar_count = self.bar_elements_count * data_points
            line_count = data_points
            if bar_count > line_count:
                by_bars = True
                data_points = bar_count

        if max_data_points < data_points:
            if by_bars:
                # calculate how many group of bars should be left
                max_data_points /= self.bar_elements_count
                # leave at least one point
                if max_data_points < 1:
                    max_data_points = 1
                
                data_points = real_data_points

            if method == 'sample':
                data_points -= 2
                max_data_points -= 2
                
                included_elements = [0, len(data['header']) - 1]
                medi_list = data['header']
                point_count_to_be_deleted = data_points - max_data_points
                if point_count_to_be_deleted > max_data_points:
                    el_num = floor(data_points / max_data_points)
                    medi_list = [k for k, v in enumerate(medi_list) if k % el_num == 0 and k != 0]
                else:
                    el_num = floor(data_points / point_count_to_be_deleted)
                    medi_list = [k for k, v in enumerate(medi_list) if k % el_num != 0]
                included_elements[1:1] = medi_list
                header = list()
                formatted_header = list()
                
                for k, v in enumerate(data['header']):
                    if k in set(included_elements):
                        header.append(v)
                        formatted_header.append(data['formatted_header'][k])

                data['header'] = header
                data['formatted_header'] = formatted_header
                #data['header'] = [(v) for k, v in enumerate(data['header']) if k in set(included_elements)]
                #data['formatted_header'] = [(data['formatted_header'][k]) for k, v in enumerate(data['header']) if k in set(included_elements)]
                
                for k, v in data['data'].iteritems():
                    data['data'][k] = [v1 for k1, v1 in enumerate(v) if k1 in set(included_elements)]
                    
                for k, v in data['formatted_data'].iteritems():
                    data['formatted_data'][k] = [v1 for k1, v1 in enumerate(data['formatted_data'][k]) if k1 in set(included_elements)]
                    
            elif method == 'drop oldest':
                data['header'] = data['header'][data_points - max_data_points: data_points]
                data['formatted_header'] = data['formatted_header'][data_points - max_data_points: data_points]
                data['data'] = OrderedDict((k, v[data_points - max_data_points: data_points]) for k, v in data['data'].iteritems())
                data['formatted_data'] = OrderedDict((k, v[data_points - max_data_points: data_points]) for k, v in data['formatted_data'].iteritems())

            elif method == 'drop smallest':
                header_value = dict()
                #rows_len = len(data['data'])
                for i, header in enumerate(data['header']):
                    sum = 0.0
                    for k in data['data']:
                        sum += data['data'][k][i] 
                    header_value[header] = sum
                
                
                to_reduce = data_points - max_data_points        
            
                excluded = nsmallest(to_reduce, header_value.iteritems(), itemgetter(1))
                excluded_headers = [item[0] for item in excluded]
                
                for k, v in data['data'].iteritems():
                    data['data'][k] = [v1 for k1, v1 in enumerate(v) if data['header'][k1] not in excluded_headers]
                    #formatted_k = self.clean_to_formatted[k]
                for k, v in data['formatted_data'].iteritems():
                    data['formatted_data'][k] = [v1 for k1, v1 in enumerate(data['formatted_data'][k]) if data['header'][k1] not in excluded_headers]
                
                data['formatted_header'] = [data['formatted_header'][k] for k, header in enumerate(data['header']) if header not in excluded_headers]
                data['header'] = [header for header in data['header'] if header not in excluded_headers]
                
                """
                if primary_field is None:
                    for k, v in data['data'].iteritems():
                        primary_field = k
                primary_field_value = data['data'][primary_field]

                excluded_elements = []
                point_count_to_be_deleted = data_points - max_data_points
                
                while len(excluded_elements) < point_count_to_be_deleted:
                    index = primary_field_value.index(min(primary_field_value))
                    excluded_elements.append(index)
                    primary_field_value[index] = []
                data['header'] = [(v) for k, v in enumerate(data['header']) if k not in set(excluded_elements)]
                for k, v in data['data'].iteritems():
                    data['data'][k] = [(v1) for k1, v1 in enumerate(v) if k1 not in set(excluded_elements)]
                    data['formatted_data'][k] = [(v1) for k1, v1 in enumerate(data['formatted_data'][k]) if k1 not in set(excluded_elements)]
                """
            
        return data
    
    def check_for_pivot_total_settings(self, chart_pivot_total_ind):
        """
        check if this is pivot with missed pivot_column_value_column_id or pivot_row_value_column_id
        in this case do not strip TOTAL column/row
        """
        self._db.Query("""SELECT pivot_column_value_column_id, pivot_row_value_column_id
                            FROM report_data_set_pivot
                            WHERE report_data_set_pivot_id = %s""", (self.pivot_id))
        pivot = self._db.record[0]
        if pivot['pivot_column_value_column_id'] and pivot['pivot_row_value_column_id']:
            return chart_pivot_total_ind
        else:
            return 'Y'
    

    def limit_top_N_pie_chart_sectors(self, result_data):
        """
        reduce number of charting elements (sectors)
        """
        header_len = len(result_data['header'])

        if self.settings['max_elements_to_chart'] and header_len > self.settings['max_elements_to_chart']:
            # remove one more element if excluded into other
            #if self.combine_excluded_elements_as_other_ind == 'Y':
            #    other = 0.0
            other = 0.0
            
            reduced_data = {'header':list(), 'formatted_header':list(), 'data': list(), 'formatted_data': list()}
            
            # create dict <element>: <value> for determine top N elements
            data = OrderedDict()
            for i in range(header_len):
                data[result_data['header'][i]] = result_data['data'][i]
            
            # if only one element should left and there is other then leave two elements
            #if self.settings['max_elements_to_chart'] <= 1 and self.combine_excluded_elements_as_other_ind == 'Y':
            #    self.settings['max_elements_to_chart'] = 2
            
            to_reduce = header_len - self.settings['max_elements_to_chart']
            
            excluded = nsmallest(to_reduce, data.iteritems(), itemgetter(1))
            excluded_headers = [item[0] for item in excluded]
            
            
            
            for i in range(header_len):
                if result_data['header'][i] in excluded_headers:
                    if self.combine_excluded_elements_as_other_ind == 'Y':
                        other += result_data['data'][i]
                else:
                    reduced_data['header'].append(result_data['header'][i])
                    reduced_data['formatted_header'].append(result_data['formatted_header'][i])
                    reduced_data['data'].append(result_data['data'][i])
                    reduced_data['formatted_data'].append(result_data['formatted_data'][i])
            
            if self.combine_excluded_elements_as_other_ind == 'Y':
                reduced_data['header'].append('Other')
                reduced_data['formatted_header'].append('Other')
                reduced_data['data'].append(other)
                #reduced_data['formatted_data'].append(self.formatter.format_half(other, self.primary_axis_display_mask_id))
                reduced_data['formatted_data'].append(self.formatter.format_full(other, self.primary_y_axis_display_mask_id))
            
            return reduced_data

        else:
            return result_data
    

    def limit_selected_pie_chart_sectors(self, result_data):
        """
        todo if needed
        """
        return result_data

    def get_date_chart_point_annotations(self, from_date, to_date):
        self._db.Query("""SELECT rdsca.*, ua.*, user.username, rdscai.*,
                                rdsca.start_time AS instance_start_time,
                                rdsca.expiration_time AS instance_expiration_time
                    FROM report_data_set_chart_annotation AS rdsca
                        LEFT JOIN user_annotation AS ua ON
                            rdsca.user_annotation_id = ua.user_annotation_id
                        LEFT JOIN report_data_set_chart_annotation_instance AS rdscai ON
                            rdsca.report_data_set_chart_annotation_id = rdscai.report_data_set_chart_annotation_id
                        LEFT JOIN user ON
                            user.user_id = ua.user_id
                    WHERE
                        rdsca.report_data_set_chart_id = %s
                        AND ua.segment_value_id = %s
                        AND ua.annotation_interval = 'point'
                        AND ua.annotation_measurement_time >= %s
                        AND ua.annotation_measurement_time <= %s
                    GROUP BY rdsca.report_data_set_chart_annotation_id
                    ORDER BY ua.annotation_measurement_time""" , (self.chart_id, self.segment_value_id, str(from_date), str(to_date)))
        annotations = [annot for annot in self._db.record]
        return annotations


    def get_non_date_chart_annotations(self):
        """
        get all annotations for chart
        """
#        where = ''
#        if self.data_set_instance_id:
#            self._db.Query("""SELECT report_data_set_chart_instance_id
#                            FROM report_data_set_chart_instance
#                            WHERE report_data_set_chart_id = %s
#                                AND report_data_set_instance_id = %s""", (self.chart_id, self.data_set_instance_id))
#            chart_instance = self._db.record[0]
#            
#            where = """OR (rdsca.annotation_scope = 'instance only' AND rdscai.report_data_set_chart_instance_id = %s)"""
#            params = (self.chart_id, self.segment_value_id, str(self.meas_time), chart_instance['report_data_set_chart_instance_id'])
#        else:
#            params = (self.chart_id, self.segment_value_id, str(self.meas_time))
#
#        sql = """SELECT rdsca.*, ua.*, user.username, rdscai.*
#                    FROM report_data_set_chart_annotation AS rdsca
#                        LEFT JOIN user_annotation AS ua ON
#                            rdsca.user_annotation_id = ua.user_annotation_id
#                        LEFT JOIN report_data_set_chart_annotation_instance AS rdscai ON
#                            rdsca.report_data_set_chart_annotation_id = rdscai.report_data_set_chart_annotation_id
#                        LEFT JOIN user ON
#                            user.user_id = ua.user_id
#                    WHERE rdsca.report_data_set_chart_id = %s
#                        AND ua.segment_value_id = %s
#                        AND (
#                                rdsca.annotation_scope = 'all instances'
#                                OR (rdsca.annotation_scope = 'until expires' AND rdsca.expiration_time >= %s)
#                            """ + where + """)
#                    GROUP BY rdsca.report_data_set_chart_annotation_id"""
#
#        self._db.Query(sql, params)
#        scopes = ''
#            scopes = """AND (
#                                (rdsca.annotation_scope = 'instance only' AND rdsca.start_time = %s)
#                                    OR (
#                                        rdsca.annotation_scope = 'until expires'
#                                        AND %s >= rdsca.start_time
#                                        AND %s <= rdsca.expiration_time
#                                        )
#                                )"""
#            meas_time = str(self.meas_time)
#            params.append(meas_time)
#            params.append(meas_time)
#            params.append(meas_time)
        meas_time = str(self.meas_time)
        self._db.Query("""SELECT rdsca.*, ua.*, user.username, rdscai.*,
                                rdsca.start_time AS instance_start_time,
                                rdsca.expiration_time AS instance_expiration_time
                    FROM report_data_set_chart_annotation AS rdsca
                        LEFT JOIN user_annotation AS ua ON
                            rdsca.user_annotation_id = ua.user_annotation_id
                        LEFT JOIN report_data_set_chart_annotation_instance AS rdscai ON
                            rdsca.report_data_set_chart_annotation_id = rdscai.report_data_set_chart_annotation_id
                        LEFT JOIN user ON
                            user.user_id = ua.user_id
                    WHERE
                        rdsca.report_data_set_chart_id = %s
                        AND ua.segment_value_id = %s
                        AND ua.annotation_interval = 'point'
                        AND (
                                (rdsca.annotation_scope = 'instance only' AND rdsca.start_time = %s)
                                    OR (
                                        rdsca.annotation_scope = 'until expires'
                                        AND %s >= rdsca.start_time
                                        AND %s <= rdsca.expiration_time
                                        )
                                )
                    GROUP BY rdsca.report_data_set_chart_annotation_id""" , (self.chart_id, self.segment_value_id, meas_time, meas_time, meas_time))
        annotations = [annot for annot in self._db.record]
        return annotations

    def _update_annotations_instances(self, report_data_set_chart_instance_id, annotations_map):
        #annots_to_update = filter(lambda annot: annot['annotation_scope']=='all instances' or (annot['annotation_scope']=='until expires' and annot['expiration_time'] >= self.meas_time), annotations_map)
        #for annot in annots_to_update:
        
        for annot in annotations_map:
            res = self._db.Query("""SELECT report_data_set_chart_annotation_instance_id FROM report_data_set_chart_annotation_instance
                        WHERE 
                            report_data_set_chart_annotation_id = %s AND 
                            report_data_set_chart_instance_id = %s""", 
                        (annot['report_data_set_chart_annotation_id'], report_data_set_chart_instance_id))
            if not res:
                self._db.Query("""INSERT INTO report_data_set_chart_annotation_instance
                        SET 
                            report_data_set_chart_annotation_id = %s,
                            report_data_set_chart_instance_id = %s""", 
                        (annot['report_data_set_chart_annotation_id'], report_data_set_chart_instance_id))
                         

    def get_data_from_orig(self):
        """
        split original data into clean data and formatted data
        """
        self.display_mask_ids = {'header': [], 'rows': []}
        self.data = {'header': [], 'rows': []}
        self.formatted_data = {'header': [], 'rows': []}
        
        self.data['header'] = [k['original_val'] for k in self.orig_data['header']]
        self.formatted_data['header'] = [k['formatted_val'] for k in self.orig_data['header']]

        self.display_mask_ids['header'] = [k['display_mask_id'] for k in self.orig_data['header']]
        
        
        for orig_row in self.orig_data['rows']:
            row = [v['original_val'] for v in orig_row]
            self.data['rows'].append(row)
            
            row = [v['formatted_val'] for v in orig_row]
            self.formatted_data['rows'].append(row)
            self.display_mask_ids['rows'].append([v['display_mask_id'] for v in orig_row])
    
    def prepare_row_fetched_data(self):
        """
        prepare fetched data for row values chart - remove non numeric columns, sum rows into one
        """
        rows = list()
        
        clean_rows = list()
        clean_header = list()
        
        if self.settings['y_axis_title']:
            title = self.settings['y_axis_title']
        else:
            title = self.settings['name']
            
        if not self.orig_data['rows']:
            self.data['header'] = [title]
            self.formatted_data['header'] = [title] 
            return
            
        
        rows.append(self.orig_data['rows'][0])
         
        header_len = len(rows[0])
        
        clean_header.append({'original_val': '', 'formatted_val': '', 'display_mask_id': 0})
        clean_rows.append([])
        clean_rows[0].append({'original_val': title, 'formatted_val': title, 'display_mask_id': 0})
        
        # sum all rows into one
        for i in range(1, len(self.orig_data['rows'])):
            for column_i in range(header_len):
                if isinstance(self.orig_data['rows'][i][column_i]['original_val'], int) or \
                        isinstance(self.orig_data['rows'][i][column_i]['original_val'], float) or \
                        isinstance(self.orig_data['rows'][i][column_i]['original_val'], long) or \
                        isinstance(self.orig_data['rows'][i][column_i]['original_val'], Decimal):
                    rows[0][column_i]['original_val'] += self.orig_data['rows'][i][column_i]['original_val']
                
        # format row values
        for column_i in range(header_len):
            #rows[0][column_i]['formatted_val'] = self.formatter.format_half(rows[0][column_i]['original_val'], rows[0][column_i]['display_mask_id'])
            rows[0][column_i]['formatted_val'] = self.formatter.format_full(rows[0][column_i]['original_val'], rows[0][column_i]['display_mask_id'])
        
        
        
        for i in range(header_len):
            if isinstance(rows[0][i]['original_val'], int) or \
                    isinstance(rows[0][i]['original_val'], float) or \
                    isinstance(rows[0][i]['original_val'], long) or \
                    isinstance(rows[0][i]['original_val'], Decimal):
                # add column to header 
                clean_header.append(self.orig_data['header'][i])
                clean_rows[0].append(rows[0][i])
        self.orig_data['header'] = clean_header
        self.orig_data['rows'] = clean_rows
        self.get_data_from_orig()
        
         
    def prepare_pie_row_fetched_data(self):
        """
        prepare fetched data for row values chart - remove non numeric columns, sum rows into one
        """
        if not self.orig_data['rows']:
            return
        clean_data = {'header':list(), 'rows': list()}
        
        clean_data['header'].append({'original_val': 'chart_by_report_data_set_column', 'formatted_val': 'chart_by_report_data_set_column', 'display_mask_id': 0})
        clean_data['header'].append({'original_val': 'sector_value_data_set_column', 'formatted_val': 'sector_value_data_set_column', 'display_mask_id': 0})
        
        self.settings['chart_by_report_data_set_column'] = 'chart_by_report_data_set_column'
        self.settings['sector_value_data_set_column'] = 'sector_value_data_set_column'
        
        rows = list()
        rows.append(self.orig_data['rows'][0])
        header_len = len(rows[0])
        
        # sum all rows into one
        for i in range(1, len(self.orig_data['rows'])):
            for column_i in range(header_len):
                rows[0][column_i]['original_val'] += self.orig_data['rows'][i][column_i]['original_val']  
        
        # format row values
        for column_i in range(header_len):
            #rows[0][column_i]['formatted_val'] = self.formatter.format_half(rows[0][column_i]['original_val'], rows[0][column_i]['display_mask_id'])
            rows[0][column_i]['formatted_val'] = self.formatter.format_full(rows[0][column_i]['original_val'], rows[0][column_i]['display_mask_id'])
        
        for i in range(header_len):
            if isinstance(rows[0][i]['original_val'], int) or \
                    isinstance(rows[0][i]['original_val'], float) or \
                    isinstance(rows[0][i]['original_val'], long) or \
                    isinstance(rows[0][i]['original_val'], Decimal):
                clean_data['rows'].append([self.orig_data['header'][i], rows[0][i]])
        self.orig_data = clean_data 
        self.get_data_from_orig()
        
    def get_thinned_x_axis(self, data):
        """
        Get top N largest x-axis values
        """
        if len(data['header']) <= self.settings['top_x_axis_values_to_chart']:
            return data
        modified_data = {'header': list(), 'formatted_header': list(), 'data': OrderedDict(), 'formatted_data': OrderedDict()}
        other = {'header': 'Other', 'formatted_header': 'Other', 'data': OrderedDict(), 'formatted_data': OrderedDict()}
        
        for item in data['data']:
            modified_data['data'][item] = list()
            other['data'][item] = 0
        for item in data['formatted_data']:
            modified_data['formatted_data'][item] = list()
            other['formatted_data'][item] = 0
        
        headers_values = dict()
        
        #for i in range(len(data['header'])):
        for i, itm in enumerate(data['header']):
            headers_values[i] = sum(data['data'][elem][i] for elem in data['data'])
        
        top_headers = nlargest(self.settings['top_x_axis_values_to_chart'], headers_values, key = lambda k: headers_values[k])
        
        #for i in range(len(data['header'])):
        for i, itm in enumerate(data['header']):
            if i in top_headers:
                modified_data['header'].append(data['header'][i])
                modified_data['formatted_header'].append(data['formatted_header'][i])
                for item in data['data']: 
                    modified_data['data'][item].append(data['data'][item][i])
                for item in data['formatted_data']: 
                    modified_data['formatted_data'][item].append(data['formatted_data'][item][i])
            
            elif self.settings['include_dropped_x_axis_values_as_other_ind'] == 'Y':
                for item in data['data']: 
                    other['data'][item] += data['data'][item][i]
        
        if self.settings['include_dropped_x_axis_values_as_other_ind'] == 'Y':
            modified_data['header'].append(other['header'])    
            modified_data['formatted_header'].append(other['formatted_header'])
            for item in data['data']: 
                modified_data['data'][item].append(other['data'][item])
                if item in self.settings['secondary_y_axis_values']:
                    modified_data['formatted_data'][self.clean_to_formatted[item]].append(self.formatter.format_full(other['data'][item], self.secondary_y_axis_display_mask_id))
                else:
                    modified_data['formatted_data'][self.clean_to_formatted[item]].append(self.formatter.format_full(other['data'][item], self.primary_y_axis_display_mask_id))
                     
            
        return modified_data
    """
    def strip_non_shown_columns(self):
        clean_header = [ header for header in self.orig_data['header']
                            if not header.has_key('show_column_in_table_display_ind') or 
                            (header.has_key('show_column_in_table_display_ind') and 
                             header['show_column_in_table_display_ind']=='Y')]
        
        clean_rows = list()
        for row in self.orig_data['rows']:
            clean_row = list()
            for i, header in enumerate(self.orig_data['header']):
                if not header.has_key('show_column_in_table_display_ind') or (header.has_key('show_column_in_table_display_ind') and header['show_column_in_table_display_ind']=='Y'):
                    clean_row.append(row[i])
                    
            clean_rows.append(clean_row)
                            
        
        self.orig_data['header'] = clean_header
        self.orig_data['rows'] = clean_rows
        self.get_data_from_orig()
    """
    
    
    def _store_charting_property(self, property_id, color_dataset_id, label, display_type, shape_id):
        """
        INSERT/UPDATE charting element property (color and shape)
        """
        if property_id:
            self._db.Query("""UPDATE report_data_set_chart_element_property
                                    SET chart_layout_dataset_id = %s,
                                        chart_data_point_shape_id = %s,
                                    last_updated_time = NOW()
                            WHERE report_data_set_chart_element_property_id = %s
                            """, (color_dataset_id, shape_id, property_id))
        else:
            self._db.Query("""INSERT INTO report_data_set_chart_element_property
                                    SET 
                                    chart_layout_dataset_id = %s,
                                    report_data_set_chart_id = %s, 
                                    chart_element_name = %s,
                                    chart_element_type = %s,
                                    chart_data_point_shape_id = %s,
                                    created_time = NOW()
                            """, (color_dataset_id,
                                  self.chart_id, 
                                  label,
                                  display_type,
                                  shape_id))


    def _is_x_axis_date(self, orig_header):
        if all([isinstance(itm, date) for itm in orig_header]):
            return True
        return False

    def get_date_chart_range_annotations(self, start_time, finish_time):
        range_annotations = list()
        self._db.Query("""SELECT rdsca.*, ua.*, user.username, rdscai.*,
                                rdsca.start_time AS instance_start_time,
                                rdsca.expiration_time AS instance_expiration_time
                    FROM report_data_set_chart_annotation AS rdsca
                        LEFT JOIN user_annotation AS ua ON
                            rdsca.user_annotation_id = ua.user_annotation_id
                        LEFT JOIN report_data_set_chart_annotation_instance AS rdscai ON
                            rdsca.report_data_set_chart_annotation_id = rdscai.report_data_set_chart_annotation_id
                        LEFT JOIN user ON
                            user.user_id = ua.user_id
                    WHERE
                        rdsca.report_data_set_chart_id = %s
                        AND ua.segment_value_id = %s
                        AND ua.annotation_interval = 'range'
                        AND ua.annotation_measurement_start_time <= %s
                        AND ua.annotation_measurement_finish_time >= %s

                    GROUP BY rdsca.report_data_set_chart_annotation_id
                    ORDER BY annotation_measurement_start_time""" , (self.chart_id, self.segment_value_id, finish_time, start_time))
        
        for ann in self._db.record:
            ann['measurement_value'] = ''
            ann['left_marker'] = True
            ann['right_marker'] = True
            ann['from_time'] = ann['annotation_measurement_start_time']
            ann['to_time'] = ann['annotation_measurement_finish_time']

            if ann['annotation_measurement_start_time'] < start_time:
                ann['left_marker'] = False
                ann['from_time'] = start_time


            if ann['annotation_measurement_finish_time'] > finish_time:
                ann['right_marker'] = False
                ann['to_time'] = finish_time
            range_annotations.append(ann)
#            from_ind = 0
#            for i, d in enumerate(header):
#                if d and ann['from_time'] <= d:
#                    from_ind = i
#                    break
#            to_ind = len(header) - 1
#            for i, d in enumerate(header):
#                to_ind = i
#                if d and ann['to_time'] < d:
#                    break
#            value = max(values[from_ind:to_ind])
#            if value is None:
#                value = max(values)
#            if not value is None:
#                ann['index'] = None
#                ann['value'] = value
#                range_annotations.append(ann)
        return range_annotations

        
    def index_annotations(self, all_annotations):
        """
        sort range/point annotations by date, type and metric order and add indexes
        """
        all_annotations.sort(key=itemgetter('order'))
        all_annotations.sort(key=itemgetter('is_range'))
        all_annotations.sort(key=itemgetter('time'))
        index = 0
        for i, annotation in enumerate(all_annotations):
            index += 1
            all_annotations[i]['index'] = index
        return all_annotations

    def parse_annotations(self, all_annotations, header_len, element):
        """
        divide all annotations list into range/point annotations lists
        """
        annotations = dict()
        range_annotations = list()
        point_annotations = [None for i in xrange(header_len + 1)]
        for annotation in all_annotations:
            if element == annotation['element']:
                if annotation['is_range']:
                    annotation['data']['index'] = annotation['index']
                    range_annotations.append(annotation['data'])
                    annotations[annotation['index']] = annotation['data']
                else:
                    point_annotations[annotation['header_index']] = annotation['index']
                    annotation['data']['index'] = annotation['index']
                    #if not annotations.has_key(annotation['index']):
                    if annotation['index'] not in annotations:
                        annotations[annotation['index']] = list()
                    annotations[annotation['index']].append(annotation['data'])

        return annotations, point_annotations, range_annotations
