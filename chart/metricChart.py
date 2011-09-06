#-*- coding: utf-8 -*-

from db.db_conn import DBManager
from sys import exit
import copy
from metric.rdp_simpl import Line, Vec2D    
from operator import itemgetter
from fontManager import FontManager
from math import ceil
from itertools import cycle
import time
import pprint


class MetricChart:
    settings = None
    metric_id = 0
    interval = None
    data = None
    chart_display_mode = ''
    type = ''
    formatter = None            

    def __init__(self, metric_id, interval, data, jfile, type, formatter):
        self.metric_id = metric_id
        self.interval = interval
        self.data = copy.deepcopy(data)
        self.metric_type = 'multi'
        self.jfile = jfile
        self._db = DBManager.get_query()
        self.chart_display_mode = ''
        self.type = type
        self.settings = self._get_chart_settings()
        self.formatter = formatter

    def _get_chart_settings(self):
        """ getting data for layout"""
        self._db.Query("""SELECT
                                dashboard_element.metric_show_max_ever_on_chart_ind,
                                dashboard_element.metric_show_min_ever_on_chart_ind,
                                dashboard_element.name,
                                dashboard_element.metric_display_label,

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
                                chart_layout.bar_shape,
                                chart_layout.bar_soft_lighting_direction,
                                chart_layout.border_color,
                                chart_layout.current_value_dot_color,
                                chart_layout.current_value_dot_size,
                                chart_layout.current_value_font_id,
                                chart_layout.current_value_font_size,
                                chart_layout.data_gap_line_type,
                                chart_layout.highlight_current_value_ind,
                                chart_layout.include_legend_ind,
                                chart_layout.include_x_axis_label_ind,
                                chart_layout.include_y_axis_label_ind,
                                chart_layout.legend_background_color,
                                chart_layout.legend_font_id,
                                chart_layout.legend_font_size,
                                chart_layout.line_data_point_dot_size,
                                chart_layout.line_width,
                                chart_layout.metric_bar_color,
                                chart_layout.metric_line_color,
                                
                                chart_layout.bar_gap,
                                chart_layout.bar_group_gap,
                                chart_layout.show_expired_zone_ind,
                                chart_layout.expired_zone_color,
                                
                                chart_layout.min_max_ever_font_id,
                                chart_layout.min_max_ever_font_size,
                                chart_layout.min_max_ever_line_color,
                                chart_layout.min_max_ever_line_type,
                                chart_layout.min_max_ever_line_width,

                                chart_layout.month_display_format,
                                chart_layout.plot_area_background_color,
                                chart_layout.plot_area_background_color_ind,
                                chart_layout.plot_area_horizontal_grid_color,
                                chart_layout.plot_area_vertical_grid_color,
                                chart_layout.show_line_data_points_ind,
                                chart_layout.show_plot_area_grid_ind,
                                chart_layout.title_font_color,
                                chart_layout.x_axis_label_font_color,
                                chart_layout.x_axis_label_font_id,
                                chart_layout.x_axis_label_font_size,
                                chart_layout.x_axis_tick_mark_position,
                                chart_layout.y_axis_font_color,
                                chart_layout.y_axis_font_id,
                                chart_layout.y_axis_font_size,
                                chart_layout.data_gap_line_type,

                                chart_layout.metric_moving_average_line_color,
                                chart_layout.moving_average_line_width,

                                chart_display_format.include_title_ind,
                                
                                chart_layout.metric_stoplight_good_range_start_color,
                                chart_layout.metric_stoplight_good_range_end_color,
                                chart_layout.metric_stoplight_bad_range_start_color,
                                chart_layout.metric_stoplight_bad_range_end_color,
                                
                                chart_layout.metric_stoplight_good_mark_color,
                                chart_layout.metric_stoplight_bad_mark_color,

                                dashboard_element.type,
                                dashboard_element.max_time_before_expired_sec,
                                dashboard_element.metric_start_y_axis_from_zero_ind AS start_y_axis_from_zero_ind,
                                chart_layout_id
                            FROM dashboard_element
                            LEFT JOIN chart_layout
                            ON chart_layout.layout_id = dashboard_element.chart_layout_id
                            LEFT JOIN chart_display_format
                            ON chart_display_format.chart_display_format_id = chart_layout.chart_display_format_id
                            WHERE element_id=%s
                        """, (self.metric_id, ))
        data = self._db.record[0]
        
        if data['type'] == 'metric':
            self.metric_type = 'single'

        #special settings for preview and thumbnail
        
        data['include_min_max_ever_ind'] = 'Y'
        data['include_compare_ind'] = 'Y'
        data['include_average_ind'] = 'Y'
        data['include_annotations_ind'] = 'Y'
        
        if self.type != 'large':
            data['include_min_max_ever_ind'] = 'N'
            data['include_compare_ind'] = 'N'
            data['include_average_ind'] = 'N'
            data['include_annotations_ind'] = 'N'
            if self.type == 'thumbnail':
                data['show_plot_area_grid_ind'] = 'N'
                data['show_line_data_points_ind'] = 'N'
                data['include_title_ind'] = 'N'
                data['include_legend_ind'] = 'N'
                data['include_x_axis_label_ind'] = 'N'
                data['include_y_axis_label_ind'] = 'N'
                if self.metric_type == 'single':
                    self._db.Query("""SELECT *
                                        FROM chart_display_format
                                        WHERE metric_thumbnail_display_format_ind = 'Y'""")
                else:
                    self._db.Query("""SELECT *
                                        FROM chart_display_format
                                        WHERE multi_metric_thumbnail_display_format_ind = 'Y'""")
                    
            elif self.type == 'preview':
                self._db.Query("""    SELECT *
                                    FROM chart_display_format
                                    WHERE preview_display_format_ind = 'Y'""")
            chart_display_format = self._db.record[0]
            for k, v in chart_display_format.iteritems():
                data[k] = v

        if data['bar_gap'] is None:
            data['bar_gap'] = 0
        if data['bar_group_gap'] is None:
            data['bar_group_gap'] = 0

        #if not data.has_key('chart_layout_id') or not data['chart_layout_id']:
        if 'chart_layout_id' not in data or not data['chart_layout_id']:
            raise Exception("missing chart_layout_id")

        if data['include_legend_ind'] == 'Y':
            self._db.Query("""SELECT * FROM font
                                WHERE font_id=%s""", (data['legend_font_id']))
            data['legend_font'] = self._db.record[0]

        if data['include_x_axis_label_ind'] == 'Y':
            # getting font for x axis label
            self._db.Query("""SELECT * FROM font
                                WHERE font_id=%s""", (data['x_axis_label_font_id']))
            data['x_axis_label_font'] = self._db.record[0]

        # getting font for y axis label
        if data['include_y_axis_label_ind'] == 'Y':
            self._db.Query("""SELECT * FROM font
                                WHERE font_id=%s""", (data['y_axis_font_id']))
            data['y_axis_label_font'] = self._db.record[0]

        # getting font for values
        self._db.Query("""SELECT * FROM font
                            WHERE font_id=%s""", (data['current_value_font_id']))
        data['current_value_font'] = self._db.record[0]

        # getting font for title
        if data['include_title_ind'] == 'Y':
            self._db.Query("""SELECT * FROM font
                                WHERE font_id=%s""", (data['title_font_id']))
            data['title_font'] = self._db.record[0]
            if data['title_font_color']:
                data['title_font_color'] = FontManager.get_db_color(data['title_font_color'])
            else:
                data['title_font_color'] = 0xffffff

        # getting styles for min max ever lines
        if data['include_min_max_ever_ind'] == 'Y':
            self._db.Query("""SELECT * FROM font
                                WHERE font_id=%s""", (data['min_max_ever_font_id']))
        
            data['min_max_ever_font'] = self._db.record[0]
        # emulate flip axes property for compatibility with report charts
        data['flip_x_and_y_axis'] = 'N'

        return data

    def generate_chart(self):
        # remove y axis title for preview single metric
        chart_data = self.get_modified_data(self.data)

        if self.type == 'preview':
            if self.metric_type == 'single':
                chart_data['y_axis_title_right'] = ''
                chart_data['y_axis_title_left'] = ''
            else:
                chart_data['y_axis_title_left'] = chart_data['preview_y_axis_title_left']
                chart_data['y_axis_title_right'] = chart_data['preview_y_axis_title_right']

        copy_props = ['x_axis_title', 'y_axis_title_left', 'y_axis_title_right', 'primary_y_axis_display_mask', 'secondary_y_axis_display_mask']
        for prop in copy_props:
            #if chart_data.has_key(prop):
            if prop in chart_data:
                self.settings[prop] = chart_data[prop]
            else:
                self.settings[prop] = None
        
        data = {'chart_data': chart_data, 'layout_data': self.settings}

        settings = {'type': self.type,
            'metric_type': self.metric_type,
            'show_stop_light': False,
            'show_std_deviation': False}

        from baseMetricChart import BaseMetricChart
        #chart = BaseMetricChart(self.metric_id, self.interval, data, self.jfile, self.formatter)
        #chart = BaseMetricChart(self.interval, data, self.jfile, self.formatter)
        chart = BaseMetricChart(data, self.jfile, settings)
        chart.set_interval(self.interval)
        chart.create_chart()
        if self.type == 'large':
            chart_map = chart.get_parsed_map()
            if data['chart_data']['show_stop_light']:
                settings['show_stop_light'] = True
                chart.set_settings(settings)
                chart.create_chart()

            if data['chart_data']['show_std_deviation']:
                settings['show_stop_light'] = False
                settings['show_std_deviation'] = True
                chart.set_settings(settings)
                chart.create_chart()

            del chart
            return chart_map
        del chart
        return None

    def get_modified_data(self, data):
        # expired zone settings
        data['expired_zone'] = dict()
        
        if data['expired_date'] and data['orig_header']:
            data['expired_zone']['start'] = data['expired_date']
            data['expired_zone']['end'] = data['orig_header'][-1]
            data['expired_zone']['color'] = FontManager.get_db_color(self.settings['expired_zone_color'])

        if self.metric_type == 'single':
            # get line point shape. for single metrics it's circle
            self._db.Query("""SELECT * FROM chart_data_point_shape WHERE chartdirector_shape_id = 'CircleShape'""")
            line_point_shapes = cycle([shape for shape in self._db.record])
        else:
            # get line point shapes for multi-metrics
            self._db.Query("""SELECT * FROM chart_data_point_shape ORDER BY charting_order""")
            line_point_shapes = cycle([shape for shape in self._db.record])

        for metric_id in data['rows']:
            if not self.chart_display_mode:
                self.chart_display_mode = data['rows'][metric_id]['data_settings']['display_type']
            elif self.chart_display_mode != data['rows'][metric_id]['data_settings']['display_type']:
                self.chart_display_mode = 'both'
            # set line point shape
            data['rows'][metric_id]['data_settings']['line_point_shape'] = None 
            if data['rows'][metric_id]['data_settings']['display_type'] == 'line':
                data['rows'][metric_id]['data_settings']['line_point_shape'] = line_point_shapes.next()

            if data['rows'][metric_id]['average_settings']:
                if self.settings['include_average_ind'] == 'N':
                    del(data['rows'][metric_id]['average_settings'])

            if data['compare_lines']:
                for compare_line in data['compare_lines']:
                    compare_setting = 'compare_settings_%s' % compare_line
                    if self.settings['include_compare_ind'] == 'N':
                        del(data['rows'][metric_id][compare_setting])
                    else:
                        data['rows'][metric_id][compare_setting]['axis_number'] = 1
                        data['rows'][metric_id][compare_setting]['display_type'] = 'line'
                        data['rows'][metric_id][compare_setting]['color'] = FontManager.get_db_color(data['rows'][metric_id][compare_setting]['line_color'])
                        data['rows'][metric_id][compare_setting]['line_style'] = data['rows'][metric_id]['data_settings']['line_style']
                if self.settings['include_compare_ind'] == 'N':
                    del(data['compare_lines'])

            if data['rows'][metric_id]['min_ever_settings']:
                if self.settings['include_min_max_ever_ind'] == 'N':
                    del(data['rows'][metric_id]['min_ever_settings'])
                else:
                    data['rows'][metric_id]['min_ever_settings']['axis_number'] = 1
                    data['rows'][metric_id]['min_ever_settings']['display_type'] = 'line'
                    data['rows'][metric_id]['min_ever_settings']['line_type'] = 'solid'
                    data['rows'][metric_id]['min_ever_settings']['color'] = FontManager.get_db_color(self.settings['min_max_ever_line_color'])
                    data['rows'][metric_id]['min_ever_settings']['line_width'] = self.settings['min_max_ever_line_width']
                    data['rows'][metric_id]['min_ever_settings']['line_style'] = 'jagged'

            if data['rows'][metric_id]['max_ever_settings']:
                if self.settings['include_min_max_ever_ind'] == 'N':
                    del(data['rows'][metric_id]['max_ever_settings'])
                else:
                    data['rows'][metric_id]['max_ever_settings']['axis_number'] = 1
                    data['rows'][metric_id]['max_ever_settings']['display_type'] = 'line'
                    data['rows'][metric_id]['max_ever_settings']['line_type'] = 'solid'
                    data['rows'][metric_id]['max_ever_settings']['color'] = FontManager.get_db_color(self.settings['min_max_ever_line_color'])
                    data['rows'][metric_id]['max_ever_settings']['line_width'] = self.settings['min_max_ever_line_width']
                    data['rows'][metric_id]['max_ever_settings']['line_style'] = 'jagged'
            
        #thinning data according to maximum data points number
        max_data_len = self.settings['max_line_data_points_to_chart']
        if self.chart_display_mode == 'line':
            max_data_len = self.settings['max_line_data_points_to_chart']
        elif self.chart_display_mode == 'bar':
            max_data_len = self.settings['max_bar_data_points_to_chart']
        elif self.chart_display_mode == 'both':
            max_data_len = min(self.settings['max_line_data_points_to_chart'], self.settings['max_bar_data_points_to_chart'])
        self.settings['is_reduced'] = False

        thinned_data = copy.deepcopy(data)

        if self.chart_display_mode == 'bar' or self.chart_display_mode == 'both':
            thinned_data = self.get_thinned_data_bar(thinned_data, max_data_len, data['thin_by_metric_id'])
        else:
            thinned_data = self.get_thinned_data(thinned_data, max_data_len, data['thin_by_metric_id'])

        return thinned_data

    def get_thinned_data_bar(self, data, max_data_len, element_id):
        data_points = len(data['orig_header'])
            
        if max_data_len >= data_points:
            return data
        
        #max_data_len = max_data_len - 2
        
        total_length = len(data['orig_header'])
        point_count_to_be_deleted = data_points - max_data_len
        #if point_count_to_be_deleted < max_data_len:
        #    return data
        if self.type != 'large':

            #for i in range(len(data['annotations'][element_id])):
            for i, itm in enumerate(data['point_annotations'][element_id]):
                data['point_annotations'][element_id][i] = False
            
        if point_count_to_be_deleted > max_data_len:
            el_num = int(ceil(data_points * 1.0 / max_data_len))
            included_elements = range(0, total_length, el_num)
        else:
             
            el_num = int(ceil(data_points * 1.0 / max_data_len))
            included_elements = range(0, total_length, el_num)
            
        if not (total_length - 1) in included_elements:
            included_elements.append(total_length - 1)

        def find(f, seq):
            # return first item in sequence where f(item) == True
            for it in (item for item in seq if item > f):
                return it
            return None

        self.settings['show_line_data_points_ind'] = 'N'
        self.settings['is_reduced'] = True
        if self.type != 'large':
            can_del = self._can_del_element2
        else:
            can_del = self._can_del_element
        
        #for i in range(len(data['rows'][element_id]['data_settings']['data'])):
        for i, itm in enumerate(data['rows'][element_id]['data_settings']['data']):
            #if not (i in set(included_elements)) and not can_del(data, element_id, i)
            if i not in included_elements and not can_del(data, element_id, i):
                a = find(i, included_elements)
                if a is None:
                    #print 'last'
                    del(included_elements[-1])
                    included_elements.append(i)
                elif not a:
                    #print 'first'
                    del(included_elements[0])
                    included_elements.insert(0, i)
                else:
                    next = included_elements.index(a)
                    prev = next - 1
                    next_point = included_elements[next]
                    prev_point = included_elements[prev]
                    del_next = False
                    del_prev = False
                    
                    if (next_point - i) < (i - prev_point):
                        del_next = True
                    elif (next_point - i) > (i - prev_point):
                        del_prev = True         
                    else:
                        del_next = True
                        del_prev = True

                    if self._can_del_element(data, element_id, prev_point) and del_prev:
                        del(included_elements[prev])
                        next -= 1
                        if self._can_del_element(data, element_id, next_point) and del_next:
                            del(included_elements[prev])

                    else:
                        if self._can_del_element(data, element_id, next_point)  and del_next:
                            del(included_elements[next])
                            
                    included_elements.insert(next, i)

        thinned_data = data
        
        thinned_data['orig_header'] = [v for i, v in enumerate(thinned_data['orig_header']) if i in included_elements]
        thinned_data['even_header'] = [v for i, v in enumerate(thinned_data['even_header']) if i in included_elements]
        thinned_data['x_scale_values'] = [v for i, v in enumerate(data['x_scale_values']) if i in included_elements]
        for metric_id in thinned_data['rows']:
            if data['show_stop_light']:
                thinned_data['rows'][metric_id]['stop_light']['bad'] = [v for i, v in enumerate(thinned_data['rows'][metric_id]['stop_light']['bad']) if i in included_elements]
                thinned_data['rows'][metric_id]['stop_light']['good'] = [v for i, v in enumerate(thinned_data['rows'][metric_id]['stop_light']['good']) if i in included_elements]
                thinned_data['rows'][metric_id]['stop_light']['deviation'] = [v for i, v in enumerate(thinned_data['rows'][metric_id]['stop_light']['deviation']) if i in included_elements]
            
            thinned_data['rows'][metric_id]['data_settings']['data'] = [v for i, v in enumerate(data['rows'][metric_id]['data_settings']['data']) if i in included_elements]
            thinned_data['point_annotations'][metric_id] = [v for i, v in enumerate(data['point_annotations'][metric_id]) if i in included_elements]
            
            #if thinned_data['rows'][metric_id].has_key('average_settings') and \
            #                thinned_data['rows'][metric_id]['average_settings']:
            if 'average_settings' in thinned_data['rows'][metric_id] and \
                            thinned_data['rows'][metric_id]['average_settings']:
                #if thinned_data['rows'][metric_id]['average_settings'].has_key('data'):
                if 'data' in thinned_data['rows'][metric_id]['average_settings']:
                    thinned_data['rows'][metric_id]['average_settings']['data'] = [v for i, v in enumerate(data['rows'][metric_id]['average_settings']['data']) if i in included_elements]
                #if thinned_data['rows'][metric_id]['average_settings'].has_key('std_deviation_data'):
                if 'std_deviation_data' in thinned_data['rows'][metric_id]['average_settings']:
                    thinned_data['rows'][metric_id]['average_settings']['std_deviation_data'] = [v for i, v in enumerate(data['rows'][metric_id]['average_settings']['std_deviation_data']) if i in included_elements]

            #if thinned_data.has_key('compare_lines') and thinned_data['compare_lines']:
            if 'compare_lines' in thinned_data and thinned_data['compare_lines']:
                for compare_line in thinned_data['compare_lines']:
                    compare_setting = 'compare_settings_%s' % compare_line
                    thinned_data['rows'][metric_id][compare_setting]['data'] = [v for i, v in enumerate(data['rows'][metric_id][compare_setting]['data']) if i in included_elements]
        return thinned_data

    def get_thinned_data(self, data, max_data_len, element_id):
        if not max_data_len:
            return data
        
        data_len = len(data['rows'][element_id]['data_settings']['data'])
         
        if data_len <= max_data_len:
            return data
        else:
            to_reduce = data_len - max_data_len

        # let's remove empty values
        keys = [i for i in xrange(data_len)]
        curr_key = 0
        
        thinned_data = copy.deepcopy(data)
        can_del_none = 0
        for k, v in enumerate(data['rows'][element_id]['data_settings']['data']):
            if v is None and self._can_del_element(thinned_data, element_id, curr_key) and k < (data_len - 1):
                can_del_none += 1
        
        if can_del_none > to_reduce:
            to_left_step, rest = divmod(can_del_none, (can_del_none - to_reduce))
            if rest:
                to_left_step += 1
        else:
            to_left_step = 1
        
        curr_step = 0 
        for k, v in enumerate(data['rows'][element_id]['data_settings']['data']):
            if v is None and self._can_del_element(thinned_data, element_id, curr_key) and k < (data_len - 1):
                can_del = True
            else:
                can_del = False
            if can_del and curr_step != to_left_step:
                self._del_element(element_id, thinned_data, keys, curr_key)
                to_reduce -= 1
            else:
                curr_key += 1
            if can_del:
                curr_step += 1
                if curr_step == to_left_step:
                    curr_step = 0
            if not to_reduce:
                break

        if not to_reduce:
            return thinned_data
        # hide data points
        self.settings['show_line_data_points_ind'] = 'N'
        self.settings['is_reduced'] = True
        
        chart_line = Line([Vec2D(thinned_data['rows'][element_id]['data_settings']['data'][i], thinned_data['x_axis'][i], keys[i]) for i, v in enumerate(thinned_data['x_axis']) if thinned_data['rows'][element_id]['data_settings']['data'][i] is not None])
        
        epsilons = list()

        thinned_data_len = len(thinned_data['rows'][element_id]['data_settings']['data'])

        chart_line.simplify(0, epsilons, thinned_data_len)

        epsilons_sorted = sorted(epsilons, key=itemgetter('dist'))
#        """
#        import operator
#        epsilons_sorted = sorted(epsilons.iteritems(), key=operator.itemgetter(1))
#        #print "sort. it took", time.time() - start, "seconds."
#        """
        epsilons_sorted_len = len(epsilons_sorted)
        
        curr_eps_i = 0
        removed = list()
#        """
#        for eps in epsilons_sorted:
#            eps_key = eps[0]
#            if self._can_del_element(thinned_data, element_id, keys.index(eps_key)):
#                self._del_element(element_id, thinned_data, keys, keys.index(eps_key))
#
#                #epsilons_sorted = filter(lambda eps: eps['id'] != eps_key, epsilons_sorted)
#                #epsilons_sorted_len = len(epsilons_sorted)
#                to_reduce = to_reduce - 1
#            if not to_reduce:
#                break
#        #print 'after points count',len(thinned_data['x_axis'])
#
#        """
        while curr_eps_i < epsilons_sorted_len and to_reduce:
            eps_key = epsilons_sorted[curr_eps_i]['id']
            if not eps_key in removed and self._can_del_element(thinned_data, element_id, keys.index(eps_key)):
                self._del_element(element_id, thinned_data, keys, keys.index(eps_key))
                removed.append(eps_key)
                #epsilons_sorted = filter(lambda eps: eps['id'] != eps_key, epsilons_sorted)
                #epsilons_sorted_len = len(epsilons_sorted)
                to_reduce -= 1
            else:
                curr_eps_i += 1
            
        #print "remove points. it took", time.time() - start, "seconds."
        return thinned_data
    
    def _can_del_element(self, data, element_id, key):
        can_del = True
        #if data['rows'][element_id]['data_settings']['data'][key] is not None and ( 
        if (data['point_annotations'][element_id][key] or
                key == (len(data['rows'][element_id]['data_settings']['data']) - 1) or
                key == 0 or
                (data[element_id]['min_for_interval'] and data['rows'][element_id]['data_settings']['data'][key] == data[element_id]['min_for_interval']['value']) or
                (data[element_id]['max_for_interval'] and data['rows'][element_id]['data_settings']['data'][key] == data[element_id]['max_for_interval']['value'])):
            can_del = False
        for metric_id in data['rows']:
            if data['point_annotations'][metric_id][key] or \
                     (data[metric_id]['min_for_interval'] and data['rows'][metric_id]['data_settings']['data'][key] == data[metric_id]['min_for_interval']['value']) or \
                     (data[metric_id]['max_for_interval'] and data['rows'][metric_id]['data_settings']['data'][key] == data[metric_id]['max_for_interval']['value']):
                can_del = False
        return can_del

    def _can_del_element2(self, data, element_id, key):
        can_del = True
        if data['rows'][element_id]['data_settings']['data'][key] is not None and ( 
            data['rows'][element_id]['data_settings']['data'][key] == data[element_id]['min_for_interval']['value'] or
            data['rows'][element_id]['data_settings']['data'][key] == data[element_id]['max_for_interval']['value']):
            can_del = False
        for metric_id in data['rows']:
            if (data[metric_id]['min_for_interval'] and data['rows'][metric_id]['data_settings']['data'][key] == data[metric_id]['min_for_interval']['value']) or \
                 (data[metric_id]['max_for_interval'] and data['rows'][metric_id]['data_settings']['data'][key] == data[metric_id]['max_for_interval']['value']):
                can_del = False
        return can_del

    def _del_element(self, element_id, data, keys, key):
        #remove key
        del(keys[key])
        
        #remove data from all metrics
        for metric_id in data['rows']:
            #if data['rows'][metric_id]['data_settings']['display_type'] != 'bar':
            #remove metric value
            del(data['rows'][metric_id]['data_settings']['data'][key])
            
            #remove stop light values
            if data['show_stop_light']:
                del(data['rows'][metric_id]['stop_light']['bad'][key])
                del(data['rows'][metric_id]['stop_light']['good'][key])
                del(data['rows'][metric_id]['stop_light']['deviation'][key])

            #remove annotation marker
            del(data['point_annotations'][metric_id][key])
            
            #remove average line value
            #if data['rows'][metric_id].has_key('average_settings') and \
            if 'average_settings' in data['rows'][metric_id] and \
                    data['rows'][metric_id]['average_settings']:
                #if data['rows'][metric_id]['average_settings'].has_key('data'):
                if 'data' in data['rows'][metric_id]['average_settings']:
                    del(data['rows'][metric_id]['average_settings']['data'][key])
                #if data['rows'][metric_id]['average_settings'].has_key('std_deviation_data'):
                if 'std_deviation_data' in data['rows'][metric_id]['average_settings']:
                    del(data['rows'][metric_id]['average_settings']['std_deviation_data'][key])
            
            #remove compare line value
            #if data.has_key('compare_lines') and data['compare_lines']:
            if 'compare_lines' in data and data['compare_lines']:
                for compare_line in data['compare_lines']:
                    compare_setting = 'compare_settings_%s' % compare_line
                    del(data['rows'][element_id][compare_setting]['data'][key])
        #remove header value
        del(data['even_header'][key])
        #remove header value
        del(data['orig_header'][key])
        #remove x axis value
        del(data['x_axis'][key])

    def get_trim_empty(self, data, max_data_len, element_id):        
        if not max_data_len:
            return data
        
        data_len = len(data['rows'][element_id]['data_settings']['data'])
         
        if data_len <= max_data_len:
            return data
        else:
            to_reduce = data_len - max_data_len
        
        # let's remove empty values
        keys = [i for i in xrange(data_len)]
        curr_key = 0
        
        thinned_data = copy.deepcopy(data)
        can_del_none = 0
        for k, v in enumerate(data['rows'][element_id]['data_settings']['data']):
            if v is None and self._can_del_element(thinned_data, element_id, curr_key) and k < (data_len - 1):
                can_del_none += 1
        
        if can_del_none > to_reduce:
            to_left_step, rest = divmod(can_del_none, (can_del_none - to_reduce))
            if rest:
                to_left_step += 1
        else:
            to_left_step = 1
        
        curr_step = 0 
        for k, v in enumerate(data['rows'][element_id]['data_settings']['data']):
            if v is None and self._can_del_element(thinned_data, element_id, curr_key) and k < (data_len - 1):
                can_del = True
            else:
                can_del = False
            if can_del and curr_step != to_left_step:
                self._del_element(element_id, thinned_data, keys, curr_key)
                to_reduce -= 1
            else:
                curr_key += 1
            if can_del:
                curr_step += 1
                if curr_step == to_left_step:
                    curr_step = 0
            if not to_reduce:
                break                
        return thinned_data
