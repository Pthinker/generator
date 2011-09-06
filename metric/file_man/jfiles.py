# -*- coding: utf-8 -*-

import os
from simplejson.ordered_dict import OrderedDict
import simplejson
import shutil
from metric.conf import ConfigReader
import marshal
from glob import glob
import sys


class JAbstractFile:
    """
    Abstract common class for inheritance
    """
    _element_id = None
    _full_path = None
    _file_owner_uid = None
    _file_owner_gid = None
    _preview_root = None
    _thumbnail_root = None
    _config = None


    def __init__(self, path, element_id):
        self._element_id = element_id
        self._full_path = path
        self._init_config()
        if path is not None:
            self._check_dir(self._full_path)
            self._check_dir(self._preview_root)
            self._check_dir(self._thumbnail_root)

    def _init_config(self):
        self._config = ConfigReader()
        self._file_owner_uid = self._config.file_owner_uid
        self._file_owner_gid = self._config.file_owner_gid
        self._preview_root = self._config.preview_root
        self._thumbnail_root = self._config.thumbnail_root

    def _check_dir(self, full_path):
        """
        check specified directory existing, write permissions
        """
        if not os.path.isdir(full_path):
            try:
                os.makedirs(full_path)
            except OSError, e:
                raise Exception("cannot create dir %s. %s" % (full_path, e))
            try:
                os.chmod(full_path, 0777)
            except OSError, e:
                raise Exception("cannot change permissions of %s. %s" % (full_path, e))
            try:
                os.chown(full_path, self._file_owner_uid, self._file_owner_gid)
            except OSError, e:
                raise Exception("cannot change owner of %s. %s" % (full_path, e))
        else:
            try:
                os.chmod(full_path, 0777)
            except OSError, e:
                #raise Exception("cannot change permissions of %s. %s" % (full_path, e))
                pass
            try:
                os.chown(full_path, self._file_owner_uid, self._file_owner_gid)
            except OSError, e:
                #raise Exception("cannot change owner of %s. %s" % (full_path, e))
                pass


    def _write_file(self, full_file_name, content):
        """
        write data to file in specified path
        """
        try:
            json_file = open(full_file_name, 'w+')
            try:
                json_file.write(content)
            except IOError, e:
                raise Exception("Cannot write to file: %s. %s"%(full_file_name, e))
            finally:
                json_file.close()
        except IOError, e:
            raise Exception("Cannot create file: %s. %s"%(full_file_name, e))
        try:
            os.chmod(full_file_name, 0777)
        except OSError, e:
            raise Exception("Cannot change permissions of %s. %s" % (full_file_name, e))
        try:
            os.chown(full_file_name, self._file_owner_uid, self._file_owner_gid)
        except OSError, e:
            raise Exception("Cannot change owner of %s. %s" % (full_file_name, e))
    
    def write_file(self, file_name, content):
        """
        write data to file in current default path
        """
        self._write_file(os.path.join(self._full_path, file_name), content)
    
    def change_perm(self, full_file_name):
        """
        set read/write permissions for file
        """
        try:
            os.chmod(full_file_name, 0777)
        except OSError, e:
            raise Exception("Cannot change permissions of %s. %s" % (full_file_name, e))
        try:
            os.chown(full_file_name, self._file_owner_uid, self._file_owner_gid)
        except OSError, e:
            raise Exception("Cannot change owner of %s. %s" % (full_file_name, e))


class JChartFile(JAbstractFile):
    """
    Class for setting permission
    """
    def __init__(self):
        JAbstractFile.__init__(self, None, None)
        self._init_config()

    def change_perm(self, full_file_name):
        try:
            os.chmod(full_file_name, 0777)
        except OSError, e:
            raise Exception("Cannot change permissions of %s. %s" % (full_file_name, e))
        try:
            os.chown(full_file_name, self._file_owner_uid, self._file_owner_gid)
        except OSError, e:
            raise Exception("Cannot change owner of %s. %s" % (full_file_name, e))


class JElementFile(JAbstractFile):
    """
    Managing json files for metric elements
    """
    segment_value_id = 0
    segment_value = None
    segment = None
    _element = None
    _full_path = None

    def __init__(self, path, element_id):
        JAbstractFile.__init__(self, os.path.join(path, str(element_id)), element_id)

    def _get_sub_element_name(self, name, segment_value):
        """
        set element name using segment value
        """
        if self.segment and segment_value:
            if self.segment['qualify_element_name_ind'] == 'Y':
                name_parts = []
                if self.segment['segment_value_placement'] == 'before element name':
                    name_parts.append(self.segment['segment_value_prefix'])
                    name_parts.append(segment_value['value_display_name'])
                    name_parts.append(self.segment['segment_value_suffix'])
                    name_parts.append(name)
                elif self.segment['segment_value_placement'] == 'after element name':
                    name_parts.append(name)
                    name_parts.append(self.segment['segment_value_prefix'])
                    name_parts.append(segment_value['value_display_name'])
                    name_parts.append(self.segment['segment_value_suffix'])
                name_parts = [unicode(name_part).strip() for name_part in name_parts if name_part]
                name = ' '.join(name_parts)
        return name

    def set_segment_value(self, segment_value):
        """
        set segment value and segment value id
        """
        self.segment_value_id = 0
        self.segment_value = segment_value
        if segment_value:
            self.segment_value_id = segment_value['segment_value_id']

    def set_segment(self, segment):
        """
        set segment info
        """
        self.segment = segment

    def _remove_file(self, file_name):
        """
        remove file from element directory
        """
        full_file_path = os.path.join(self._full_path, file_name)
        return self._remove_custom_file(full_file_path)

    def _remove_custom_file(self, full_file_path):
        """
        remove file from by full file name
        """
        try:
            if os.path.isfile(full_file_path):
                os.unlink(full_file_path)
                return True
            else:
                return False
        except OSError:
            pass
        return False

    def purge_preview_files(self, segment_value_id):
        """
        delete preview and thumbnails files for specified segment
        """
        self.segment_value_id = segment_value_id
        self._remove_custom_file(self.get_thumbnail_file_name())
        self._remove_custom_file(self.get_preview_file_name())
        self._remove_custom_file(self.get_resized_preview_file_name())

    def purge_files(self, segments):
        """
        delete all files in report directory
        """
        shutil.rmtree(self._full_path)

        for segment_id in segments:
            self.purge_preview_files(segment_id)

    def get_thumbnail_file_name(self):
        """
        abstract  method
        """
        pass
    def get_preview_file_name(self):
        """
        abstract  method
        """
        pass
    def get_resized_preview_file_name(self):
        """
        abstract  method
        """
        pass

class JMetricFile(JElementFile):
    """
    Managing json files for metric elements
    """

    def __init__(self, path, element_id):
        #JAbstractFile.__init__(self, os.path.join(path, str(element_id)), element_id)
        JElementFile.__init__(self, path, element_id)

    def purge_segment_file(self, segment_value_id):
        """
        delete specified segment meta file
        """
        self._remove_file("metric_%s_%s.json" % (self._element_id, segment_value_id))

    def purge_interval_files(self, charting_interval_id, segment_value_id):
        """
        delete specified interval files in metric directory
        """
        self._remove_file("metric_%s_%s_%s.json" % (self._element_id, segment_value_id, charting_interval_id))

        self._remove_file("metric_%s_%s_%s.png" % (self._element_id, segment_value_id, charting_interval_id))
        self._remove_file("metric_%s_%s_%s_stoplight.png" % (self._element_id, segment_value_id, charting_interval_id))
        self._remove_file("metric_%s_%s_%s_std_dev.png" % (self._element_id, segment_value_id, charting_interval_id))

    def set_data(self, element):
        """
        set metric data
        """
        self._element = element

    def make_meta(self, data, charting_intervals, available_intervals, drill_to, related, metrics, segment_values, available_views):
        """
        create meta file
        """
        json_data = OrderedDict()

        json_data["chart_element_id"] = self._element_id
        json_data["segment_value_id"] = self.segment_value_id
        json_data["segment_name"] = ''
        if self.segment:
            json_data["segment_name"] = self.segment['name'] 
            
        json_data["chart_name"] = self._get_sub_element_name(self._element['name'], self.segment_value)
        json_data["chart_descr"] = self._element['description']
        json_data["chart_last_updated"] = str(data['last_updated_time'])
        json_data["chart_category"] = self._element['category']
        json_data["chart_topic"] = self._element['topic_name']
        json_data["chart_business_owner"] = self._element['business_owner']
        json_data["chart_tech_owner"] = self._element['technical_owner']

        # index charting interval
        json_data["chart_index_interval_id"] = 0
        if charting_intervals:
            chart_index_interval_id = 0
            for ci in charting_intervals:
                if ci['index_charting_interval_ind'] == 'Y':
                    chart_index_interval_id = ci['charting_interval_id']
                    break
            if chart_index_interval_id:
                json_data["chart_index_interval_id"] = chart_index_interval_id
            else:
                json_data["chart_index_interval_id"] = charting_intervals[0]['charting_interval_id']

        json_data['chart_meas_interval_id'] = self._element['measurement_interval_id']
        json_data["chart_meas_interval_button_name"] = self._element['measurement_interval_button_name']
        json_data['chart_moving_average_interval'] = '' 

        if self._element['metric_moving_average_interval']:
            json_data['chart_moving_average_interval'] = self._element['metric_moving_average_interval']

        json_data['chart_intervals'] = []

        for charting_interval in charting_intervals:
            charting_interval_data = OrderedDict()
            charting_interval_data['interval_id'] = charting_interval['charting_interval_id']
            charting_interval_data['interval_name'] = charting_interval['charting_interval_button_name']
            charting_interval_data['link_to_element'] = self._element_id
            charting_interval_data['link_to_element_instance'] = charting_interval['charting_interval_id']
            charting_interval_data['link_to_segment'] = self.segment_value_id
            json_data['chart_intervals'].append(charting_interval_data)

        json_data["segments"] = []

        if segment_values:
            for segment_value in segment_values:
                json_data["segments"].append({'segment_value_id': segment_value['segment_value_id'], 'segment': segment_value['value_display_name']})
        elif not self.segment_value_id:
            json_data["segments"].append({'segment_value_id':0, 'segment': ''})

        json_data["metrics"] = []

        for metric in metrics:
            metric_value = OrderedDict()
            metric_value['metric_element_id'] = metric['metric_element_id']
            metric_value['segment_value_id'] = metric['segment_value_id']
            #metric_value['metric_name'] = self._get_sub_element_name(metric['metric_name'], self.segment_value)
            #metric_value['metric_name'] = self._get_sub_element_name(metric['metric_name'], metric['segment_value'])
            metric_value['metric_name'] = metric['metric_name']

            if metric['segment_value']:
                metric_value['segment_value_display_name'] = metric['segment_value']['value_display_name']
            else:
                metric_value['segment_value_display_name'] = ''

            metric_value['pure_metric_name'] =  metric['pure_metric_name']
            metric_value['metric_descr'] = metric['metric_descr']
            metric_value['metric_dashboard_category'] = metric['metric_dashboard_category']
            metric_value['metric_primary_topic'] = metric['metric_primary_topic']
            metric_value['metric_business_owner'] = metric['metric_business_owner']
            metric_value['metric_tech_owner'] = metric['metric_tech_owner']
            metric_value['metric_interval_id'] = metric['metric_interval_id']
            metric_value['metric_moving_average_interval'] = metric['metric_moving_average_interval']
            metric_value['curr_value'] = metric['curr_value']
            metric_value['min_value'] = metric['min_value']
            metric_value['max_value'] = metric['max_value']
            metric_value['min_reached_on'] = metric['min_reached_on']
            metric_value['max_reached_on'] = metric['max_reached_on']
            metric_value['compare_to'] =  metric['compare_to']
            json_data["metrics"].append(metric_value)

        json_data["meas_intervals"] = []

        for available_interval in available_intervals:
            interval = OrderedDict()
            interval['interval_id'] = available_interval['measurement_interval_id']
            interval['interval_name'] = available_interval['measurement_interval_button_name']
            interval['link_to_segment'] = self.segment_value_id
            interval['interval_time'] = ''
            interval['link_to_element'] = available_interval['element_id']
            interval['link_to_element_instance'] = ''
            json_data["meas_intervals"].append(interval)

        json_data["drill_data"] = []

        for drill_report in drill_to:
            drill = OrderedDict()
            drill['drill_id'] = drill_report['metric_drill_to_report_id']
            drill['report_id'] = drill_report['report_element_id']
            drill['sv_id'] = self.segment_value_id
            drill['instance_id'] = ''
            drill['metric_id'] = self._element_id
            drill['Label'] = drill_report['metric_drill_to_label']
            drill['earliest_point'] = ''
            json_data["drill_data"].append(drill)

        json_data["related"] = []

        for related_element in related:
            related_item = OrderedDict()
            related_item['element_id'] = related_element['element_id']
            related_item['instance_id'] = ''
            related_item['sv_id'] = self.segment_value_id
            related_item['element_type'] = related_element['type']
            related_item['element_name'] = related_element['name']
            related_item['element_desc'] = related_element['description']
            related_item['external_url'] = ''
            if related_element['type'] == 'external report' or related_element['type'] == 'external content':
                related_item['external_url'] = related_element['external_report_url']
            json_data["related"].append(related_item)
        
        json_data["available_views"] = available_views 
        
        file_name = "metric_%s_%s.json" % (self._element_id, self.segment_value_id)

        content = simplejson.dumps(json_data, indent=4, check_circular=False)
        self.write_file(file_name, content)

    def make_chart_interval(self, map, charting_interval_id, charting_interval, annotations):
        """
        create chart interval file
        """
        json_data = OrderedDict()
        json_data["chart_element_id"] = self._element_id
        json_data["chart_name"] = self._get_sub_element_name(self._element['name'], self.segment_value)
        json_data["interval_id"] = charting_interval_id
        json_data["interval_name"] = charting_interval['charting_interval_button_name']
        json_data["map"] = map
        json_data["annotations"] = []

        for annot_element in annotations:
            annot_item = OrderedDict()
            annot_item['index'] = annot_element['index']
            annot_item['ann_id'] = annot_element['user_annotation_id']
            annot_item['metric_id'] = annot_element['metric_id']
            annot_item['metric_instance_id'] = annot_element['metric_instance_id']
            annot_item['ann_time'] = str(annot_element['annotation_time'])
            annot_item['ann_text'] = annot_element['annotation_text']
            annot_item['ann_by_id'] = annot_element['user_id']
            annot_item['ann_by_name'] = annot_element['username']
            annot_item['meas_value'] = annot_element['measurement_value']
            annot_item['meas_index'] = annot_element['meas_index']
            annot_item['raw_meas_index'] = str(annot_element['raw_meas_index'])
            annot_item['shape'] = annot_element['shape']
            annot_item['coords'] = annot_element['coords']
            annot_item['annotation_interval'] = annot_element['annotation_interval']
            annot_item['start_time'] = annot_element['start_time']
            annot_item['finish_time'] = annot_element['finish_time']
            annot_item['raw_start_time'] = str(annot_element['raw_start_time'])
            annot_item['raw_finish_time'] = str(annot_element['raw_finish_time'])
            json_data["annotations"].append(annot_item)

        file_name = "metric_%s_%s_%s.json" % (self._element_id, self.segment_value_id, charting_interval_id)
        content = simplejson.dumps(json_data, indent=4, check_circular=False)
        self.write_file(file_name, content)

    def get_chart_file_name(self, interval, suffix):
        """
        return chart file name
        """
        file_name = os.path.join(self._full_path, "metric_%s_%s_%s%s.png" % (self._element_id, self.segment_value_id, interval, suffix))
        return file_name

    def get_thumbnail_file_name(self):
        """
        return thumbnail file name
        """
        file_name = os.path.join(self._thumbnail_root, "%s_%s_thumbnail.png" % (self._element_id, self.segment_value_id))
        return file_name

    def get_preview_file_name(self):
        """
        return preview file name
        """
        file_name = os.path.join(self._preview_root, "%s_%s_preview.png" % (self._element_id, self.segment_value_id))
        return file_name

    def get_resized_preview_file_name(self):
        """
        return resized preview file name
        """
        file_name = os.path.join(self._preview_root, "%s_%s_resized_preview.png" % (self._element_id, self.segment_value_id))
        return file_name

class JFile(JElementFile):
    """
    Managing json files for internal reports
    """
    meas_time = None

    def __init__(self, path, element_id, element):
        #JAbstractFile.__init__(self, os.path.join(path, str(element_id)), element_id)
        JElementFile.__init__(self, path, element_id)
        self._element = element

    def set_meas_time(self, meas_time):
        """
        set current measurement time
        """
        self.meas_time = meas_time

    def _get_data_set_file_name(self, instance_id):
        """
        Returns json data set file name
        """
        if self._element['report_save_historical_instances_ind'] == 'Y':
            file_name = "report_dataset_data_%s_%s.json" % (self._element_id, instance_id)
        else:
            file_name = "report_dataset_data_%s_%s_current.json" % (self._element_id, self.segment_value_id)
        return file_name

    def purge_chart_files(self, chart_id, instance_ids, segment_value_id):
        """
        delete chart instance files (.json and .png)
        """
        for instance_id in instance_ids:
            self._remove_file("dataset_chart_%s_%s_%s.png" % (self._element_id, chart_id, instance_id))
            self._remove_file("dataset_chart_%s_%s_%s.json" % (self._element_id, chart_id, instance_id))

#        # lets find chart files with non-existing instance ids with wildcard
#        img_files = glob(os.path.join(self._full_path, "dataset_chart_%s_%s_*.png" % (self._element_id, chart_id)))
#        for file in img_files:
#            #    self._remove_file(file)
#
#        json_files = glob(os.path.join(self._full_path, "dataset_chart_%s_%s_*.json" % (self._element_id, chart_id)))
#        for file in json_files:
#            #    self._remove_file(file)

        # delete chart current instance files (.json and .png)
        self._remove_file("dataset_chart_%s_%s_%s_current.png" % (self._element_id, chart_id, segment_value_id))
        self._remove_file("dataset_chart_%s_%s_%s_current.json" % (self._element_id, chart_id, segment_value_id))

    
    def purge_pivot_files(self, pivot_id, instance_ids, segment_value_id):
        """
        delete pivot instance file
        """
        for instance_id in instance_ids:
            self._remove_file("report_dataset_pivot_data_%s_%s_%s.json" % (self._element_id, pivot_id, instance_id))

#        # lets find pivot files with non-existing instance ids with wildcard
#        report_files = glob(os.path.join(self._full_path, "report_dataset_pivot_data_%s_%s_*.json" % (self._element_id, pivot_id)))
#        for file in report_files:
#        #    self._remove_file(file)

        # delete pivot current instance file
        self._remove_file("dataset_pivot_data_%s_%s_%s_current.json" % (self._element_id, pivot_id, segment_value_id))

    def purge_dataset_files(self, instance_ids, segment_value_id):
        """
        delete data set instance file
        """
        for instance_id in instance_ids:
            self._remove_file("report_dataset_data_%s_%s.json" % (self._element_id, instance_id))
            self._remove_file("generated_data_set_%s.dmp" % instance_id)

        # delete data set current instance file, and metadata files
        self._remove_file("generated_data_%s.dmp" % segment_value_id)
        self._remove_file("report_dataset_data_%s_%s_current.json" % (self._element_id, segment_value_id))
        self._remove_file("report_metadata_%s_%s_current.json" % (self._element_id, segment_value_id))

    def create_data_set(self, instance_id, instance, meas_time):
        """
        create data set json file
        """
        json_data = OrderedDict()
        json_data["element_id"] = self._element_id
        #json_data["instance_id"] = instance_id
        json_data["element_type"] = "dataset"
        json_data['measurement_time'] = str(meas_time)

        json_data["element_name"] = self._get_sub_element_name(self._element['name'], self.segment_value)
        json_data["element_desc"] = self._element['description']

        json_data["data"] = OrderedDict()
        json_data['data']['header'] = instance['header']
        json_data['data']['rows'] = instance['rows']

        content = simplejson.dumps(json_data, indent=4, check_circular=False)
        file_name = self._get_data_set_file_name(instance_id)
        self.write_file(file_name, content)

    def _get_pivot_file_name(self, report_data_set_pivot_id, data_set_instance_id):
        """
        return pivot file name
        """
        if self._element['report_save_historical_instances_ind'] == 'Y':
            file_name = "report_dataset_pivot_data_%s_%s_%s.json" % (self._element_id, report_data_set_pivot_id, data_set_instance_id)
        else:
            file_name = "dataset_pivot_data_%s_%s_%s_current.json" % (self._element_id, report_data_set_pivot_id, self.segment_value_id)

        return file_name

    def create_pivot(self, pivot, data_set_instance_id, instance):
        """
        create pivot json file
        """
        json_data = OrderedDict()
        # dashboard element id
        json_data["element_id"] = self._element_id
        json_data["element_type"] = "pivot"
        json_data["element_name"] = self._get_sub_element_name(pivot['name'], self.segment_value)
        json_data["element_desc"] = ''

        json_data["data"] = OrderedDict()
        json_data['data']['header'] = instance['header']
        json_data['data']['rows'] = instance['rows']
        content = simplejson.dumps(json_data, indent=4, check_circular=False)
        file_name = self._get_pivot_file_name(pivot['report_data_set_pivot_id'], data_set_instance_id)
        self.write_file(file_name, content)

    def make_current_saved_data_set(self, data_set_instance_id):
        """
        copy last saved data set file to current data set file
        """
        src = os.path.join(self._full_path, "generated_data_set_%s.dmp" % data_set_instance_id)
        dst = os.path.join(self._full_path, "generated_data_%s.dmp" % self.segment_value_id)

        try:
            shutil.copy2(src, dst)
        except IOError, e:
            raise Exception("cannot create current saved dataset %s. %s" % (dst, e))
        except OSError, e:
            raise Exception("cannot create current saved dataset %s. %s" % (dst, e))
        self.change_perm(dst)

    def make_current_data_set(self, data_set_instance_id):
        """
        copy last data set file to current data set file
        """
        src = os.path.join(self._full_path, "report_dataset_data_%s_%s.json" % (self._element_id, data_set_instance_id))
        dst = os.path.join(self._full_path, "report_dataset_data_%s_%s_current.json" % (self._element_id, self.segment_value_id))

        try:
            shutil.copy2(src, dst)
        except IOError, e:
            raise Exception("cannot create current dataset %s. %s" % (dst, e))
        except OSError, e:
            raise Exception("cannot create current dataset %s. %s" % (dst, e))
        self.change_perm(dst)

    def make_current_pivot_set(self, pivot_id, data_set_instance_id):
        """
        copy last pivot file to current pivot file
        """
        src = os.path.join(self._full_path, "report_dataset_pivot_data_%s_%s_%s.json" % (self._element_id, pivot_id, data_set_instance_id))
        dst = os.path.join(self._full_path, "dataset_pivot_data_%s_%s_%s_current.json" % (self._element_id, pivot_id, self.segment_value_id))

        try:
            shutil.copy2(src, dst)
        except IOError, e:
            raise Exception("Cannot create current pivot %s. %s" % (dst, e))
        except OSError, e:
            raise Exception("Cannot create current pivot %s. %s" % (dst, e))
        self.change_perm(dst)

    def make_current_chart_set(self, chart_id, data_set_instance_id):
        """
        copy last chart files to current chart files
        """
        # json
        src = os.path.join(self._full_path, "dataset_chart_%s_%s_%s.json" % (self._element_id, chart_id, data_set_instance_id))
        dst = os.path.join(self._full_path, "dataset_chart_%s_%s_%s_current.json" % (self._element_id, chart_id, self.segment_value_id))

        try:
            shutil.copy2(src, dst)
        except IOError, e:
            raise Exception("Cannot create current chart %s. %s" % (dst, e))
        except OSError, e:
            raise Exception("Cannot create current chart %s. %s" % (dst, e))
        self.change_perm(dst)

        # png
        src = os.path.join(self._full_path, "dataset_chart_%s_%s_%s.png" % (self._element_id, chart_id, data_set_instance_id))
        dst = os.path.join(self._full_path, "dataset_chart_%s_%s_%s_current.png" % (self._element_id, chart_id, self.segment_value_id))
        try:
            shutil.copy2(src, dst)
        except IOError, e:
            raise Exception("Cannot create current chart image %s. %s" % (dst, e))
        except OSError, e:
            raise Exception("Cannot create current chart image %s. %s" % (dst, e))
        self.change_perm(dst)
    
    def _get_meta_file_name(self):
        """
        return meta file name
        """
        return "report_metadata_%s_%s_current.json" % (self._element_id, self.segment_value_id)

    #def make_current_meta(self, last_data_set_instance, available_meas_times, available_intervals, drill_by, related, elements, segment_values):
    def make_current_meta(self, last_data_set_instance, available_meas_times, available_intervals, related, elements, segment_values):
        """
        create meta file
        """
        json_data = OrderedDict()
        # report info
        json_data["report_id"] = self._element_id
        json_data["report_name"] = self._get_sub_element_name(self._element['name'], self.segment_value)
        json_data["report_desc"] = self._element['description']
        json_data["report_category"] = self._element['category']
        json_data["report_topic"] = self._element['topic_name']
        json_data["business_owner"] = self._element['business_owner']
        json_data["technical_owner"] = self._element['technical_owner']

        if self._element['report_save_historical_instances_ind']=='Y':
            json_data["report_show_intervals"] = 'Y'
        else:
            json_data["report_show_intervals"] = 'N'

        # report measurement interval id
        json_data["report_meas_interval_id"] = self._element['measurement_interval_id']
        json_data["report_meas_interval_button_name"] = self._element['measurement_interval_button_name']

        if self._element['report_save_historical_instances_ind'] == 'Y' and last_data_set_instance:
            # last data set id
            json_data["report_interval_id"] = last_data_set_instance['report_data_set_instance_id']
            # last data set meas time
            json_data["meas_time"] = last_data_set_instance['measurement_time']

        else:
            json_data["report_interval_id"] = ''
            json_data["meas_time"] = ''

        # current segment value id
        json_data["segment_value_id"] = self.segment_value_id
        json_data["segment_name"] = ''

        if self.segment:
            json_data["segment_name"] = self.segment['name'] 

        # all segment values
        json_data["segments"] = []
        if segment_values:
            for segment_value in segment_values:
                json_data["segments"].append({'segment_value_id':segment_value['segment_value_id'], 'segment': segment_value['value_display_name']})
        elif not self.segment_value_id:
            json_data["segments"].append({'segment_value_id':0, 'segment': ''})

        # all data set instances
        json_data["intervals"] = []

        # all measurement times
        for available_meas_instance in available_meas_times:
            meas_instance = OrderedDict()
            meas_instance['interval_id'] = available_meas_instance['report_data_set_instance_id']
            meas_instance['interval_name'] = available_meas_instance['measurement_time']
            meas_instance['segment_value_id'] = self.segment_value_id
            meas_instance['link_to_element'] = self._element_id
            meas_instance['link_to_element_instance'] = available_meas_instance['report_data_set_instance_id']

            json_data["intervals"].append(meas_instance)

        # available measurement intervals
        json_data["meas_intervals"] = []

        for available_interval in available_intervals:
            interval = OrderedDict()
            interval['interval_id'] = available_interval['measurement_interval_id']
            interval['interval_name'] = available_interval['measurement_interval_button_name']
            interval['interval_time'] = ''
            interval['link_to_element'] = available_interval['element_id']
            interval['link_to_element_instance'] = ''
            interval['link_to_segment'] = self.segment_value_id
            json_data["meas_intervals"].append(interval)

        # todo: here 'drill by' will go in future.
        json_data["drill_data"] = []

        # related elements
        json_data["related"] = []
        for related_element in related:
            related_item = OrderedDict()
            related_item['element_id'] = related_element['element_id']
            related_item['instance_id'] = ''
            related_item['element_type'] = related_element['type']
            related_item['element_name'] = related_element['name']
            related_item['element_desc'] = related_element['description']
            related_item['external_url'] = ''
            related_item['segment_value_id'] = related_element['segment_value_id']
            if related_element['type'] == 'external report' or related_element['type'] == 'external content':
                related_item['external_url'] = related_element['external_report_url']
            json_data["related"].append(related_item)

        # elements displayed on the page
        json_data["content"] = []
        for element in elements:
            json_data["content"].append(element)

        content = simplejson.dumps(json_data, indent=4, check_circular=False)

        file_name = self._get_meta_file_name()
        self.write_file(file_name, content)

    def get_chart_file_name(self, report_data_set_chart_id, report_data_set_instance_id):
        """
        return chart image file name
        """
        if not report_data_set_instance_id:
            return os.path.join(self._full_path, "dataset_chart_%s_%s_%s_current.png" % (self._element_id, report_data_set_chart_id, self.segment_value_id))
        else:
            return os.path.join(self._full_path, "dataset_chart_%s_%s_%s.png" % (self._element_id, report_data_set_chart_id, report_data_set_instance_id))

    def make_chart_file(self, data, annotations, report_data_set_chart_id, report_data_set_instance_id, chart_type, is_x_axis_date):
        """
        create chart json file
        """
        json_data = OrderedDict()
        json_data['element_id'] = self._element_id
        json_data['chart_type'] = chart_type
        json_data['x_axis_type'] = 'non date'
        if is_x_axis_date:
            json_data['x_axis_type'] = 'date'

        json_data['raw_meas_time'] = str(self.meas_time)
        json_data['map'] = data
        
        json_data["annotations"] = []
        for annot_element in annotations:
            annot_item = OrderedDict()
            annot_item['index'] = annot_element['index']
            annot_item['ann_id'] = annot_element['user_annotation_id']
            annot_item['report_data_set_chart_annotation_id'] = annot_element['report_data_set_chart_annotation_id']
            annot_item['ann_time'] = str(annot_element['annotation_time'])
            annot_item['ann_text'] = annot_element['annotation_text']
            annot_item['ann_by_id'] = annot_element['user_id']
            annot_item['ann_by_name'] = annot_element['username']
            annot_item['chart_by_column_value'] = annot_element['chart_by_column_value']
            annot_item['raw_chart_by_column_value'] = str(annot_element['raw_chart_by_column_value'])
            annot_item['chart_element_identifier'] = annot_element['chart_element_identifier']
            annot_item['annotation_scope'] = annot_element['annotation_scope']
            annot_item['instance_start_time'] = str(annot_element['instance_start_time'])
            annot_item['instance_start_time_formatted'] = annot_element['instance_start_time_formatted']
            annot_item['expiration_time'] = ''
            annot_item['instance_expiration_time'] = ''
            annot_item['instance_expiration_time_formatted'] = ''
            if annot_element['instance_expiration_time']:
                annot_item['instance_expiration_time'] = str(annot_element['instance_expiration_time'])
                annot_item['instance_expiration_time_formatted'] = annot_element['instance_expiration_time_formatted']
                annot_item['expiration_time'] = ''
            annot_item['value'] = annot_element['value']
            annot_item['shape'] = annot_element['shape']
            annot_item['coords'] = annot_element['coords']
            annot_item['annotation_interval'] = annot_element['annotation_interval']
            annot_item['start_time'] = annot_element['start_time']
            annot_item['finish_time'] = annot_element['finish_time']
            annot_item['raw_start_time'] = str(annot_element['raw_start_time'])
            annot_item['raw_finish_time'] = str(annot_element['raw_finish_time'])

            json_data["annotations"].append(annot_item)
        content = simplejson.dumps(json_data, indent=4, check_circular=False)
        
        full_file_name = self.get_chart_json_file_name(report_data_set_chart_id, report_data_set_instance_id)
        self._write_file(full_file_name, content)
        
    def get_chart_json_file_name(self, report_data_set_chart_id, report_data_set_instance_id):
        """
        return chart json file name
        """
        if not report_data_set_instance_id:
            file_name = os.path.join(self._full_path, 'dataset_chart_%s_%s_%s_current.json' % (self._element_id, report_data_set_chart_id, self.segment_value_id))

        else:
            file_name = os.path.join(self._full_path, 'dataset_chart_%s_%s_%s.json' % (self._element_id, report_data_set_chart_id, report_data_set_instance_id))
        return file_name

    def get_thumbnail_file_name(self):
        """
        return thumbnail image file name
        """
        return os.path.join(self._thumbnail_root, "%s_%s_thumbnail.png" % (self._element_id, self.segment_value_id))

    def get_preview_file_name(self):
        """
        return preview image file name
        """
        return os.path.join(self._preview_root, "%s_%s_preview.png" % (self._element_id, self.segment_value_id))

    def get_resized_preview_file_name(self):
        """
        return resized preview image file name
        """
        return os.path.join(self._preview_root, "%s_%s_resized_preview.png" % (self._element_id, self.segment_value_id))

    def save_fetch_settings(self, data):
        """
        save current report settings
        """
        self.write_file("generation_settings.dmp", marshal.dumps(data))
    
    def save_data_fetch(self, data):
        """
        save current fetched data for reports without history
        """
        self.write_file("generated_data_%s.dmp" % self.segment_value_id, marshal.dumps(data))

    def save_data_fetch_instance(self, data, instance_id):
        """
        save current fetched data instance for reports with history
        """
        self.write_file("generated_data_set_%s.dmp" % instance_id, marshal.dumps(data))
        
    def _get_saved_data(self, file_name):
        """
        return saved instance
        """
        return self._get_saved_file(file_name)

    def _get_saved_file(self, file_name):
        """
        return file content
        """
        content = ''
        try:
            json_file = open(file_name, 'r')
            try:
                content = json_file.read()
            except IOError:
                return None
            finally:
                json_file.close()
        except IOError:
            return None
        if content:
            try:
                #info = cPickle.loads(content)
                info = marshal.loads(content)
            except Exception:
                return None
            return info

        return None

    def get_current_stored_dataset(self):
        """
        return fetched data instance for reports with history
        """
        return self._get_saved_data(os.path.join(self._full_path, "generated_data_%s.dmp" % self.segment_value_id))

    def get_stored_dataset(self, dataset_id):
        """
        return fetched data instance for reports with history
        """
        return self._get_saved_data(os.path.join(self._full_path, "generated_data_set_%s.dmp" % dataset_id))


class JFileValidator(JFile):
    """
    Managing json files for internal reports validation
    """
    _saved_data_directory = "saved_data"
    def __init__(self, path, element_id, element):
        #JAbstractFile.__init__(self, os.path.join(path, str(element_id), "validation"), element_id, "dir creation")
        JAbstractFile.__init__(self, os.path.join(path, str(element_id), "validation"), element_id)
        #JFile.__init__(self, path, element_id, element)
        self._element = element
        self._path = os.path.join(path, str(element_id))
        self._saved_data_path = os.path.join(self._full_path, self._saved_data_directory)

    def _get_data_set_file_name(self, instance_id):
        return "validated_data_set_%s_current.json" % self._element_id

    def _get_pivot_file_name(self, report_data_set_pivot_id, data_set_instance_id):
        return "validated_pivot_%s_%s.json" % (self._element_id, report_data_set_pivot_id)

    def _get_meta_file_name(self):
        return "validated_report_metadata_%s_current.json" % self._element_id

    def write_instance(self, full_file_name, content):
        self._write_file(full_file_name, content)

    def read_instance(self, full_file_name):
        content = ''
        try:
            file = open(full_file_name, 'r')
            try:
                content = file.read()
            except IOError, e:
                raise Exception("Cannot read from file: %s. %s" % (full_file_name, e))
            finally:
                file.close()
        except IOError, e:
            raise Exception("Cannot open file: %s. %s" % (full_file_name, e))
        return content


    def get_chart_file_name(self, report_data_set_chart_id, report_data_set_instance_id):
        return os.path.join(self._full_path, "report_editor_preview_%s_%s.png" % (self._element_id, report_data_set_chart_id))

    def get_chart_json_file_name(self, report_data_set_chart_id, report_data_set_instance_id):
        return os.path.join(self._full_path, "report_editor_preview_%s_%s.json" % (self._element_id, report_data_set_chart_id))

    def get_validation_data(self):
        content = ''
        
        full_file_name = os.path.join(self._full_path, "valid.json")
        try:
            json_file = open(full_file_name, 'r')
            try:
                content = json_file.read()
            except IOError, e:
                raise Exception("Cannot read from file: %s. %s" % (full_file_name, e))
            finally:
                json_file.close()
        except IOError, e:
            raise Exception("Cannot open file: %s. %s" % (full_file_name, e))
        if content:
            json = simplejson.loads(content)
        else:
            raise Exception("File %s is empty" % full_file_name)
        return json
    
    def _get_saved_info(self, file_name):
        info = self._get_saved_file(file_name)
        if info and info['sql']:
            info['sql'] = info['sql'].strip().replace('\r\n','\n')
        return info

    def get_generation_fetch_settings(self):
        return self._get_saved_info(os.path.join(self._path, "generation_settings.dmp"))

    def get_generated_dataset(self):
        return self._get_saved_data(os.path.join(self._path, "generated_data_%s.dmp" % self.segment_value_id))

    def get_generated_dataset_instance(self, instance_id):
        return self._get_saved_data(os.path.join(self._path, "generated_data_set_%s.dmp" % instance_id))
    
    def get_validation_fetch_settings(self):
        return self._get_saved_info(os.path.join(self._full_path, "validation_settings.dmp"))

    def get_validated_dataset(self):
        return self._get_saved_data(os.path.join(self._full_path, "validated_data_%s.dmp" % self.segment_value_id))
    
    def get_last_validation_fetch_settings(self):
        return self._get_saved_info(os.path.join(self._full_path, "last_validation_settings.dmp"))

    def get_last_validated_dataset(self):
        return self._get_saved_data(os.path.join(self._full_path, "last_validated_data_%s.dmp" % self.segment_value_id))

    def save_fetch_settings(self, data):
        self.write_file("validation_settings.dmp", marshal.dumps(data))
    
    def save_data_fetch(self, data):
        self.write_file("validated_data_%s.dmp" % self.segment_value_id, marshal.dumps(data))
        
    def save_last_validation_fetch_settings(self, data):
        self.write_file("last_validation_settings.dmp", marshal.dumps(data))
    
    def save_last_validated_data_fetch(self, data):
        self.write_file("last_validated_data_%s.dmp" % self.segment_value_id, marshal.dumps(data))

    def save_validation_data(self):
        """
        copy all files from <report_id>/validation/ to emptied <report_id>/validation/saved_data directory
        """
        self._check_dir(self._saved_data_path)

        saved_files = glob(os.path.join(self._saved_data_path, "*"))
        for saved_file in saved_files:
            if not os.path.isdir(saved_file):
                self._remove_custom_file(saved_file)

        current_files = glob(os.path.join(self._full_path, "*"))
        for current_file in current_files:
            if not os.path.isdir(current_file):
                filename = os.path.basename(current_file)
                dst = os.path.join(self._saved_data_path, filename)
                try:
                    shutil.copy2(current_file, dst)
                except IOError, e:
                    raise Exception("Cannot copy file into %s. %s" % (dst, e))
                except OSError, e:
                    raise Exception("Cannot copy file into  %s. %s" % (dst, e))
                self.change_perm(dst)

    def restore_validation_data(self):
        """
        copy all files from <report_id>/validation/saved_data to emptied <report_id>/validation/ directory
        """
        self._check_dir(self._saved_data_path)

        current_files = glob(os.path.join(self._full_path, "*"))
        for current_file in current_files:
            if not os.path.isdir(current_file):
                self._remove_custom_file(current_file)

        saved_files = glob(os.path.join(self._saved_data_path, "*"))
        for saved_file in saved_files:
            if not os.path.isdir(saved_file):
                filename = os.path.basename(saved_file)
                dst = os.path.join(self._full_path, filename)
                try:
                    shutil.copy2(saved_file, dst)
                except IOError, e:
                    raise Exception("Cannot copy file into %s. %s" % (dst, e))
                except OSError, e:
                    raise Exception("Cannot copy file into  %s. %s" % (dst, e))
                self.change_perm(dst)
