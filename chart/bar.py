from os.path import dirname, realpath
project_path = realpath(dirname(__file__))

from sys import path
path.append(project_path + '/chartDirector')

from pychartdir import *

from baseChart import BaseChart

class Bar(BaseChart, object):
    
	def __init__(self, element_id, report_data_set_chart_id, report_data_set_instance_id, data, settings, jfile):
		self.chart_type = 'bar'
		super(Bar, self).__init__(element_id, report_data_set_chart_id, report_data_set_instance_id, data, settings, jfile)
        
        
            
	
