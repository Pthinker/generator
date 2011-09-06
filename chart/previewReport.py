from db.db_conn import DBManager
#from metric.conf import ConfigReader
from reportChart import ReportChart

class PreviewReport(ReportChart, object):

	def __init__(self, chart_id, element_id, data_set_instance_id, data):
		self._db = DBManager.get_query()
		self.type = 'preview_report'
		self.include_title = True		
		self.legend = True	
		
		""" Check if axis should be included """
		self._db.Query("""	SELECT chart_layout.include_x_axis_label_ind FROM report_data_set_chart
							LEFT JOIN chart_layout 
							ON chart_layout.layout_id = report_data_set_chart.layout_id
							WHERE report_data_set_chart.report_data_set_chart_id=%s""",(chart_id))
		if self._db.record[0]['include_x_axis_label_ind'] == 'Y':
			self.include_axis = True
		else:
			self.include_axis = False
		super(PreviewReport, self).__init__(chart_id, element_id, data_set_instance_id, data)

