from reportChart import ReportChart

class ThumbnailReport(ReportChart, object):
    
	def __init__(self, chart_id, element_id, data_set_instance_id, data):
		self.type = 'thumbnail_report'
		self.include_title = False
		self.include_axis = False
		self.legend = False		
		super(ThumbnailReport, self).__init__(chart_id, element_id, data_set_instance_id, data)

