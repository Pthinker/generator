from reportChart import ReportChart

class LargeReport(ReportChart, object):
    
	def __init__(self, chart_id, element_id, data_set_instance_id, data, jfile):
		self.type = 'large_report'
		self.include_title = True
		self.include_axis = True
		self.legend = True		
		super(LargeReport, self).__init__(chart_id, element_id, data_set_instance_id, data, jfile)

