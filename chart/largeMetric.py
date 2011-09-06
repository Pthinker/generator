from metricChart import MetricChart

class LargeMetric(MetricChart, object):
    
	def __init__(self, metric_id, interval, data):
		self.type = 'large_metric'
		self.include_title = True
		self.include_axis = True
		self.legend = True		
		super(LargeMetric, self).__init__(metric_id, interval, data)

