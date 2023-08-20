import datetime

def input_paths(date, depth): 
	dt = datetime.datetime.strptime(date, '%Y-%m-%d') 
	return [f"/user/mustdayker/data/events/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}/event_type=message" for x in range(depth)]