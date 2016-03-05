from flask.ext.sqlalchemy import SQLAlchemy, sqlalchemy
import datetime
import pytz
import time
import dateutil.parser
import requests
import json
import csv
#from flask_mail import Mail, Message
from operator import itemgetter
from appdev import app, db, Pipeline

def datetime_est(days_ago):
	global est_zone
	utc_zone = pytz.timezone('UTC')
	est_zone = pytz.timezone('US/Eastern')
	now_utc_naive = datetime.datetime.utcnow()
	now_utc_aware = utc_zone.localize(now_utc_naive)
	now_est_aware = now_utc_aware.astimezone(est_zone)
	today_12am_est = now_est_aware.replace(hour=0, minute=0, second=0)
	days_ago = datetime.timedelta(days=days_ago)
	desired_time_est = today_12am_est - days_ago
	return desired_time_est

# takes days_ago parameter and returns Salesforce API query parameter, i.e. UTC isoformat datetime
def datetime_iso(days_ago):
	global est_zone
	utc_zone = pytz.timezone('UTC')
	est_zone = pytz.timezone('US/Eastern')
	now_utc_naive = datetime.datetime.utcnow()
	now_utc_aware = utc_zone.localize(now_utc_naive)
	now_est_aware = now_utc_aware.astimezone(est_zone)
	today_12am_est = now_est_aware.replace(hour=0, minute=0, second=0)
	days_ago = datetime.timedelta(days=days_ago)
	desired_time_est = today_12am_est - days_ago
	desired_time_est_in_utc = desired_time_est.astimezone(utc_zone)
	desired_time_est_in_utc_naive = desired_time_est_in_utc.replace(tzinfo=None)
	desired_time_est_in_utc_iso = desired_time_est_in_utc_naive.isoformat()
	return desired_time_est_in_utc_iso

def fill_json(start_days_ago, end_days_ago):
	counties = ["Suffolk", "Nassau", "Richmond", "Queens"]
	categories = ["sales", "ssq", "adq", "designed", "bad_roof", "cancelled", "g2g"]
	pipelinelw = {"Suffolk": {"county": "Suffolk", "sales": 0, "ssq": 0, "adq": 0, "designed": 0, "bad_roof": 0, "cancelled": 0, "g2g": 0},
				  "Nassau": {"county": "Nassau", "sales": 0, "ssq": 0, "adq": 0, "designed": 0, "bad_roof": 0, "cancelled": 0, "g2g": 0},
				  "Richmond": {"county": "Richmond", "sales": 0, "ssq": 0, "adq": 0, "designed": 0, "bad_roof": 0, "cancelled": 0, "g2g": 0},
				  "Queens": {"county": "Queens", "sales": 0, "ssq": 0, "adq": 0, "designed": 0, "bad_roof": 0, "cancelled": 0, "g2g": 0},
				  "total": {"sales": 0, "ssq": 0, "adq": 0, "designed": 0, "bad_roof": 0, "cancelled": 0, "g2g": 0}}
	today = datetime_est(start_days_ago)
	week_ago = datetime_est(end_days_ago)
	saleslw = Pipeline.query.filter(sqlalchemy.and_(Pipeline.sale_date.between(week_ago, today))).all()
	for sale in saleslw:
		pipelinelw[sale.county]["sales"] += 1
		if sale.design_done == "Yes":
			pipelinelw[sale.county]["designed"] += 1
		if sale.roof_pass == "No":
			pipelinelw[sale.county]["bad_roof"] += 1
		if sale.cad_outcome == "cancel":
			pipelinelw[sale.county]["cancelled"] += 1
		if sale.site_survey == "No" and sale.cad_outcome != "cancel":
			pipelinelw[sale.county]["ssq"] += 1
		if sale.site_survey == "Yes" and sale.cad_outcome != "cancel" and sale.design_done == "No":
			pipelinelw[sale.county]["adq"] += 1
		if sale.cad_outcome == "goodtogo":
			pipelinelw[sale.county]["g2g"] +=1
	for county in counties:
		for category in categories:
			pipelinelw["total"][category] += pipelinelw[county][category]
	data_file = open("/root/dboard/data/pipelinelw.json", "w")
	json.dump(pipelinelw, data_file)
	data_file.close()

fill_json()

