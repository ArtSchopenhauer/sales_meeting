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
from app import app, db, Pipeline

leads = "https://levelsolar.secure.force.com/api/services/apexrest/leads"
contacts = "https://levelsolar.secure.force.com/api/services/apexrest/contacts"
interactions = "https://levelsolar.secure.force.com/api/services/apexrest/interactions"
accounts = "https://levelsolar.secure.force.com/api/services/apexrest/accounts"
cases = "https://levelsolar.secure.force.com/api/services/apexrest/cases"
metrics = "https://levelsolar.secure.force.com/api/services/apexrest/metrics"

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

def api_response(url, parameters={}, start_date=0, full_list=None, try_num=0):
	if full_list == None:
		full_list = []
	parameters["ordering"] = "createddate"
	if start_date != 0:
		parameters["createddate__gt"] = start_date
	if try_num <= 9:
		r = requests.get(url, params=parameters)
		if r.status_code == 200:
			page_list = r.json()
			full_list.extend(page_list)
			if len(page_list) == 100:
				start_date = page_list[-1]["createddate"]
				api_response(url, parameters, start_date, full_list, 0)
		else:
			try_num = try_num + 1
			time.sleep(1)
			api_response(url, parameters, try_num)
	else:
		print "Tried and failed 10 times."
		#with app.app_context():
		#	send_error()
	return full_list

def county_lookup():
	muni_counties = {}
	with open("county.csv") as csvfile:
		csv_r = csv.DictReader(csvfile)
		for row in csv_r:
			muni_counties[row["municipality"]] = row["county"]
	csvfile.close()
	return muni_counties

def fill_Pipeline(days_ago):
	records = []
	start = datetime_iso(days_ago)
	muni_counties = county_lookup()
	cad_appts = api_response(interactions, {"subject": "CAD Appointment"}, start, [], 0)
	cad_outcomes = api_response(interactions, {"subject": "CAD Appointment Outcome"}, start, [], 0)
	install_cases_opened = api_response(cases, {"type_name": "Install"}, start, [], 0)
	for item in install_cases_opened:
		account_id = item["account"]["id"]
		account_url = accounts + "/" + account_id
		account = api_response(account_url, {}, 0, [], 0)[0]
		account_num = account["account_number"]
		if " - " in account["name"]:
			street_address = account["name"].split(" - ")[0]
			city = account["name"].split(" - ")[1]
			zip_code = account["name"].split(" - ")[2]
		else:
			street_address = "Missing"
			city = "Missing"
			zip_code = "Missing"
		if account["municipality"]:
			municipality = account["municipality"]["name"]
		else:
			municipality = "Missing"
		all_contacts = api_response(contacts, {"account": account_id}, 0, [], 0)
		customer = 0
		for contact in all_contacts:
			if contact["email"]:
				customer = customer + 1
				primary_contact = contact
				name = primary_contact["name"]
				contact_id = primary_contact["id"]
				if primary_contact["phone"]:
					home_phone = primary_contact["phone"]
				else:
					home_phone = ""
				if primary_contact["mobilephone"]:
					mobile = primary_contact["mobilephone"]
				else:
					mobile = ""
				if home_phone == "" and mobile == "":
					phone = "Missing"
				elif home_phone != "" and mobile == "":
					phone = home_phone
				elif home_phone == "" and mobile != "":
					phone = mobile
				else:
					phone = home_phone + ", " + mobile + " (M)"
			if customer == 0:
				pass
			else:
				install_cases_list = api_response(cases, {"account": account_id, "type_name": "Install"}, 0, [], 0)
				if len(install_cases_list) == 0:
					installed = "No"
				else:
					install_case = install_cases_list[0]
					sale_date = dateutil.parser.parse(install_case["createddate"]).astimezone(est_zone)
					if install_case["status"]:
						if install_case["status"] == "Installed as designed" or install_case["status"] == "Installed different than design":
							installed = "Yes"
						else:
							installed = "No"
				permit_case_list = api_response(cases, {"account": account_id, "type_name": "Document"}, 0, [], 0)
				if len(permit_case_list) == 0:
					permit_submitted = None
					permit_received = None
				else:
					permit_case = permit_case_list[0]
					if permit_case["town_permit_submitted"]:
						permit_submitted = dateutil.parser.parse(permit_case["town_permit_submitted"]).astimezone(est_zone)
					else:
						permit_submitted = None
					if permit_case["town_permit_received"]:
						permit_received = dateutil.parser.parse(permit_case["town_permit_received"]).astimezone(est_zone)
					else:
						permit_received = None
				survey_case_list = api_response(cases, {"account": account_id, "type_name": "Survey"}, 0, [], 0)
				if len(survey_case_list) == 0:
					site_survey = "No"
				else:
					survey_case = survey_case_list[0]
					if survey_case["status"]:
						if survey_case["status"] == "Closed":
							site_survey = "Yes"
						else:
							site_survey = "No"
				cad_appts_list = []
				cad_outcomes_list = []
				for item in cad_appts:
					if item["contact"]:
						if item["contact"]["id"] == contact_id:
							cad_appts_list.append(item)
				for item in cad_outcomes:
					if item["contact"]:
						if item["contact"]["id"] == contact_id:
							cad_outcomes_list.append(item)
				if len(cad_appts_list) == 0:
					cad_date = None
				else:
					cad_appt_obj = sorted(cad_appts_list, key=itemgetter("scheduled_date"))[-1]
					if cad_appt_obj["scheduled_date"]:
						cad_date = dateutil.parser.parse(cad_appt_obj["scheduled_date"]).astimezone(est_zone)
					else:
						cad_date = None
				if len(cad_outcomes_list) == 0:
					cad_outcome = None
					cad_closer = None
					cad_notes = None
				else:
					cad_outcome_obj = sorted(cad_appts_list, key=itemgetter("interaction_date"))[-1]
					cad_outcome = cad_outcome_obj["outcome"]
					cad_closer = "Missing"
					if cad_outcome_obj["comments"]:
						cad_notes = cad_outcome_obj["comments"]
					else:
						cad_notes = "None"	
				if muni_counties["municipality"]:
					county = muni_counties["municipality"]
				else:
					county = "Missing"
				closer = "Josh"
				hq_notes = "None"
		record = Pipeline(account_id, account_num, name, street_address, city, zip_code, county, municipality, phone, sale_date, closer, site_survey,
                 roof_pass, cad_date, cad_closer, cad_outcome, cad_notes, permit_submitted, permit_received, installed, hq_notes)
		records.append(record)
	for item in records:
		db.session.add(item)
	db.session.commit()

#fill_Pipeline(1)


