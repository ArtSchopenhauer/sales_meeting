from flask.ext.sqlalchemy import SQLAlchemy, sqlalchemy
import datetime
import pytz
import time
import dateutil.parser
import requests
import json
import csv
#from flask_mail import Mail, Message
from operator import itemgetter, attrgetter
from appdev2 import app, db, Pipeline, CAD, Permit, Appointment

leads = "https://levelsolar.secure.force.com/api/services/apexrest/leads"
contacts = "https://levelsolar.secure.force.com/api/services/apexrest/contacts"
interactions = "https://levelsolar.secure.force.com/api/services/apexrest/interactions"
accounts = "https://levelsolar.secure.force.com/api/services/apexrest/accounts"
cases = "https://levelsolar.secure.force.com/api/services/apexrest/cases"
metrics = "https://levelsolar.secure.force.com/api/services/apexrest/metrics"

{"n1": [{"name": "Ryan Samida (L)", "id": "a027000000RvoO8"},
			   {"name": "Brenda Larsen", "id": "a027000000SKNZq"},
			   {"name": "Lonnie Edwards", "id": "a027000000Qqn7u"},
			   {"name": "Johnny Rivera", "id": "a023900000TeFll"}], 
		"n2": [{"name": "Casey O'Brien (L)", "id": "a027000000Q86c5"},
			   {"name": "Andrew Field", "id": "a027000000Rlv53"},
			   {"name": "Dorothy Pitti", "id": "a027000000SgtbB"},
			   {"name": "Carlo Echeverri", "id": "a023900000TMyEw"},
			   {"name": "Doug Stevens", "id": "a027000000RwlSZ"}],
		"n3": [{"name": "Caitlyn Weiss (L)", "id": "a027000000SeQfE"},
			   {"name": "Jeremy Weissman", "id": "a027000000PXjka"},
			   {"name": "Andrew Malca", "id": "a027000000SHtTj"},
			   {"name": "Alejandro Henriquez", "id": "a023900000Tcxv9"}],
		"s1": [{"name": "Nestor Colon (L)", "id": "a027000000PxIOQ"},
			   {"name": "Carlos Vega", "id": "a027000000R3JB2"},
			   {"name": "Robert Dees", "id": "a027000000RvoZ7"},
			   {"name": "Savannah Pavelchak", "id": "a023900000TOyJf"}],
		"s2": [{"name": "Francis D'Erasmo (L)", "id": "a027000000Px6vS"},
			   {"name": "Brian Yurasits", "id": "a023900000TOyJk"},
			   {"name": "Kyle Hempe", "id": "a023900000TeClD"}],
		"s3": [{"name": "Michael Desiderio (L)", "id": "a027000000QsUeE"},
			   {"name": "Raymond Armstrong", "id": "a027000000SKN4R"},
			   {"name": "Victor Borisov", "id": "a027000000RyOFN"}]}

amb_reps = ["Ryan Samida", "Brenda Larsen", "Lonnie Edwards", "Johnny Rivera", "Casey O'Brien",
			"Andrew Field", "Dorothy Pitti", "Carlo Echeverri", "Doug Stevens", "Caitlyn Weiss",
			"Jeremy Weissman", "Andrew Malca", "Alejandro Henriquez", "Nestor Colon", "Carlos Vega",
			"Robert Dees", "Savannah Pavelchak", "Francis D'Erasmo", "Brian Yurasits", "Kyle Hempe",
			"Michael Desiderio", "Raymond Armstrong", "Victor Borisov", "Michael Callahan", "Dan Robin",
			"Taylor Colucci", "Anthony Quezada"]

event_reps = ["Justin Schimmenti", "Amalia Reyes", "Meagan Walsh", "Jordan Elian"]

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

def api_response(url, parameters={}, created_start_date=0, full_list=None, try_num=0):
	if full_list == None:
		full_list = []
	parameters["ordering"] = "createddate"
	if created_start_date != 0:
		parameters["createddate__gt"] = created_start_date
	if try_num <= 9:
		r = requests.get(url, params=parameters)
		if r.status_code == 200:
			page_list = r.json()
			full_list.extend(page_list)
			if len(page_list) == 100:
				created_start_date = page_list[-1]["createddate"]
				api_response(url, parameters, created_start_date, full_list, 0)
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

# fills Pipeline and CAD tables with data from Salesforce
def fill_pipeline(days_ago):
	records = []
	cad_records = []
	start = datetime_iso(days_ago)
	cad_appts = api_response(interactions, {"subject": "CAD Appointment"}, start, [], 0)
	cad_outcomes = api_response(interactions, {"subject": "CAD Appointment Outcome"}, start, [], 0)
	install_cases_opened = api_response(cases, {"type_name": "Install"}, start, [], 0)
	for item in install_cases_opened:
		account_id = item["account"]["id"]
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
				if primary_contact["account"]:
					account = primary_contact["account"]
					if account["account_number"]:
						account_num = account["account_number"]
					else:
						account_num = "Missing"
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
						if account["municipality"]["county"]:
							county = account["municipality"]["county"]["name"]
						else:
							county = "Missing"
					else:		
						municipality = "Missing"
						county = "Missing"
			if customer == 0:
				pass
			else:
				install_cases_list = api_response(cases, {"account": account_id, "type_name": "Install"}, 0, [], 0)
				if len(install_cases_list) == 0:
					installed = "No"
				else:
					install_case = install_cases_list[0]
					if install_case["createddate"]:
						sale_date = dateutil.parser.parse(install_case["createddate"]).astimezone(est_zone)
					else:
						sale_date = None
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
					cad_appt_date = None
				else:
					cad_appt_obj = sorted(cad_appts_list, key=itemgetter("scheduled_date"))[-1]
					if cad_appt_obj["scheduled_date"]:
						cad_appt_date = dateutil.parser.parse(cad_appt_obj["scheduled_date"]).astimezone(est_zone)
					else:
						cad_appt_date = None
				if len(cad_outcomes_list) == 0:
					cad_outcome_date = None
					cad_outcome = None
					cad_closer = None
					cad_notes = None
				else:
					cad_outcome_obj = sorted(cad_outcomes_list, key=itemgetter("interaction_date"))[-1]
					if cad_outcome_obj["interaction_date"]:
						cad_outcome_date = dateutil.parser.parse(cad_outcome_obj["interaction_date"]).astimezone(est_zone)
					else:
						cad_outcome_date = None
					if cad_outcome_obj["outcome"]:
						cad_outcome = cad_outcome_obj["outcome"]
					else:
						cad_outcome = "Missing"
					cad_closer = "Missing"
					if cad_outcome_obj["comments"]:
						cad_notes = cad_outcome_obj["comments"]
					else:
						cad_notes = "None"
				if site_survey == "No":
					design_done = "No"
					roof_pass = "TBD"
				else:
					design_case_list = api_response(cases, {"account": account_id, "type_name": "Design"}, 0, [], 0)
					if len(design_case_list) == 0:
						design_done = "No"
						roof_pass = "TBD"
					else:
						for design in design_case_list:
							if design["status"] == "New":
								design_done = "No"
								roof_pass = "TBD"
							if design["status"] == "Closed":
								design_done = "Yes"
								roof_pass = "Yes"
							if design["status"] == "LVL To Cancel (Pre-BMC Created)" or design["status"] == "Tree Removal Needed":
								design_done = "N/A"
								roof_pass = "No"
							if design["status"] == "Cancelled":
								if cad_outcome != "cancel":
									design_done = "N/A"
									roof_pass = "No"
								else:
									design_done = "N/A"
									roof_pass = "N/A"
				sales_doc = api_response(cases, {"account": account_id, "type_name": "Sales Document"}, 0, [], 0)[0]
				if sales_doc["salesRepName"]:
					closer = sales_doc["salesRepName"]
				else:
					closer = "Missing"
				hq_notes = "None"
			record = Pipeline(account_id, account_num, name, street_address, city, zip_code, county, municipality, phone, sale_date, closer, site_survey,
                 roof_pass, design_done, cad_appt_date, cad_outcome_date, cad_closer, cad_outcome, cad_notes, permit_submitted, permit_received, installed, hq_notes)
			db.session.add(record)
			db.session.commit()

def fill_cad(days_ago):
	start = datetime_iso(days_ago)
	cad_outcomes = api_response(interactions, {"subject": "CAD Appointment Outcome"}, start, [], 0)
	for obj in cad_outcomes:
		if obj["contact"]:
			if obj["contact"]["name"]:
				name_c = obj["contact"]["name"]
			else:
				name_c = "Missing"
			if obj["outcome"]:
				cad_outcome_c = obj["outcome"]
			else:
				cad_outcome_c = "Missing"
			if obj["interaction_date"]:
				cad_outcome_date_c = dateutil.parser.parse(obj["interaction_date"]).astimezone(est_zone)
			else:
				cad_outcome_date_c = None
			if obj["comments"]:
				cad_notes_c = obj["comments"]
			else:
				cad_notes_c = "Missing"
			contact_id_c = obj["contact"]["id"]
			contact_c = api_response((contacts + "/" + contact_id_c), {}, 0, [], 0)[0]
			if contact_c:
				if contact_c["account"]["municipality"]["county"]["name"]:
					county_c = contact_c["account"]["municipality"]["county"]["name"]
				else:
					county_c = "Missing"
			else:
				county_c = "Missing"
			cad_record = CAD(name_c, contact_id_c, county_c, cad_outcome_date_c, cad_outcome_c, cad_notes_c)
			db.session.add(cad_record)
			db.session.commit()


# removes duplicate outcomes from CAD table for a given customer, leaving the most recent
def clean_cad_db():
	contact_ids = []
	dupe_ids = []
	cads = CAD.query.all()
	for cad in cads:
		contact_ids.append(cad.contact_id)
	for item in contact_ids:
		instances = contact_ids.count(item)
		if instances > 1:
			dupe_ids.append(item)
	dupes_set = list(set(dupe_ids))
	for item in dupes_set:
		dupe_records = CAD.query.filter(CAD.contact_id==item).all()
		for item in dupe_records:
			if item.cad_outcome_date == None:
				item.cad_outcome_date = datetime.datetime.now().replace(month=1, day=1, year=2016)
		dupes_sorted = sorted(dupe_records, key=attrgetter("cad_outcome_date"))
		keep = dupes_sorted.pop()
		for item in dupes_sorted:
			db.session.delete(item)
			db.session.commit()

# removes duplicate permit cases from Permit table for a given customer, leaving the one for the correct account
def clean_permit_db():
	account_nums = []
	dupe_nums = []
	permits = Permit.query.all()
	for permit in permits:
		account_nums.append(permit.account_num)
	for item in account_nums:
		instances = account_nums.count(item)
		if instances > 1:
			dupe_nums.append(item)
	dupes_set = list(set(dupe_nums))
	for item in dupes_set:
		dupe_records = Permit.query.filter(Permit.account_num==item).all()
		dupe_with_received = []
		dupe_no_received = []
		for item in dupe_records:
			if item.received != None:
				dupe_with_received.append(item)
			else:
				dupe_no_received.append(item)
		if len(dupe_with_received) > 0:
			dupe_with_received_sorted = sorted(dupe_with_received, key=attrgetter("submitted"))
			keep = dupe_with_received_sorted.pop()
			for item in dupe_with_received_sorted:
				db.session.delete(item)
			for item in dupe_no_received:
				db.session.delete(item)
		else:
			dupe_no_received_sorted = sorted(dupe_no_received, key=attrgetter("submitted"))
			keep = dupe_no_received_sorted.pop()
			for item in dupe_no_received_sorted:
				db.session.delete(item)
	db.session.commit()

# removes duplicate records from Pipeline table for given customer, leaving most up-to-date record
def clean_pipeline_db():
	account_nums = []
	dupe_nums = []
	records = Pipeline.query.all()
	for record in records:
		account_nums.append(record.account_num)
	for item in account_nums:
		instances = account_nums.count(item)
		if instances > 1:
			dupe_nums.append(item)
	dupes_set = list(set(dupe_nums))
	for item in dupes_set:
		dupe_records = Pipeline.query.filter(Pipeline.account_num==item).all()
		dupe_with_outcome = []
		dupe_no_outcome = []
		for item in dupe_records:
			if item.cad_outcome_date != None:
				dupe_with_outcome.append(item)
			else:
				dupe_no_outcome.append(item)
		if len(dupe_with_outcome) > 0:
			dupe_with_outcome_sorted = sorted(dupe_with_outcome, key=attrgetter("cad_outcome_date"))
			keep = dupe_with_outcome_sorted.pop()
			for item in dupe_with_outcome_sorted:
				db.session.delete(item)
			for item in dupe_no_outcome:
				db.session.delete(item)
		else:
			keep = dupe_no_outcome.pop()
			for item in dupe_no_outcome:
				db.session.delete(item)
	db.session.commit()

# fills Permit table with Salesforce data
def fill_permits():
	est_zone = pytz.timezone('US/Eastern')
	permit_cases = api_response(cases, {"type_name": "Document"})
	for case in permit_cases:
		if case["town_permit_submitted"]:
			submitted = dateutil.parser.parse(case["town_permit_submitted"]).replace(hour=10)
			if case["town_permit_received"]:
				received = dateutil.parser.parse(case["town_permit_received"]).replace(hour=10)
			else:
				received = None
			if case["account"]:
				account_num = case["account"]["account_number"]
				account_id = case["account"]["id"]
				account = api_response(accounts + "/" + account_id)[0]
				if account:
					if account["municipality"]:
						if account["municipality"]["county"]:
							county = account["municipality"]["county"]["name"]
						else:
							county = "Missing"
					else:
						county = "Missing"
				else:
					county = "Missing"
			else:
				account_num = "Missing"
				county = "Missing"
		record = Permit(account_num, county, submitted, received)
		db.session.add(record)
		db.session.commit()

# fills Appointment table with Salesforce data
def fill_appointment(days_ago):
	start = datetime_iso(days_ago)
	end = datetime_iso(0)
	appts = api_response(interactions, {"subject": "Closer Appointment", "scheduled_date__gt": start, "scheduled_date__lt": end})
	for appt in appts:
		interaction_id = appt["id"]
		if appt["scheduled_date"]:
			scheduled_date = dateutil.parser.parse(appt["scheduled_date"]).astimezone(est_zone)
		else:
			scheduled_date = None
		if appt["assigned_to"]:
			assigned_to = appt["assigned_to"]["name"]
		else:
			assigned_to = "Unassigned"
		if appt["canceled"] == True:
			cancelled = "Yes"
		else:
			cancelled = "No"
		if appt["lead"]:
			lead_id = appt["lead"]["id"]
			lead = api_response(leads + "/" + lead_id)[0]
			if lead["first_name"]:
				first = lead["first_name"]
			else:
				first = "(blank)"
			if lead["last_name"]:
				last = lead["last_name"]
			else:
				last = "(blank)"
			lead_name = first + " " + last
			if lead["hq_rep"]:
				hq_rep = lead["hq_rep"]["name"]
			else:
				hq_rep = None
			if lead["county"]:
				county = lead["county"]
			else:
				county = "Missing"
			if lead["city"]:
				city = lead["city"]
			else:
				city = "Missing"
			if lead["closer"]:
				closer = lead["closer"]["name"]
			else:
				closer = "Missing"
			if lead["outcome"]:
				outcome = lead["outcome"]
				if outcome == "sale":
					sale = "Yes"
				else:
					sale = "No"
				if outcome == "failedcredit":
					failed_credit = "Yes"
				else:
					failed_credit = "No"
				if outcome == "sale" or outcome == "passedcreditnosale" or outcome == "failedcredit":
					run_credit = "Yes"
				else:
					run_credit = "No"
			else:
				outcome = "Missing"
				sale = "No"
				failed_credit = "No"
				run_credit = "No"
			if lead["status"]:
				if lead["status"]["appointment_sit"] == True:
					sit = "Yes"
				else:
					sit = "No"
				if lead["status"]["sale"] == True:
					sale = "Yes"
				else:
					sale = "No"
			else:
				sit = "No"
				sale = "No"
			if lead["lead_type"]:	
				lead_type = lead["lead_type"]["name"]
			else:
				lead_type = "Missing"
			if lead["ambassador"]:
				amb_id = lead["ambassador"]["id"]
				amb_name = lead["ambassador"]["name"]
				if amb_name in amb_reps:
					lead_type = "Ambassador"
				elif amb_name in event_reps:
					lead_type = "Events"
			else:
				amb_id = None
				amb_name = None
			if lead["lead_Source_Hierarchy"]:
				lead_source_hierarchy = lead["lead_Source_Hierarchy"]
			else:
				lead_source_hierarchy = "Missing"
			if lead["partner"]:
				partner = lead["partner"]["name"]
			else:
				partner = None
			record = Appointment(interaction_id, scheduled_date, assigned_to, cancelled, lead_id, lead_name, lead_type, lead_source_hierarchy, partner, amb_name, amb_id,
                 hq_rep, county, city, closer, outcome, sit, run_credit, failed_credit, sale)
			db.session.add(record)
			db.session.commit()

# removes duplicate appointment records from Appointment table (duplicate = same lead, same scheduled datetime)
def clean_appointment_db():
	lead_ids = []
	dupe_lead_ids = []
	appts = Appointment.query.all()
	for appt in appts:
		lead_ids.append(appt.lead_id)
	for item in lead_ids:
		instances = lead_ids.count(item)
		if instances > 1:
			dupe_lead_ids.append(item)
	for item in dupe_lead_ids:
		dupe_lead_appts = Appointment.query.filter(Appointment.lead_id==item).all()
		keep = dupe_lead_appts.pop()
		for d in dupe_lead_appts:
			db.session.delete(d)
			db.session.commit()

def refresh_db():
	Pipeline.query.delete()
	Permit.query.delete()
	CAD.query.delete()
	Appointment.query.delete()
	fill_permits()
	fill_pipeline(60)
	fill_cad(2)
	fill_appointment(2)
	clean_pipeline_db()
	clean_permit_db()
	clean_cad_db()
	clean_appointment_db()

#refresh_db()

