from flask import Flask, render_template, request
from flask.ext.sqlalchemy import SQLAlchemy, sqlalchemy
import requests
import datetime
import pytz
import json
from operator import itemgetter

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:////root/dboard/database.db'
db = SQLAlchemy(app)

app.config.update(
    DEBUG = True,
    MAIL_SERVER = 'smtp.gmail.com',
    MAIL_PORT = 465,
    MAIL_USE_SSL = True,
    MAIL_USE_TLS = False,
    MAIL_USERNAME = 'zack.gray@levelsolar.com',
    MAIL_PASSWORD = 'levelsolar'
    )

class Pipeline(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    account_id = db.Column(db.String)
    account_num = db.Column(db.String)
    name = db.Column(db.String)
    street_address = db.Column(db.String)
    city = db.Column(db.String)
    zip_code = db.Column(db.String)
    county = db.Column(db.String)
    municipality = db.Column(db.String)
    phone = db.Column(db.String)
    sale_date = db.Column(db.DateTime)
    closer = db.Column(db.String)
    site_survey = db.Column(db.String)
    roof_pass = db.Column(db.String)
    design_done = db.Column(db.String)
    cad_appt_date = db.Column(db.DateTime)
    cad_closer = db.Column(db.String)
    cad_outcome = db.Column(db.String)
    cad_outcome_date = db.Column(db.DateTime)
    cad_notes = db.Column(db.Text)
    permit_submitted = db.Column(db.DateTime)
    permit_received = db.Column(db.DateTime)
    installed = db.Column(db.String)
    hq_notes = db.Column(db.Text)

    def __init__(self, account_id, account_num, name, street_address, city, zip_code, county, municipality, phone, sale_date, closer, site_survey,
                 roof_pass, design_done, cad_appt_date, cad_outcome_date, cad_closer, cad_outcome, cad_notes, permit_submitted, permit_received, installed, hq_notes):
        self.account_id = account_id
        self.account_num = account_num
        self.name = name
        self.street_address = street_address
        self.city = city
        self.zip_code = zip_code
        self.county = county
        self.municipality = municipality
        self.phone = phone
        self.sale_date = sale_date
        self.closer = closer
        self.site_survey = site_survey
        self.roof_pass = roof_pass
        self.design_done = design_done
        self.cad_appt_date = cad_appt_date
        self.cad_outcome_date = cad_outcome_date
        self.cad_closer = cad_closer
        self.cad_outcome = cad_outcome
        self.cad_notes = cad_notes
        self.permit_submitted = permit_submitted
        self.permit_received = permit_received
        self.installed = installed
        self.hq_notes = hq_notes

    def __repr__(self):
        return '%r' % (self.account_num)

class CAD(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String)
    contact_id = db.Column(db.String)
    account_num = db.Column(db.String)
    city = db.Column(db.String)
    county = db.Column(db.String)
    sale_date = db.Column(db.DateTime)
    closer = db.Column(db.String)
    cad_closer = db.Column(db.String)
    cad_sit = db.Column(db.String)
    cad_outcome_date = db.Column(db.DateTime)
    cad_outcome = db.Column(db.String)
    cad_notes = db.Column(db.Text)

    def __init__(self, name, contact_id, account_num, city, county, sale_date, closer, cad_closer, cad_sit, cad_outcome_date, cad_outcome, cad_notes):
        self.name = name
        self.contact_id = contact_id
        self.account_num = account_num
        self.city = city
        self.county = county
        self.sale_date = sale_date
        self.closer = closer
        self.cad_closer = cad_closer
        self.cad_sit = cad_sit
        self.cad_outcome_date = cad_outcome_date
        self.cad_outcome = cad_outcome
        self.cad_notes = cad_notes

    def __repr__(self):
        return '%r' % (self.name)

class Permit(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    account_num = db.Column(db.String)
    county = db.Column(db.String)
    submitted = db.Column(db.DateTime)
    received = db.Column(db.DateTime)
  
    def __init__(self, account_num, county, submitted, received):
        self.account_num = account_num
        self.county = county
        self.submitted = submitted
        self.received = received

    def __repr__(self):
        return '%r' % (self.account_num)

class Appointment(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    interaction_id = db.Column(db.String)
    scheduled_date = db.Column(db.DateTime)
    assigned_to = db.Column(db.String)
    cancelled = db.Column(db.String)
    lead_id = db.Column(db.String)
    lead_name = db.Column(db.String)
    lead_type = db.Column(db.String)
    lead_source_hierarchy = db.Column(db.String)
    partner = db.Column(db.String)
    amb_name = db.Column(db.String)
    amb_id = db.Column(db.String)
    hq_rep = db.Column(db.String)
    county = db.Column(db.String)
    city = db.Column(db.String)
    closer = db.Column(db.String)
    outcome = db.Column(db.String)
    sit = db.Column(db.String)
    run_credit = db.Column(db.String)
    failed_credit = db.Column(db.String)
    sale = db.Column(db.String)

    def __init__(self, interaction_id, scheduled_date, assigned_to, cancelled, lead_id, lead_name, lead_type, lead_source_hierarchy, partner, amb_name, amb_id,
                 hq_rep, county, city, closer, outcome, sit, run_credit, failed_credit, sale):
        self.interaction_id = interaction_id
        self.scheduled_date = scheduled_date
        self.assigned_to = assigned_to
        self.cancelled = cancelled
        self.lead_id = lead_id
        self.lead_name = lead_name
        self.lead_type = lead_type
        self.lead_source_hierarchy = lead_source_hierarchy
        self.partner = partner
        self.amb_name = amb_name
        self.amb_id = amb_id
        self.hq_rep = hq_rep
        self.county = county
        self.city = city
        self.closer = closer
        self.outcome = outcome
        self.sit = sit
        self.run_credit = run_credit
        self.failed_credit = failed_credit
        self.sale = sale

    def __repr__(self):
        return '%r - %r' % (self.interaction_id, self.lead_name)

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

# returns Monday 12am EST as naive UTC datetime
def get_week_start():
    utc_zone = pytz.timezone('UTC')
    est_zone = pytz.timezone('US/Eastern')
    now_utc_naive = datetime.datetime.utcnow()
    now_utc_aware = utc_zone.localize(now_utc_naive)
    now_est_aware = now_utc_aware.astimezone(est_zone)
    today_12am_est = now_est_aware.replace(hour=0, minute=0, second=0)
    one_day = datetime.timedelta(days=1)
    today_int = now_est_aware.weekday()
    week_start = today_12am_est - (one_day * today_int)
    return week_start

def fill_json(weeks_ago, period=None):
    counties = ["Suffolk", "Nassau", "Richmond", "Queens"]
    categories = ["sales", "ssq", "adq", "designed", "bad_roof", "cancelled", "g2g"]
    info_sales = {"Suffolk": {"county": "Suffolk", "sales": 0, "ssq": 0, "adq": 0, "designed": 0, "bad_roof": 0, "cancelled": 0, "g2g": 0},
                  "Nassau": {"county": "Nassau", "sales": 0, "ssq": 0, "adq": 0, "designed": 0, "bad_roof": 0, "cancelled": 0, "g2g": 0},
                  "Richmond": {"county": "Richmond", "sales": 0, "ssq": 0, "adq": 0, "designed": 0, "bad_roof": 0, "cancelled": 0, "g2g": 0},
                  "Queens": {"county": "Queens", "sales": 0, "ssq": 0, "adq": 0, "designed": 0, "bad_roof": 0, "cancelled": 0, "g2g": 0},
                  "total": {"sales": 0, "ssq": 0, "adq": 0, "designed": 0, "bad_roof": 0, "cancelled": 0, "g2g": 0}}
    info_cads = {"counties": [{"county": "Suffolk", "potential_g2g": 0, "g2g": 0, "nmt": 0, "designchange": 0, "postponed": 0, "cancelled": 0, "rescheduled": 0},
                              {"county": "Nassau", "potential_g2g": 0, "g2g": 0, "nmt": 0, "designchange": 0, "postponed": 0, "cancelled": 0, "rescheduled": 0},
                              {"county": "Richmond", "potential_g2g": 0, "g2g": 0, "nmt": 0, "designchange": 0, "postponed": 0, "cancelled": 0, "rescheduled": 0},
                              {"county": "Queens", "potential_g2g": 0, "g2g": 0, "nmt": 0, "designchange": 0, "postponed": 0, "cancelled": 0, "rescheduled": 0}],
                 "total": {"potential_g2g": 0, "g2g": 0, "nmt": 0, "designchange": 0, "postponed": 0, "cancelled": 0, "rescheduled": 0}}
    info_permits = {"counties": [{"county": "Suffolk", "submitted": 0, "received": 0, "pending": 35, "in_house": 27},
                                 {"county": "Nassau", "submitted": 0, "received": 0, "pending": 34, "in_house": 28},
                                 {"county": "Richmond", "submitted": 0, "received": 0, "pending": 1, "in_house": 0},
                                 {"county": "Queens", "submitted": 0, "received": 0, "pending": 0, "in_house": 0}],
                    "total": {"submitted": 0, "received": 0, "pending": 70, "in_house": 55}}
    one_week = datetime.timedelta(days=7)
    week_start = get_week_start()
    start = week_start - (one_week * weeks_ago)
    if period == "to_date":
        end = week_start
    else:
        end = week_start - (one_week * (weeks_ago - 1))
    sales = Pipeline.query.filter(sqlalchemy.and_(Pipeline.sale_date.between(start, end))).all()
    for sale in sales:
        info_sales[sale.county]["sales"] += 1
        if sale.design_done == "Yes":
            info_sales[sale.county]["designed"] += 1
        if sale.roof_pass == "No":
            info_sales[sale.county]["bad_roof"] += 1
        if sale.cad_outcome == "cancel":
            info_sales[sale.county]["cancelled"] += 1
        if sale.site_survey == "No" and sale.cad_outcome != "cancel":
            info_sales[sale.county]["ssq"] += 1
        if sale.site_survey == "Yes" and sale.cad_outcome != "cancel" and sale.design_done == "No":
            info_sales[sale.county]["adq"] += 1
        if sale.cad_outcome == "goodtogo":
            info_sales[sale.county]["g2g"] +=1
    for county in counties:
        for category in categories:
            info_sales["total"][category] += info_sales[county][category]
    tot = info_sales["total"]
    correction = tot["sales"] - (tot["ssq"] + tot["adq"] + tot["designed"] + tot["bad_roof"] + tot["cancelled"])
    info_sales["Nassau"]["adq"] += correction
    tot["adq"] += correction
    cads = CAD.query.filter(sqlalchemy.and_(CAD.cad_outcome_date.between(start, end))).all()
    for cad in cads:
        for item in info_cads["counties"]:
            if cad.county == item["county"]:
                item["potential_g2g"] += 1
                info_cads["total"]["potential_g2g"] += 1
                if cad.cad_outcome == "goodtogo":
                    item["g2g"] +=1
                    info_cads["total"]["g2g"] += 1
                if cad.cad_outcome == "designchange":
                    item["designchange"] +=1
                    info_cads["total"]["designchange"] += 1
                if cad.cad_outcome == "moretime" or cad.cad_outcome == "moretime-incomplete":
                    item["nmt"] +=1
                    info_cads["total"]["nmt"] += 1
                if cad.cad_outcome == "cancel":
                    item["cancelled"] +=1
                    info_cads["total"]["cancelled"] += 1
                if cad.cad_outcome == "postponed":
                    item["postponed"] +=1
                    info_cads["total"]["postponed"] += 1
                if cad.cad_outcome == "rescheduled":
                    item["rescheduled"] +=1
                    info_cads["total"]["rescheduled"] += 1
    tot_cads = info_cads["total"]
    correction_cads = tot_cads["potential_g2g"] - (tot_cads["g2g"] + tot_cads["nmt"] + tot_cads["designchange"] + tot_cads["postponed"] + tot_cads["cancelled"] + tot_cads["rescheduled"])
    info_cads["counties"][1]["rescheduled"] += correction_cads
    tot_cads["rescheduled"] += correction_cads
    permits_submitted = Permit.query.filter(sqlalchemy.and_(Permit.submitted.between(start, end))).all()
    permits_received = Permit.query.filter(sqlalchemy.and_(Permit.received.between(start, end))).all()
    for item in info_permits["counties"]:
        for permit_submitted in permits_submitted:
            if item["county"] == permit_submitted.county:
                item["submitted"] += 1
                info_permits["total"]["submitted"] += 1
        for permit_received in permits_received:
            if item["county"] == permit_received.county:
                item["received"] += 1
                info_permits["total"]["received"] += 1
    info = {"sales": info_sales, "cads": info_cads, "permits": info_permits}
    return info

def json_sm():
    counties = ["Suffolk", "Nassau", "Richmond", "Queens"]
    info = {"kbd": {"dates": [], "sales": [], "g2g": [], "permits": []},
            "gross": {"total": [0,0,0,0,0,0,0,0], "Suffolk": [], "Nassau": [], "Richmond": [], "Queens": []},
            "g2g": {"total": [0,0,0,0,0,0,0,0], "Suffolk": [], "Nassau": [], "Richmond": [], "Queens": []},
            "permits": {"total": [0,0,0,0,0,0,0,0], "Suffolk": [], "Nassau": [], "Richmond": [], "Queens": []},
            "Suffolk": {"total": [0,0,0,0,0,0,0,0], "amb": [], "schack": [], "events": [], "hq": []},
            "Nassau": {"total": [0,0,0,0,0,0,0,0], "amb": [], "schack": [], "events": [], "hq": []},
            "Richmond": {"total": [0,0,0,0,0,0,0,0], "amb": [], "schack": [], "events": [], "hq": []},
            "Queens": {"total": [0,0,0,0,0,0,0,0], "amb": [], "schack": [], "events": [], "hq": []}
           }
    one_week = datetime.timedelta(days=7)
    week_start = get_week_start()
    for i in range(1,9):
        start = week_start - (one_week * i)
        end = start + one_week
        info["kbd"]["dates"].append(str(start.month) + "/" + str(start.day))
        info["kbd"]["sales"].append(str(len(Pipeline.query.filter(sqlalchemy.and_(Pipeline.sale_date.between(start, end))).all())))
        info["kbd"]["g2g"].append(str(len(CAD.query.filter(sqlalchemy.and_(CAD.cad_outcome_date.between(start, end)), CAD.cad_outcome=="goodtogo").all())))
        info["kbd"]["permits"].append(str(len(Permit.query.filter(sqlalchemy.and_(Permit.submitted.between(start, end))).all())))
        for county in counties:
            gross_sales = len(Pipeline.query.filter(sqlalchemy.and_(Pipeline.sale_date.between(start, end)), Pipeline.county==county).all())
            info["gross"][county].append(str(gross_sales))
            info["gross"]["total"][(i-1)] += gross_sales
            g2g = len(CAD.query.filter(sqlalchemy.and_(CAD.cad_outcome_date.between(start, end)), CAD.county==county, CAD.cad_outcome=="goodtogo").all())
            info["g2g"][county].append(str(g2g))
            info["g2g"]["total"][(i-1)] += g2g
            permits = len(Permit.query.filter(sqlalchemy.and_(Permit.submitted.between(start, end)), Permit.county==county).all())
            info["permits"][county].append(str(permits))
            info["permits"]["total"][(i-1)] += permits
            amb = len(Appointment.query.filter(sqlalchemy.and_(Appointment.scheduled_date.between(start, end)), Appointment.county==county, Appointment.lead_type=="Ambassador", Appointment.outcome=="sale").all())
            info[county]["amb"].append(str(amb))
            schack = len(Pipeline.query.filter(sqlalchemy.and_(Pipeline.sale_date.between(start, end)), Pipeline.county==county, Pipeline.closer=="Robert Schack").all())
            info[county]["schack"].append(str(schack))
            events = len(Appointment.query.filter(sqlalchemy.and_(Appointment.scheduled_date.between(start, end)), Appointment.county==county, Appointment.lead_type=="Events").all())
            info[county]["events"].append(str(events))
            info[county]["total"][(i-1)] = gross_sales
            hq = gross_sales - (amb + events + schack)
            if hq < 0:
                hq = abs(hq)
            info[county]["hq"].append(hq)

    #for value in info["kbd"].itervalues():
    #    value.reverse()
    return info

def json_closers():
    counties = ["Suffolk", "Nassau", "Richmond"]
    info = {"Suffolk": {"closers":
                            [{"closer": "Josh Lilly", "sales": 0, "assigned": 0, "sits": 0, "run_credit": 0, "failed_credit": 0, "run_rate": 0, "close_rate": 0, "g2g": 0},
                             {"closer": "Richard Kahn", "sales": 0, "assigned": 0, "sits": 0, "run_credit": 0, "failed_credit": 0, "run_rate": 0, "close_rate": 0, "g2g": 0},
                             {"closer": "Steven Cook", "sales": 0, "assigned": 0, "sits": 0, "run_credit": 0, "failed_credit": 0, "run_rate": 0, "close_rate": 0, "g2g": 0},
                             {"closer": "Robert Schack", "sales": 0, "assigned": 0, "sits": 0, "run_credit": 0, "failed_credit": 0, "run_rate": 0, "close_rate": 0, "g2g": 0}],
                        "total":
                             {"sales": 0, "assigned": 0, "sits": 0, "run_credit": 0, "failed_credit": 0, "run_rate": 0, "close_rate": 0, "g2g": 0}},
            "Nassau":  {"closers": 
                            [{"closer": "Tyler Rhoton", "sales": 0, "assigned": 0, "sits": 0, "run_credit": 0, "failed_credit": 0, "run_rate": 0, "close_rate": 0, "g2g": 0},
                             {"closer": "Brandon Parlante", "sales": 0, "assigned": 0, "sits": 0, "run_credit": 0, "failed_credit": 0, "run_rate": 0, "close_rate": 0, "g2g": 0},
                             {"closer": "Mark Campbell", "sales": 0, "assigned": 0, "sits": 0, "run_credit": 0, "failed_credit": 0, "run_rate": 0, "close_rate": 0, "g2g": 0}],
                        "total":
                             {"sales": 0, "assigned": 0, "sits": 0, "run_credit": 0, "failed_credit": 0, "run_rate": 0, "close_rate": 0, "g2g": 0}},

            "Richmond": {"closers":
                            [{"closer": "Taylor Colucci", "sales": 0, "assigned": 0, "sits": 0, "run_credit": 0, "failed_credit": 0, "run_rate": 0, "close_rate": 0, "g2g": 0},
                            {"closer": "Anthony Quezada", "sales": 0, "assigned": 0, "sits": 0, "run_credit": 0, "failed_credit": 0, "run_rate": 0, "close_rate": 0, "g2g": 0}],
                        "total": {"sales": 0, "assigned": 0, "sits": 0, "run_credit": 0, "failed_credit": 0, "run_rate": 0, "close_rate": 0, "g2g": 0}},
            "Elliott": {"sales": 0, "g2g": 0},
            "total": {"sales": 0, "assigned": 0, "sits": 0, "run_credit": 0, "failed_credit": 0, "run_rate": 0, "close_rate": 0, "g2g": 0}
           }
    one_week = datetime.timedelta(days=7)
    end = get_week_start()
    start = end - one_week
    all_g2g = CAD.query.filter(sqlalchemy.and_(CAD.cad_outcome_date.between(start, end)), CAD.cad_outcome=="goodtogo").all()
    info["total"]["g2g"] = str(len(all_g2g))
    for item in all_g2g:
        closer = Pipeline.query.filter(Pipeline.name==item.name).first().closer
        if closer == "Steven Elliott":
            info["Elliott"]["g2g"] += 1
        for county in counties:
            for thing in info[county]["closers"]:
                if thing["closer"] == closer:
                    thing["g2g"] += 1
    for county in counties:
        for item in info[county]["closers"]:
            item["sales"] = str(len(Pipeline.query.filter(sqlalchemy.and_(Pipeline.sale_date.between(start, end)), Pipeline.closer==item["closer"]).all()))
            info[county]["total"]["sales"] += int(item["sales"])
            item["assigned"] = str(len(Appointment.query.filter(sqlalchemy.and_(Appointment.scheduled_date.between(start, end)), Appointment.assigned_to==item["closer"]).all()))
            info[county]["total"]["assigned"] += int(item["assigned"])
            info["total"]["assigned"] += int(item["assigned"])
            item["sits"] = str(len(Appointment.query.filter(sqlalchemy.and_(Appointment.scheduled_date.between(start, end)), Appointment.assigned_to==item["closer"],
                                   Appointment.sit=="Yes").all()))
            info[county]["total"]["sits"] += int(item["sits"])
            info["total"]["sits"] += int(item["sits"])
            item["run_credit"] = str(len(Appointment.query.filter(sqlalchemy.and_(Appointment.scheduled_date.between(start, end)), Appointment.assigned_to==item["closer"],
                                   Appointment.run_credit=="Yes").all()))
            info[county]["total"]["run_credit"] += int(item["run_credit"])
            info["total"]["run_credit"] += int(item["run_credit"])
            item["failed_credit"] = str(len(Appointment.query.filter(sqlalchemy.and_(Appointment.scheduled_date.between(start, end)), Appointment.assigned_to==item["closer"],
                                   Appointment.failed_credit=="Yes").all()))
            info[county]["total"]["failed_credit"] += int(item["failed_credit"])
            info["total"]["failed_credit"] += int(item["failed_credit"])
            if int(item["sits"]) == 0:
                item["run_rate"] = "0%"
                item["close_rate"] = "0%"
            else:
                item["run_rate"] = "{0:.0f}%".format((float(item["run_credit"])/float(item["sits"]))*100)
                item["close_rate"] = "{0:.0f}%".format((float(item["sales"])/float(item["sits"]))*100)
        if info[county]["total"]["sits"] > 0:
            info[county]["total"]["run_rate"] = "{0:.0f}%".format((float(info[county]["total"]["run_credit"]) / float(info[county]["total"]["sits"]))*100)
            info[county]["total"]["close_rate"] = "{0:.0f}%".format((float(info[county]["total"]["sales"]) / float(info[county]["total"]["sits"]))*100)
        else:
            info[county]["total"]["run_rate"] = "0%"
            info[county]["total"]["close_rate"] = "0%"
    info["Elliott"]["sales"] = str(len(Pipeline.query.filter(sqlalchemy.and_(Pipeline.sale_date.between(start, end)), Pipeline.closer=="Steven Elliott").all()))
    info["total"]["sales"] = str(len(Pipeline.query.filter(sqlalchemy.and_(Pipeline.sale_date.between(start, end))).all()))
    if info["total"]["sits"] > 0:
        info["total"]["run_rate"] = "{0:.0f}%".format((float(info["total"]["run_credit"]) / float(info["total"]["sits"]))*100)
        info["total"]["close_rate"] = "{0:.0f}%".format((float(info["total"]["sales"]) / float(info["total"]["sits"]))*100)
    else:
        info[county]["total"]["run_rate"] = "0%"
        info[county]["total"]["close_rate"] = "0%"
    return info

def json_cads():
    info = {"Suffolk": {"sits": 0, "g2g": 0, "g2g_rate": 0, "outcomes": []},
            "Nassau": {"sits": 0, "g2g": 0, "g2g_rate": 0, "outcomes": []},
            "Richmond": {"sits": 0, "g2g": 0, "g2g_rate": 0, "outcomes": []},
            "Missing": {"sits": 0, "g2g": 0, "g2g_rate": 0, "outcomes": []}
            }
    counties = ["Suffolk", "Nassau", "Richmond", "Missing"]
    one_week = datetime.timedelta(days=7)
    end = get_week_start()
    start = end - one_week
    outcomes_lw = CAD.query.filter(sqlalchemy.and_(CAD.cad_outcome_date.between(start, end))).all()
    for item in outcomes_lw:
        name = item.name
        county = item.county
        account_num = item.account_num
        city = item.city
        sale_date = (str(item.sale_date.month) + "/" + str(item.sale_date.day))
        closer = item.closer
        cad_outcome_date = (str(item.cad_outcome_date.month) + "/" + str(item.cad_outcome_date.day))
        cad_outcome = item.cad_outcome
        if cad_outcome == "Good To Go":
            order = 1
        else:
            order = 2
        cad_notes = item.cad_notes
        cad_closer = "Sarah Krolus"
        if len(Permit.query.filter(Permit.account_num==account_num).all()) > 0:
            permit_submitted = "Yes"
        else:
            permit_submitted = "No"
        cad_sit = item.cad_sit
        if cad_sit == "Yes":
            info[county]["sits"] += 1
        if cad_outcome == "Good To Go":
            info[county]["g2g"] += 1
        record = {"account_num": account_num, "name": name, "city": city, "sale_date": sale_date, "closer": closer,
                  "cad_outcome_date": cad_outcome_date, "cad_closer": cad_closer, "cad_outcome": cad_outcome, "cad_notes": cad_notes,
                  "permit_submitted": permit_submitted, "order": order}
        info[county]["outcomes"].append(record)
    for county in counties:
        info[county]["outcomes"] = sorted(info[county]["outcomes"], key=itemgetter("order"))
        if info[county]["sits"] > 0:
            info[county]["g2g_rate"] = "{0:.0f}%".format((float(info[county]["g2g"])/float(info[county]["sits"]))*100)
        else:
            info[county]["g2g_rate"] = "0%"
    return info

def append_deal(deal, json):
        if deal.cad_outcome_date:
            deal.cad_outcome_date = (str(deal.cad_outcome_date.month) + "/" + str(deal.cad_outcome_date.day))
        if deal.sale_date:
            deal.sale_date = (str(deal.sale_date.month) + "/" + str(deal.sale_date.day))
        if deal.permit_submitted:
            deal.permit_submitted = "Yes"
        else:
            deal.permit_submitted = "No"
        if deal.cad_outcome == "goodtogo" or deal.cad_outcome == "Good To Go":
            json["summary"]["g2g"] += 1
        json["summary"]["sales"] += 1
        json["deals"].append(deal)


def json_deals(county, closer, status):
    info = {"deals": [], "summary": {"sales": 0, "g2g": 0}}
    if county == "All Counties" and closer == "All Closers":
        deals = Pipeline.query.all()
    elif county == "All Counties":
        deals = Pipeline.query.filter(Pipeline.closer==closer).all()
    elif closer == "All Closers":
        deals = Pipeline.query.filter(Pipeline.county==county)
    else:
        deals = Pipeline.query.filter(Pipeline.county==county, Pipeline.closer==closer)
    for deal in deals:
        if status == "Good To Go":
            if deal.cad_outcome == "goodtogo" or deal.cad_outcome == "Good To Go":
                append_deal(deal, info)
        elif status == "Not G2G":
            if deal.cad_outcome != "goodtogo" and deal.cad_outcome != "Good To Go":
                append_deal(deal, info)
        else:
            append_deal(deal, info)
    return info

@app.route('/deals', methods=["GET", "POST"])
def deals():
    if request.method == "POST":
        county = request.form["county"]
        closer = request.form["closer"]
        status = request.form["status"]
        info = json_deals(county, closer, status)
        return render_template("deal_review.html", info=info, closer=closer, county=county, status=status)
    else:
        county = "All Counties"
        closer = "All Closers"
        status = "All Deals"
        info = json_deals(county, closer, status)
        return render_template("deal_review.html", info=info, closer=closer, county=county, status=status)

@app.route("/sm")
def sm():
    info = json_sm()
    return render_template("salesmeeting.html", info=info)

@app.route("/closers")
def closers():
    info = json_closers()
    return render_template("closers.html", info=info)

@app.route("/cads/suffolk")
def cads_suffolk():
    info = json_cads()
    return render_template("cads_suffolk.html", info=info)

@app.route("/cads/nassau")
def cads_nassau():
    info = json_cads()
    return render_template("cads_nassau.html", info=info)

@app.route('/', methods=["GET", "POST"])
def two_pm():
    if request.method == "POST":
        period = request.form["period"]
        if period == "lastweek":
            info = fill_json(1)
            week = "Last Week"
            return render_template("2pm.html", info=info, week=week)
        if period == "twoweeksago":
            info = fill_json(2)
            week = "Two Weeks Ago"
            return render_template("2pm.html", info=info, week=week)
        if period == "pastmonth":
            info = fill_json(4, "to_date")
            week = "Past Month"
            return render_template("2pm.html", info=info, week=week)
        if period == "yeartodate":
            weeks_ago = datetime.datetime.now().isocalendar()[1]
            info = fill_json(weeks_ago, "to_date")
            week = "Year To Date"
            return render_template("2pm.html", info=info, week=week)
    else:
        info = fill_json(1)
        week = "Last Week"
        return render_template("2pm.html", info=info, week=week)

@app.route('/ahead', methods=["GET", "POST"])
def two_pm_ahead():
    return render_template("2pmahead.html")

@app.route('/test', methods=["GET", "POST"])
def test():
    sfdc = request.form["sfdc"]
    second = request.form["second"]
    customer = Pipeline.query.filter(Pipeline.account_num==sfdc).first()
    name = customer.name
    reply = name + " and " + second
    return reply


#if __name__ == "__main__":
 #   app.run(debug=True)

