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
    county = db.Column(db.String)
    cad_outcome_date = db.Column(db.DateTime)
    cad_outcome = db.Column(db.String)
    cad_notes = db.Column(db.Text)

    def __init__(self, name, contact_id, county, cad_outcome_date, cad_outcome, cad_notes):
        self.name = name
        self.contact_id = contact_id
        self.county = county
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
    info_cads = {"counties": [{"county": "Suffolk", "potential_g2g": 0, "g2g":0, "nmt": 0, "designchange": 0, "postponed": 0, "cancelled": 0, "rescheduled": 0},
                              {"county": "Nassau", "potential_g2g": 0, "g2g":0, "nmt": 0, "designchange": 0, "postponed": 0, "cancelled": 0, "rescheduled": 0},
                              {"county": "Richmond", "potential_g2g": 0, "g2g":0, "nmt": 0, "designchange": 0, "postponed": 0, "cancelled": 0, "rescheduled": 0},
                              {"county": "Queens", "potential_g2g": 0, "g2g":0, "nmt": 0, "designchange": 0, "postponed": 0, "cancelled": 0, "rescheduled": 0}],
                 "total": {"potential_g2g": 0, "g2g":0, "nmt": 0, "designchange": 0, "postponed": 0, "cancelled": 0, "rescheduled": 0}}
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
    info = {"kbd": []}
    one_week = datetime.timedelta(days=7)
    week_start = get_week_start()
    for i in range(1,9):
        start = week_start - (one_week * i)
        end = start + one_week
        date = str(start.month) + "/" + str(start.day)
        sales = Pipeline.query.filter(sqlalchemy.and_(Pipeline.sale_date.between(start, end))).all()
        print date + " - " + sales

json_sm()

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

