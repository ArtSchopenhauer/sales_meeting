from flask import Flask, render_template
from flask.ext.sqlalchemy import SQLAlchemy, sqlalchemy
import requests
import datetime
import pytz
import json
from operator import itemgetter

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///C:\\Users\\Zack\\desktop\\dboard\\database.db'
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
    cad_date = db.Column(db.DateTime)
    cad_closer = db.Column(db.String)
    cad_outcome = db.Column(db.String)
    cad_notes = db.Column(db.Text)
    permit_submitted = db.Column(db.DateTime)
    permit_received = db.Column(db.DateTime)
    installed = db.Column(db.String)
    hq_notes = db.Column(db.Text)

    def __init__(self, account_id, account_num, name, street_address, city, zip_code, county, municipality, phone, sale_date, closer, site_survey,
                 roof_pass, cad_date, cad_closer, cad_outcome, cad_notes, permit_submitted, permit_received, installed, hq_notes):
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
        self.cad_date = cad_date
        self.cad_closer = cad_closer
        self.cad_outcome = cad_outcome
        self.cad_notes = cad_notes
        self.permit_submitted = permit_submitted
        self.permit_received = permit_received
        self.installed = installed
        self.hq_notes = hq_notes

    def __repr__(self):
        return '%r - %r' % (self.account_num)

#if __name__ == "__main__":
#    app.run(debug=True)

