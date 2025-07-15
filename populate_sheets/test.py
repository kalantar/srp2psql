import psycopg2
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from gspread_dataframe import set_with_dataframe

# Define the scope
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Load credentials from the JSON key file
creds = Credentials.from_service_account_file('google-sheets-credentials.json', scopes=SCOPES)

# Authorize the client
client = gspread.authorize(creds)

# Open the sheet by name or URL
# spreadsheet = client.open("cluster")
spreadsheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1IIQuzIu0nROJ8Xkat5AK4ZUNDYxC_wPAr0iydNesNns/edit?gid=0#gid=0")
worksheet = spreadsheet.sheet1  # or spreadsheet.worksheet("Sheet2")



db_options = {
    'dbname': 'kalantar_test',
    'user': 'kalantar',
    'host': 'localhost',
    'port': '5432'
}
connection = psycopg2.connect(**db_options)

# sql = '''select individuals.firstname, individuals.familyname, 
#    EXTRACT(YEAR FROM age(individuals.birthdate)) AS age,
#    localities.name, clusters.name, 

#    activitystudyitemindividuals.individualtype, 
#    studyitems.activitystudyitemtype, localizedstudyitems.shortname, 
#    activitystudyitemindividuals.activitystudyitemid,
#    activities.startdate
# from activitystudyitemindividuals 
# inner join individuals on activitystudyitemindividuals.individualid = individuals.id 
# inner join studyitems on studyitems.id = activitystudyitemindividuals.studyitemid
# inner join localizedstudyitems on localizedstudyitems.studyitemid = studyitems.id
# inner join activities on activities.id = activitystudyitemindividuals.activityid
# inner join localities on localities.id = individuals.localityid
# inner join clusters on clusters.id = localities.clusterid

# where localizedstudyitems.language='en-US' 
#    and activitystudyitemindividuals.individualtype = 1
#    and localizedstudyitems.shortname='B1'
#    and birthdate BETWEEN CURRENT_DATE - INTERVAL '25 years' 
#                   AND CURRENT_DATE - INTERVAL '14 years'

# order by activities.startdate desc
# ;
# '''

sql = '''select individuals.firstname, individuals.familyname, 
   EXTRACT(YEAR FROM age(individuals.birthdate)) AS age,
   localities.name, clusters.name, 
   min(activities.startdate), max(activities.enddate), max(activities.enddate)-min(activities.startdate)
from activitystudyitemindividuals 
inner join individuals on activitystudyitemindividuals.individualid = individuals.id 
inner join studyitems on studyitems.id = activitystudyitemindividuals.studyitemid
inner join localizedstudyitems on localizedstudyitems.studyitemid = studyitems.id
inner join activities on activities.id = activitystudyitemindividuals.activityid
inner join localities on localities.id = individuals.localityid
inner join clusters on clusters.id = localities.clusterid
where localizedstudyitems.language='en-US' 
   and activitystudyitemindividuals.individualtype = 1
   and studyitems.activitystudyitemtype = 'Book'
   and birthdate BETWEEN CURRENT_DATE - INTERVAL '25 years' 
                  AND CURRENT_DATE - INTERVAL '14 years'
group by individuals.firstname, individuals.familyname, age, localities.name, clusters.name 
order by individuals.familyname, individuals.firstname, min(activities.startdate)
;
'''

df = pd.read_sql(sql, connection)

# Populate the sheet (starting at top-left)
set_with_dataframe(worksheet, df)
