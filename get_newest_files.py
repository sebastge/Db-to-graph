import requests
import os
from os import listdir
from os.path import isfile, join
from datetime import date

today = date.today()
print("Today's date:", today)

organizations_url = 'https://cordis.europa.eu/data/cordis-h2020organizations.csv'
projects_url = 'https://cordis.europa.eu/data/cordis-h2020projects.csv'
project_deliverables_url = 'https://cordis.europa.eu/data/cordis-h2020projectDeliverables.csv'
project_publications_url = 'https://cordis.europa.eu/data/cordis-h2020projectDeliverables.csv'
report_summaries_url = 'https://cordis.europa.eu/data/cordis-h2020reports.csv'

mypath = 'newest_files/'

file = [f for f in listdir(mypath) if isfile(join(mypath, f))]

for i in file:
    os.replace(f"newest_files/{i}", f"older_files/{i}")

r = requests.get(projects_url, allow_redirects=True)

open(f'newest_files/h2020organizations_{today}.csv', 'wb').write(r.content)

print('done')