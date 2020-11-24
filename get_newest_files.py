import requests
import os
from datetime import date

today = date.today()
print("Today's date:", today)

organizations_url = 'https://cordis.europa.eu/data/cordis-h2020organizations.csv'
projects_url = 'https://cordis.europa.eu/data/cordis-h2020projects.csv'
project_deliverables_url = 'https://cordis.europa.eu/data/cordis-h2020projectDeliverables.csv'
project_publications_url = 'https://cordis.europa.eu/data/cordis-h2020projectDeliverables.csv'
report_summaries_url = 'https://cordis.europa.eu/data/cordis-h2020reports.csv'

os.replace("/newest_files/cordis-h2020projects.csv", "older_files/cordis-h2020projects.csv")