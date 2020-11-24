import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from queries import drop_and_recreate_db


class PopulateDatabase(object):

    def __init__(self):
        self.organizations_raw = pd.read_csv("cordis-h2020organizations.csv", encoding='utf-8', error_bad_lines=False,
                                             sep=';')
        print('Shape of organizations DataFrame: ' + str(self.organizations_raw.shape))

        self.projects_raw = pd.read_csv("cordis-h2020projects.csv", encoding='utf-8', error_bad_lines=False, sep=';')
        print('Shape of projects DataFrame: ' + str(self.projects_raw.shape))

        self.deliverables_raw = pd.read_csv("cordis-h2020projectDeliverables.csv", encoding='utf-8',
                                            error_bad_lines=False, sep=';')
        print('Shape of deliverables DataFrame: ' + str(self.projects_raw.shape))

        # Create sqlalchemy engine to hangle sql queries

        #self.engine = create_engine("mysql+pymysql://" + 'root' + ":" + 'N4tt1ng51993!' + "@" + 'localhost' + "/" + 'cordis')

        self.engine = create_engine(
            "mysql+pymysql://" + 'admin' + ":" + 'N4tt1ng51993!' + "@" + 'aws-se2.clwi7ydbgwxc.us-east-2.rds.amazonaws.com' + "/" + 'cordis')


        drop_and_recreate_db(self.engine)

    # Open organizations .csv file and loading as pd.DataFrame

    def populate_countries_table(self):

        # Create a copy pd.DataFrame of all the countries

        self.countries = self.organizations_raw[['country']].copy()
        self.countries['country'].fillna('OTHER', inplace=True)

        country_list = self.countries['country'].unique()

        # Create empty countries pd.DataFrame. Populating it with list of countries

        self.countries = pd.DataFrame(columns=['country_code'])
        self.countries['country_code'] = country_list

        # Sort the frame and give each country an ID based on the index. AA = 0 etc.

        self.countries = self.countries.sort_values(by=['country_code'])
        self.countries = self.countries.reset_index(drop=True)
        self.countries['id'] = self.countries.index
        cols = list(self.countries.columns)
        a, b = cols.index('country_code'), cols.index('id')
        cols[b], cols[a] = cols[a], cols[b]
        self.countries = self.countries[cols]

        # Populate country table with countries pd.DataFrame

        self.countries.to_sql('country', con=self.engine, if_exists='append', index=False, chunksize=1000)

    def populate_projects_table(self):
        # Remove columns not needed and loading into new pd.DataFrame

        projects = self.projects_raw[
            ['id', 'acronym', 'status', 'title', 'startDate', 'endDate', 'projectUrl', 'objective', 'totalCost', 'call',
             'fundingScheme']].copy()

        # Converting currency to float. Given in €

        projects['totalCost'] = self.projects_raw['totalCost'].str.replace(',', '.').astype(float)

        # Rename columns to match the database columns
        projects_sql = projects.rename(columns={'acronym': 'project_acronym', 'status': 'project_status',
                                                'title': 'project_title', 'startDate': 'project_start_date',
                                                'endDate': 'project_end_date', 'projectUrl': 'project_url',
                                                'objective': 'project_objective', 'totalCost': 'project_total_cost',
                                                'call': 'project_call', 'fundingScheme': 'project_funding_scheme'})

        # Remove any duplicates from projects pd.DataFrame

        projects_sql.drop_duplicates(subset="id", keep='first', inplace=True)

        print('Rows in projects pd.DataFrame after removing duplicates: ' + str(projects_sql.shape[0]))

        # Populate project table with projects pd.DataFrame

        projects_sql.to_sql('project', con=self.engine, if_exists='append', index=False, chunksize=1000)

    def make_programmes_df(self):

        # Make copy of projects_raw
        programmes = self.projects_raw

        # Remove H2020- substring from programme string
        programmes['programme'] = programmes['programme'].str.replace(r'H2020-', '')

        # Variable name of column to be used in function below

        lst_col = 'programme'

        # Create pd.DataFrame copy where the programme string is split on ; and put into list

        x = self.projects_raw.assign(**{lst_col: self.projects_raw[lst_col].str.split(';')})

        # Create a pd.DataFrame copy where each programme-list with more than one element is exploded into a new row
        programmes = pd.DataFrame(
            {col: np.repeat(x[col].values, x[lst_col].str.len()) for col in x.columns.difference([lst_col])}).assign(
            **{lst_col: np.concatenate(x[lst_col].values)})[x.columns.tolist()]

        # Create a list to hold the unique programmes

        programme_list = []

        # Create a a list where all the unieq programmes are copied from the pd.DataFrame

        programme_strings = programmes.programme.unique()

        # Loop through list, split items on ; and add to programme_list
        for i in programme_strings:
            raw_string = i.split(';')
            for k in raw_string:
                programme_list.append(k)

        # Create a new pd.DataFrame that contains the programme names and a given id based on the index. Name is sorted.

        programmes = pd.DataFrame(columns=['programme'])
        programmes['programme'] = programme_list
        programmes = programmes.sort_values(by=['programme'])
        programmes = programmes.reset_index(drop=True)
        programmes['id'] = programmes.index
        cols = list(programmes.columns)
        a, b = cols.index('programme'), cols.index('id')
        cols[b], cols[a] = cols[a], cols[b]
        programmes = programmes[cols]
        programmes = programmes.rename(columns={'programme': 'programme_name'})

        return programmes

    def populate_programmes_table(self):

        programmes_sql = self.make_programmes_df()

        # Populate programmes table with programmes pd.DataFrame

        programmes_sql.to_sql('programme', con=self.engine, if_exists='append', index=False, chunksize=1000)
        print(programmes_sql.head())

    def populate_programme_project_table(self):

        programmes = self.make_programmes_df()

        lst_col = 'programme'

        # Create pd.DataFrame copy where the programme string is split on ; and put into list

        x = self.projects_raw.assign(**{lst_col: self.projects_raw[lst_col].str.split(';')})

        # Create a pd.DataFrame copy where each programme-list with more than one element is exploded into a new row
        programmes_df = pd.DataFrame(
            {col: np.repeat(x[col].values, x[lst_col].str.len()) for col in x.columns.difference([lst_col])}).assign(
            **{lst_col: np.concatenate(x[lst_col].values)})[x.columns.tolist()]

        # Create a pd.DataDRame copy where only the project id and programme name is included
        project_id_programme_string = programmes_df[["id", "programme"]]

        # Create copy of pd.DataFrame where columns are renmaed
        project_id_programme_string = project_id_programme_string.rename(
            columns={'id': 'project_id', 'programme': 'programme_name'})

        print(project_id_programme_string.head())

        # Create a dict with programme name and id
        mapping_dict = pd.Series(programmes.id.values, index=programmes.programme_name).to_dict()

        # Create a pd.DataFrame copy with project id and connected program id
        project_id_programme_id = project_id_programme_string.replace({'programme_name': mapping_dict})
        project_id_programme_id = project_id_programme_id.rename(columns={'programme_name': 'programme_id'})

        # Populate programme_project table with programmes pd.DataFrame

        project_id_programme_id.to_sql('programme_project', con=self.engine, if_exists='append', index=False,
                                       chunksize=1000)

    def populate_organization_table(self):

        # Remove columns not needed and loading into new pd.DataFrame

        organizations = self.organizations_raw[
            ['id', 'name', 'shortName', 'country', 'street', 'city', 'postCode', 'organizationUrl', 'vatNumber',
             'contactForm']].copy()

        # Create a pd.DataDRame copy where only the project id and programme name is included
        organization_id_country_code = organizations[["id", "country"]]

        # Create copy of pd.DataFrame where columns are renmaed
        organization_id_country_code = organization_id_country_code.rename(
            columns={'id': 'organization_id', 'country': 'country_code'})

        # Create a dict with programme name and id
        mapping_dict = pd.Series(self.countries.id.values, index=self.countries.country_code).to_dict()

        # Create a pd.DataFrame copy with project id and connected program id
        organization_id_country_code['country_code'].fillna('OTHER', inplace=True)
        organization_id_country_id = organization_id_country_code.replace({'country_code': mapping_dict})
        organization_id_country_id = organization_id_country_id.rename(columns={'country_code': 'country_id'})

        # Remove any duplicates from projects pd.DataFrame

        organization_id_country_id.drop_duplicates(subset="organization_id", keep='first', inplace=True)

        organizations.country = organization_id_country_id.country_id

        organizations.drop_duplicates(subset="id", keep='first', inplace=True)

        organizations['country'] = organizations['country'].astype(int)

        organizations = organizations.rename(columns={'country': 'country_id'})

        # Rename columns to match database

        organizations_sql = organizations.rename(
            columns={'name': 'organization_name', 'shortName': 'organization_acronym',
                     'street': 'organization_street', 'city': 'organization_city', 'postCode':
                         'organization_post_code', 'organizationUrl': 'organization_url', 'vatNumber'
                     : 'organization_vat_num', 'contactForm': 'organization_contact_form'})

        # Remove duplicates
        organizations_sql.drop_duplicates(subset="id", keep='first', inplace=True)

        organizations_sql.to_sql('organization', con=self.engine, if_exists='append', index=False, chunksize=1000)

    def populate_project_participation_table(self):
        # Create pd.DataFrame copy from organizations_raw

        project_participations = self.organizations_raw[
            ['projectID', 'role', 'id', 'activityType', 'endOfParticipation', 'ecContribution']].copy()

        # Convert currency string to float. Given in €

        project_participations['ecContribution'] = project_participations['ecContribution'].str.replace(',', '.'). \
            astype(float)

        # Create a pd.DataFrame copy and rename to match database

        project_participations_sql = project_participations.rename(
            columns={'projectID': 'project_id', 'role': 'project_participation_role',
                     'id': 'organization_id', 'activityType': 'project_participation_activity_type',
                     'endOfParticipation': 'project_participation_ended',
                     'ecContribution': 'project_participation_contribution'})

        project_participations_sql = project_participations_sql.drop_duplicates(keep=False)

        project_participations_sql.to_sql('project_participation', con=self.engine, if_exists='append', index=False,
                                          chunksize=1000)

    def populate_deliverable_table(self):

        # Remove columns not needed and loading into new pd.DataFrame

        deliverables = self.deliverables_raw[
            ['title', 'projectID', 'description', 'deliverableType', 'url', 'lastUpdateDate']].copy()

        # Rename columns to match the database columns
        deliverables_sql = deliverables.rename(columns={'title': 'deliverable_title', 'projectID': 'project_id',
                                                        'description': 'deliverable_description',
                                                        'deliverableType': 'deliverable_type',
                                                        'url': 'deliverable_url',
                                                        'lastUpdatedate': 'deliverable_last_updated_at',
                                                        'lastUpdateDate': 'deliverable_last_updated_at'})

        deliverables_sql['id'] = deliverables_sql.index

        # Remove any duplicates from projects pd.DataFrame

        deliverables_sql.drop_duplicates(subset="id", keep='first', inplace=True)

        print('Rows in projects pd.DataFrame after removing duplicates: ' + str(deliverables_sql.shape[0]))

        # Populate deliverable table with deliverables pd.DataFrame

        deliverables_sql.to_sql('deliverable', con=self.engine, if_exists='append', index=False, chunksize=1000)

    def populate_funding_scheme_table(self):

        # Create a pd.DataFrame copy of projects_raw to get the different funding schemes

        self.funding_schemes = self.projects_raw[['fundingScheme']].copy()

        # Creata a list where all the unique funding_schemes are copied from the pd.DataFrame

        funding_scheme_list = self.funding_schemes.fundingScheme.unique()

        # Create a new pd.DataFrame that contains the programme names and a given id based on the index. Name is sorted.

        funding_scheme_list = list(dict.fromkeys(funding_scheme_list))

        self.funding_schemes = pd.DataFrame(columns=['funding_scheme_name'])

        self.funding_schemes['funding_scheme_name'] = funding_scheme_list

        self.funding_schemes = self.funding_schemes.sort_values(by=['funding_scheme_name'])
        self.funding_schemes = self.funding_schemes.reset_index(drop=True)
        self.funding_schemes['id'] = self.funding_schemes.index
        cols = list(self.funding_schemes.columns)
        a, b = cols.index('funding_scheme_name'), cols.index('id')
        cols[b], cols[a] = cols[a], cols[b]
        self.funding_schemes = self.funding_schemes[cols]

        # Populate programmes table with programmes pd.DataFrame

        self.funding_schemes.to_sql('funding_scheme', con=self.engine, if_exists='append', index=False, chunksize=1000)

    def populate_project_funding_scheme_table(self):

        # Create copy of pd.DataFrame where columns are renmaed
        project_id_funding_scheme_string = self.projects_raw[['id', 'fundingScheme']].copy()
        project_id_funding_scheme_string = project_id_funding_scheme_string.rename(
            columns={'id': 'project_id', 'fundingScheme': 'funding_scheme_name'})

        # Create a dict with programme name and id
        mapping_dict = pd.Series(self.funding_schemes.id.values,
                                 index=self.funding_schemes.funding_scheme_name).to_dict()

        # Create a pd.DataFrame copy with project id and connected program id

        project_id_funding_scheme_id = project_id_funding_scheme_string.replace({'funding_scheme_name': mapping_dict})
        project_id_funding_scheme_id = project_id_funding_scheme_id.rename(
            columns={'funding_scheme_name': 'funding_scheme_id'})

        # Populate programme_project table with programmes pd.DataFrame

        project_id_funding_scheme_id.to_sql('project_funding_scheme', con=self.engine, if_exists='append', index=False,
                                            chunksize=1000)

    def run(self):
        self.populate_countries_table()
        self.populate_projects_table()
        self.populate_programmes_table()
        self.populate_programme_project_table()
        self.populate_organization_table()
        self.populate_project_participation_table()
        self.populate_deliverable_table()
        self.populate_funding_scheme_table()
        self.populate_project_funding_scheme_table()


x = PopulateDatabase()

x.run()
