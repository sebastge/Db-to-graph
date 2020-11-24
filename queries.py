# Predefined query strings

create_country_table_query = 'CREATE TABLE IF NOT EXISTS country (id INT, country_code VARCHAR(100), PRIMARY KEY (id))'

create_project_query = 'CREATE TABLE IF NOT EXISTS project (id INT, project_acronym VARCHAR(100) NULL DEFAULT ' \
                       'NULL, project_status VARCHAR(45) NULL DEFAULT NULL, project_title VARCHAR(1000) NULL ' \
                       'DEFAULT NULL,  project_start_date DATE NULL DEFAULT NULL,  project_end_date DATE NULL ' \
                       'DEFAULT NULL, project_url VARCHAR(1000) NULL DEFAULT NULL, project_objective BLOB ' \
                       'NULL DEFAULT NULL, project_total_cost DECIMAL(13,2) NULL DEFAULT NULL, ' \
                       'project_contribution DECIMAL(13,2) NULL DEFAULT NULL, project_call VARCHAR(1000) NULL ' \
                       'DEFAULT NULL, project_funding_scheme VARCHAR(300) NULL DEFAULT NULL, PRIMARY KEY (id)) '

create_organization_query = 'CREATE TABLE IF NOT EXISTS organization (id INT, organization_name VARCHAR(1000) ' \
                            'NULL DEFAULT NULL, organization_acronym VARCHAR(1000) NULL DEFAULT NULL, ' \
                            'country_id INT NULL DEFAULT NULL, organization_city VARCHAR(' \
                            '1000) NULL DEFAULT NULL, organization_post_code VARCHAR(1000) NULL DEFAULT NULL, ' \
                            'organization_street VARCHAR(1000) NULL DEFAULT NULL, organization_url VARCHAR(' \
                            '1000) NULL DEFAULT NULL, organization_vat_num VARCHAR(1000) NULL DEFAULT NULL, ' \
                            'organization_contact_form VARCHAR(1000) NULL DEFAULT NULL, PRIMARY KEY (id), FOREIGN ' \
                            'KEY(country_id) REFERENCES country(id) ON DELETE CASCADE) '

create_programme_query = 'CREATE TABLE IF NOT EXISTS programme (id INT, parent_id INT, programme_name ' \
                         'VARCHAR(300) NULL DEFAULT NULL, programme_funding DECIMAL(13,2), ' \
                         'programme_objective BLOB NULL DEFAULT NULL, country_id INT, PRIMARY KEY (id), FOREIGN KEY(' \
                         'country_id) REFERENCES country(id) ON DELETE CASCADE) '

create_project_participation_query = 'CREATE TABLE IF NOT EXISTS project_participation (project_id INT, ' \
                                     'organization_id INT, project_participation_role VARCHAR(100) NULL ' \
                                     'DEFAULT NULL, project_participation_activity_type VARCHAR(100) NULL ' \
                                     'DEFAULT NULL, project_participation_ended VARCHAR(50) NULL DEFAULT ' \
                                     'NULL, project_participation_contribution DECIMAL(13,2) NULL DEFAULT ' \
                                     'NULL, PRIMARY KEY (project_id, organization_id), FOREIGN KEY(' \
                                     'project_id) REFERENCES project(id) ON DELETE CASCADE, FOREIGN KEY(' \
                                     'organization_id) REFERENCES organization(id) ON DELETE CASCADE) '

create_programme_project_query = 'CREATE TABLE IF NOT EXISTS programme_project (programme_id INT, project_id ' \
                                 'INT, PRIMARY KEY (programme_id, project_id), FOREIGN KEY(programme_id) ' \
                                 'REFERENCES programme(id) ON DELETE CASCADE, FOREIGN KEY(project_id) ' \
                                 'REFERENCES project(id) ON DELETE CASCADE) '

create_deliverable_table_query = 'CREATE TABLE IF NOT EXISTS deliverable (id INT, deliverable_title VARCHAR(' \
                                 '1000), project_id INT, deliverable_description BLOB, deliverable_type ' \
                                 'VARCHAR(300), deliverable_url VARCHAR(300), deliverable_last_updated_at ' \
                                 'DATE, PRIMARY KEY (id), FOREIGN KEY(project_id) REFERENCES project(id) ON ' \
                                 'DELETE CASCADE) '

create_funding_scheme_table_query = 'CREATE TABLE IF NOT EXISTS funding_scheme (id INT, funding_scheme_name ' \
                                    'VARCHAR(1000), PRIMARY KEY (id)) '

create_project_funding_scheme_table_query = 'CREATE TABLE IF NOT EXISTS project_funding_scheme (project_id ' \
                                            'INT, funding_scheme_id INT, PRIMARY KEY (project_id, ' \
                                            'funding_scheme_id), FOREIGN KEY(project_id) REFERENCES project(' \
                                            'id) ON DELETE CASCADE, FOREIGN KEY(funding_scheme_id) REFERENCES ' \
                                            'funding_scheme(id) ON DELETE CASCADE) '

drop_top_level_db_query = 'DROP TABLE IF EXISTS programme_project, project_participation, deliverable, ' \
                          'project_funding_scheme; '

drop_lower_level_db_query = 'DROP TABLE IF EXISTS project, programme, organization, funding_scheme, country;'


# Drop all tables in database if the exist and then create them again

def drop_and_recreate_db(engine):
    with engine.connect() as connection:
        connection.execute(drop_top_level_db_query)
        connection.execute(drop_lower_level_db_query)
        connection.execute(create_country_table_query)
        connection.execute(create_project_query)
        connection.execute(create_organization_query)
        connection.execute(create_programme_query)
        connection.execute(create_project_participation_query)
        connection.execute(create_programme_project_query)
        connection.execute(create_deliverable_table_query)
        connection.execute(create_funding_scheme_table_query)
        connection.execute(create_project_funding_scheme_table_query)

        print('done')
