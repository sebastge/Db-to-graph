import boto3
import requests
from datetime import datetime

today = datetime.now().strftime('%Y-%m-%d')
bucket_name = "se-db"

print('Today: ' + today)


organizations_url = 'https://github.com/nytimes/covid-19-data/blob/master/us-states.csv'
projects_url = 'https://github.com/nytimes/covid-19-data/blob/master/us-states.csv'
project_deliverables_url = 'https://github.com/nytimes/covid-19-data/blob/master/us-states.csv'
project_publications_url = 'https://github.com/nytimes/covid-19-data/blob/master/us-states.csv'
report_summaries_url = 'https://github.com/nytimes/covid-19-data/blob/master/us-states.csv'


def download_to_s3(url, type):
    r = requests.get(url, allow_redirects=True)
    file_name = f'h2020{type}_{today}.csv'
    lambda_path = "/tmp/" + file_name
    s3_path = "newest_files/" + file_name
    open(lambda_path, 'wb').write(r.content)
    s3 = boto3.client("s3")
    s3.upload_file(lambda_path, 'se-db', s3_path)

def move_and_delete_files():
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket('se-db')
    for object_summary in my_bucket.objects.filter(Prefix="newest_files/"):
        file_name = object_summary.key[13:]
        copy_source = {
            'Bucket': f'{object_summary.bucket_name}',
            'Key': f'{object_summary.key}'
        }
        s3.meta.client.copy(copy_source, 'se-db', f'old_files/{object_summary.last_modified.strftime("%Y-%m-%d")}/{file_name}')
        s3.Object('se-db', object_summary.key).delete()

move_and_delete_files()
print(1)
download_to_s3(organizations_url, 'organizations')
print(2)
download_to_s3(projects_url, 'projects')
print(3)
download_to_s3(project_deliverables_url, 'project_deliverables')
print(4)
download_to_s3(project_publications_url, 'project_publications')
print(5)
download_to_s3(report_summaries_url, 'report_summaries')


print('done')