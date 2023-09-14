from google.cloud import bigquery
import os
import re
import datetime

def crete_dataset_and_table_bigquery(data, context):
    today = datetime.date.today()
    client = bigquery.Cliente()
    dataset_name = format(data['name'])
    date = str(today)
    dataset_name_solo = date + os.path.splitext(dataset_name)[0]
    dataset_name_limpia = re.sub(r'[^a-zA-Z0-9_\s]+', '', dataset_name_solo)
    dataset_name_solo_10 = dataset_name_limpia[0:30]
    print("name file:----------------------------------- " + dataset_name_solo_10)
    dataset_id = "{}.{}".format(client.project, dataset_name_solo_10)
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "us-west1"
    dataset = client.create_dataset(dataset)
    print("created dataset {}.{}".format(client.project, dataset.dataset_id))
