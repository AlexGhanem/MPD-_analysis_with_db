import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

#Setting up the connection to the project's BigQuery SQL database
creds = service_account.Credentials.from_service_account_file("./Data/data-key-viewer.json")
project_id="dash-app-318517"
client = bigquery.Client(credentials=creds, project=project_id)

df_arrests = client.query("select * from mpd_dash_database.arrests").to_dataframe()
data_full = client.query("select * from mpd_dash_database.stops").to_dataframe()


data_full.to_csv('stops.csv',index=False)
df_arrests.to_csv('arrests.csv',index=False)