import pandas as pd
import geopandas as gp
from google.cloud import bigquery
from google.oauth2 import service_account

#Setting up the connection to the project's BigQuery SQL database
creds = service_account.Credentials.from_service_account_file("./Data/data-key-viewer.json")
project_id="dash-app-318517"
client = bigquery.Client(credentials=creds, project=project_id)

def furnish_arrests():
    df_arrests = client.query("select * from mpd_dash_database.arrests").to_dataframe()
    #formating the time data
    df_arrests['Arrest_Hour'] = pd.to_datetime(df_arrests['Arrest_Hour'],format='%H')
    df_arrests['Arrest_Hour'] = df_arrests['Arrest_Hour'].dt.strftime('%I %p')
    
    return df_arrests

def furnish_stops():
    data_full = client.query("select * from mpd_dash_database.stops").to_dataframe()
    #dropping na values from important columns. they each have less than 0.01% na
    data_full.dropna(subset=['stop_district','stop_time','stop_duration_minutes','race_ethnicity'], inplace=True)
    #removing ages = to unknown - count < 0.02%
    data_full = data_full[data_full['age']!='Unknown']
    #children ages are set to "Juvenile" string type. Converting that to 16. Assuming mean.
    data_full['age'].replace('Juvenile',16, inplace=True)
    #mapping age data to integer
    data_full['age']=data_full['age'].map(int)
    #creating a stop datetime column with date from both date and time columns for ease of analysis
    data_full['stop_datetime'] = pd.to_datetime(data_full['stop_date']+'T'+data_full['stop_time'], format= r'%Y-%m-%dT%H:%M')
    data_full['race_ethnicity'].replace(['Unknown','Multiple'],'Other',inplace=True )
    #adding a weekday column
    week = ['Monday', 'Tuesday', 'Wednesday',  'Thursday', 'Friday', 'Saturday', 'Sunday']

    def date_to_weekday(date):
        return week[date.weekday()]

    data_full['weekdays'] = data_full['stop_datetime'].map(date_to_weekday)

    return data_full

def furnish_geo(data_full, districts_geo):
    #grouping by district and adding metrics to the geofile
    districts_geo['avg_age'] = data_full.groupby('stop_district').mean()['age'].values
    districts_geo['avg_stop_duration'] = data_full.groupby('stop_district').mean()['stop_duration_minutes'].values
    districts_geo['count_child'] = data_full[data_full['age']==16].groupby('stop_district').count()['stop_duration_minutes'].values
    districts_geo['count_adult'] = data_full[data_full['age']>16].groupby('stop_district').count()['stop_duration_minutes'].values
    districts_geo['person_searches'] = data_full[data_full['person_search_or_protective_pat_down']==1].groupby('stop_district').count()['stop_duration_minutes'].values
    districts_geo['property_searches'] = data_full[data_full['property_search_or_protective_pat_down']==1].groupby('stop_district').count()['stop_duration_minutes'].values
    districts_geo['person_warrant'] = data_full[data_full['person_search_warrant']==1].groupby('stop_district').count()['stop_duration_minutes'].values
    districts_geo['property_warrant'] = data_full[data_full['property_search_warrant']==1].groupby('stop_district').count()['stop_duration_minutes'].values

    districts_geo.index=range(1,8)

    return districts_geo

def furnish_daily(data_full):
    #creating an array with the counts of daily stops
    
    df = data_full[['stop_date','stop_duration_minutes']].copy()
    df['stop_date'] = pd.to_datetime(df['stop_date'], format='%Y-%m-%d')
    df.set_index('stop_date', inplace=True)
    daily_count = df['stop_duration_minutes'].resample('D').count()


    #getting the rolling weekly average
    daily_count['rolling avg'] = daily_count.rolling(7).mean()

    return daily_count

def furnish_hourly(data_full):
    #creating an array with the counts of stops by hour of day
    df = data_full[['stop_time','stop_date']].copy()
    df['stop_time'] = pd.to_datetime(df['stop_time'], format='%H:%M')
    df.set_index('stop_time', inplace=True)
    hourly_count = df['stop_date'].resample('H').count()

    return hourly_count