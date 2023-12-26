# Import necessary packages
import streamlit as st
import pandas as pd
import os
import json 
import mysql.connector
import plotly.express as px
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from streamlit_option_menu import option_menu
import plotly.graph_objects as go
from geopy.geocoders import Nominatim
from retrying import retry
from PIL import Image

# ---------------------Functions for Data Extraction------------

# Locating input files in local machine
def GetDirectoriesList():
    root_dir_path = \
        r'D:\Chindhu\RoadMap for Career\Data Science\Guvi\Python Codes\Phonepe\pulse\data'
    sub_dir1 = os.listdir(root_dir_path)
    sub_dir2 = ['transaction','user']
    sub_dir3 = 'country'
    sub_dir4 = 'india'
    additional_dir = 'hover'
    sub_dir5 = 'state'

    directories_list = []
    for dir1 in sub_dir1:
        for dir2 in sub_dir2:
            if (dir1 == 'map'):
                dir_path = os.path.join(root_dir_path, 
                                        dir1, 
                                        dir2, 
                                        additional_dir, 
                                        sub_dir3, 
                                        sub_dir4, 
                                        sub_dir5
                                    )
            else:
                dir_path = os.path.join(root_dir_path,
                                        dir1, 
                                        dir2, 
                                        sub_dir3, 
                                        sub_dir4, 
                                        sub_dir5
                                        )
            directories_list.append(dir_path)

    return directories_list

# Aggregated Transaction - loading Json file
def GetAggregatedTransactions(path):
    aggregated_trans = {'state':[],
                        'year':[],
                        'quarter':[],
                        'trans_type':[],
                        'trans_count':[],
                        'trans_amt':[]
                        }
    states = os.listdir(path)
    for state in states:
            yrs = os.listdir(os.path.join(path, state))
            for yr in yrs:
                file_location = os.path.join(path, state, yr)
                quarter_files = os.listdir(file_location)
                for qfile in quarter_files:
                    with open(os.path.join(file_location, qfile),'r') as file:
                        data=json.load(file)
                    for i in data['data']['transactionData']:
                        aggregated_trans['state'].append(state)
                        aggregated_trans['year'].append(yr)
                        aggregated_trans['quarter'].append(int(qfile.strip('.json')))
                        aggregated_trans['trans_type'].append(i['name'])
                        aggregated_trans['trans_count'].append(i['paymentInstruments'][0]['count'])
                        aggregated_trans['trans_amt'].append(i['paymentInstruments'][0]['amount'])

    # Converting the extracted data as dataframe
    aggregated_trans_df = pd.DataFrame(aggregated_trans)

    # Data Cleaning
    aggregated_trans_df['state'] = aggregated_trans_df['state'].str.replace('-',' ')
    aggregated_trans_df['state'] = aggregated_trans_df['state'].str.title()
    aggregated_trans_df['state'] = \
        aggregated_trans_df['state'].replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')

    # Exporting dataframe to csv
    aggregated_trans_df.to_csv(
        r'D:\Chindhu\RoadMap for Career\Data Science\Guvi\Python Codes\Phonepe\pulse\CSV Files\agg_trans.csv',
        index=False
    )

    return aggregated_trans_df

# Aggregated User - loading Json file
def GetAggregatedUsers(path):
    aggregated_user = {'state':[],
                       'year':[],
                       'quarter':[],
                       'user_brand':[],
                       'user_count':[],
                       'user_percentage':[]
                       }
    states = os.listdir(path)
    for state in states:
            yrs = os.listdir(os.path.join(path, state))
            for yr in yrs:
                file_location = os.path.join(path, state, yr)
                quarter_files = os.listdir(file_location)
                for qfile in quarter_files:
                    with open(os.path.join(file_location, qfile),'r') as file:
                        data = json.load(file)
                    userdata = data['data'].get('usersByDevice')
                    if userdata is None:
                        continue
                    for i in userdata:
                        aggregated_user['state'].append(state)
                        aggregated_user['year'].append(yr)
                        aggregated_user['quarter'].append(int(qfile.strip('.json')))
                        aggregated_user['user_brand'].append(i['brand'])
                        aggregated_user['user_count'].append(i['count'])
                        aggregated_user['user_percentage'].append(i['percentage'])
                        
    # Converting the extracted data as dataframe    
    aggregated_user_df = pd.DataFrame(aggregated_user)

    # Data Cleaning
    aggregated_user_df['state'] = \
        aggregated_user_df['state'].str.replace('-',' ')
    aggregated_user_df['state'] = \
        aggregated_user_df['state'].str.title()
    aggregated_user_df['state'] = \
        aggregated_user_df['state'].replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')

    # Exporting dataframe to csv
    aggregated_user_df.to_csv(
        r'D:\Chindhu\RoadMap for Career\Data Science\Guvi\Python Codes\Phonepe\pulse\CSV Files\agg_user.csv',
        index=False
    )

    return aggregated_user_df

# Map Transaction - Loading JSON file
def GetMapTransactions(path):
    map_trans = {'state':[],
                 'year':[],
                 'quarter':[],
                 'hover_name':[],
                 'hover_count':[],
                 'hover_amt':[]
                }
    states = os.listdir(path)
    for state in states:
            yrs = os.listdir(os.path.join(path, state))
            for yr in yrs:
                file_location = os.path.join(path, state, yr)
                quarter_files = os.listdir(file_location)
                for qfile in quarter_files:
                    with open(os.path.join(file_location, qfile),'r') as file:
                        data = json.load(file)
                    for i in data['data']['hoverDataList']:
                        map_trans['state'].append(state)
                        map_trans['year'].append(yr)
                        map_trans['quarter'].append(int(qfile.strip('.json')))
                        map_trans['hover_name'].append(i['name'])
                        map_trans['hover_count'].append(i['metric'][0]['count'])
                        map_trans['hover_amt'].append(i['metric'][0]['amount'])
                        
    # Converting the extracted data as dataframe    
    map_trans_df = pd.DataFrame(map_trans)

    # Data Cleaning
    map_trans_df['state'] = \
        map_trans_df['state'].str.replace('-',' ')
    map_trans_df['state'] = \
        map_trans_df['state'].str.title()
    map_trans_df['state'] = \
        map_trans_df['state'].replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')

    # Exporting dataframe to csv
    map_trans_df.to_csv(
        r'D:\Chindhu\RoadMap for Career\Data Science\Guvi\Python Codes\Phonepe\pulse\CSV Files\map_trans.csv',
        index=False
    )

    return map_trans_df

# Map User - loading Json file
def GetMapUsers(path):
    map_user = {'state':[],
                'year':[],
                'quarter':[],
                'location':[],
                'apps_open':[],
                'registered_users':[]
            }
    states = os.listdir(path)
    for state in states:
            yrs = os.listdir(os.path.join(path, state))
            for yr in yrs:
                file_location = os.path.join(path, state, yr)
                quarter_files = os.listdir(file_location)
                for qfile in quarter_files:
                    with open(os.path.join(file_location, qfile),'r') as file:
                        data = json.load(file)
                    userdata = data['data'].get('hoverData')
                    if userdata is None:
                        continue
                    for i in userdata.keys():
                        map_user['state'].append(state)
                        map_user['year'].append(yr)
                        map_user['quarter'].append(int(qfile.strip('.json')))
                        map_user['location'].append(i)
                        map_user['apps_open'].append(userdata[i]['appOpens'])
                        map_user['registered_users'].append(userdata[i]['registeredUsers'])

    # Converting the extracted data as dataframe    
    map_user_df = pd.DataFrame(map_user)

    # Data Cleaning
    map_user_df['state'] = \
        map_user_df['state'].str.replace('-',' ')
    map_user_df['state'] = \
        map_user_df['state'].str.title()
    map_user_df['state'] = \
        map_user_df['state'].replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')

    #Exporting dataframe to csv
    map_user_df.to_csv(
        r'D:\Chindhu\RoadMap for Career\Data Science\Guvi\Python Codes\Phonepe\pulse\CSV Files\map_user.csv',
        index=False
    )

    return map_user_df 

# Top Transaction - Loading JSON file
def GetTopTransactions(path):
    top_trans = {'state':[],
                 'year':[],
                 'quarter':[],
                 'loc_type':[],
                 'loc_entity_name':[],
                 'loc_entity_amt':[],
                 'loc_entity_count':[]
                }
    states = os.listdir(path)
    for state in states:
            yrs = os.listdir(os.path.join(path, state))
            for yr in yrs:
                file_location = os.path.join(path, state, yr)
                quarter_files = os.listdir(file_location)
                for qfile in quarter_files:
                    with open(os.path.join(file_location, qfile),'r') as file:
                        data = json.load(file)
                    for i in data['data'].keys():
                        if data['data'][i] is not None :
                            for j in range(0,len(data['data'][i])):
                                top_trans['state'].append(state)
                                top_trans['year'].append(yr)
                                top_trans['quarter'].append(int(qfile.strip('.json')))
                                top_trans['loc_type'].append(i)
                                top_trans['loc_entity_name'].append(data['data'][i][j]['entityName'])
                                top_trans['loc_entity_amt'].append(data['data'][i][j]['metric']['amount'])
                                top_trans['loc_entity_count'].append(data['data'][i][j]['metric']['count'])

    # Converting the extracted data as dataframe    
    top_trans_df = pd.DataFrame(top_trans)

    # Data Cleaning
    top_trans_df['state'] = \
        top_trans_df['state'].str.replace('-',' ')
    top_trans_df['state'] = \
        top_trans_df['state'].str.title()
    top_trans_df['state'] = \
        top_trans_df['state'].replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')

    # Exporting dataframe to csv
    top_trans_df.to_csv(
        r'D:\Chindhu\RoadMap for Career\Data Science\Guvi\Python Codes\Phonepe\pulse\CSV Files\top_trans.csv',
        index=False
    )

    return top_trans_df

# Top User - loading Json file
def GetTopUsers(path):
    top_user = {'state':[],
                'year':[],
                'quarter':[],
                'loc_type':[],
                'loc_entity_name':[],
                'loc_registered_users':[]
            }
    states = os.listdir(path)
    for state in states:
            yrs = os.listdir(os.path.join(path, state))
            for yr in yrs:
                file_location = os.path.join(path, state, yr)
                quarter_files = os.listdir(file_location)
                for qfile in quarter_files:
                    with open(os.path.join(file_location, qfile),'r') as file:
                        data = json.load(file)
                    for i in data['data'].keys():
                        if data['data'][i] is not None :
                            for j in range(0,len(data['data'][i])):
                                top_user['state'].append(state)
                                top_user['year'].append(yr)
                                top_user['quarter'].append(int(qfile.strip('.json')))
                                top_user['loc_type'].append(i)
                                top_user['loc_entity_name'].append(data['data'][i][j]['name'])
                                top_user['loc_registered_users'].append(data['data'][i][j]['registeredUsers'])

    # Converting the extracted data as dataframe    
    top_user_df = pd.DataFrame(top_user)

    # Data Cleaning
    top_user_df['state'] = \
        top_user_df['state'].str.replace('-',' ')
    top_user_df['state'] = \
        top_user_df['state'].str.title()
    top_user_df['state'] = \
        top_user_df['state'].replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')

    #Exporting dataframe to csv
    top_user_df.to_csv(
        r'D:\Chindhu\RoadMap for Career\Data Science\Guvi\Python Codes\Phonepe\pulse\CSV Files\top_user.csv',
        index=False
    )

    return top_user_df

# -----------------Functions for storing in a DB-----------------------

# Deleting the existing table if they exists
def DropExistingTables(mycursor):
    query = 'DROP TABLE IF EXISTS agg_trans'
    mycursor.execute(query)
    query = 'DROP TABLE IF EXISTS agg_user'
    mycursor.execute(query)

    query = 'DROP TABLE IF EXISTS map_trans'
    mycursor.execute(query)
    query = 'DROP TABLE IF EXISTS map_user'
    mycursor.execute(query)

    query = 'DROP TABLE IF EXISTS top_trans'
    mycursor.execute(query)
    query = 'DROP TABLE IF EXISTS top_user'
    mycursor.execute(query)

# Reading CSV Files and inserting into the respective tables
def Insertion_To_DB():
    # Connecting to MySQL
    hostname="localhost"
    dbname="phonepe"
    uname="root"
    pwd="Chinka@SQL123"

    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                    .format(host=hostname, db=dbname, user=uname, pw=quote_plus(pwd)))
    
    # Locating directory of the csv files stored
    root_dir = \
        r'D:\Chindhu\RoadMap for Career\Data Science\Guvi\Python Codes\Phonepe\pulse\CSV Files'
    files = os.listdir(root_dir)

    # Reading files and inserting to sql 
    for i in files:
        df = pd.read_csv(os.path.join(root_dir, i))
        table_name = i[:-4]
        df.to_sql(table_name, engine, index=False)
        print("Inserted "+table_name)

# Executing Query using Cursor
def ExecuteQuery(mycursor,query):
    mycursor.execute(query)
    res = mycursor.fetchall()
    field_headings = [i[0] for i in mycursor.description]
    return pd.DataFrame(res, columns = field_headings)

# Executing Query using Cursor along with Data
def ExecuteQueryWithData(mycursor,query,data):
    mycursor.execute(query,data)
    res = mycursor.fetchall()
    field_headings = [i[0] for i in mycursor.description]
    return pd.DataFrame(res, columns = field_headings)

# Data Transformation function
def DataTransform(mycursor):
    directories_list=GetDirectoriesList()
    agg_trans_df=GetAggregatedTransactions(directories_list[0])
    agg_user_df=GetAggregatedUsers(directories_list[1])
    map_trans_df=GetMapTransactions(directories_list[2])
    map_user_df=GetMapUsers(directories_list[3])
    top_trans_df=GetTopTransactions(directories_list[4])
    top_user_df=GetTopUsers(directories_list[5])
    DropExistingTables(mycursor)
    Insertion_To_DB()
    return agg_trans_df, \
            agg_user_df, \
            map_trans_df, \
            map_user_df, \
            top_trans_df, \
            top_user_df

# --------------Geo Locators for retrieving Latitude and Longitude-----

# Retrieve lat and long given district
@retry(wait_exponential_multiplier = 1000, wait_exponential_max = 10000,\
        stop_max_attempt_number = 5)
def get_lat_lng_from_district(district):
    geolocator = Nominatim(user_agent = "district_locator", timeout = 10)
    location = geolocator.geocode(f"{district}, India")
    if location:
        return location.latitude, location.longitude
    else:
        return None, None

# Retrieve lat and long given pincode
@retry(wait_exponential_multiplier = 1000, wait_exponential_max = 10000,\
        stop_max_attempt_number = 5)
def get_lat_lng_from_pincode(pincode):
    geolocator = Nominatim(user_agent = "pincode_locator", timeout = 10)
    location = geolocator.geocode(f"{pincode}, India")
    if location:
        return location.latitude, location.longitude
    else:
        return None, None
    
# --------------Visualizing data in different formats------------------

# --------------------Transactions-------------------------------------

# --------------Aggregated Transactions--------------------------------

# Visualize State based on Aggregated Transaction Amount
def VisualizeStateTransAmt(mycursor):
    query = 'SELECT state,sum(trans_amt) as total_trans_amt \
            from agg_trans GROUP BY state'
    vis_agg_trans_df = ExecuteQuery(mycursor, query)
    fig = px.choropleth(vis_agg_trans_df,
                        geojson = \
                            "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                        featureidkey = 'properties.ST_NM',
                        locations = 'state',
                        color = 'total_trans_amt',
                        color_continuous_scale = 'Temps',
                        title = "Statewise Transaction Amount",
                        labels = {'total_trans_amt' : 
                                  'Total Transaction Amount'},
                        height = 500,
                        width = 1000
                        )
    fig.update_geos(fitbounds = "locations", 
                    visible = False
                    )
    
    return fig

# Visualize Year based on the Aggregated Transaction Amount
def VisualizeYearTransAmt(mycursor):
    query = 'SELECT year, sum(trans_amt) as total_trans_amt \
            from agg_trans group by year'
    vis_agg_trans_df = ExecuteQuery(mycursor, query)
    fig = px.line(vis_agg_trans_df, x = 'year', y = 'total_trans_amt')
    fig.update_layout(
        title = "Yearwise Transaction Amount",
        xaxis_title = 'Year',
        yaxis_title = 'Total Transaction Amount',
    )

    return fig

# Visualize Quarter based on the Aggregated Transaction Amount
def VisualizeQuarterTransAmt(mycursor):
    query = 'SELECT year, quarter, sum(trans_amt) as total_trans_amt \
            from agg_trans group by year, quarter'
    vis_agg_trans_df = ExecuteQuery(mycursor, query)
    fig = px.histogram(vis_agg_trans_df, x = "quarter", \
                       y = "total_trans_amt",color = 'year', barmode = 'group')
    fig.update_layout(
        title = "Quarterwise Transaction Amount",
        xaxis_title = 'Quarter',
        yaxis_title = 'Total Transaction Amount',
    )

    return fig

# Visualize Transaction Type based on the Aggregated Transaction Amount
def VisualizeTransTypeTransAmt(mycursor):
    query = 'SELECT trans_type, sum(trans_amt) as total_trans_amt \
            from agg_trans group by trans_type'
    vis_agg_trans_df = ExecuteQuery(mycursor, query)
    fig = px.pie(vis_agg_trans_df, 
                 values = "total_trans_amt", 
                 names = "trans_type", 
                 hole = .3, 
                 title = "Transaction Typewise Transaction Amount")

    return fig

# Visualize State based on the Aggregated Transaction Count
def VisualizeStateTransCount(mycursor):
    query = 'SELECT state,sum(trans_count) as total_trans_count \
            from agg_trans GROUP BY state'
    vis_agg_trans_df = ExecuteQuery(mycursor, query)
    vis_agg_trans_df['total_trans_count'] = \
        pd.to_numeric(vis_agg_trans_df['total_trans_count'])
    fig = px.choropleth(vis_agg_trans_df,
                        geojson = \
                            "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                        featureidkey = 'properties.ST_NM',
                        locations = 'state',
                        color = 'total_trans_count',
                        color_continuous_scale = 'Peach',
                        title = "Statewise Transaction Count",
                        labels = {'total_trans_count' : \
                                  'Total Transaction Count'},
                        height = 500,
                        width = 1000
                        )
    fig.update_geos(fitbounds = "locations", visible = False)

    return fig

# Visualize Year based on the Aggregated Transaction Count
def VisualizeYearTransCount(mycursor):
    query = 'SELECT year, sum(trans_count) as total_trans_count \
            from agg_trans group by year'
    vis_agg_trans_df = ExecuteQuery(mycursor, query)
    vis_agg_trans_df['total_trans_count'] = \
        pd.to_numeric(vis_agg_trans_df['total_trans_count'])
    fig = px.line(vis_agg_trans_df, x = 'year', y = 'total_trans_count')
    fig.update_layout(
        title = "Yearwise Transaction Count",
        xaxis_title = 'Year',
        yaxis_title = 'Total Transaction Count',
    )

    return fig

# Visualize Quarter based on the Aggregated Transaction Count
def VisualizeQuarterTransCount(mycursor):
    query = 'SELECT year, quarter, sum(trans_count) as total_trans_count \
            from agg_trans group by year, quarter'
    vis_agg_trans_df = ExecuteQuery(mycursor, query)
    vis_agg_trans_df['total_trans_count'] = \
        pd.to_numeric(vis_agg_trans_df['total_trans_count'])
    fig = px.histogram(vis_agg_trans_df, x = "quarter", \
                       y = "total_trans_count", color = 'year', \
                        barmode = 'group', height = 400)
    fig.update_layout(
        title = "Quarterwise Transaction Count",
        xaxis_title = 'Quarter',
        yaxis_title = 'Total Transaction Count',
    )

    return fig

# Visualize Transaction Type based on the Aggregated Transaction Count
def VisualizeTransTypeTransCount(mycursor):
    query = 'SELECT trans_type, sum(trans_count) as total_trans_count \
            from agg_trans group by trans_type'
    vis_agg_trans_df = ExecuteQuery(mycursor, query)
    vis_agg_trans_df['total_trans_count'] = \
        pd.to_numeric(vis_agg_trans_df['total_trans_count'])
    fig = px.pie(vis_agg_trans_df, 
                 values = "total_trans_count", 
                 names = "trans_type", 
                 hole = .3, 
                 title = "Transaction Typewise Transaction Count")
    
    return fig

# -----------------------Map Transactions------------------------------

# Visualize District based on the Map Transaction Amount
def VisualizeDistrictMapTransAmt(mycursor, state):
    query = 'SELECT hover_name,sum(hover_amt) as total_trans_amt \
            from map_trans  WHERE state=%s group by hover_name'
    data = (state,)
    vis_map_trans_df = ExecuteQueryWithData(mycursor, query, data)
    fig = px.bar(vis_map_trans_df, x = 'hover_name', y = 'total_trans_amt',\
                 color_discrete_sequence = ['indianred'] )
    fig.update_layout(
        title = "Districtwise Transaction Amount",
        xaxis_title = 'Districts',
        yaxis_title = 'Total Transaction Amount',
    )

    return fig

# Visualize Year based on the Map Transaction Amount
def VisualizeYearMapTransAmt(mycursor, state):
    query = 'SELECT hover_name, year, sum(hover_amt) as total_trans_amt \
        from map_trans  WHERE state=%s group by hover_name, year'
    data = (state,)
    vis_map_trans_df = ExecuteQueryWithData(mycursor, query, data)
    fig = px.scatter(vis_map_trans_df, x = 'hover_name', y = 'total_trans_amt',\
                     color = 'year',color_continuous_scale = 'Viridis')
    fig.update_layout(
        title = "Districtwise Year Transaction Amount",
        xaxis_title = 'Districts',
        yaxis_title = 'Total Transaction Amount',
    )

    return fig

# Visualize Quarter based on the Map Transaction Amount
def VisualizeQuarterMapTransAmt(mycursor, state):
    query = 'SELECT hover_name, quarter, sum(hover_amt) as total_trans_amt \
            from map_trans  WHERE state=%s group by quarter,hover_name'
    data = (state,)
    vis_map_trans_df = ExecuteQueryWithData(mycursor, query, data)
    fig = px.line(vis_map_trans_df, x = 'hover_name', y = 'total_trans_amt',\
                  color = 'quarter',symbol = 'quarter',\
                    color_discrete_sequence = px.colors.qualitative.G10)
    fig.update_layout(
        title = "Districtwise Quarter Transaction Amount",
        xaxis_title = 'Districts',
        yaxis_title = 'Total Transaction Amount',
    )
    return fig

# Visualize District based on the Map Transaction Count
def VisualizeDistrictMapTransCount(mycursor,state):
    query = 'SELECT state,hover_name,sum(hover_count) as total_trans_count \
            from map_trans  WHERE state=%s group by hover_name'
    data = (state,)
    vis_map_trans_df = ExecuteQueryWithData(mycursor, query, data)
    fig = px.bar(vis_map_trans_df, x = 'hover_name', y = 'total_trans_count',\
                 color_discrete_sequence = ['indianred'] )
    fig.update_layout(
        title = "Districtwise Transaction Amount",
        xaxis_title = 'Districts',
        yaxis_title = 'Total Transaction Count',
    )
    return fig

# Visualize Year based on the Map Transaction Count
def VisualizeYearMapTransCount(mycursor, state):
    query = 'SELECT hover_name, year, sum(hover_count) as total_trans_count \
            from map_trans  WHERE state=%s group by year,hover_name'
    data = (state,)
    vis_map_trans_df = ExecuteQueryWithData(mycursor, query, data)
    fig = px.scatter(vis_map_trans_df, x = 'hover_name', y = 'total_trans_count',\
                     color = 'year',color_continuous_scale = 'Viridis')
    fig.update_layout(
        title = "Districtwise Year Transaction Count",
        xaxis_title = 'Districts',
        yaxis_title = 'Total Transaction Count',
    )
    return fig

# Visualize Quarter based on the Map Transaction Count
def VisualizeQuarterMapTransCount(mycursor, state):
    query = 'SELECT hover_name, quarter, sum(hover_count) as total_trans_count \
            from map_trans  WHERE state=%s group by quarter,hover_name'
    data = (state,)
    vis_map_trans_df = ExecuteQueryWithData(mycursor, query, data)
    fig = px.line(vis_map_trans_df, x = 'hover_name', y = 'total_trans_count',\
                  color = 'quarter',symbol = 'quarter',\
                    color_discrete_sequence = px.colors.qualitative.G10)
    fig.update_layout(
        title = "Districtwise Quarter Transaction Count",
        xaxis_title = 'Districts',
        yaxis_title = 'Total Transaction Count',
    )
    return fig
# -----------------------Top Transactions------------------------------

# Visualize Top 10 Districts based on the Transaction Amount
def VisualizeOverallTopTransAmtByDistrict(mycursor):
    query = 'SELECT loc_entity_name, sum(loc_entity_amt) as total_trans_amount\
             from top_trans WHERE loc_type="districts" GROUP BY loc_entity_name\
             ORDER BY total_trans_amount DESC LIMIT 10 '
    
    vis_top_trans_df = ExecuteQuery(mycursor,query)
    lat = []
    lon = []
    for district in vis_top_trans_df['loc_entity_name']:
        coordinates = get_lat_lng_from_district(district)
        if coordinates:
            lat.append(coordinates[0])
            lon.append(coordinates[1])
    vis_top_trans_df.loc[:, 'lat'] = lat
    vis_top_trans_df.loc[:, 'lon'] = lon
	
	# Set center and projection scale for India
    center = {"lat": 20.5937, "lon": 78.9629}
	
    fig = px.scatter_geo(
		vis_top_trans_df,
		lat = 'lat',
		lon = 'lon',
        hover_name = 'loc_entity_name',
        size = 'total_trans_amount',
		color = 'total_trans_amount',
        color_continuous_scale = 'bluered',
		center = center,
		projection = 'natural earth',
		title = 'Scatter Plot on Top 10 Districts based on Transaction Amount'
	)
    fig.update_geos(fitbounds = "locations", visible = True)

    return fig

# Visualize Top 10 Pincodes based on the Transaction Amount
def VisualizeOverallTopTransAmtByPincode(mycursor):
    query = 'SELECT loc_entity_name, sum(loc_entity_amt) as total_trans_amount\
             from top_trans WHERE loc_type="pincodes" GROUP BY  loc_entity_name\
             ORDER BY total_trans_amount DESC LIMIT 10 '
    vis_top_trans_df = ExecuteQuery(mycursor, query)
	
    lat = []
    lon = []
    for pincode in vis_top_trans_df['loc_entity_name']:
        coordinates = get_lat_lng_from_pincode(pincode)
        if coordinates:
            lat.append(coordinates[0])
            lon.append(coordinates[1])
    vis_top_trans_df.loc[:, 'lat'] = lat
    vis_top_trans_df.loc[:, 'lon'] = lon
	
	# Set center and projection scale for India
    center = {"lat": 20.5937, "lon": 78.9629}
	
    fig = px.scatter_geo(
		vis_top_trans_df,
		lat = 'lat',
		lon = 'lon',
        hover_name = 'loc_entity_name',
        size = 'total_trans_amount',
		color = 'total_trans_amount',
        color_continuous_scale = 'solar',
		center = center,
		projection = 'natural earth',
		title = 'Scatter Plot on Pincodes based on Transaction Amount'
	)
    fig.update_geos(fitbounds = "locations", visible = True)

    return fig

# Visualize Top 10 Districts based on the Transaction Count
def VisualizeOverallTopTransCountByDistrict(mycursor):
    query = 'SELECT loc_entity_name, sum(loc_entity_count) as total_trans_count \
             from top_trans WHERE loc_type="districts" GROUP BY  loc_entity_name \
                ORDER BY total_trans_count DESC LIMIT 10 '
    vis_top_trans_df = ExecuteQuery(mycursor, query)
    vis_top_trans_df['total_trans_count'] = \
        pd.to_numeric(vis_top_trans_df['total_trans_count'])

    lat = []
    lon = []
    for district in vis_top_trans_df['loc_entity_name']:
        coordinates = get_lat_lng_from_district(district)
        if coordinates:
            lat.append(coordinates[0])
            lon.append(coordinates[1])
    vis_top_trans_df.loc[:, 'lat'] = lat
    vis_top_trans_df.loc[:, 'lon'] = lon
	
	# Set center and projection scale for India
    center = {"lat": 20.5937, "lon": 78.9629}
	
    fig = px.scatter_geo(
		vis_top_trans_df,
		lat = 'lat',
		lon = 'lon',
        hover_name = 'loc_entity_name',
        size = 'total_trans_count',
		color = 'total_trans_count',
        color_continuous_scale = 'bluered',
		center = center,
		projection = 'natural earth',
		title = 'Scatter Plot on Districts based on Transaction Count'
	)
    fig.update_geos(fitbounds = "locations", visible = True)

    return fig

# Visualize Top 10 Pincodes based on the Transaction Count
def VisualizeOverallTopTransCountByPincode(mycursor):
    query = 'SELECT loc_entity_name, sum(loc_entity_count) as total_trans_count \
            from top_trans WHERE loc_type="pincodes" GROUP BY  loc_entity_name \
                ORDER BY total_trans_count DESC LIMIT 10 '
    vis_top_trans_df = ExecuteQuery(mycursor, query)
    vis_top_trans_df['total_trans_count'] = \
        pd.to_numeric(vis_top_trans_df['total_trans_count'])

    lat = []
    lon = []
    for pincode in vis_top_trans_df['loc_entity_name']:
        coordinates = get_lat_lng_from_pincode(pincode)
        if coordinates:
            lat.append(coordinates[0])
            lon.append(coordinates[1])
    vis_top_trans_df.loc[:, 'lat'] = lat
    vis_top_trans_df.loc[:, 'lon'] = lon
	
	# Set center and projection scale for India
    center = {"lat": 20.5937, "lon": 78.9629}
	
    fig = px.scatter_geo(
		vis_top_trans_df,
		lat = 'lat',
		lon = 'lon',
        hover_name = 'loc_entity_name',
        size = 'total_trans_count',
		color = 'total_trans_count',
        color_continuous_scale = 'solar',
		center = center,
		projection = 'natural earth',
		title = 'Scatter Plot on Pincodes based on Transaction Count'
	)
    fig.update_geos(fitbounds = "locations", visible = True)

    return fig

# -----------------------User Details----------------------------------

# -----------------------Aggregated Users------------------------------

# Visualize State based on the Aggregated User Count
def VisualizeStateUserCount(mycursor):
    query = 'SELECT state,sum(user_count) as total_user_count \
            from agg_user GROUP BY state'
    vis_agg_user_df = ExecuteQuery(mycursor, query)
    vis_agg_user_df['total_user_count'] = \
        pd.to_numeric(vis_agg_user_df['total_user_count'])
    fig = px.choropleth(vis_agg_user_df,
                        geojson = \
                            "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                        featureidkey = 'properties.ST_NM',
                        locations = 'state',
                        color = 'total_user_count',
                        color_continuous_scale = 'Teal',
                        title = "Statewise User Count",
                        labels = {'total_user_count' : \
                                  'Total User Count'},
                        height = 500,
                        width = 1000
                        )
    fig.update_geos(fitbounds = "locations", visible = False)

    return fig 

# Visualize Year based on the Aggregated User Count
def VisualizeYearUserCount(mycursor):
    query = 'SELECT year, sum(user_count) as total_user_count \
            from agg_user group by year'
    vis_agg_user_df = ExecuteQuery(mycursor, query)
    vis_agg_user_df['total_user_count'] = \
        pd.to_numeric(vis_agg_user_df['total_user_count'])
    fig=px.line(vis_agg_user_df, x = 'year', y = 'total_user_count',\
                 markers = True)
    fig.update_layout(
        title = "Yearwise User Count",
        xaxis_title = 'Year',
        yaxis_title = 'Total User Count',
    )

    return fig

# Visualize Quarter based on the Aggregated User Count
def VisualizeQuarterUserCount(mycursor):
    query = 'SELECT year, quarter, sum(user_count) as total_user_count \
            from agg_user group by year, quarter'
    vis_agg_user_df = ExecuteQuery(mycursor, query)
    vis_agg_user_df['total_user_count'] = \
        pd.to_numeric(vis_agg_user_df['total_user_count'])
    fig = px.histogram(vis_agg_user_df, x = "quarter", y = "total_user_count",\
                        color = 'year', barmode = 'group')
    fig.update_layout(
        title = "Quarterwise User Count",
        xaxis_title = 'Quarter',
        yaxis_title = 'Total User Count',
    )

    return fig

# Visualize User Brand based on the Aggregated User Count
def VisualizeUserBrandUserCount(mycursor):
    query = 'SELECT user_brand, sum(user_count) as total_user_count \
            from agg_user group by user_brand'
    vis_agg_user_df = ExecuteQuery(mycursor,query)
    vis_agg_user_df['total_user_count'] = \
        pd.to_numeric(vis_agg_user_df['total_user_count'])
    fig = px.pie(vis_agg_user_df, values = "total_user_count", \
                 names = "user_brand", hole = .3, \
                    title = "User Brandwise User Count")

    return fig

# -----------------------Map Users------------------------------

# Visualize District based on the Total Registered Users
def VisualizeDistrictMapRegUsers(mycursor, state):
    query = 'SELECT state,location,sum(registered_users) as total_registered_users \
            from map_user  WHERE state=%s group by location'
    data = (state,)
    vis_map_user_df = ExecuteQueryWithData(mycursor, query, data)
    fig = px.bar(vis_map_user_df, x = 'location', y = 'total_registered_users',\
                 color_discrete_sequence = ['indianred'] )
    fig.update_layout(
        title = "Districtwise Registered Users",
        xaxis_title = 'Districts',
        yaxis_title = 'Total Registered Users',
    )

    return fig

# Visualize Year based on the Total Registered Users
def VisualizeYearMapRegUsers(mycursor,state):
    query = 'SELECT location, year, sum(registered_users) as total_registered_users\
             from map_user  WHERE state=%s group by year,location'
    data = (state,)
    vis_map_user_df = ExecuteQueryWithData(mycursor, query, data)
    fig = px.scatter(vis_map_user_df, x = 'location', y = 'total_registered_users',\
                     color = 'year',color_continuous_scale = 'Viridis')
    fig.update_layout(
        title = "Districtwise Registered Users based on Year",
        xaxis_title = 'Districts',
        yaxis_title = 'Total Registered Users',
    )

    return fig
	
# Visualize Quarter based on the Total Registered Users
def VisualizeQuarterMapRegUsers(mycursor,state):
    query = 'SELECT location, quarter, sum(registered_users) \
            as total_registered_users from map_user WHERE state=%s \
            group by quarter,location'
    data = (state,)
    vis_map_user_df = ExecuteQueryWithData(mycursor, query, data)
    fig = px.line(vis_map_user_df, x = 'location', y = 'total_registered_users',\
                  color = 'quarter', symbol = 'quarter', \
                    color_discrete_sequence = px.colors.qualitative.G10)
    fig.update_layout(
        title = "Districtwise Registered Users based on Quarter",
        xaxis_title = 'Districts',
        yaxis_title = 'Total Registered Users',
    )

    return fig

# Visualize District based on the Total App Opens
def VisualizeDistrictMapAppOpens(mycursor, state):
    query = 'SELECT state,location,sum(apps_open) as total_app_opens\
             from map_user  WHERE state=%s group by location'
    data = (state,)
    vis_map_user_df = ExecuteQueryWithData(mycursor, query, data)
    fig = px.bar(vis_map_user_df, x = 'location', y = 'total_app_opens',\
                 color_discrete_sequence = ['indianred'] )
    fig.update_layout(
        title = "Districtwise App Opens",
        xaxis_title = 'Districts',
        yaxis_title = 'Total App Opens',
    )
    return fig

# Visualize Year based on the Total App Opens
def VisualizeYearMapAppOpens(mycursor, state):
    query = 'SELECT location, year, sum(apps_open) as total_app_opens\
             from map_user  WHERE state=%s group by year,location'
    data = (state,)
    vis_map_user_df = ExecuteQueryWithData(mycursor, query, data)
    fig = px.scatter(vis_map_user_df, x = 'location', y = 'total_app_opens',\
                     color = 'year', color_continuous_scale = 'Viridis')
    fig.update_layout(
        title = "Districtwise App Opens based on Year",
        xaxis_title = 'Districts',
        yaxis_title = 'Total App Opens',
    )

    return fig

# Visualize Quarter based on the Total App Opens
def VisualizeQuarterMapAppOpens(mycursor, state):
    query = 'SELECT location, quarter, sum(apps_open) as total_app_opens \
            from map_user WHERE state=%s group by quarter,location'
    data = (state,)
    vis_map_user_df = ExecuteQueryWithData(mycursor, query, data)
    fig = px.line(vis_map_user_df, x = 'location', y = 'total_app_opens',\
                  color = 'quarter', symbol = 'quarter', \
                    color_discrete_sequence = px.colors.qualitative.G10)
    fig.update_layout(
        title = "Districtwise App Opens based on Quarter",
        xaxis_title = 'Districts',
        yaxis_title = 'Total App Opens',
    )

    return fig

# ---------------------Top Users---------------------------------------

# Visualize Top 10 Districts based on the User Count
def VisualizeOverallTopUserCountByDistrict(mycursor):
    query = 'SELECT loc_entity_name, sum(loc_registered_users) as total_user_count \
             from top_user WHERE loc_type="districts" GROUP BY  loc_entity_name \
                  ORDER BY total_user_count DESC LIMIT 10 '
    vis_top_user_df = ExecuteQuery(mycursor, query)
    vis_top_user_df['total_user_count'] = \
        pd.to_numeric(vis_top_user_df['total_user_count'])
    
    lat = []
    lon = []
    for district in vis_top_user_df['loc_entity_name']:
        coordinates = get_lat_lng_from_district(district)
        if coordinates == (None,None) and district == 'north twenty four parganas':
            coordinates = (22.7392,88.3787)
        if coordinates:
            lat.append(coordinates[0])
            lon.append(coordinates[1])
    vis_top_user_df.loc[:, 'lat'] = lat
    vis_top_user_df.loc[:, 'lon'] = lon
	
	# Set center and projection scale for India
    center = {"lat": 20.5937, "lon": 78.9629}
    
    fig = px.scatter_geo(
		vis_top_user_df,
		lat = 'lat',
		lon = 'lon',
        hover_name = 'loc_entity_name',
        size = 'total_user_count',
		color = 'total_user_count',
        color_continuous_scale = 'temps',
		center = center,
		projection = 'natural earth',
		title = 'Scatter Plot on Districts based on the no. of Registered Users'
	)
    fig.update_geos(fitbounds = "locations", visible = True)

    return fig

# Visualize Top 10 Pincodes based on the User Count
def VisualizeOverallTopUsersCountByPincode(mycursor):
    query = 'SELECT loc_entity_name, sum(loc_registered_users) as total_user_count \
             from top_user WHERE loc_type="pincodes" GROUP BY  loc_entity_name \
                  ORDER BY total_user_count DESC LIMIT 10 '
    vis_top_user_df = ExecuteQuery(mycursor, query)
    vis_top_user_df['total_user_count'] = \
        pd.to_numeric(vis_top_user_df['total_user_count'])
    
    lat = []
    lon = []
    for pincode in vis_top_user_df['loc_entity_name']:
        coordinates = get_lat_lng_from_pincode(pincode)
        if coordinates:
            lat.append(coordinates[0])
            lon.append(coordinates[1])
    vis_top_user_df.loc[:, 'lat'] = lat
    vis_top_user_df.loc[:, 'lon'] = lon

	# Set center and projection scale for India
    center = {"lat": 20.5937, "lon": 78.9629}
    fig = px.scatter_geo(
		vis_top_user_df,
		lat = 'lat',
		lon = 'lon',
        hover_name = 'loc_entity_name',
        size = 'total_user_count',
		color = 'total_user_count',
        color_continuous_scale = 'jet',
		center = center,
		projection = 'natural earth',
		title = 'Scatter Plot on Pincodes based on the no. of Registered Users'
	)
    fig.update_geos(fitbounds = "locations", visible = True)

    return fig

# ----------------Visualization Methods--------------------------------

# Visualize Aggregated Transactions
def VisualizeAggregatedTransaction(mycursor):

    # View transaction details based on Transaction Amount or Transaction Count
    agg_trans_view = st.radio(
        "Radio Buttons",
        ["Transaction Amount", "Transaction Count"], 
        label_visibility = "hidden", 
        horizontal = True,
        key = 'agg_trans_radio_buttons'
    )

    # Visualize aggregated transaction based on State, Year, Quarter, Transaction Type
    agg_trans_sub_views = st.multiselect(
        "Select field(s) to visualize ", 
        ["State", "Year", "Quarter", "Transaction type"], 
        key = 'agg_trans_multi_select' 
    )

    if agg_trans_view == 'Transaction Amount':
        if (agg_trans_sub_views):
            if(st.button('Visualize', key = 'agg_trans_button')):
                for sub_view in agg_trans_sub_views:
                    if sub_view == 'State':
                        st.plotly_chart(VisualizeStateTransAmt(mycursor), 
                                        use_container_width=True, 
                                        config={'responsive': True})
                    if sub_view == 'Year':
                        st.plotly_chart(VisualizeYearTransAmt(mycursor), 
                                        use_container_width=True, 
                                        config={'responsive': True})
                    if sub_view == 'Quarter':
                        st.plotly_chart(VisualizeQuarterTransAmt(mycursor), 
                                        use_container_width=True, 
                                        config={'responsive': True})
                    if sub_view == 'Transaction type':
                        st.plotly_chart(VisualizeTransTypeTransAmt(mycursor), 
                                        use_container_width=True, 
                                        config={'responsive': True})

    elif agg_trans_view == 'Transaction Count':
        if (agg_trans_sub_views):
            if(st.button('Visualize', key = 'agg_trans_button')):
                for sub_view in agg_trans_sub_views:
                    if sub_view == 'State':
                        st.plotly_chart(VisualizeStateTransCount(mycursor), 
                                        use_container_width=True, 
                                        config={'responsive': True})
                    if sub_view == 'Year':
                        st.plotly_chart(VisualizeYearTransCount(mycursor), 
                                        use_container_width=True, 
                                        config={'responsive': True})
                    if sub_view == 'Quarter':
                        st.plotly_chart(VisualizeQuarterTransCount(mycursor), 
                                        use_container_width=True, 
                                        config={'responsive': True})
                    if sub_view == 'Transaction type':
                        st.plotly_chart(VisualizeTransTypeTransCount(mycursor), 
                                        use_container_width=True, 
                                        config={'responsive': True})

# Visualize Map Transactions
def VisualizeMapTransaction(mycursor):

    # View transaction details based on any one of the selected state from list
    query = 'SELECT DISTINCT state from map_trans group by state'
    states = ExecuteQuery(mycursor,query)
    state = st.selectbox(
        "Select the state to view districts transactions",
        states,
        index = None,
    )

    if state:
        # Sub View transaction details based on Transaction Amount or Transaction Count
        map_trans_view = st.radio(
            "Radio Buttons",
            ["Transaction Amount", "Transaction Count"], 
            label_visibility = "hidden", 
            horizontal = True, 
            key = "map_trans_radio_buttons"
        )

        if map_trans_view == "Transaction Amount":
            if(st.button('Visualize', key = 'map_trans_button')):
                st.plotly_chart(VisualizeDistrictMapTransAmt(mycursor,state), 
                                use_container_width=True, 
                                config={'responsive': True})
                st.plotly_chart(VisualizeYearMapTransAmt(mycursor,state), 
                                use_container_width=True, 
                                config={'responsive': True})
                st.plotly_chart(VisualizeQuarterMapTransAmt(mycursor,state), 
                                use_container_width=True, 
                                config={'responsive': True})

        elif map_trans_view == "Transaction Count":
            if(st.button('Visualize', key = 'map_trans_button')):
                st.plotly_chart(VisualizeDistrictMapTransCount(mycursor,state), 
                                use_container_width=True, 
                                config={'responsive': True})
                st.plotly_chart(VisualizeYearMapTransCount(mycursor,state), 
                                use_container_width=True, 
                                config={'responsive': True})
                st.plotly_chart(VisualizeQuarterMapTransCount(mycursor,state), 
                                use_container_width=True, 
                                config={'responsive': True})
                
# Visualize Top Transactions
def VisualizeTopTransaction(mycursor):

    # View transaction details based on Transaction Amount or Transaction Count
    top_trans_view = st.radio(
        "Radio Buttons",
        ["Transaction Amount", "Transaction Count"], 
        label_visibility="hidden", 
        horizontal=True, 
        key="top_trans_radio_buttons"
    )

    if top_trans_view == "Transaction Amount":
        if(st.button('Visualize', key = 'top_trans_button')):
            st.write(" ##### Districtwise and Pincodewise Top Transaction Details based on Amount")
            st.write(VisualizeOverallTopTransAmtByDistrict(mycursor))
            st.write(VisualizeOverallTopTransAmtByPincode(mycursor))

    elif top_trans_view == "Transaction Count":
        if(st.button('Visualize', key = 'top_trans_button')):
            st.write(" ##### Districtwise and Pincodewise Top Transaction Details based on Count")
            st.write(VisualizeOverallTopTransCountByDistrict(mycursor))
            st.write(VisualizeOverallTopTransCountByPincode(mycursor))

# Visualize Aggregated Users
def VisualizeAggregatedUser(mycursor):  

    # View user transaction based on State, Year, Quarter,  User Brand  
    agg_user_sub_views = st.multiselect(
        "Select field(s) to visualize ", 
        ["State", "Year", "Quarter", "User Brand"],
        key = 'agg_user_multi_select'
    )

    if (agg_user_sub_views):
        if(st.button('Visualize', key = 'agg_user_button')):
            for sub_view in agg_user_sub_views:
                if sub_view == 'State':
                    st.plotly_chart(VisualizeStateUserCount(mycursor), 
                                    use_container_width=True, 
                                    config={'responsive': True})
                if sub_view == 'Year':
                    st.plotly_chart(VisualizeYearUserCount(mycursor), 
                                    use_container_width=True, 
                                    config={'responsive': True})
                if sub_view == 'Quarter':
                    st.plotly_chart(VisualizeQuarterUserCount(mycursor), 
                                    use_container_width=True, 
                                    config={'responsive': True})
                if sub_view == 'User Brand':
                    st.plotly_chart(VisualizeUserBrandUserCount(mycursor), 
                                    use_container_width=True, 
                                    config={'responsive': True})

# Visualize Map Users
def VisualizeMapUser(mycursor):

    # View user details based on any one of the selected state from list
    query = 'SELECT DISTINCT state from map_user group by state'
    states = ExecuteQuery(mycursor,query)
    state = st.selectbox(
        "Select the state to view districtwise app and user details",
        states,
        index=None,
    )

    if state:
        # Sub View user details based on Registered Users or App Opens
        map_user_view=st.radio(
            "Radio Buttons",
            ["Registered Users", "App Opens"], 
            label_visibility = "hidden",
            horizontal = True, 
            key = "map_user_radio_buttons"
        )

        if map_user_view == "Registered Users":
            if(st.button('Visualize', key = 'map_user_button')):
                st.plotly_chart(VisualizeDistrictMapRegUsers(mycursor,state), 
                                use_container_width=True, 
                                config={'responsive': True})
                st.plotly_chart(VisualizeYearMapRegUsers(mycursor,state), 
                                use_container_width=True, 
                                config={'responsive': True})
                st.plotly_chart(VisualizeQuarterMapRegUsers(mycursor,state), 
                                use_container_width=True, 
                                config={'responsive': True})

        elif map_user_view == "App Opens":
            if(st.button('Visualize', key = 'map_user_button')):
                st.plotly_chart(VisualizeDistrictMapAppOpens(mycursor,state), 
                                use_container_width=True, 
                                config={'responsive': True})
                st.plotly_chart(VisualizeYearMapAppOpens(mycursor,state), 
                                use_container_width=True, 
                                config={'responsive': True})
                st.plotly_chart(VisualizeQuarterMapAppOpens(mycursor,state), 
                                use_container_width=True, 
                                config={'responsive': True})

# Visualize Top Users
def VisualizeTopUser(mycursor):

    # View user details based on Top 10 Districts or Pincodes
    top_user_views = st.multiselect(
        "Select option(s) ", 
        ["Top 10 Districts", "Top 10 Pincodes"],
        key = 'top_user_multi_select'
    )

    if (top_user_views):
        if(st.button('Visualize', key = 'top_user_button')):
            for view in top_user_views:
                if view == 'Top 10 Districts':
                    st.write(VisualizeOverallTopUserCountByDistrict(mycursor))
                if view == 'Top 10 Pincodes':
                    st.write(VisualizeOverallTopUsersCountByPincode(mycursor))
             
# --------------------Main Method--------------------------------------
def main():

    # MySQL DB Connection 
    mydb = mysql.connector.connect(
        host = 'localhost',
        user = 'root',
        password = 'Chinka@SQL123',
        db = 'phonepe'
    )
    mycursor = mydb.cursor()

    #Streamlit page setup
    st.set_page_config(
        page_title = "Phonepe Pulse Data Visualization and Exploration",
        page_icon =  ":chart_with_upwards_trend:",
        initial_sidebar_state = "auto"
        )
    
    #Sidebar population
    with st.sidebar:
        selected_menu =  option_menu(
            menu_title = "PhonePe Pulse",
            options = ['Home', 
                       'Data Extraction', 
                       'Transactions' , 
                       'User'],
            icons = ['house', 
                     'database-gear', 
                     'arrow-left-right', 
                     'person-circle'],
            menu_icon = 'graph-up-arrow'
        )

    if selected_menu == "Home":
        st.title("Phonepe Data Visualization and Exploration\n\n")
        image_path='D:/Chindhu/RoadMap for Career/Data Science/Guvi/Python Codes/Phonepe/phonepe.png'
        pil_image = Image.open(image_path)
        st.image(pil_image, caption = 'phonepe logo', use_column_width=True)
        st.write("\n\nPhonePe, India’s leading fintech platform, launched PhonePe Pulse, \
                 India’s first interactive website with data, \
                 insights and trends on digital payments in the country. \
                 The PhonePe Pulse showcases more than 2000+ Crore transactions \
                 by consumers on an interactive map of India.\n")
        st.write("Developed by Chindhu as an open source project which deals with \
                 extracting data from phonepe pulse dataset available \
                 in github, transforming and loading it into MySQL database. \
                 Data visualization and exploration are done using Streamlit and Plotly charts or graphs. \
                 Do leave your \
                 suggestions or bottle necks while using the portal, so that \
                 it can be revised and improved.\n")
        st.write('Email - chindhual@gmail.com')
    
    elif selected_menu == 'Data Extraction':
        st.header('Data Extraction')
        if(st.button('Extract Data from Source', key = 'top_user_button1')):
            agg_trans_df, \
            agg_user_df, \
            map_trans_df, \
            map_user_df, \
            top_trans_df, \
            top_user_df = DataTransform(mycursor)
            if not agg_trans_df.empty:
                st.write('Data successfully extracted and stored in agg_trans')
            if not agg_user_df.empty:
                st.write('Data successfully extracted and stored in agg_user')

            if not map_trans_df.empty:
                st.write('Data successfully extracted and stored in map_trans')
            if not map_user_df.empty:
                st.write('Data successfully extracted and stored in map_user')
            
            if not top_trans_df.empty:
                st.write('Data successfully extracted and stored in top_trans')
            if not top_user_df.empty:
                st.write('Data successfully extracted and stored in top_user')

    elif selected_menu == "Transactions":
        st.header("Transaction Details")
        tab1, tab2, tab3 = st.tabs(["Aggregated View", 
                                    "Districts View", 
                                    "Top Transactions"])

        with tab1 :
            VisualizeAggregatedTransaction(mycursor)
        with tab2:
            VisualizeMapTransaction(mycursor)
        with tab3:
            VisualizeTopTransaction(mycursor)

    elif selected_menu == 'User':
        st.header("User Details")
        tab1, tab2, tab3 = st.tabs(["Aggregated View", 
                                    "Districts View", 
                                    "Top Users"])

        with tab1 :
            VisualizeAggregatedUser(mycursor)
        with tab2:
            VisualizeMapUser(mycursor)
        with tab3:
            VisualizeTopUser(mycursor)

if __name__ == "__main__":
    main()