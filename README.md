# PhonePe-Data-Visualization-and-Exploration
This is an open-source project repo which deals with extracting data from PhonePe Pulse dataset available in [github](https://github.com/PhonePe/pulse/tree/master
), cleaning, transforming and loading it into MySQL database. Data visualization and exploration are done using Streamlit and Plotly charts or graphs.

## Introduction
PhonePe, India’s leading fintech platform, launched PhonePe Pulse, India’s first interactive website with data, insights and trends on digital payments in the country. The PhonePe Pulse showcases more than 2000+ Crore transactions by consumers on an interactive map of India.   
This project aims at Extracting, Transforming and Loading data from [source repository](https://github.com/PhonePe/pulse/tree/master
) into MySQL and visualizing, exploring the data by creating a Streamlit application along with Plotly charts.   
Note : This project works on the data timelined between **2018 and 2023**.

## Table of Contents
1. Pre-requisites
2. Technology Stacks 
3. Usage
4. Data Extraction
5. Data Cleaning and Transformation
6. Migrating to MySQL
8. Visualization and Exploration
9. Further Improvements

## Pre-requsites
Install the following packages to run the project. 
```
pip install streamlit
pip install pandas
pip install os
pip install json
pip install mysql.connector
pip install plotly
pip install SQLAlchemy
pip install streamlit_option_menu
pip install geopy
pip install retrying
pip install Pillow

```

## Technology Stack
- Python scripting 
- SQL - MySQL
- Streamlit App development
- Github Data Extraction
- Plotly

## Usage
Clone the repo from the below mentioned link.  
[PhonePe-Data-Visualization-and-Exploration](https://github.com/Chindhu-Alagappan/PhonePe-Data-Visualization-and-Exploration.git)    
Install packages from "requirement.txt"  
Run the streamlit application using `streamlit run .\PhonePe.py`  
View the portal in your [localhost](http://localhost:8501/)    

## Data Extraction 
Clone the PhonePe Pulse data from the source repo and save it to your local.

Extract the following data as dataframe from the source directory.
- agg_trans_df - ./pulse/data/aggregated/transaction/country/india/state/
- agg_user_df - ./pulse/data/aggregated/user/country/india/state/

- map_trans_df - ./pulse/data/map/transaction/hover/country/india/state/
- map_user_df - ./pulse/data/map/user/hover/country/india/state/

- top_trans_df - ./pulse/data/top/transaction/country/india/state
- top_user_df - ./pulse/data/top/user/country/india/state

## Data Cleaning and Transformation
1. Convert the state names to title case
2. Replace '-' in state names to space
3. Find 'Andaman & Nicobar Islands' and replace with 'Andaman & Nicobar'
4. Load data to a CSV for reference

## Migrating to MySQL 
Connection must to be established between python and MySQL using sqlalchemy.create_engine(for creating tables directly from dfs) and mysql.connector package(for queries).   
The tables' schema has been shown below.

**Table : agg_trans**
| Column Name | Data Type | Description |
| :---------- | :-------- | :---------- |
| state | text | State of Transaction |
| year | bigint | Year of Transaction |
| quarter | bigint  | Quarter to which the Transaction belongs |
| trans_type | text | Transaction Type |
| trans_count | bigint | Total Count of Transactions |
| trans_amt | double | Total Transaction Amount |

**Table : agg_user**
| Column Name | Data Type | Description |
| :---------- | :-------- | :---------- |
| state | text | State of Transaction |
| year | bigint | Year of Transaction |
| quarter | bigint  | Quarter to which the Transaction belongs |
| user_brand | text | Brand Names of Users |
| user_count | bigint | Total Count of Users |
| user_percentage | double | Total Percentage of Users |

**Table : map_trans**
| Column Name | Data Type | Description |
| :---------- | :-------- | :---------- |
| state | text | State of Transaction |
| year | bigint | Year of Transaction |
| quarter | bigint  | Quarter to which the Transaction belongs |
| hover_name | text | District to which the Transaction belongs |
| hover_count | bigint | Count of Transactions |
| hover_amt | double | Transaction Amount |

**Table : map_user**
| Column Name | Data Type | Description |
| :---------- | :-------- | :---------- |
| state | text | State of Transaction |
| year | bigint | Year of Transaction |
| quarter | bigint  | Quarter to which the Transaction belongs |
| location | text | District to which the Transaction belongs |
| apps_open | bigint | Count of App Opens |
| registered_users | double | Count of Registered Users |

**Table : top_trans**
| Column Name | Data Type | Description |
| :---------- | :-------- | :---------- |
| state | text | State of Transaction |
| year | bigint | Year of Transaction |
| quarter | bigint  | Quarter to which the Transaction belongs |
| loc_type | text | Either Distrcits / Pincodes |
| loc_entity_name | text | District name or pincode |
| loc_entity_amt | double | Transaction Amount |
| loc_entity_count | bigint | Count of Transactions |

**Table : top_user**
| Column Name | Data Type | Description |
| :---------- | :-------- | :---------- |
| state | text | State of Transaction |
| year | bigint | Year of Transaction |
| quarter | bigint  | Quarter to which the Transaction belongs |
| loc_type | text | Either Distrcits / Pincodes |
| loc_entity_name | text | District name or pincode |
| loc_registered_users | double | Count of Registered Users |

## Visualization and Exploration
Data from these 6 tables are presented in a visually appealing form using plotly express plots, plotly choropleth charts. <br>
Also, streamlit's visualization functions help us understand the power of visualization by displaying these charts in a readily available format. <br>
Some snippets from the portal are displayed below. <br><br>
Transaction Details : <br><br>
![Transaction Details](https://github.com/Chindhu-Alagappan/PhonePe-Data-Visualization-and-Exploration/blob/bf895d18262a9734e3a3cb4a0f70da77fce8ece2/Snapshots-portal/Img_3.png) 
<br>
<br>
<br>
User Details :  <br><br>
![User Details](https://github.com/Chindhu-Alagappan/PhonePe-Data-Visualization-and-Exploration/blob/bf895d18262a9734e3a3cb4a0f70da77fce8ece2/Snapshots-portal/Img_5.png) <br>

## Further Improvements 
The project can further be enhanced by plotting 3D charts and building models to predict the transaction and user details for the upcoming quarters. This will help us to better understand and organize the fintech platform and ensure to accomodate add servers to handle forthcoming demands.
If you encounter any issues or have suggestions for improvements, feel free to reach out.  
  
Email : *chindhual@gmail.com*  
LinkedIn : *https://www.linkedin.com/in/chindhu-alagappan-57605112a/*
  
Thanks for showing interest in this repository ! 

