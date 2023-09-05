# -*- coding: utf-8 -*-
"""

Candidate: Ben Simmons
Poll Tracker Assignment
Last Edited: September 4, 2023

"""

import pandas as pd
from IPython.display import display_html
import datetime
import numpy as np
import urllib
from bs4 import BeautifulSoup

"""

Step 1: Read in polling data to be used in trendline analysis

"""

# Fetch the html file
    
    # def __init__(self):
        
def html_to_df():
    response = urllib.request.urlopen('https://cdn-dev.economistdatateam.com/jobs/pds/code-test/index.html')
    html_doc = response.read()
    
    # Parse the html file
    soup = BeautifulSoup(html_doc, 'html.parser')
    
    print("Parsed html using Beautiful Soup.\n")
    
    # Read in html file and convert to single table
    tables = pd.read_html(str(soup))
    
    table_raw = tables[0]
    
    print("Converted raw data to Pandas DataFrame.\n")
    
    return table_raw

"""

Step 2: Clean data and write out polls file

"""

def clean_polls(table_raw):
    
    table = table_raw
    
    # Remove commas from sample
    
    table.replace(',','', regex=True, inplace=True)
    
    print("Removed commas from Sample column. \n")
    
    # Remove percentage sign from candidate columns
    
    table.replace('%','', regex=True, inplace=True)
    
    print("Removed percentage sign (%) from column for each candidate. \n")
    
    # Remove * indicator for polls that exclude overseas territories
    
    table.replace('\*','', regex=True, inplace=True)
    
    print("Removed '*' indicator for polls that exclude overseas territories. \n")
    
    # Convert date column from object to datetime type
    
    table['Date'] = pd.to_datetime(table['Date']).dt.date
    
    # Convert columns indicating sample size and level of support for each candidate from object to numeric
    
    table[['Sample', 'Bulstrode', 'Lydgate','Vincy','Casaubon','Chettam','Others']] = table[['Sample', 'Bulstrode', 'Lydgate','Vincy','Casaubon','Chettam','Others']].apply(pd.to_numeric)
    
    # Correct data entry error: the level of support for Bulstrode in the Policy Voice Polling poll
    # on 11/18 should be 36.8%, not 63.8%
    
    table.loc[101,"Bulstrode"]=36.8
    
    print("Corrected data entry error for Policy Voice Polling poll from November 18. \n")
    
    # Reverse order of table to ensure earliest poll (from October) is the top row
    
    table_reverse = table.iloc[::-1].reset_index(drop=True)
    
    print("Reversed order of table to ensure earliest poll is top row. \n")
    
    candidate_list = ['Bulstrode','Lydgate','Casaubon','Vincy','Chettam','Others']
    
    for candidate in candidate_list:
        table_reverse[f'{candidate}'] = table_reverse[f'{candidate}']/100
    
    print("Divided numbers contained in all reported polling columns by 100 to present individual figures as decimals between 0 and 1. \n")
            
    table_reverse.to_csv(r'polls.csv',header=True)

    print("Wrote out polls.csv file. \n")
    
    table_cleaned = table_reverse
    
    return table_cleaned


"""

Step 3: Clean data in preparation for trend analysis

"""

def clean_for_trends(table_cleaned):
    
    table = table_cleaned
    
    # Choose one of two University of Bellville-sur-Mer polls in three instances where alternate question included
    # Duplication occurs on 1/26, 2/23, and 3/22
    # Model will discontinue prediction for Chettam after January 4, when the candidate appears to drop out
    # This step will ensure the poll that does not include Chettam is chosen

    table = table.drop(index = [89,117,144]).reset_index(drop=True)
    
    print("Dropped duplicate University of Belleville-sur-Mer polls that included support for Chettam after candidate appeared to have dropped out of race.\n")
    
    # Eliminate Chettam on Jan 4, add his total to that of "Others" moving forward
    
    table['New Others'] = np.where(
        ((table['Date'] > pd.to_datetime('2024-01-04')) & (pd.notnull(table['Chettam']))), table['Chettam'] + table['Others'], table['Others']) 
    
    table['New Chettam'] = np.where(
        table['Date'] > pd.to_datetime('2024-01-04'), None, table['Chettam']) 
    
    print("Reassigned Chettam vote to 'Others' for all relevant polls after January 4.\n")
    
    table = table.drop(['Chettam','Others'],axis=1)
    
    table = table.rename(columns={"New Chettam": "Chettam", "New Others": "Others"})
    
    table_for_trends = table
    
    return table_for_trends

"""

Step 4: Calculate trends for each candidate and write out data to trends file

"""

def create_trends(table_for_trends):
    
    start_date, end_date = datetime.date(2023, 10, 11), datetime.date(2024, 3, 25)

    date_range = pd.date_range(start_date, end_date - datetime.timedelta(days = 1), freq='d')

    # create list of days between start and end of range contained in polls data
    
    date_range = [str(d.date()) for d in date_range]
    
    print("Created list of each day between October 11, 2023 and March 24, 2024. \n")

    candidate_list = ['Bulstrode','Lydgate','Casaubon','Vincy','Chettam','Others']
    
    # trends_overall_df = pd.DataFrame()
    
    # create empty DataFrame that will capture data relevant to each day in for loop below
    
    trends_day_df = pd.DataFrame()
    
    # create empty dictionary to be used in for loop to capture calculated figure for each candidate on each day
    
    trends_overall_dict = {}
        
    outfile = open('log.txt','w')
    
    print("Created .txt file to record any instances where no polls had been conducted in previous seven days. \n")
        
    for date in date_range:
        
        # Format each date in the established date range to datetime format
        
        date = datetime.datetime.strptime(date,'%Y-%m-%d')
        
        # Create a seven day window of polls for each day included in larger range (between October and March)
        
        start_date, end_date = date - pd.to_timedelta("7day"), date
        
        date_range_for_date = pd.date_range(start_date, end_date, freq='d')
       
        date_range_for_date = [str(d.date()) for d in date_range_for_date]
        
        # Using dataframe created before for loop, create column to be used for date and convert to string
        
        trends_day_df["Date"] = date_range_for_date
        
        table_for_trends["Date"] = table_for_trends["Date"].astype(str)
        
        # convert date back to string for log purposes
        
        date = date.strftime('%Y-%m-%d')
        
        # Join the dataframe (one column of dates) with the table containing actual polls on the Date field
        
        trends_day_join = trends_day_df.merge(table_for_trends, left_on = "Date", right_on = "Date", how = 'left')
        
        # Weight by sample size (relative to overall sample size for polls in past seven days)
        
        sample_size_sum = trends_day_join["Sample"].sum()
        
        trends_day_join["Sample_Size_Weights"] = trends_day_join["Sample"] / sample_size_sum
        
        # Log which days did not have polls in previous seven days
        
        if trends_day_join['Pollster'].isnull().all() == True:
            output = "For " + str(date) + str(": No Polls in Previous 7 Days") + "\n"
            print(output)
            outfile.write(output)
            
        # Calculate average for the most recent day for each candidate
        
        for candidate in candidate_list:
            average = sum(trends_day_join[f'{candidate}'].fillna(0)*trends_day_join['Sample_Size_Weights'].fillna(0))
            trends_overall_dict[date,candidate] = average
    
    print("Created dictionary containing a weighted seven-day moving polling average for each combination of date and candidate.\n")
    
    # Populate DataFrame with output stored in dictionary for every combination of date and candidate
    
    trends_overall_df = pd.DataFrame(trends_overall_dict.items(), columns=['Date and Candidate','Average'])
    
    # Divide date and candidate into two columns
    
    trends_overall_df['Date and Candidate'].tolist()
    
    trends_overall_df[['Date', 'Candidate']] = pd.DataFrame(trends_overall_df['Date and Candidate'].tolist(), index=trends_overall_df.index)
    
    trends_overall_df = trends_overall_df.drop('Date and Candidate',axis=1)
    
    print("Separated date and candidate information captured in dictionary into two columns for final table.\n")
        
    trends_overall_pivot = trends_overall_df.pivot(index='Date', columns='Candidate', values='Average')
    
    # Address the end of December, when there are no polls, by assigning value from Dec. 25
    
    for candidate in candidate_list:
        trends_overall_pivot[f'{candidate}'] = np.where((trends_overall_pivot.index > '2023-12-25') & (trends_overall_pivot.index < '2023-12-30'), trends_overall_pivot.loc[trends_overall_pivot.index == '2023-12-25', f'{candidate}'].iloc[0], trends_overall_pivot[f'{candidate}'])
    
    print("Assigned most recent calculated value for dates in late December where no polls were conducted in previous seven days.\n")
    
    # Write out trends.csv file
    
    trends_overall_pivot.to_csv(r'trends.csv',header=True)
    
    print("Wrote out trends.csv file.\n")
    
    outfile.close()

    print("Wrote out log file.\n")
    
    return trends_overall_pivot

""" 

Having created functions above, execute them below.

"""

table_raw = html_to_df()
table_cleaned = clean_polls(table_raw)
table_for_trends = clean_for_trends(table_cleaned)
trends_overall_pivot = create_trends(table_for_trends)
