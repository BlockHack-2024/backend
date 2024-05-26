import pyfredapi as pf
import pandas as pd
from datetime import datetime, timedelta

today = datetime.today()
formatted_date = today.strftime("%Y-%m-%d")
oneYearAgo = today - timedelta(weeks=52)
oneYearAgoUpdated = oneYearAgo.strftime("%Y-%m-%d")


extra_parameters = {
    "observation_start": oneYearAgoUpdated,
    "observation_end": formatted_date,
}

#EXAMPLE OF USING API
#data = pf.get_series(series_id='DGS10', api_key ='ce685b1a0a415f55e32c4deb1885616f', **extra_parameters)
#print(data)




## Create function to get a series, but make sure that names are "series short cut - date" etc.
# Function currently does not save the new data. We can possible have funtion return in and then add to larger one

def create_dataframe_correctdates(series_id):
    series_id = str(series_id)
    data = pf.get_series(series_id=series_id, api_key ='ce685b1a0a415f55e32c4deb1885616f', **extra_parameters)
    new_data = data.rename(columns={'date': series_id + " " + 'date', 'value': series_id + ' ' + 'value'})
    return new_data

#Create a Dictionary of all of the series we will be looking at. For now only various bonds
bonds = {
    '1-Year': 'DGS1',
    '2-Year': 'DGS2',
    '5-Year': 'DGS5',
    '10-Year': 'DGS10',
    '30-Year': 'DGS30',
    '10-year minus 2 year': 'T10Y2Y'
}


for value in bonds.values():


    additional_data = create_dataframe_correctdates(value)
    try:
        fed_dataframe
    
        for col_name, cole_data in additional_data.items():
            fed_dataframe[col_name] = cole_data

    except NameError:

        fed_dataframe = pd.DataFrame(additional_data)
    

print(fed_dataframe)
# fed_dataframe created, now create a function that will take it in and request predictions in the future
# in other words the miner!

# Validator
# Create the request/API stuff while sending data and a time stamp






def fed_miner(df):

    #We are taking in the fed_data frame, consider creating checks for ensuring it is the right df
    #ARGS: pandas dataframe of al the various interests rates we will be guessing

    #returns a dictionary with the values and guesses 

    df = df.sort_values(by='DGS1 date', ascending=True)

    ## To Do : Ensure all dates are the same
    ##
