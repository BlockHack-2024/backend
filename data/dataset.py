import pyfredapi as pf


datasets = [{'task_id' : 'LNS12027662', 'task_name': "Employment Level - Bachelor's Degree and Higher, 25 Yrs. & over", 'type': 'employment'},
            {'task_id' : 'QGMPUSMP', 'task_name': "Total Quantity Indexes for Real GDP for United States Metropolitan Portion", 'type': 'national_account'},
            {'task_id' : 'ISRATIO', 'task_name': "Total Business: Inventories to Sales Ratio", 'type': 'production_business_activity'},
            {'task_id' : 'EXPINF10YR', 'task_name': "10-Year Expected Inflation", 'type' : 'prices'},
            {'task_id' : 'QCAR628BIS', 'task_name': "Real Residential Property Prices for Canada", 'type':'international' },
            {'task_id': 'X02LIL', 'task_name': 'Total Loans in All Banks in the United States', 'type':'academic' },
            {'task_id' : 'CAUR', 'task_name': "nemployment Rate in California", 'type': 'regional'}]

"""
datasets = {
    'DGS1' : {'task_name': '1-Year Bond Predictinon', 'task_description': 'Predict the Bond Value for next day/week/month'}
}
"""

# read specific dataset for the task_id returning as a dataframe
def read_dataset(task_id):
    return pf.get_series(series_id=task_id, api_key="1f1e05bb87c20653b2af4cd3c9a17397")

# print(read_dataset('QCAR628BIS'))

