import pandas as pd
from cryptography.fernet import Fernet
import sys

## Assigning the command line argument to a variable
argument = sys.argv

## Check if the value of argument is assigned and assign to filepath variable.
if len(argument) > 1:
    filepath = argument[1]
    # Generate a random encryption key
    key = Fernet.generate_key()

    # Initialize a new empty Dataframe for lookup
    lookup_df=pd.DataFrame()

    # Create a Fernet instance with the encryption key
    fernet = Fernet(key)

    # Read the Excel file into a pandas dataframe
    df = pd.read_excel(filepath,sheet_name='L1 Cloud Fitment')

    # Store the Server Names in the new lookup dataframe.
    lookup_df['Servers'] = df['Server (Hostname)'].unique()

    # Create a dictionary to map unique values to encrypted values
    unique_values = df['Server (Hostname)'].unique()
    value_map = {val: fernet.encrypt(val.encode()).decode()[-10:] for val in unique_values}

    # Apply the mapping to the column and write the result to a new column
    df['Server (Hostname)'] = df['Server (Hostname)'].apply(lambda x: value_map[x])

    #Store the Encrypted names in a new column in lookup dataframe.
    lookup_df['Encrypted_Names'] = df['Server (Hostname)'].unique()

    # Write the dataframe back to the Excel file
    df.to_csv('Anonymized-excel.csv', index=False)
    lookup_df.to_csv('lookup-file.csv',index=False)
else:
    raise Exception ("Please Enter the correct path")
    
