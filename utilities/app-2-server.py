################################################### no dependencies found ####################################################

import pandas as pd
import re

# Load IP mapping from the first Excel sheet
ip_mapping_file = "C:/Users/supadhyay045/Downloads/Server-To-Server-Dependencies.xlsx"  # Replace with your file path
ip_mapping_df = pd.read_excel(ip_mapping_file,sheet_name="Sheet1")

# Load app mappings from the second Excel sheet
app_mapping_file = "C:/Users/supadhyay045/Downloads/App-Server-Mapping.xlsx"  # Replace with your file path
app_mapping_df = pd.read_excel(app_mapping_file,sheet_name="Compressed")

# Create a list to store the output mapping
output_mapping = []
count=0

# Iterate through IP mapping and generate the output mapping
for _, row in ip_mapping_df.iterrows():
    source_ip = row['IP']
    target_ips = row['Connected IPs']
    target_ips = re.findall( r'[0-9]+(?:\.[0-9]+){3}', target_ips )
    source_apps = app_mapping_df[app_mapping_df['IP'] == source_ip]['Applications'].tolist()
    target_apps = []
    for target_ip in target_ips:
        target_app_names = app_mapping_df[app_mapping_df['IP'] == target_ip]['Applications'].tolist()
        target_apps.extend(target_app_names)
        
    # Run the Condition if the target Ip list is empty    
    if not target_apps:
        count+=1
        target_apps = [f'None found{count}']

    if source_apps:
        for source_app in source_apps:
            for target_app in target_apps:
                 output_mapping.append([source_app, target_app])
        

##Create a DataFrame from the output mapping list
columns = ['Source App', 'Target App']
output_mapping_df = pd.DataFrame(output_mapping, columns=columns)

# output_mapping_df = output_mapping_df[output_mapping_df['Source App'] != output_mapping_df['Target App']]
# output_mapping_df = output_mapping_df.drop_duplicates(subset=['Source App', 'Target App'], keep='first')

##Write the output mapping to a new Excel sheet
output_file = "a2a-compressed-new.xlsx"  # Replace with your desired output file path
output_mapping_df.to_excel(output_file, index=False)
# with pd.ExcelWriter("abcd.xlsx", mode='a',if_sheet_exists='replace') as writer:  
#     output_mapping_df.to_excel(writer, sheet_name='op3',index=False)



