import pandas as pd
filepath = 'filepath'

df =pd.read_excel(filepath,sheet_name='Sheet1')
df = df.astype(str)

# Extract unique apps and sort
unique_apps = sorted(list(set(df['source_app']) | set(df['dest_app'])))

# Create an empty dictionary to store connected apps
app_connections = {}

# Initialize the dictionary with unique_apps as keys
for app in unique_apps:
    app_connections[app] = []

# Populate the connected apps for each app
for _, row in df.iterrows():
    source_app = row['source_app']
    dest_app = row['dest_app']
    if source_app != dest_app:  # Avoid self-loop
        app_connections[source_app].append(dest_app)
        app_connections[dest_app].append(source_app)

# Iterate through the dictionary and remove duplicates from each list
for key in app_connections:
    app_connections[key] = list(set(app_connections[key]))

# Create a new DataFrame for storing app connections and their counts
app_connections_new = pd.DataFrame(app_connections.items(), columns=['IP', 'Connected IPs'])
# Add a column to store the count of connected apps for each app
app_connections_new['Dependencies Count'] = app_connections_new['Connected IPs'].apply(len)

with pd.ExcelWriter('App_dep_updated.xlsx', mode='a',if_sheet_exists='replace') as writer:  
    app_connections_new.to_excel(writer, sheet_name='latest',index=False)
