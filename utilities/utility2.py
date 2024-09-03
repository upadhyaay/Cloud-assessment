import pandas as pd
import sys
from datetime import timedelta, datetime

## Assigning the command line argument to a variable
argument = sys.argv

## Check if the value of argument is assigned and assign to filepath variable.
if len(argument) > 1:
    filepath = argument[1]
    # filepath = 'C:/Users/supadhyay045/Desktop/TestFile.xlsx'
    df =pd.read_excel(filepath,sheet_name='Sheet3')
    df2 =pd.read_excel(filepath,sheet_name='Fields')
    df2=df2.reset_index(drop=True)
    df3 =pd.read_excel(filepath,sheet_name='OS')
    df4 =pd.read_excel(filepath,sheet_name='DB')


    def cloud_score():
        unique_apps =[]
        unique_apps= df['App'].unique()

    # Iterate over each row and check the conditions and set the score in a seperate column for further use.   
        for v in unique_apps:
            dummy_df = df[df['App'] == v]
            score = 0
            if dummy_df['Special Compliance requirement'].unique()[0] == 'No':
                score +=1
            if dummy_df['Special HW Depedency'].unique()[0] == 'No':
                score +=1

            if 'Yes' in dummy_df['Is Legacy'].values:
                pass
            else:
                score +=1

            if 'Yes' in dummy_df['Special Latency Requirement'].values:
                pass
            else:
                score +=1

            if (any(dummy_df['AWS Supported OS'] == 'No') or any(dummy_df['Azure Supported OS'] == 'No')) or (dummy_df['AWS Supported OS'].isnull().values.any() or dummy_df['Azure Supported OS'].isnull().values.any()):
               pass
            else:
                score +=1

            if (any(dummy_df['AWS Supported DB'] == 'No') or any(dummy_df['Azure Supported DB'] == 'No')) or (dummy_df['AWS Supported DB'].isnull().values.any() or dummy_df['Azure Supported DB'].isnull().values.any()):
                pass
            else:
                score +=1

    ## Update the value of score in a column named Cloud Score      
            df.loc[df['App']==v,'Cloud Score'] = score

    def eol_date():
        compare_date = pd.to_datetime(datetime.now().date() + timedelta(days=730))
        df['OS EOL'] = 'Yes'
        for value in df['OS']:
            dummy = df3[df3['Operating system'] == value]
            if value in dummy['Operating system'].values:
                df.loc[df['OS'] == value , 'OS EOL Date'] = dummy['EOL date'].values[0]
                df.loc[df['OS EOL Date'] >= compare_date, 'OS EOL'] = 'No'
            else:
                df.loc[df['OS'] == value , 'OS EOL Date'] = '2020-01-14T00:00:00.000000000'

        df['DB EOL'] = 'Yes'
        for value in df['DB']:
            dummy = df4[df4['Database'] == value]
            if value in dummy['Database'].values:
                df.loc[df['DB'] == value , 'DB EOL Date'] = dummy['EOL date'].values[0]
                df.loc[df['DB EOL Date'] >= compare_date, 'DB EOL'] = 'No'
            else:
                df.loc[df['DB'] == value , 'DB EOL Date'] = '2020-01-14T00:00:00.000000000'
    
    def no_of_nodes():
        count_df = df.groupby('App')['App'].count().reset_index(name='count')

        for index, row in count_df.iterrows():
            value = row['App']
            count = row['count']
            df.loc[df['App'] == value , 'No. of Nodes'] = count

    def part_of_complexCluster():
    # Group the dataframe by the column of interest and count the occurences of each unique value
        count_df = df.groupby('Server (Hostname)')['Server (Hostname)'].count().reset_index(name='count')

    # Create a new column in the original database with default value 'no'
        df['Part of Complex Cluster'] = 'no'

    # Iterate over the rows of the count dataframe and set the value of 'Part of Complex Cluster' to yes 
        for index, row in count_df.iterrows():
            value = row['Server (Hostname)']
            count = row['count']
            if count>=3:
                df.loc[df['Server (Hostname)'] == value , 'Part of Complex Cluster'] = 'yes'

    def Server_Complexity():
        
    # Group the dataframe by the column of interest and count the occurences of each unique value
        count_df = df.groupby('Server (Hostname)')['Server (Hostname)'].count().reset_index(name='count')

    # Create a new column in the original database with default value 'Low'
        df['Cluster Complexity'] = 'Low'

    # Iterate over the rows of the count dataframe and set the value of 'Cluster Complexity' to High/Medium/Low 
        for index, row in count_df.iterrows():
            value = row['Server (Hostname)']
            count = row['count']
            if count>=5:
                df.loc[df['Server (Hostname)'] == value , 'Cluster Complexity'] = 'High'
            elif count>=3 and count<5:
                df.loc[df['Server (Hostname)'] == value , 'Cluster Complexity'] = 'Medium'
            else:
                df.loc[df['Server (Hostname)'] == value , 'Cluster Complexity'] = 'Low'

    def Migration_complexity():

    # Group the dataframe by the column of interest and count the occurences of each unique value
        count_df = df.groupby('App')['App'].count().reset_index(name='count')

    # Iterate over the rows of the count dataframe and set the value of 'Migration Complexity' to High/Medium/Low 
        for index, row in count_df.iterrows():
            value = row['App']
            count = row['count']
            dummy_df = df[df['App'] == value]
            if (count>=5):
                df.loc[df['App'] == value , 'Migration Complexity Factor'] = 'High'

            elif (count>=3 and count<5):
                if (any(dummy_df['Cluster Complexity'] == 'High')):
                    df.loc[df['App'] == value , 'Migration Complexity Factor'] = 'High'
                else:
                    df.loc[df['App'] == value , 'Migration Complexity Factor'] = 'Medium'

            else:
                if (any(dummy_df['Cluster Complexity'] == 'Medium')):
                    df.loc[df['App'] == value , 'Migration Complexity Factor'] = 'Medium'
                elif (any(dummy_df['Cluster Complexity'] == 'High')):
                    df.loc[df['App'] == value , 'Migration Complexity Factor'] = 'High'
                else:
                    df.loc[df['App'] == value , 'Migration Complexity Factor'] = 'Low'

    def aws_supported_os(df3):
        df3 = df3[df3['CSP'] == 'AWS']
        df['AWS Supported OS'] = 'No'
        for value in df['OS']:
            dummy = df3[df3['Operating system'] == value]
            if value in dummy['Operating system'].values:
                df.loc[df['OS'] == value , 'AWS Supported OS'] = 'Yes'

    def azure_supported_os(df3):
        df3 = df3[df3['CSP'] == 'Azure']
        df['Azure Supported OS'] = 'No'
        for value in df['OS']:
            dummy = df3[df3['Operating system'] == value]
            if value in dummy['Operating system'].values:
                df.loc[df['OS'] == value , 'Azure Supported OS'] = 'Yes'

    def cloud_fitment():
        cloud_score()
        
    ## Cloud Agnostic
        df.loc[(df['Cloud Score'] == 6) & (df['AWS Supported OS'] == 'Yes')   & (df['AWS Supported DB'] == 'Yes'),'Cloud Fitment Result'] = 'Cloud Ready'
        df.loc[(df['Cloud Score'] == 6) & (df['Azure Supported OS'] == 'Yes') & (df['Azure Supported DB'] == 'Yes'),'Cloud Fitment Result'] = 'Cloud Ready'
        df.loc[(df['Cloud Score'] == 6) & (df['Azure Supported OS'] == 'No')  & (df['Azure Supported DB'] == 'Yes'),'Cloud Fitment Result'] = 'Cloud Potential'
        df.loc[(df['Cloud Score'] == 6) & (df['Azure Supported OS'] == 'Yes') & (df['Azure Supported DB'] == 'No'),'Cloud Fitment Result'] = 'Cloud Potential'
        df.loc[(df['Cloud Score'] == 6) & (df['AWS Supported OS'] == 'Yes')   & (df['AWS Supported DB'] == 'No'),'Cloud Fitment Result'] = 'Cloud Potential'
        df.loc[(df['Cloud Score'] == 6) & (df['AWS Supported OS'] == 'No')    & (df['AWS Supported DB'] == 'Yes'),'Cloud Fitment Result'] = 'Cloud Potential'
        df.loc[(df['Cloud Score'] >3) & (df['Cloud Score'] <6),'Cloud Fitment Result'] = 'Cloud Potential'
        df.loc[(df['Cloud Score'] <= 3),'Cloud Fitment Result'] = 'Tough Nuts'
        df.loc[(df['Special HW Depedency'] == 'Yes') & (df['Special Compliance requirement'] == 'Yes'),'Cloud Fitment Result'] = 'Retained'

    def aws_supported_db(df4):
        df4 = df4[df4['CSP'] == 'AWS']
        df['AWS Supported DB'] = 'No'
        for value in df['DB']:
            dummy = df4[df4['Database'] == value]
            if value in dummy['Database'].values:
                df.loc[df['DB'] == value , 'AWS Supported DB'] = 'Yes'

    def migration_groups():
        # Initialize a dictionary to store the migration groups
        migration_groups = {}

        # Helper function to assign migration group based on common servers
        def assign_migration_group(app):
            for group, servers in migration_groups.items():
                if any(server in df[df['App'] == app]['Server (Hostname)'].values for server in servers):
                    return group
            return None

        # Iterate over each row in the DataFrame
        for index, row in df.iterrows():
            app = row['App']
            server = row['Server (Hostname)']
            
            # Check if the app is already assigned to a migration group
            if app not in migration_groups:
                group = assign_migration_group(app)
                if group is None:
                    group = f"MG{len(migration_groups) + 1}"
                    migration_groups[group] = set()
                migration_groups[group].add(server)
            
            # Assign the migration group to the row
            df.at[index, 'App-to-Server-Migration-Group'] = group
            
    def azure_supported_db(df4):
        df4 = df4[df4['CSP'] == 'Azure']
        df['Azure Supported DB'] = 'No'
        for value in df['DB']:
            dummy = df4[df4['Database'] == value]
            if value in dummy['Database'].values:
                df.loc[df['DB'] == value , 'Azure Supported DB'] = 'Yes'

    def setting_output_sheet():
        output_df = pd.DataFrame()
        output_df['Application Name']               = df['App']
        output_df['Is Legacy']                      = df['Is Legacy']
        output_df['Server Dependency']              = df['Server (Hostname)']
        output_df['OS']                             = df['OS']
        output_df['OS EOL']                         = df['OS EOL']
        output_df['OS Supported- AWS']              = df['AWS Supported OS']
        output_df['OS Supported- Azure']            = df['Azure Supported OS']
        output_df['DB']                             = df['DB']
        output_df['Database EOL']                   = df['DB EOL']
        output_df['DB Supported- AWS']              = df['AWS Supported DB']
        output_df['DB Supported- Azure']            = df['Azure Supported DB']
        output_df['Special Compliance App']         = df['Special Compliance requirement']
        output_df['Special Hardware Dependent']     = df['Special HW Depedency']
        output_df['No of Nodes']                    = df['No. of Nodes']
        output_df['Is Part of Complex Cluster']     = df['Part of Complex Cluster']
        output_df['Server Complexity']              = df['Cluster Complexity']
        output_df['Migration Complexity']           = df['Migration Complexity Factor']
        output_df['Cloud Fitment Result']           = df['Cloud Fitment Result']
        output_df['Migration Group']                = df['Migration Group']

        return output_df

    def paas_supported(df3):
        df3 = df3[df3['CSP'] == 'AWS']
        df['PaaS supported'] = 'No'
        for value in df['Technology Framework']:
            print(value)
            dummy = df3[df3['Framework'] == value]
            if value in dummy['Framework'].values:
                df.loc[df['Technology Framework'] == value , 'PaaS supported'] = 'Yes'

        df3 = df3[df3['CSP'] == 'Azure']
        for value in df['Technology Framework']:
            dummy = df3[df3['Framework'] == value]
            if value in dummy['Framework'].values:
                df.loc[df['Technology Framework'] == value , 'PaaS supported'] = 'Yes'

    def app_to_app_clusters():
        # Create an empty dictionary to store the clusters and their numbering
        clusters = {}
        cluster_numbering = 0

        # Function to find the cluster for an app
        def find_cluster(app):
            if app in clusters:
                return clusters[app]
            return None

        # Iterate over the rows of the DataFrame
        for _, row in df.iterrows():
            source_app = row['source_app']
            dest_app = row['dest_app']

            source_cluster = find_cluster(source_app)
            dest_cluster = find_cluster(dest_app)

            if source_cluster is None and dest_cluster is None:
                # Both apps are not in any existing clusters, create a new cluster
                cluster_numbering += 1
                cluster_name = f"Cluster{cluster_numbering}"
                clusters[source_app] = cluster_name
                clusters[dest_app] = cluster_name
            elif source_cluster is not None and dest_cluster is None:
                # Source app is already in a cluster, add the dest app to the same cluster
                clusters[dest_app] = source_cluster
            elif source_cluster is None and dest_cluster is not None:
                # Destination app is already in a cluster, add the source app to the same cluster
                clusters[source_app] = dest_cluster
            elif source_cluster != dest_cluster:
                # Both apps are in different clusters, merge the clusters into one
                for app, cluster in clusters.items():
                    if cluster == source_cluster:
                        clusters[app] = dest_cluster

        # Create a new 'clusters' column in the DataFrame
        df['app-to-app-clusters'] = df['source_app'].apply(lambda app: clusters.get(app, None))
        print(clusters)

        # # Create a new 'count_unique_apps' column to store the count of unique related apps for each source app
        # df['count_unique_apps'] = df.groupby('source_app')['dest_app'].transform('nunique')

        # print(df['count_unique_apps'])
    
    def app_dependency():

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

        with pd.ExcelWriter('App_dep_updated_02.xlsx', mode='a',if_sheet_exists='replace') as writer:  
            app_connections_new.to_excel(writer, sheet_name='New',index=False)


    if __name__ == '__main__':
            #This function checks the EOL date and sets the OS/HW/DB EOL to Yes/No
            # eol_date()

            # #This function checks wheather the server is part of complex cluster by checking if the server to app dependency exceeds or equal to 3.
            # part_of_complexCluster()
            
            # ##This function checks the number of servers attached to an Application and gives the count.
            # no_of_nodes()

            # #This function provides more detailed analysis by categorizing the Cluster Complexity by High/Medium/Low server wise.
            # Server_Complexity()
            
            # #This function provides more detailed analysis by categorizing the Migration Complexity by High/Medium/Low , app wise.
            # Migration_complexity()
            
            # #This function checks the OS version provided by user with the OS master sheet according to CSP and populates value as Yes/No. 
            # aws_supported_os(df3)

            # #Checks the OS version supportability according to the CSP. 
            # azure_supported_os(df3)

            #This function checks the DB provided by user with the DB master sheet according to CSP and populates value as Yes/No. 
            # aws_supported_db(df4)

            # #Checks the DB supportability according to the CSP. 
            # azure_supported_db(df4)

            # ## sets the Migration group 
            # migration_groups()

            ###App to App Clustering Function
            app_to_app_clusters()

            ## Stores the apps dependency count in a new column
            # app_dependency()

            # ## Cloud Fitment Function
            # cloud_fitment()

            ### PaaS supported frameworks
            # paas_supported(df3) 

            # ## set valaues
            # output_df = setting_output_sheet()

            ##Writes the overall modified dataframe to Excel and stores in a new file.
            df.to_excel('Modified-excel.xlsx',index=False)
            # output_df.to_excel('Modified-excel-updated.xlsx',index=False)

            # app_specific_columns = output_df[['Application Name',
            #                                   'Special Compliance App',
            #                                   'Special Hardware Dependent',
            #                                   'No of Nodes',
            #                                   'Migration Complexity',
            #                                   'Cloud Fitment Result',
            #                                   'App-to-Server-Migration-Group'
            #                                   ]]
            # app_data = app_specific_columns.drop_duplicates()
            
            # server_specific_columns = output_df[['Server Dependency',
            #                                      'Is Legacy',
            #                                      'OS',
            #                                      'OS EOL',
            #                                      'OS Supported- AWS',
            #                                      'OS Supported- Azure',
            #                                      'DB','Database EOL',
            #                                      'DB Supported- AWS',
            #                                      'DB Supported- Azure',
            #                                      'Is Part of Complex Cluster',
            #                                      'Server Complexity']]
            # server_data = server_specific_columns.drop_duplicates()
## Writing app specific columns in a different worksheet 
#         with pd.ExcelWriter('Modified-excel-updated.xlsx', mode='a',if_sheet_exists='replace') as writer:  
#             app_data.to_excel(writer, sheet_name='App-specific-data',index=False)

# ## Writing server specific columns in a different worksheet             
#         with pd.ExcelWriter('Modified-excel-updated.xlsx', mode='a',if_sheet_exists='replace') as writer:  
#             server_data.to_excel(writer, sheet_name='Server-specific-data',index=False)

        # with pd.ExcelWriter(filepath, mode='a',if_sheet_exists='replace') as writer:  
        #     df.to_excel(writer, sheet_name='Final_output',index=False)


else:
    raise Exception ("Please provide the Filepath")
