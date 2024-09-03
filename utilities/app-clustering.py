import pandas as pd

filepath = 'C:/Users/supadhyay045/Documents/Data/SBIL_utilities/2-Utility/abcd.xlsx'
df =pd.read_excel(filepath,sheet_name='Sheet1')

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
    source_app = row['Source App']
    dest_app = row['Target App']

    source_cluster = find_cluster(source_app)
    dest_cluster = find_cluster(dest_app)

    if source_cluster is None and dest_cluster is None:
        # Both apps are not in any existing clusters, create a new cluster
        cluster_numbering += 1
        cluster_name = "Cluster{}".format(cluster_numbering)
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
df['app-to-app-clusters'] = df['Source App'].apply(lambda app: clusters.get(app, None))

##Writes the overall modified dataframe to excel and stores in a new file.
# df.to_excel('clusters.xlsx',index=False)
with pd.ExcelWriter('clusters.xlsx', mode='a',if_sheet_exists='replace') as writer:  
    df.to_excel(writer, sheet_name='latest',index=False)
