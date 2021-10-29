import datetime
import pandas as pd
from sqlalchemy import create_engine
from io import StringIO
from azure.storage.blob import BlobClient, ContainerClient
import urllib


def run():
        try:
# Step 1: setup azure storage account and database connnections
                connect_str = "DefaultEndpointsProtocol=https;AccountName=compassmaceheaderddap;AccountKey=Xx6q+RqApIixr232z+pO8HS/kurWRj2rAxHhKtmF6bzkNoTjs8qzUjj6YsXbt76tlkMTVk3AU1KMHDqbb+NlrQ==;BlobEndpoint=https://compassmaceheaderddap.blob.core.windows.net/;QueueEndpoint=https://compassmaceheaderddap.queue.core.windows.net/;TableEndpoint=https://compassmaceheaderddap.table.core.windows.net/;FileEndpoint=https://compassmaceheaderddap.file.core.windows.net/;"
                container = ContainerClient.from_connection_string(conn_str=connect_str, container_name="raw-attachments")
                
        # establish connection to DB location
                server = 'MIPOCSQL01'
                #port = '5432'
                dbName = 'DATAMGT_interreg_compass'
                raw_table = 'compass_raw'
                #erddap_table = 'erddap_compass'
                username = 'compassadmin'
                password = 'Compass@2021'

                params = urllib.parse.quote_plus("DRIVER={ODBC+Driver+17+for+SQL+Server};SERVER="+server+";DATABASE="+dbName+";UID="+username+";PWD="+password)
                engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

                #engine = create_engine('mssql+pyodbc://'+username+':'+password+'@'+server+':'+port+'/'+dbName)

        # Step 2: parse parse through each blob in container
                combined_df = pd.DataFrame()
                blob_count = 0
                blob_list = container.list_blobs()
                
                for b in blob_list:
                        print("Processing file: "+b.name)
                        blob = BlobClient.from_connection_string(conn_str=connect_str, container_name="raw-attachments", blob_name = b.name)
                        blob_data = blob.download_blob().content_as_text()
                
                        dfDataHeaders = pd.read_csv(StringIO(blob_data), sep=',', skiprows=[0], nrows=2,)
                # dataframe with first 2/3 rows as columns
                        dfGetUnitsFreq = dfDataHeaders.transpose().reset_index()
                # rename columns
                        dfGetUnitsFreq.rename(columns={'index': 'Parameter',0: 'Unit', 1: 'Frequency'},inplace=True)
                
                # Step 3: transpose data from row to column into a separate dataframe
                # combined structure is: identifier, timestamp, parameter name, unit, frequency, value
                
                # dataframe with rest of data
                
                        dfData = pd.read_csv(StringIO(blob_data), sep=',', skiprows=[0, 2, 3])
                        df_meltData = pd.melt(dfData, id_vars=['TIMESTAMP', 'RECORD'], value_vars=None, var_name='Parameter', value_name='Value')
                
                # Step 4: join dataframes - left join on parameter columns
                        combined_df = pd.merge(df_meltData, dfGetUnitsFreq, how='left', left_on=['Parameter'], right_on=['Parameter'])

                        # Cast timestamp to datetime, text values to numeric and fill blanks 
                        
                        combined_df['TIMESTAMP'] = pd.to_datetime(combined_df['TIMESTAMP'])
                        
                        combined_df['Value'] = pd.to_numeric(combined_df['Value'], errors='coerce').fillna(0)
                        
                # Step 5: create table if not created in SQL sever database, append dataframes to table

                # Write the raw DataFrame to a table in the sql DB
                        print("Loading to raw table...")
                        combined_df.to_sql(raw_table, engine, if_exists='append', index=False)
                        print("Load to raw table complete!")
                        print("Number of rows inserted:"+str(combined_df.shape[0]))

                        # Pivot flat data for ERDDAP
                        
                        #print("Loading to ERDDAP table...")
                        
                        #pivot_df = pd.pivot_table(combined_df, values='Value', index=['TIMESTAMP', 'RECORD'], columns='Parameter').reset_index()
                        #pivot_df['TIMESTAMP'] = pd.to_datetime(pivot_df['TIMESTAMP'])
                        #pivot_df['Latitude'] = 53.198769
                        #pivot_df['Longitude'] = -9.5605
                        #pivot_df['Station_id'] = 'Mace Head'
                        
                # Insert into database
                        #pivot_df.to_sql(erddap_table, engine, if_exists='append', index=False)
                        #print("Load to ERDDAP table complete!")
                        #print("Number of rows inserted:"+str(pivot_df.shape[0]))
                        
                # Move processed blob to a processed container
                        # destination blob with a unique name.
                        dest_blob = BlobClient.from_connection_string(conn_str=connect_str, container_name="processed-attachments", blob_name = b.name)
                
                        # Start the copy operation.
                        print("Copying file to processed folder")
                        dest_blob.start_copy_from_url(blob.url)
                        
                        # Get the destination blob's properties to check the copy status.
                        properties = dest_blob.get_blob_properties()
                        copy_props = properties.copy
                
                        # Display the copy status.
                        print("Copy status: " + copy_props["status"])
                        print("Copy progress: " + copy_props["progress"])
                        print("Completion time: " + str(copy_props["completion_time"]))
                        print("Total bytes: " + str(properties.size))
                        
                        #Delete blob from source folder
                        print("Deleting file from raw folder")
                        blob.delete_blob()
                        print('')
                        
                        blob_count+=1

                print("Files successfully loaded to database!")
                print("Total files processed:"+str(blob_count))
                print('')
        
        except Exception as e:
                print(e)
        
# Main method.
if __name__ == '__main__':
        now = datetime.datetime.now()
        print("COMPASS database updation started - %s"%(now.strftime("%Y-%m-%d %H:%M:%S")))
        run()