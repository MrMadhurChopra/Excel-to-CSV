#!/usr/bin/env python  
import argparse 
import pandas as pd  
#from azure.identity import ClientSecretCredential  
from datetime import datetime
from azure.storage.blob import BlobServiceClient, generate_account_sas, ResourceTypes, AccountSasPermissions 


parser = argparse.ArgumentParser(description= 'Process Excel files into csv files')  
parser.add_argument('-con', '-- container', type= str, metavar= '', required= True, help= 'Name of target container', dest= 'container')  
parser.add_argument('-constr', '-- connection_string', type= str, metavar= '', required= True, help= 'Connection String for the storage account', dest= 'connection_string')  
#parser.add_argument('-act', '-- account_name)', type= str, metavar= '', required= True, help= 'Name of storage account', dest= 'account_name')  
parser.add_argument('-bloburl', '-- base_blob_url', type= str, metavar= '', required= True, help= 'Storage container base url without container', dest= 'baseblob_url')  
#parser.add_argument('-key', '-- secret', type= str, metavar= '', required= True, help= 'Secret from service priniciple', dest= 'key')  
parser.add_argument('-srcdir', '-- src_dir', type= str, metavar= '', required= True, help= 'Source directory inside the container from where the files need to be read eg: base_dir/', dest= 'source_directory')  
parser.add_argument('-tgtdir', '-- tgt_dir', type= str, metavar= '', required= True, help= 'Target directory inside the container where the new files need to be stored eg: base_dir/', dest= 'target_directory')  
parser.add_argument('-arcdir', '-- arc_dir', type= str, metavar= '', required= True, help= 'Archive directory inside the container where the Source Files need to be stroed after processing eg: base_dir/', dest= 'archive_directory')  
args = parser.parse_args()  


#key = args.key 
#acct_name = args.account_name 
container_name = args.container 
conn_str = args.connection_string 
base_blob_url = args.base_blob_url

# Create the BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(conn_str) 
container_client = blob_service_client.get_container_client(container_name) 

 
def excel_to_csv (src_dir, tgt_dir, archive_dir, timestamp):  
    files_processed = 0 
    try: 
        blob_list = container_client.list_blobs() 
        for blob in blob_list: 
            file_name = blob.name[len(src_dir):] 
            is_base_file = file_name.find("/") 
            if is_base_file == -1: 
                print('Found a file to process file_name:' + file_name) 
                split_excel_to_csv(src_dir, tgt_dir, file_name, timestamp) 
                archive_file(src_dir, file_name, archive_dir, timestamp, 1) 
                files_processed = files_processed + 1 
        print(str(files_processed) + ' files processed') 
    except Exception as e: 
        print('In excel_to_csv. Exception occoured. ' + str(e)) 


def archive_file(src_dir, src_file_name, archive_dir, timestamp, _is_move):  
    full_file_name = src_dir + src_file_name 
    try: 
        blob_url = base_blob_url + container_name + "/"+ src_dir + src_file_name 
        blob_client = container_client.get_blob_client(full_file_name) 
        tgt_blob_client = container_client.get_blob_client(archive_dir + timestamp + "_" + src_file_name) # Create a blank file in Azure Storage. This will create a folder/directory if one dosent exist. 
        tgt_blob_client.start_copy_from_url(blob_url) 
        if _is_move: # if the file needs to be moved, delete the original file after copy 
            blob_client.delete_blob()  
    except Exception as e: 
        print("In archive_file. " + str(e)) 


def split_excel_to_csv (src_dir, tgt_dir, src_file_name, timestamp):  
    full_file_name = src_dir + src_file_name 
    blob_client = container_client.get_blob_client(full_file_name) 
    try: 
        download_stream = blob_client.download_blob() 
        x1 = pd.ExcelFile(download_stream.readall()) 
        for sheet in x1.sheet_names: 
            df = pd.read_excel(x1,sheet_name=sheet) 
            output = df.to_csv(path_or_buf=None,sep='\t',header=True,index=False) # Create csv file and store in a variable 
            try: 
                new_file_location = tgt_dir + sheet.lower() + "/" + sheet.lower() + "_" + timestamp + ".txt" # define the new file name in azure 
                new_blob_client = container_client.get_blob_client(new_file_location) # Create a blank file in Azure Storage. This will create a folder/directory if one dosent exist. 
                new_blob_client.upload_blob(output, blob_type = "BlockBlob") # Upload file to azure storage 
            except Exception as e: 
                print("Updating file name " + new_file_location + " as similar name already exists") 
                new_file_location = tgt_dir + sheet.lower() + "/2" + sheet.lower() + "_" + timestamp + ".txt" # define the new file name in azure 
                new_blob_client = container_client.get_blob_client(new_file_location) # Create a blank file in Azure Storage. This will create a folder/directory if one dosent exist. 
                new_blob_client.upload_blob(output, blob_type = "BlockBlob") # Upload file to azure storage 
    except Exception as e: 
        print("In split_excel_to_csv. " + str(e)) 
 

def main():  
    src_dir = args.source_directory 
    tgt_dir = args.target_directory 
    archive_dir = args.archive_directory 
    timestamp = str(datetime.now(tz=None)) 
    excel_to_csv(src_dir,tgt_dir, archive_dir, timestamp) 


if __name__ == "__main__": 
    main() 
 