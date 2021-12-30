# Excel-to-CSV
Requirements:
  * Read an excel file and split it into multiple csv files, one for each sheet in the workbook.
  * Add a timestamp to the csv file name
  * The excel file to be read is in the Azure Blob Storage
  * The csv file to be created will be in the same Azure storage but in individual folders named after the tabs in the excel workbook
  * Archive the excel file after processing into an archive folder in Azure


Authentication:
 * BlobServiceClient used to connect to the Azure Storage is Authenticated using the Connection String
 *  
