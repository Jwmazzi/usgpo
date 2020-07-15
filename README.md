## U.S. Government Publishing Office to ArcGIS Online

This repository contains a simple extractor that can be used to fetch bills from the U.S. Government Publishing Office (USGPO)
and push them into a Hosted Feature Layer within ArcGIS Online. Though we have not tested this tool against ArcGIS 
Enterprise, there is a very good chance it will work in that environment too. In addition to a Python script that can 
be run as a scheduled task, you will find a File Geodatabase (under /schema)  that can be added to your organization to 
get started. Instructions are posted below if you would like to start tracking/updating bill data in your own 
Esri organization. Please reach out to the team of you have questions or ideas on how we can make this better. 

## Getting Started - USGPO

* Publish the Sponsors_Schema.zip file found in the /schema directory to ArcGIS Online as a File Geodatabase. 
Insert the Item ID of the newly created Hosted Feature Layer into the membs_id attribute within the 
configuration file.

* Collect a free [API Key](https://api.data.gov/signup) to access the USGPO endpoints and put this value in the
api_key attribute of the configuration file.

* Add your login information for ArcGIS Online to the configuration file and execute the runner.py file.

