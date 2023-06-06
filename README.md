# SPLUS-query
This script is inteded to download S-PLUS photometry datasets from a list of objects.
This is based on Erik Vinicius S-PLUS query script, later tweaked by Lawrence B. to filter out the concatenated tables by any duplicated objects.
This last step used STILTS that can be downloaded here, or located in your computer if a complete was installed. You just have to look for the
.jar file, add copy it to the same path of this script. 

The following table is an example and contains the cluster id, ra, dec, redshift, 5d200 that were obtained from Cristobal Sifon tables.
The tables that can be downloaded here, and their corresponding README file is here. 


The script will create
concatenate tables
cleaned tables to avoid duplicated objects
list of tables with S-PLUS coverage
list of tables with NO S-PLUS coverage
list of SPLUS tiles used to generate the concatenated tables. 

IMPORTANT
Note that if an object is found in the S-PLUS cloud, does not necessarily means that these datasert does not include coverage for the complete aperture adopted for the query. 
