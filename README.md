# SPLUS-query
This script is inteded to download S-PLUS photometry datasets from a list of objects.
This is based on Erik Vinicius S-PLUS query script, later tweaked by Lawrence B. to filter out the concatenated tables by any duplicated objects.
This last step uses STILTS which can be downloaded [here]([https://ui.adsabs.harvard.edu/abs/2015A%26A...576A..79L/abstract](https://www.star.bris.ac.uk/~mbt/stilts/)), or if Full Starjava was already installed in your computer, simply point out to the stilts.jar complete path
or make a copy of it in the same directory where this script is running.

SPLUS-download.csv is an example table that contains importat cluster info: id, ra, dec, redshift, 5d200 that were obtained from Cristobal Sifon tables.
The tables can be downloaded [here](https://drive.google.com/drive/u/0/folders/1DHXo9Boi2rQAs-QfDMlE24eeQTzjPlCM), and their corresponding README file is [here](https://docs.google.com/document/d/1bWPq8471kwvNqi7_LJCtqLERiCUDAtDWSajBnJVQ1js/edit). 

### The script will create
- concatenate tables
- cleaned tables to avoid duplicated objects
- list of tables with S-PLUS coverage
- list of tables with NO S-PLUS coverage
- list of SPLUS tiles used to generate the concatenated tables. 

### Final tables directory (```Fields/Final```):
- Clusters_COVERED.dat
- Clusters_NOT_COVERED.dat
- Cluster_ConcatenatedFields.csv
- Cluster_NoDups.csv (if any)

### Partial tables directory (```Fields/Partial```) contain the individual S-PLUS field tables used to create the corresponding concatenated tables. This list of tables for each cluster will be saved in their corrresponding file:
- Cluster_downloaded_fields.dat


## IMPORTANT
**Note that if an object is found in the S-PLUS cloud, this does not necessarily means that the dataset COMPLETELY covers the cone region defined by the input aperture adopted for the query.**
