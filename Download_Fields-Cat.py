import pandas as pd
import numpy as np
import splusdata
import os
import time
import threading

from os.path import expanduser

home = expanduser("~") + '/CHANCES/'


# Establish a connection with SPLUS Cloud using your username and password
conn = splusdata.connect('user', 'password!')


# Set the input folder nametbl_ipt = home + 'TABLES/'
tbl_ipt = home + 'TABLES/'
fit_ipt = home + 'FITS/'

# Set the output folder name
res_dir = './'
res_tbl_dir = './'
res_plt_dir = './'

Output_Dir   = res_dir + 'Fields/'
Output_Dir2  = Output_Dir + 'Final/'
Output_Dir3  = Output_Dir + 'Partial/'

DIR_RES     = [Output_Dir,Output_Dir2,Output_Dir3]

# Create output folder if it doesn't exist
for directory_list in DIR_RES:
    if os.path.isdir(directory_list) == False:
        os.makedirs(directory_list)
else:
    pass


ipt_cat_dwn = tbl_ipt + 'SPLUS-download.csv'

os.system('clear')

print ()
print ('Reading table: ' + ipt_cat_dwn)
print ()

cat_dwn = pd.read_csv(ipt_cat_dwn)

id_cat    = np.asarray(cat_dwn['Cluster_Name'])
ra_cat    = np.asarray(cat_dwn['RA_J2000'])
dec_cat   = np.asarray(cat_dwn['Dec_J2000'])
d200_cat = np.asarray(cat_dwn['5d200'])

CLS_CVR = []
CLS_NO_CVR = []

for idx,(cls_id,cls_ra,cls_dec,cls_5d200) in enumerate(zip(id_cat,ra_cat,dec_cat,d200_cat)):
    print ()
    print ('Searching S-PLUS datasets for object:')
    print (cls_id,cls_ra)
    print ()

    cluster = cls_id#'A0119'
    RA_Center = cls_ra#14.067
    DE_Center = cls_dec#-1.255
    Radius    = cls_5d200#2.60 # degrees
        
    # Query for a list of fields to be downloaded
    Query_Fields = f"""SELECT det.field
                      FROM  idr4_dual.idr4_detection_image as det 
                      WHERE 1 = CONTAINS(POINT('ICRS', det.ra, det.dec), CIRCLE('ICRS', {RA_Center}, {DE_Center}, {Radius}))
                      GROUP BY det.field"""

    # Make the query
    Result_Fields = conn.query(Query_Fields, publicdata=False) # publicdata=False to get internal data

    # Save the resulting table to the output folder as a csv
    Result_Fields.write(f'FieldsToDownload.csv', overwrite=True) # To save the resulting table

    # Loading the FieldsToDownload file as a DF
    All_Fields = pd.read_csv('FieldsToDownload.csv')

    # These lines are used to verify if there are any downloaded files already.
    Fields_Downloaded = [s.replace('.csv', '') for s in os.listdir(Output_Dir) if s.endswith('.csv')]
    Field_List        = np.setdiff1d(All_Fields, Fields_Downloaded) # Fields that will be downloaded
    Field_List        = pd.DataFrame(Field_List, columns=['field']) # Transforming it into a DataFrame so we don't need to change the code

    print('# Total number of fields: ', len(All_Fields))
    print('# Number of fields remaining:', len(Field_List))

    # Configuring data to be downloaded
    def thread_function(dataframe):
       for key, value in dataframe.iterrows():
           print('Starting '+f'{value.field}')
           try:
               My_Query = f"""SELECT det.id, det.RA, det.DEC, det.SEX_FLAGS_DET, det.KRON_RADIUS, det.ISOarea, det.MU_MAX_INST, det.A, det.B, det.FWHM_n, u.u_aper_6, j0378.j0378_aper_6, j0395.j0395_aper_6, j0410.j0410_aper_6, j0430.j0430_aper_6, g.g_petro, g.g_aper_6, j0515.j0515_aper_6, r.r_petro, r.r_aper_6, j0660.j0660_aper_6, i.i_aper_6, j0861.j0861_aper_6, z.z_aper_6, u.e_u_aper_6, j0378.e_j0378_aper_6, j0395.e_j0395_aper_6, j0410.e_j0410_aper_6, j0430.e_j0430_aper_6, g.e_g_petro, g.e_g_aper_6, j0515.e_j0515_aper_6, r.e_r_petro, r.e_r_aper_6, j0660.e_j0660_aper_6, i.e_i_aper_6, j0861.e_j0861_aper_6, z.e_z_aper_6, pz.zml, pz.odds, pz.zml_84q, pz.zml_16q, sgq.PROB_GAL
                              FROM idr4_dual.idr4_detection_image as det 
                              JOIN idr4_dual.idr4_dual_u     as u     ON (u.ID     = det.ID) 
                              JOIN idr4_dual.idr4_dual_j0378 as j0378 ON (j0378.ID = det.ID) 
                              JOIN idr4_dual.idr4_dual_j0395 as j0395 ON (j0395.ID = det.ID) 
                              JOIN idr4_dual.idr4_dual_j0410 as j0410 ON (j0410.ID = det.ID) 
                              JOIN idr4_dual.idr4_dual_j0430 as j0430 ON (j0430.ID = det.ID) 
                              JOIN idr4_dual.idr4_dual_g     as g     ON (g.ID     = det.ID) 
                              JOIN idr4_dual.idr4_dual_j0515 as j0515 ON (j0515.ID = det.ID) 
                              JOIN idr4_dual.idr4_dual_r     as r     ON (r.ID     = det.ID) 
                              JOIN idr4_dual.idr4_dual_j0660 as j0660 ON (j0660.ID = det.ID) 
                              JOIN idr4_dual.idr4_dual_i     as i     ON (i.ID     = det.ID) 
                              JOIN idr4_dual.idr4_dual_j0861 as j0861 ON (j0861.ID = det.ID) 
                              JOIN idr4_dual.idr4_dual_z     as z     ON (z.ID     = det.ID) 
                              JOIN idr4_vacs.idr4_photoz     as pz ON (pz.ID = det.ID) 
                              JOIN idr4_vacs.idr4_star_galaxy_quasar as sgq ON (sgq.ID = det.ID)
                              WHERE det.field = '{value.field}'
                              AND ((pz.zml_84q - pz.zml_16q) <0.02)"""

               # Make the query
               Result = conn.query(My_Query, publicdata=False) # publicdata=False to get internal data

               # Save the resulting table to the output folder as a csv
               Result.write(f'{Output_Dir}{value.field}.csv') # To save the resulting table

           # If a given field could not be downloaded for some reason, this will print the name of the field
           except:
               print(f"Error on {value.field}")

    # To speed-up the download of data we use multithreading.
    Num_Parallel = 3 # The number of files downloaded in parallel. Do not change this
    Threads = np.arange(0, len(Field_List)+1, 1) # The number of threads (each thread will be used to download a field)

    print('# Number of simultaneous downloads:', Num_Parallel)

    # Create a dictionary to store the processes (threads)
    Processes = {}

    # Populating the dictionary with the processes (one thread per item of the dictionary, each thread downloads a field)
    for i in range(len(Threads)-1):
       Processes[i] = threading.Thread(target=thread_function, args=(Field_List[Threads[i]: Threads[i+1]],))

    # Starting the threads (downloading 5 at a time to not overload the cloud)
    for list_of_fields in np.array_split(np.arange(0, len(Threads)-1), np.ceil(len(Threads)/Num_Parallel)):
       print('# Starting threads:', list_of_fields)
       for i in list_of_fields:
           Processes[i].start()
           time.sleep(1.5)

       for i in list_of_fields:
           Processes[i].join()
       print('# Finished threads:', list_of_fields)
       print()

    # Concatenate fields?
    try:
        print('# Concatenating fields...')
        Files = [s for s in os.listdir(Output_Dir) if s.endswith('.csv')]

        DFs = []
        for file in Files:
            DFs.append(pd.read_csv(Output_Dir+file))
            
        Concat_DF = pd.concat(DFs)
        Concat_DF = Concat_DF.reset_index(drop=True)
        Concat_DF = Concat_DF.sort_values(by='r_aper_6') # Sorting the table by magnitudes, from lowest fo highest (used in the duplicate removal step)
                                                         # If you download the magnitudes in another aperture, you should change this and the aperture in the stilts command below
        Concat_DF.to_csv(Output_Dir2+cluster+'_ConcatenatedFields.csv', index=False) # Save as a final catalogue

        print('# Done concatenating fields')

        [os.system('mv ' + Output_Dir + file + ' ' + Output_Dir3) for file in Files]
        print ()
        print ('Moving field tables to: ' + Output_Dir3)
        [print ('mv ' + Output_Dir + file + ' ' + Output_Dir3) for file in Files]
        print ()

        # Remove duplicates
        print('# Using STILTS to remove duplicates...')

        os.system(f"""java -jar stilts.jar tmatch1 in={Output_Dir2+cluster+'_ConcatenatedFields.csv'} icmd='sort r_aper_6' matcher=sky+1d values='RA DEC r_aper_6' params='1 1' action=keep1 ocmd='sort RA' out={Output_Dir2+cluster+'_NoDups.csv'}""")

        print('# Done')
        np.savetxt(Output_Dir3 + cluster + '_downloaded_fields.dat'  , Files  ,  delimiter=" ", fmt="%s", newline='\n')
        CLS_CVR.append(cluster)
    except ValueError:
        pass
        CLS_NO_CVR.append(cluster)

np.savetxt(Output_Dir2 + 'Clusters_COVERED.dat'  , CLS_CVR  ,  delimiter=" ", fmt="%s", newline='\n')
np.savetxt(Output_Dir2 + 'Clusters_NOT_COVERED.dat'  , CLS_NO_CVR  ,  delimiter=" ", fmt="%s", newline='\n')

print ()
print ('Objects with S-PLUS coverage: ' + Output_Dir2 + 'Clusters_COVERED.dat')
print ('Objects without S-PLUS coverage: ' + Output_Dir2 + 'Clusters_NOT_COVERED.dat')
print ()
