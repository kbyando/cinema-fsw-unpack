#!/usr/bin/env python
# file: quickgen_script.py

import os, sys
import cinema_unpack_v0_8_1 as unpack
import hsk_unpack_v0_8_0 as hsk
import magic_clocktime_v0_8_0 as mclock

# define source and output directories
source_directory = "cinema_telemetry_data/"
output_directory = "cinema_unpacked_data/" 


# define spacecraft
sc = "CIN1"     # CINEMA1 (UC Berkeley)


contents_of_src_path = os.listdir(source_directory)
src_candidates = [candidate for candidate in contents_of_src_path if ("BGS.CINEMA.TLM_VC0" in candidate)]
print src_candidates


contents_of_out_path = os.listdir(output_directory)
out_candidates = [candidate for candidate in contents_of_out_path if (sc in candidate)]

for filepath in src_candidates:
    telemetry_id = filepath.split('.')
    contact_id = telemetry_id[3]
    

    out_path = sc+'_'+contact_id
    if (out_path in out_candidates):
        # directory already exists (presume populated)
        print filepath + " (skipping)"
    else:
        packet_tuple = unpack.read_raw_hexbytes(source_directory + os.sep + filepath)
        contents = sum(map(len,packet_tuple[0:3]))
        if (contents==0):
            # renames file, or otherwise note it
            print filepath + " (empty)"
            pass
        else:
            # create a new directory 
            print filepath + " (processing)"
            expanded_out_path = os.path.normpath(output_directory + os.sep + out_path)
            if (os.access(expanded_out_path, os.F_OK)):
	    	# already exists somehow
		pass
	    else:
	    	os.mkdir(expanded_out_path)
            
            # populate with desired ASCII files
            if len(packet_tuple[1]) > 0:        # recorded HSK
                data = packet_tuple[1]
                hsk.save_data_as(data, type="SLOW", filename=expanded_out_path + os.sep + out_path + '_slow_v0_0.txt') 
                hsk.save_data_as(data, type="FAST", filename=expanded_out_path + os.sep + out_path + '_fast_v0_0.txt')
		print (expanded_out_path + os.sep + out_path + '_fast_v0_0.txt')

            if False and len(packet_tuple[3]) > 0:        # science packets
                # STEIN
                
                # MAGIC
                magic_packets = [packet for packet in packet_tuple[3] if packet['type']=='MAGIC']
                mo = 1  # get these from the STEIN/MAGIC interleave
                dy = 1
                data,qa = mclock.calc_fitted_sampletime(magic_packets, year=2012, month=mo, day=dy)
                unpack.save_data_as(data, filename=expanded_out_path + os.sep + out_path + '_mag_v0_1.txt')
