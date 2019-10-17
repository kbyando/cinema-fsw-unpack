# magic_unpack.py - Python code to perform first unpack of FSW-routed MAGIC data#    - produces an event list from an integer number of MAGIC packets
#    - data prodcuts (per sample)
#       STATUS (mode, sensor, mt) 
#       (raw) (Bx, By, Bz, TEMP)
#         *or alternatively* 
#       (raw) (TEMP1, TEMP2, TEMP1, TEMP2)
#    - and additionally (per packet):
#       (raw) PACKET_TIMESTAMP
#
#    Version Information:
#       (production)
#       v0.8.0 10/02/2012 "magic_unpack" initial production code; v0.8.x series interfaces
#
#       (beta)
#       v0.2.0 10/01/2012 much updated as v0.1.9; updated ASCII write options
#       v0.1.9 09/14/2012 "ASCII-raw" option
#       v0.1.8 08/13/2012 provisions for CCSDS-tagged data packets
#       v0.1.4 08/02/2012 implementation of median-absolute 
#       v0.1.3 08/02/2012 save_mag_data_as() renamed to save_data_as(); 
#               provisions for terminal ASCII data
#       v0.1.2 08/01/2012 define ASCII text output for MAGIC
#       v0.1.1 07/11/2012 specialized parse_frame as parse_magic_frame
#              and externalized parse_frame and read_fsw_hexbytes
#       v0.1.0 07/11/2012 initial code
#
import os, datetime


# function to extract MAG samples from the 507-byte MAGIC data block
def extract_samples(magic_frame):
    # 507-byte block, comprising 39 vector samples of 13 bytes each
    increment = 13              # (bytes)
    n_samples = 39              # (samples)

    # per-frame sample log
    sample_log = []

    for i in range(n_samples):
        # populate "working_bytes"
        working_bytes = magic_frame[i*increment:i*increment+increment]
        sample_log.append(working_bytes)
    return sample_log


def parse_magic_report(magic_sample):
    # parse "status" byte {Unused Unused Unused MODE2 MODE1 MODE0 SENSOR MT}
    status = magic_sample[0]
  
    # CINEMA 1 FSW MODE assignments (*not the same as MAGIC ICD*)
    # MODE - Instrument Mode
    #   000 Attitude Config     (mode "0") [no data returned]
    #   001 Attitude            (mode "1") [1x 13-byte report]
    #   010 Science A'          (mode "2") [1x 13-byte report] 
    #   011 Gradiometer         (mode "3") [2x 13-byte report]
    #   100 Unused
    #   101 Unused
    #   110 Unused
    #   111 Unused
    mode = (status >> 2) & 7    # shift off 2 LSB, mask 3 MSB
    
    # SENSOR 
    #   0 Outboard Sensor
    #   1 Inboard Sensor
    sensor = (status >> 1) & 1  # shift off 1 LSB, mask 6 MSB 
    
    # MT - Magnetic Field or Temperature
    #   0 Magnetic Field Vector
    #   1 Temperature Measurements (MODE & SENSOR are unused in this case)
    mt = (status & 1)            # mask 7 MSB
    
    # pack status elements as a tuple
    status_tuple = (mode, sensor, mt)

    # repack (Bx,By,Bz) as a tuple      
    # 3 Bytes yields a LONG
    bx = (magic_sample[1] << 16) + (magic_sample[2] << 8) + magic_sample[3]
    by = (magic_sample[4] << 16) + (magic_sample[5] << 8) + magic_sample[6]
    bz = (magic_sample[7] << 16) + (magic_sample[8] << 8) + magic_sample[9]

    # repack TEMP
    # 3 Bytes yields a LONG
    temp = (magic_sample[10] << 16) + (magic_sample[11] << 8) + magic_sample[12]
    
    return (status_tuple, (bx, by, bz, temp))


# parse MAGIC frame of hex packet_bytes
def parse_magic_frame(packet_bytes, includes_ccsds):
    """Parse a packet of MAGIC data, returning a dictionary structure.

    Arguments:
    packet_bytes -- iterable of bytes comprising a MAGIC data packet
    includes_ccsds -- Boolean argument, indicating presence of CCSDS header
    """

    # MAGIC DATA PACKET PARAMETERS
    # packet_size = 518  # size (BYTES) of one packet of MAGIC data
    # NOTE: 6-byte CCSDS header + 512-bytes science
    if includes_ccsds:
        ccsds_size = 6          # size (BYTES) of CCSDS header
    else: 
        ccsds_size = 0          # if stripped by GSEOS
    #
    packetheader_size = 1   # size (BYTES) of packet header (e.g. "0xBE" for MAGIC)
    timestamp_size = 4      # size (BYTES) of packet timestamp
    magicframe_size = 507   # size (BYTES) of MAGIC packet data subframe
        #sparebyte_size = 2      # size (BYTES) of packet unused bytes
        # NOTE: the observed packet size is actually 514 bytes; 
        #   the final 2 bytes are spurious:  still true???
    sample_cnt = 39


    # PARSE HEX BYTES
    cursor = 0          # byte-position cursor (for packet bytes)

    # CCSDS
    packet_ccsds = packet_bytes[cursor:cursor+ccsds_size]
    cursor += ccsds_size

    # PACKET HEADER
    packet_header = packet_bytes[cursor:cursor+packetheader_size]
    cursor += packetheader_size
    # PACKET TIMESTAMP
    packet_timestamp = packet_bytes[cursor:cursor+timestamp_size]
    cursor += timestamp_size

    # -----------------
    # MAGIC DATA
    #   NOTE: properly, this should be protected with if/else-statements
           
    # get MAGIC bytes
    magic_frame = packet_bytes[cursor:cursor+magicframe_size]
    cursor += magicframe_size
                          
    # generate a samples list from these bytes
    sample_log = extract_samples(magic_frame)     # extract samples
                                     
    # parse each sample into (MODE,SENSOR,M),(Bx,By,Bz),TEMP
    magic_data = []
    for i in range(sample_cnt):
        magic_data.append(parse_magic_report(sample_log[i]))
    # -----------------
                                                  
    # SPARE BYTES (UNIMPLEMENTED)


    # return a dictionary structure
    this_frame = {'apid':0x241,
        'type':"MAGIC",
        'tframe_header':None,                   # to be filled in, if available
        'packet_ccsds':tuple(packet_ccsds),
        'packet_header':tuple(packet_header),   # should always be 0xBE for MAGIC
        'packet_timestamp':tuple(packet_timestamp),
        'packet_timestamp_format':('HH','mm','ss','ff'),
        'refined_timetamp':None,                 # to be filled in, with a datetime.datetime
        'magic_data':magic_data,                # a tuple 
        'magic_data_format':(('MODE','SENSOR','M'), ('Bx','By','Bz','TEMP')),
        'alt_magic_data_format':(('MODE','SENSOR','M'), ('TEMP_A','TEMP_B','TEMP_A','TEMP_B')),
        'clock_time':None,                     # this is filled in later
        'clock_time_format':"YYYY MM DD HH mm ss ffffff",
        'clock_time_quality':None,             # this is filled in later
        'source_file':None,                     # to contain filename/path
        'source_file_hash':None,                # to contain a hash
        'source_file_hash_format':"SHA1",       # hashing algorithm is SHA1
        'extraction_date':None                  # to contain the date
        }
    return this_frame

def save_data_as(data_packet_dict, type="ASCII", filename=None, overwrite=False):
    # to be fleshed out, with export options for ASCII, CDF, python-pickle, etc.
    source_hashes = [packet['source_file_hash'] for packet in data_packet_dict]
    hash_set = set(source_hashes)
    source_info = [] 
    for hash in hash_set:
        pos = source_hashes.index(hash)
        source_filename = data_packet_dict[pos]['source_file']
        if (source_filename == None):
            source_info.append("%   Not available\r\n")
        else:
            #source_info.append("%   Not available")
            source_info.append("%   " + source_filename + "\r\n%     SHA1 hash:" + hash + "\r\n")
    header_lines = (
            "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\r\n",
            "% CINEMA[1] MAGIC Event List (example)\r\n",
            "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\r\n",
            "% Support Info:\r\n",
            "% SOURCE:\r\n",
            "".join(source_info),
            "% SOURCE DESCRIPTION:\r\n",
            "% -- VARIABLEs --\r\n",
            "% CINEMA 1 FSW MODE assignments (*not the same as MAGIC ICD*)\r\n",
            "% MODE - Instrument Mode\r\n",
    	    "%   000 Attitude Config     (mode '0') [no data returned]\r\n",
    	    "%   001 Attitude            (mode '1') [1x 13-byte report]\r\n",
    	    "%   010 Science A'          (mode '2') [1x 13-byte report]\r\n",
    	    "%   011 Gradiometer         (mode '3') [2x 13-byte report]\r\n",
    	    "% SENSOR\r\n",
    	    "%   0 Outboard Sensor\r\n",
    	    "%   1 Inboard Sensor\r\n",
    	    "% MT - Magnetic Field or Temperature\r\n",
    	    "%   0 Magnetic Field Vector\r\n",
    	    "%   1 Temperature Measurements (MODE & SENSOR are unused in this case)\r\n",
    	    "% Bx, By, Bz, TEMP\r\n",
    	    "% \r\n% (engineering values)\r\n",
    	    "% PACKET TIMESTAMP QUANTITIES \r\n",
    	    "% HH - RTC Hours\r\n",
    	    "% mm - RTC Minutes\r\n",
    	    "% ss - RTC Seconds\r\n",
    	    "% ff - RTC Frac Seconds (centiseconds)\r\n",
    	    "% PACKET_CNT - CCSDS count for MAGIC data APID\r\n",
            "% Generated {0}\r\n".format(datetime.datetime.now().replace(microsecond=0).isoformat()),
            "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\r\n"
            )
    if (filename != None) and (type=="ASCII"):
        # check to see whether a file already exists under the specified filename
        if os.path.isfile(filename) and (overwrite==False):
            print("save_data_as: file already exists.  Specify 'OVERWRITE' keyword to continue.")
            return 1
        # else, attempt to open file for writing
        with open(filename,'w') as f:
            # write out MAGIC sample list
            f.write("".join(header_lines))
            f.write("% {timestamp} MODE SENSOR M Bx By Bz TEMP HH mm ss ff PACKET_CNT\r\n".format(
                timestamp="YYYY-MM-DDTHH:MM:SS.mmmmmm"))
            for i,packet in enumerate(data_packet_dict):
                for j,sample in enumerate(packet['magic_data']):
                    if (packet['clock_time'] is None):
                        print(i,j, "Invalid Timestamp")
                    else:
                        f.write("{timestamp}{mode:2d}{sensor:3d}{m:3d}{bx:9d}{by:9d}{bz:9d}{temp:9d}{hr:3d}{min:3d}{sec:3d}{fsec:3d}{packet_cnt:6d}\r\n".format(
                            timestamp=packet['clock_time'][j].isoformat(),
                            mode=sample[0][0],
                            sensor=sample[0][1],
                            m=sample[0][2],
                            bx=sample[1][0],
                            by=sample[1][1],
                            bz=sample[1][2],
                            temp=sample[1][3],
                            hr=packet['packet_timestamp'][0],
                            min=packet['packet_timestamp'][1],
                            sec=packet['packet_timestamp'][2],
                            fsec=packet['packet_timestamp'][3],
                            packet_cnt=((packet['packet_ccsds'][2] & 0b111111) << 8) + \
                                    packet['packet_ccsds'][3]
                            ))
        return 0
    if (filename != None) and (type=="ASCII-RAW"):
        # check to see whether a file already exists under the specified filename
        if os.path.isfile(filename) and (overwrite==False):
            print("save_data_as: file already exists.  Specify 'OVERWRITE' keyword to continue.")
            return 1
        # else, attempt to open file for writing
        with open(filename,'w') as f:
            # write out MAGIC event list
            f.write("".join(header_lines))
            f.write("% {timestamp} MODE SENSOR M Bx By Bz TEMP HH mm ss ff PACKET_CNT\r\n".format(
                timestamp="YYYY-MM-DDTHH:MM:SS.mmmmmm"))
            for i,packet in enumerate(data_packet_dict):
                for j,sample in enumerate(packet['magic_data']):
                    if (packet['clock_time'] is None):
                        print(i,j, "Invalid Timestamp")
                    else:
                        f.write("{timestamp}{mode:2d}{sensor:3d}{m:3d}{bx:9d}{by:9d}{bz:9d}{temp:9d}\r\n".format(
                            timestamp="YYYY-MM-DDTHH:MM:SS.mmmmmm",
                            mode=sample[0][0],
                            sensor=sample[0][1],
                            m=sample[0][2],
                            bx=sample[1][0],
                            by=sample[1][1],
                            bz=sample[1][2],
                            temp=sample[1][3]))
        return 0
    if (filename != None) and (type=="CDF"):
        pass
    if (filename != None) and (type=="pickle"):
        pass
    #else:    
    # print ASCII
    print("".join(header_lines))
    print("# frame / EVCODE / ADD / DET_ID / timestamp / DATA / [MM-DDTHH:mm:ss.fracsec]")
    sample_number = 0
    for i in range(len(data_packet_dict)):
        for j in range(len(data_packet_dict[i]['stein_data'])):
            if (data_packet_dict[i]['event_time'] is None):
                pass
            #print i,j,event_number, "Invalid Timestamp"
            else:
                print i,j,event_number, (data_packet_dict[i]['event_time'][j]).isoformat(), \
                        data_packet_dict[i]['stein_data'][j]
                print("{packet_i} {sample_j} {sample_n} {timestamp}{mode:2d}{sensor:3d}{m:3d}{bx:9d}{by:9d}{bz:9d}{temp:9d}\r\n".format(
                    packet_i=i, sample_j=j, sample_n=sample_number,
                    timestamp=packet['clock_time'][j].isoformat(),
                    mode=sample[0][0],
                    sensor=sample[0][1],
                    m=sample[0][2],
                    bx=sample[1][0],
                    by=sample[1][1],
                    bz=sample[1][2],
                    temp=sample[1][3]))
            sample_number += 1 
    return 0
