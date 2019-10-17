# stein_unpack.py - Python code to perform first unpack of FSW-routed STEIN data
#    - produces an event list from an integer number of STEIN packets
#    - data products (per event):
#        EVCODE
#        ADD (if applicable)
#        DET_ID (if applicable)
#        (raw) TIMESTAMP (if applicable)
#        (raw) DATAVALUE (if applicable)
#        nominal timestamp
#    - and additionally (per packet):
#        (raw) PACKET_TIMESTAMP
#
#    Version Information:
#        (production)
#        v0.8.0 10/02/2012 "stein_unpack" initial production code; v0.8.x series interfaces
#
#        (beta)
#        v0.7.8 08/13/2012 provisions for CCSDS-tagged data packets
#        v0.7.6 08/02/2012 define STEIN specific save_data_as() sub-procedure
#        v0.7.5 07/11/2012 specialized parse_frame as parse_stein_frame 
#               and externalized parse_frame and read_fsw_hexbytes
#        v0.7.4 07/09/2012 parsing of IIB housekeeping data 
#        v0.7.3 06/22/2012 correction to interpretation of EVCODE3/TYPE2 event
#        v0.7.2 06/22/2012 calc_nominal_eventtime() moved to cinema_eventtime.py
#        v0.7.1 adapted as "cinema_unpack.py" (from C++ "fsw_steinunpackA.cpp")
#        v0.7.0 syntax changes in ASCII output
#        v0.5.0 generation of nominal timestamps (as fsw_timestampX.cpp)
#        
#        preceded by: fsw_unpack.cpp (raw unpack of STEIN event fields)
#

import os, datetime


# function to extract events from the 495-byte STEIN data block
def extract_events(stein_frame):
    # 495-byte block, comprising 198 events of 20 bits each
    # So.. every 5 bytes gives us 2 complete STEIN events
    increment = 5               # (bytes)
    n_events = 198              # (events)
   
    # (use NumPy arrays here)
    # 5-byte chunk to work with 
    #working_bytes = np.zeros(increment, dtype=uint32)  

    # per-frame event log 
    #event_log = np.zeros(n_events, dtype=uint32)
    event_log = []

    for i in range(n_events/2):
        # populate "working_bytes"
        working_bytes = stein_frame[i*increment:i*increment + increment]
        #for j in range(increment):
        #    working_bytes[j] = stein_frame[j + i*increment]
        #
        # split, bitshift, and re-construct event values
        event1 = ((working_bytes[2] & 15) << 16) + (working_bytes[1] << 8) + (working_bytes[0])
        event2 = (working_bytes[4] << 12) + (working_bytes[3] << 4) + (working_bytes[2] >> 4)
        event_log.append(event1)
        event_log.append(event2)
        #event_log[2*i] = event1         # place in event_log
        #event_log[2*i + 1] = event2     #      """
    return event_log


def parse_event_report(stein_event):
    evcode = stein_event >> 18;
    if (evcode == 0):            # (i.e., is a data packet)
        # no ADD bit (0 bits; all bits dropped)
        add=-1
        # DET_ID (5 bits)
        det_id = (stein_event >> (20 - (2+5))) & 31
        # TIMESTAMP (6 bits; 2 LSB dropped)
        time_stamp = (stein_event >> (20 - (2+5+6))) & 63
        # EVENT_DATA (7 bits; 9 bits dropped via log-binning)
        event_data = (stein_event >> (20 - (2+5+6+7))) & 127
    elif (evcode == 1):          # (i.e., Sweep checksum/# of triggers per second)
        # no ADD bit (0 bits; all bits dropped)
        add = -1
        # no DET_ID bits (0 bits; all bits dropped)
        det_id = -1
        # TIMESTAMP (6 bits; 2 MSB dropped)
        time_stamp = (stein_event >> (20 - (2+6))) & 63
        # DATA (12 bits; 4 lower bits dropped)
        event_data = (stein_event >> (20 - (2+6+12))) & 4095
    elif (evcode == 2):         # (i.e., Sweep checksum/# of events per second)
        # no ADD bit (0 bits; all bits dropped)
        add = -1
        # no DET_ID bits (0 bits; all bits dropped)
        det_id = -1
        # TIMESTAMP (6 bits; 2 MSB dropped)
        time_stamp = (stein_event >> (20 - (2+6))) & 63
        # DATA (12 bits; 4 lower bits dropped)
        event_data = (stein_event >> (20 - (2+6+12))) & 4095
    elif (evcode == 3): 
        # ADD bit (1 bit; no bits dropped)
        add =(stein_event >> (20 - (2+1))) & 1
        if (add == 0):          # EVCODE3 TYPE 1 (noise event)
            # DET_ID bits (1 bit; 4 MSB dropped)
            det_id = (stein_event >> (20 - (2+1+1))) & 1
            # TIMESTAMP (0 bits; all bits dropped)
            time_stamp = -1
            # DATA (16 bits; no bits dropped)
            event_data = stein_event & 65535L
        elif (add == 1):        # EVCODE3 TYPE 2 (status event)
            # DET_ID bits (0 bits; all bits dropped)
            det_id = -1
            # TIMESTAMP (9 bits; no bits dropped)
            #   (really "STATUS_ID")
            time_stamp = (stein_event >> (20 - (2+1+9))) & 255
            # DATA (8 bits; 8 MSB dropped)
            event_data = stein_event & 255
        else:
            raise Exception("Error! (INVALID ADD!)")
            add = -1
            det_id = -1
            time_stamp = -1
            event_data = -1
    else:
        raise Exception("Error! (INVALID EVCODE!)")
        add = -1
        det_id = -1
        time_stamp = -1
        event_data = -1
    # return result as a tuple
    return (evcode, add, det_id, time_stamp, event_data)




# parse STEIN frame of hex packet_bytes
def parse_stein_frame(packet_bytes, includes_ccsds):
    """Parse a packet of STEIN data, returning a dictionary structure.

    Arguments:
    packet_bytes -- iterable of bytes comprising a STEIN data packet
    includes_ccsds -- Boolean argument, indicating presence of CCSDS header
    """

    # STEIN DATA PACKET PARAMETERS
    # packet_size = 518         # size (BYTES) of one packet of STEIN data
    # NOTE: 6-byte CCSDS header + 512-bytes science
    # 
    if includes_ccsds:
        ccsds_size = 6          # size (BYTES) of CCSDS header
    else: 
        ccsds_size = 0          # if stripped by GSEOS
    #
    packetheader_size = 1       # size (BYTES) of packet header 
                                #   (e.g. "0xAF" for STEIN)
    timestamp_size = 6          # size (BYTES) of packet timestamp
    steinframe_size = 495       # size (BYTES) of STEIN packet data subframe
        # NOTE: this is 495-bytes, and not the original 510-bytes
    steinblock_size = steinframe_size/3  
        # size (BYTES) of IIB-FPGA block transfer to FSW (STEIN data)
        # NOTE: this is 165-bytes, and not the original 170-bytes
    housekeep_size = 8          # size (BYTES) of packet housekeeping subframe
        #sparebyte_size = 2          # size (BYTES) of packet unused bytes
        # NOTE: the observed packet size is actually 514 bytes; 
        #   the final 2 bytes are spurious
    event_cnt = (steinframe_size*8)/20
        # size (# STEIN EVENTS) in one packet of FSW data [expect 198]
        # NOTE: this is 198 events, and not the original 204
    block_cnt = (steinblock_size*8)/20
        # size (# STEIN EVENTS) in FPGA-IIB transfer block


    # PARSE HEX BYTES
    cursor = 0          # byte-position cursor (for packet bytes)

    # CCSDS
    packet_ccsds = packet_bytes[cursor:cursor+ccsds_size]
    
    # PACKET HEADER
    packet_header = packet_bytes[cursor:cursor+packetheader_size]
    cursor += packetheader_size
    # PACKET TIMESTAMP
    packet_timestamp = packet_bytes[cursor:cursor+timestamp_size]
    cursor += timestamp_size
    
    # -----------------
    # STEIN DATA
    #   NOTE: properly, this should be protected with if/else-statements
    
    # get STEIN bytes
    stein_frame = packet_bytes[cursor:cursor+steinframe_size]
    cursor += steinframe_size
    
    # generate an events list from these bytes
    event_log = extract_events(stein_frame)     # extract events
    
    # parse each event into EVCODE, ADD, DETID, TIMESTAMP & EVENTDATA
    stein_data = []
    for i in range(event_cnt):
        stein_data.append(parse_event_report(event_log[i]))
    # -----------------
    
    # HOUSEKEEPING
    packet_housekeeping = packet_bytes[cursor:cursor+housekeep_size]
    cursor += housekeep_size
    #
    # SPARE BYTES (UNIMPLEMENTED)
    
    # return a dictionary structure
    this_frame = {'apid':0x240,
        'type':"STEIN",
        'tframe_header':None,                   # to be filled in, if available
        'packet_ccsds':tuple(packet_ccsds), 
        'packet_header':tuple(packet_header),   # should always be 0xAF for STEIN 
        'packet_timestamp':tuple(packet_timestamp), 
        'packet_timestamp_format':('MM','DD','HH','mm','ss','ff'),
        'refined_timestamp':None,                # to be filled in with a datetime.datetime
        'stein_data':stein_data,                # a tuple 
        'stein_data_format':('EVCODE','ADD','DET_ID','TIMESTAMP','EVENT_DATA'),
        'packet_hkpg':tuple(packet_housekeeping),
        'packet_hkpg_format':('IIB SPI Buffer Overflow Count',
                'IIB SPI Buffer Underflow Count', 
                'IIB SPI Buffer Checksum Error Count', 
                'IIB I2C Buffer Checksum Error Count', 
                'IIB I2C Buffer Underflow Count', 
                'IIB I2C Buffer Overflow Count', 
                'IIB CDI Parity Error Count',
                'IIB CDI Framing Error Count'),
        'clock_time':None,                      # this is filled in later
        'clock_time_format':"YYYY MM DD HH mm ss ffffff",
        'clock_time_quality':None,              # this is filled in later
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
            source_info.append("%   " + source_filename + "\r\n%     SHA1 hash:" + hash + "\r\n")
    header_lines = (
        "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\r\n",
        "% CINEMA[1] STEIN Event List (example)\r\n",
        "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\r\n",
        "% Support Info:\r\n",
        "% SOURCE:\r\n",
        "".join(source_info),
        "% SOURCE DESCRIPTION:\r\n",
        "% -- VARIABLEs --\r\n",
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
            # write out STEIN event list
            f.write("".join(header_lines))
            f.write("% {timestamp} EVCODE ADD DET_ID EVENT_DATA\n".format(
                timestamp="YYYY-MM-DDTHH:MM:SS.mmmmmm"))
            for i,packet in enumerate(data_packet_dict):
                for j,event in enumerate(packet['stein_data']):
                    if (packet['event_time'] is None):
                        print(i,j, "Invalid Timestamp")
                    else:
                        f.write("{timestamp}{evcode:2d}{add:3d}{det_id:3d}{data:4d}\n".format(
                            timestamp=packet['event_time'][j].isoformat(),
                            evcode=event[0],
                            add=event[1],
                            det_id=event[2],
                            data=event[4]))
        return 0
    if (filename != None) and (type=="CDF"):
        pass
    if (filename != None) and (type=="pickle"):
        pass
    #else:    
    # print ASCII 
    print("".join(header_lines))
    print("# frame / EVCODE / ADD / DET_ID / timestamp / DATA / [MM-DDTHH:mm:ss.fracsec]")
    event_number = 0
    for i in range(len(data_packet_dict)):
        for j in range(len(data_packet_dict[i]['stein_data'])):
            if (data_packet_dict[i]['event_time'] is None):
                pass
            #print i,j,event_number, "Invalid Timestamp"
            else:
                print i,j,event_number, (data_packet_dict[i]['event_time'][j]).isoformat(), \
                        data_packet_dict[i]['stein_data'][j]
            event_number += 1 
    return 0

