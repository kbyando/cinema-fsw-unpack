# cinema_unpack.py - generic Python code to perform first unpack of FSW-routed data
#
#    Version Information:
#        (production)
#        [we should consider producing stand-alone and python-packages]
#        v0.8.1 10/08/2012 transparent handling of GZIP archives; inclusion of progressbar 
#        v0.8.0 10/02/2012 "cinema_unpack" initial production code; v0.8.x series interfaces
#
#        (beta)
#        v0.7.9 intermediate beta series
#        v0.7.8 09/25/2012 corrected hex value for several APIDs
#               08/13/2012 provisions for CCSDS-tagged data packets;
#               renamed "read_fsw_hexbytes" to "read_gse_hexbytes";
#               introduce byte-arrays for data reads 
#        v0.7.7 08/10/2012 definition of read_raw_hexbytes()   
#        v0.7.6 08/02/2012 externalized save_data_as to stein_unpack.py and magic_unpack.py
#        v0.7.5 07/11/2012 externalized STEIN specific function to stein_unpack.py
#        v0.7.4 07/09/2012 parsing of IIB housekeeping data 
#        v0.7.3 06/22/2012 correction to interpretation of EVCODE3/TYPE2 event
#        v0.7.2 06/22/2012 calc_nominal_eventtime() moved to cinema_eventtime.py
#        v0.7.1 adapted as "cinema_unpack.py" (from C++ "fsw_steinunpackA.cpp")
#        v0.7.0 syntax changes in ASCII output
#        v0.5.0 generation of nominal timestamps (as fsw_timestampX.cpp)
#        
#        preceded by: fsw_unpack.cpp (raw unpack of STEIN event fields)
#

import array
import csv
import hashlib
import datetime
import gzip
import os
import progressbar
import stein_unpack_v0_8_0 as stein
import magic_unpack_v0_8_0 as magic
import hsk_unpack_v0_8_0 as hsk
    
# define bit patterns
asm = 0x1acffc1d    # CCSDS "Attached Synchronization Marker" (ASM)
apid140 = 0x0940    # APID 140 (STEIN Sweep Table)
apid150 = 0x0950    # APID 150 (ACS Ephemeris Table)
apid160 = 0x1960    # APID 160 (Immediate Command Table)
apid161 = 0x1961    # APID 161 (Delayed Command Table)
apid170 = 0x0970    # APID 170 (Upload MAG Calibration Matrix)
apid261 = 0x1a61    # APID 261 (Memory Load)
apid262 = 0x0a62    # APID 262 (Memory Dump Packet)
apid264 = 0x0a64    # APID 264 (Recorded HSK)
apid265 = 0x0a65    # APID 265 (Overflow Packet)
apid364 = 0x0b64    # APID 364 (Recent HSK)
apid240 = 0x0a40    # APID 240 (STEIN Packet)
apid241 = 0x0a41    # APID 241 (MAGIC Packet)

# parse frame of hex packet_bytes
def parse_frame(packet_bytes, ccsds_size=None):
    """Parse a packet of CINEMA data, returning a dictionary structure.

    Arguments:
    packet_bytes -- iterable of bytes comprising a CINEMA data packet

    Keyword arguments:
    ccsds_size -- the size of the CCSDS header, in bytes (default None) 
    """
    # NOTE: this is called for ALL frames, and operates on the presence
    #   or lack of a CCSDS header.
    #   If CCSDS_SIZE == 0, assume GSE hexbytes (SCIENCE only)
    #   IF CCSDS_SIZE == 6, assume FSW hexbytes (EVERYTHING)
    
    packetheader_size = 1               # size (BYTES) of packet header (if applicable)
    packet_size = len(packet_bytes)     # size (BYTES) of one packet of data
    # NOTE: the packet usually occupies 518-bytes.  If the CCSDS 
    #   header has been stripped by GSEOS, it will be 512-bytes.
    #   Overflow packets are 62-bytes long (not handled)
    
    # if 'ccsds_size' not specified, determine from 'packet_size'
    if (ccsds_size == None and packet_size == 518):
        ccsds_size = 6  # CCSDS header appears present
    elif (ccsds_size == None and packet_size == 512):
        ccsds_size = 0  # CCSDS header appears absent
    elif (ccsds_size == None):
        # unable to match from packet size; halt execution
        raise ValueError("'packet_bytes' not of expected size (512 or 518 \
                element list); 'ccsds_size' could not be automatically determined.")
    elif (packet_size != 512 and packet_size != 518):
        raise RuntimeWarning("'packet_bytes' not of expected size (512 or \
                518 element list).  Proceeding.")

    # PARSE HEX BYTES
    cursor = 0          # byte-position cursor (for packet bytes)

    # CCSDS 
    packet_ccsds = packet_bytes[cursor:cursor+ccsds_size]
    cursor += ccsds_size
    
    # PACKET HEADER
    packet_header = packet_bytes[cursor:cursor+packetheader_size]
    cursor += packetheader_size
   
    if len(packet_ccsds) > 0:
        # assume FSW hexbytes: examine APID (and where applicable, packet_header)
        apid = (packet_ccsds[0] << 8) + packet_ccsds[1]
    
        if ((apid == apid240) and (packet_header[0] == 0xAF)):
            # bytes belong to a STEIN packet
            this_frame = stein.parse_stein_frame(packet_bytes, includes_ccsds=(ccsds_size==6))

        elif ((apid == apid241) and (packet_header[0] == 0xBE)):
            # bytes belong to a MAGIC packet
            this_frame = magic.parse_magic_frame(packet_bytes, includes_ccsds=(ccsds_size==6))

        elif ((apid == apid364) or (apid == apid264)):
            # bytes belong to a HSK packet
            this_frame = hsk.parse_hsk_frame(packet_bytes, includes_ccsds=(ccsds_size==6))

        elif (apid == apid265):
            # bytes belong to an OVERFLOW packet
            # ** NOT HANDLED **
            this_frame = None

        else:
            # unhandled APID
            this_frame = None
    else:
        # assume GSE hexbytes: examine the packet_header:
        if len(packet_header) != 0:
            if (packet_header[0] == 0xAF):
                # bytes belong to a STEIN packet
                this_frame = stein.parse_stein_frame(packet_bytes, includes_ccsds=(ccsds_size==6))
            elif (packet_header[0] == 0xBE):
                # bytes belong to a MAGIC packet
                this_frame = magic.parse_magic_frame(packet_bytes, includes_ccsds=(ccsds_size==6))
            else:
                # complain loudly
                # ultimately, we could raise an exception here (shouldn't do that yet; decode errors)
                print("\nInvalid packet header: {}".format(hex(packet_header[0])))
                this_frame=None

    return this_frame


def save_data_as(data_packet_dict, type="ASCII", filename=None, overwrite=False):
    # to be fleshed out, with export options for ASCII, CDF, python-pickle, etc.
    if 'magic_data' in data_packet_dict[0].keys():
        magic.save_data_as(data_packet_dict, type=type, filename=filename, overwrite=overwrite)
        return 0
    elif 'stein_data' in data_packet_dict[0].keys():
        stein.save_data_as(data_packet_dict, type=type, filename=filename, overwrite=overwrite)
        return 0
    elif 'hsk_data' in data_packet_dict[0].keys():
        hsk.save_data_as(data_packet_dict, type=type, filename=filename, overwrite=overwrite)
        return 0
    else:
        return 0


# procedure to read in and parse ASCII text dumps of CINEMA
#  flight software output bytes
def read_gse_hexbytes(filename=None, dialect="whitespace"):
    
    # do we have a valid filename?
    try:
        f = open(filename, 'rU')
    except IOError as errno:
        print("read_fsw_hexbytes: Invalid filename or path")
        print("I/O error({0}):".format(errno))
    else:
        # use the CSV reader to simplify the process
        reader = csv.reader(f,dialect=dialect)
       
        packets = []
        # examine and parse each record
        for row in reader:
            # FSW-generated logs consist of hexbytes printed with 
            #   ASCII characters (e.g. 0xAFL)
            hexbyte_list = array.array('B')
            if len(row) > 0:    # actual data; begin parsing
                del row[-1]         # delete a spurious final entry
                for hexbyte in row:
                    # convert ASCII hexbytes to real hexbytes
                    hexbyte_list.append(int(hexbyte.rstrip('L'),16))
                # parse the frame in its entirety
                packets.append(parse_frame(hexbyte_list)) 
            # else: an empty row; do nothing
    finally:
        f.close()
    return packets



def read_raw_hexbytes(filename=None):
    # Define CINEMA/CCSDS telemetry frame constants
    # master frame size [including SMEX header]
    tm_frame_size = 1289        # (bytes)
   
    # transparent GZIP handling
    suffix = 'gz'
    filetype = filename.split('.')[-1]
    if (filetype==suffix):
        fileopen = gzip.open
    else:
        fileopen = open

    # read-in of binary data
    raw_frames = []     # list of telemetry master frames
    bytedump = array.array('B')
    with fileopen(filename,'rb') as f:
        chunk = f.read(tm_frame_size)
        while chunk:
            bytedump = bytedump*0       # clear contents
            bytedump.fromstring(chunk)  # convert read() result (a string) to numerical bytes
            raw_frames.append(bytedump) # store
            chunk = f.read(tm_frame_size)
        # Acknowledge EOF by terminating read
        #print("Acknowledged EOF (extra bytes: {0})".format(len(chunk)))

    # define bit patterns
    #asm = 0x1acffc1d    # CCSDS "Attached Synchronization Marker" (ASM)
    #apid140 = 0x0940    # APID 140 (STEIN Sweep Table)
    #apid150 = 0x0950    # APID 150 (ACS Ephemeris Table)
    #apid160 = 0x1960    # APID 160 (Immediate Command Table)
    #apid161 = 0x1961    # APID 161 (Delayed Command Table)
    #apid170 = 0x0970    # APID 170 (Upload MAG Calibration Matrix)
    #apid261 = 0x1a61    # APID 261 (Memory Load)
    #apid262 = 0x0a62    # APID 262 (Memory Dump Packet)
    #apid264 = 0x0a64    # APID 264 (Recorded HSK)
    #apid265 = 0x0a65    # APID 265 (Overflow Packet)
    #apid364 = 0x0b64    # APID 364 (Recent HSK)
    #apid240 = 0x0a40    # APID 240 (STEIN Packet)
    #apid241 = 0x0a41    # APID 241 (MAGIC Packet)
    apids = [apid140, apid150, apid160, apid161, apid170, \
            apid261, apid262, apid264, apid265, \
            apid364, apid240, apid241]
    science = [apid240, apid241]
    recentHSK = apid364
    recordHSK = apid264
    overflow = apid265
    supported = [apid240, apid241, apid264, apid364]

    # BGS only passes complete frames, so pass data is always 
    #   frame-aligned.  Begin extraction/parsing

    # As each master frame is unpacked:
    #   - discard ASM and RSCODE
    #   - identify contents of transfer frame's "DATA" field by APID
    #   - append packet as appropriate, with support data:
    #           ([transfer frame header], packet)
    #   - the transfer frame header consists of a 13-byte sequence:
    #    (Frame ID, MC Cnt, VC Cnt, Frame Status, Sec Hdr ID, Xmit Time)
    
    # Instantiate storage lists
    recentHSK_packet = []
    recordHSK_packet = []
    overflow_packet = []
    science_packets = []
    other_packets = []

    # Generate access index
    spacing = [0, 10, 4, 13, 518, 518, 62, 4, 160]
    key = [sum(spacing[0:i+1]) for i in range(len(spacing))]

    miss_count = 0
    apid_miss = 0
    miss_flag = False
    
    # work through our list of master telemetry frames, whittling it 
    #   down as appropriate (this should help with memory)
    frame_id = 0
    n_raw_frames = len(raw_frames)
    #widgets = ['Frames Processed: ', progressbar.Percentage(), progressbar.Bar()]
    widgets = [progressbar.FormatLabel('Processing: %(value)d of %(max)d')]
    pbar = progressbar.ProgressBar(widgets=widgets, maxval=n_raw_frames).start()
    while len(raw_frames) > 0:
        
        #print "processing {0} of {1}".format( (n_raw_frames - len(raw_frames)), n_raw_frames)
        frame = raw_frames.pop(0)
        el = 0          # index to "element" of key
        
        # move through the master and transfer frames
        # get the 10-bytes of SMEX header data
        smex_header = (frame[key[el]:key[el+1]])
        el += 1
        # get the 4-byte ASM sequence
        asm_code = frame[key[el]:key[el+1]]
        el += 1
        # get the 13-byte transfer frame header
        tf_header = frame[key[el]:key[el+1]]
        el += 1
        # get the first 518-byte data packet
        packet_1 = frame[key[el]:key[el+1]]
        el += 1
        # get the second 518-byte data packet
        packet_2 = frame[key[el]:key[el+1]]
        el += 1
        # get the 62-byte overflow packet
        packet_3 = frame[key[el]:key[el+1]]
        el += 1
        # get the 4-byte OCF sequence
        ocf_code = frame[key[el]:key[el+1]]
        el += 1
        # get the 160-byte RS CODE sequence
        rs_code = frame[key[el]:key[el+1]]
        
        # examine APIDs of data/overflow packets
        # for packet_1
        packet1_apid = (packet_1[0] << 8) + packet_1[1]
        if (packet1_apid in supported):
            packet = parse_frame(packet_1, ccsds_size=6)
            if (packet != None):
                packet['tframe_header'] = tuple(tf_header)
                packet['source_file'] = filename
                packet['source_file_hash'] = hashlib.sha1(open(filename,'rb').read()).hexdigest()
                packet['extraction_date'] = datetime.datetime.now() 
            if (packet1_apid in science):
                science_packets.append(packet)
            elif (packet1_apid == recordHSK):
                recordHSK_packet.append(packet)
            elif (packet1_apid == recentHSK):
                recentHSK_packet.append(packet)
            else:
                print("Unexpected APID [packet 1a]")
                apid_miss += 1
                miss_flag = True
                other_packets.append((tf_header,packet_1))
            packet = None
        else:
            print("Unexpected APID [packet 1b]")
            apid_miss += 1
            miss_flag = True
            other_packets.append((tf_header,packet_1))
        
        # for packet_2
        packet2_apid = (packet_2[0] << 8) + packet_2[1]
        if (packet2_apid in supported):
            packet = parse_frame(packet_2, ccsds_size=6)
            if (packet != None):
                packet['tframe_header'] = tuple(tf_header)
                packet['source_file'] = filename
                packet['source_file_hash'] = hashlib.sha1(open(filename,'rb').read()).hexdigest()
                packet['extraction_date'] = datetime.datetime.now() 
            if (packet2_apid in science):
                science_packets.append(packet)
            elif (packet2_apid == recordHSK):
                recordHSK_packet.append(packet)
            elif (packet2_apid == recentHSK):
                recentHSK_packet.append(packet)
            else:
                print("Unexpected APID [packet 2a]")
                apid_miss += 1
                miss_flag = True
                other_packets.append((tf_header,packet_2))
            packet = None
        else:
            print("Unexpected APID [packet 2b]")
            apid_miss += 1
            miss_flag = True
            other_packets.append((tf_header,packet_2))
        
        #packet2_apid = (packet_2[0] << 8) + packet_2[1]
        #if (packet2_apid in science):
        #    packet = parse_frame(packet_2, ccsds_size=6)
        #    if (packet != None):
        #        packet['tframe_header'] = tuple(tf_header)
        #    science_packets.append(packet)
        #    packet = None
        #elif (packet2_apid == recordHSK):
        #    recordHSK_packet.append((tf_header,packet_2))
        #elif (packet2_apid == recentHSK):
        #    recentHSK_packet.append((tf_header,packet_2))
        #else:
        #    print("Unexpected APID [packet 2]") 
        #    apid_miss += 1
        #    miss_flag = True
        #    other_packets.append((tf_header,packet_2))
        
        # for packet_3
        packet3_apid = (packet_3[0] << 8) + packet_3[1]
        if (packet3_apid == overflow):
            overflow_packet.append((tf_header,packet_3))
        else:
            print("Unexpected APID [packet 3]")
            apid_miss += 1
            miss_flag = True
            other_packets.append((tf_header,packet_3))
       
        # examine ASM for legitimacy
        asm_code = (asm_code[0] << 24) + (asm_code[1] << 16) + (asm_code[2] << 8) + asm_code[3]
        if (asm_code != asm):
            print("Invalid ASM")
            miss_count += 1
            miss_flag = True
        else:
            pass
       
        # if errors, make report
        if (miss_flag):
            print(frame_id, hex(asm_code), hex(packet1_apid), hex(packet2_apid), hex(packet3_apid))
            miss_flag = False
   
        frame_id += 1
        pbar.update(n_raw_frames - len(raw_frames))
        #print "processing {0} of {1}".format( (n_raw_frames - len(raw_frames)), n_raw_frames)
    print("Misses/APID Misses: ", miss_count, apid_miss, n_raw_frames)
    pbar.finish()
    print("*****************************")
    
    return (recentHSK_packet, recordHSK_packet, overflow_packet, science_packets, other_packets)

csv.register_dialect("whitespace", delimiter=' ', skipinitialspace=True) 
