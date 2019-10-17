# hsk_unpack.py - Python code to perform first unpack of CINEMA HSK data
#    - data products 
#        EVCODE
#        nominal timestamp
#    - and additionally (per packet):
#        (raw) PACKET_TIMESTAMP
#
#    Version Information:
#        (production)
#
#        (beta)
#

import os, datetime
import cinema_timeops_v0_1_0 as timeops

def extract_slowHSK(slowHSK_frame):
    # byte lengths, for values as follow:
    # FLIGHTMODE(1), FSW_VERSION(1), DEVENABLE(2), PERIPHENABLE(2), MISC(16), SSR_STATE(10), 
    #   DEPLOY_CONTROL(2), POWER_CONTROL(4), ACS(21>24?), MAG_HOUSEKEEPING(16>13?), STEIN_HOUSEKEEPING(4), [SPARE(??)] 
    field_lengths = [1,1,2,2,16,10,2,4,24,13,4]
    
    i = 0
    start = 0
    
    stop = start + field_lengths[i]
    # get FLIGHTMODE bytes
    flightmode = tuple(slowHSK_frame[start:stop])
    
    start = stop
    i = i+1
    stop = start + field_lengths[i]
    # get FSW_VERSION bytes
    fsw_version = tuple(slowHSK_frame[start:stop])
    
    start = stop
    i = i+1
    stop = start + field_lengths[i]
    # get DEVENABLE bytes
    devenable = tuple(slowHSK_frame[start:stop])
    
    start = stop
    i = i+1
    stop = start + field_lengths[i]
    # get PERIPHENABLE bytes
    periphenable = tuple(slowHSK_frame[start:stop])
    
    start = stop
    i = i+1
    stop = start + field_lengths[i]
    # get MISC bytes
    misc = tuple(slowHSK_frame[start:stop])
    
    start = stop
    i = i+1
    stop = start + field_lengths[i]
    # get SSR_STATE bytes
    ssr_state = tuple(slowHSK_frame[start:stop])
    
    start = stop
    i = i+1
    stop = start + field_lengths[i]
    # get DEPLOY_CONTROL bytes
    deploy_control = tuple(slowHSK_frame[start:stop])
    
    start = stop
    i = i+1
    stop = start + field_lengths[i]
    # get POWER_CONTROL bytes
    power_control = tuple(slowHSK_frame[start:stop])
    
    start = stop
    i = i+1
    stop = start + field_lengths[i]
    # get ACS bytes
    acs = tuple(slowHSK_frame[start:stop])
    
    start = stop
    i = i+1
    stop = start + field_lengths[i]
    # get MAG_HOUSEKEEPING bytes
    mag_housekeeping = tuple(slowHSK_frame[start:stop])
    
    start = stop
    i = i+1
    stop = start + field_lengths[i]
    # get STEIN_HOUSEKEEPING bytes
    stein_housekeeping = tuple(slowHSK_frame[start:stop])
    
    hsk_log = {'FLIGHTMODE':flightmode, 'FSW_VERSION':fsw_version, 'DEVENABLE':devenable, 
            'PERIPHENABLE':periphenable, 'MISC':misc, 'SSR_STATE':ssr_state,
            'DEPLOY_CONTROL':deploy_control, 'POWER_CONTROL':power_control, 'ACS':acs, 
            'MAG_HOUSEKEEPING':mag_housekeeping, 'STEIN_HOUSEKEEPING':stein_housekeeping} 
    
    return hsk_log

def extract_fastHSK(fastHSK_frame):
    # 420-byte block, comprising 336 values of 10 bits each
    # So.. every 5 bytes gives us 4 complete HSK values
    increment = 5              # (bytes)
    n_values = 336             # (total number of fastHSK/spare values)

    hsk_log = []
    for i in range(n_values/4):
        # populate "working_bytes"
        working_bytes = fastHSK_frame[i*increment:i*increment+increment]
        value1 = (working_bytes[0] << 2) + (working_bytes[1] >> 6)
        value2 = ((working_bytes[1] & 63) << 4) + (working_bytes[2] >> 4)
        value3 = ((working_bytes[2] & 15) << 6) + (working_bytes[3] >> 2)
        value4 = ((working_bytes[3] & 3) << 8) + (working_bytes[4])

        hsk_log.append(value1)
        hsk_log.append(value2)
        hsk_log.append(value3)
        hsk_log.append(value4)
    return hsk_log

def parse_slowHSK_report(slow_hsk_log):
    # XXX-byte block
    keys = slow_hsk_log.keys()
    
    flightmode = {'FLIGHTMODE':slow_hsk_log['FLIGHTMODE'][0]}
    
    # parse FSW_VERSION
    fsw_high = (slow_hsk_log['FSW_VERSION'][0]) >> 4
    fsw_low = slow_hsk_log['FSW_VERSION'][0] & 15
    # store in dictionary
    fsw_version = {'FSW_HIGH':fsw_high, 'FSW_LOW':fsw_low}
    
    # parse DEVENABLE
    # first byte
    ena_flash = (slow_hsk_log['DEVENABLE'][0] & 255) >> 7
    ena_sband = (slow_hsk_log['DEVENABLE'][0] & 127) >> 6
    ena_torq = (slow_hsk_log['DEVENABLE'][0] & 63) >> 5
    ena_act = (slow_hsk_log['DEVENABLE'][0] & 31) >> 4
    ena_mag = (slow_hsk_log['DEVENABLE'][0] & 15) >> 3
    ena_stein = (slow_hsk_log['DEVENABLE'][0] & 7) >> 2
    ena_att = (slow_hsk_log['DEVENABLE'][0] & 3) >> 1
    ena_hv = (slow_hsk_log['DEVENABLE'][0] & 1) >> 0
    # second byte
    ena_scan = (slow_hsk_log['DEVENABLE'][1] & 255) >> 7
    ena_rtc = (slow_hsk_log['DEVENABLE'][1] & 127) >> 6
    ena_iib = (slow_hsk_log['DEVENABLE'][1] & 63) >> 5
    ena_uhf = (slow_hsk_log['DEVENABLE'][1] & 31) >> 4
    # store in dictionary
    devenable = {
            'ENA_FLASH':ena_flash, 'ENA_SBAND':ena_sband, 
            'ENA_TORQ':ena_torq, 'ENA_ACT':ena_act, 
            'ENA_MAG':ena_mag, 'ENA_STEIN':ena_stein, 
            'ENA_ATT':ena_att, 'ENA_HV':ena_hv, 
            'ENA_SCAN':ena_scan, 'ENA_RTC':ena_rtc, 
            'ENA_IIB':ena_iib, 'ENA_UHF':ena_uhf}

    # parse PERIPHENABLE
    # first byte
    timer2 = (slow_hsk_log['PERIPHENABLE'][0] & 255) >> 7
    timer3 = (slow_hsk_log['PERIPHENABLE'][0] & 127) >> 6
    timer4 = (slow_hsk_log['PERIPHENABLE'][0] & 63) >> 5
    i2c1 = (slow_hsk_log['PERIPHENABLE'][0] & 31) >> 4
    i2c2 = (slow_hsk_log['PERIPHENABLE'][0] & 15) >> 3
    uart2 = (slow_hsk_log['PERIPHENABLE'][0] & 7) >> 2
    adc = (slow_hsk_log['PERIPHENABLE'][0] & 3) >> 1
    uart1 = (slow_hsk_log['PERIPHENABLE'][0] & 1) >> 0
    # second byte
    spi1 = (slow_hsk_log['PERIPHENABLE'][1] & 255) >> 7
    spi2 = (slow_hsk_log['PERIPHENABLE'][1] & 127) >> 6
    ic1 = (slow_hsk_log['PERIPHENABLE'][1] & 63) >> 5
    ic5 = (slow_hsk_log['PERIPHENABLE'][1] & 31) >> 4
    oc4 = (slow_hsk_log['PERIPHENABLE'][1] & 15) >> 3
    # store in dictionary
    periphenable = {
            'TIMER2':timer2, 'TIMER3':timer3, 'TIMER4':timer4, 'I2C1':i2c1,
            'I2C2':i2c2, 'UART2':uart2, 'ADC':adc, 'UART1':uart1,
            'SPI1':spi1, 'SPI2':spi2, 'IC1':ic1, 'IC5':ic5,
            'OC4':oc4}

    # parse MISC
    trigger = (slow_hsk_log['MISC'][0] << 8) + (slow_hsk_log['MISC'][1])
    errctr = slow_hsk_log['MISC'][2]
    errdata = (slow_hsk_log['MISC'][3] << 8) + (slow_hsk_log['MISC'][4])
    errcode = slow_hsk_log['MISC'][5]
    evtctr = slow_hsk_log['MISC'][6]
    evtcode = slow_hsk_log['MISC'][7]
    
    cmdtot = (slow_hsk_log['MISC'][8] << 8) + (slow_hsk_log['MISC'][9])
    immcmdsize = slow_hsk_log['MISC'][10]
    dlycmdsize = (slow_hsk_log['MISC'][11] << 8) + (slow_hsk_log['MISC'][12])
    cinemastate = slow_hsk_log['MISC'][13]
    beaconstate = slow_hsk_log['MISC'][14]
    srampage = slow_hsk_log['MISC'][15]
    # store in dictionary
    misc = {
            'TRIGGER':trigger, 'ERRCTR':errctr, 
            'ERRDATA':errdata, 'ERRCODE':errcode,
            'EVTCTR':evtctr, 'EVTCODE':evtcode, 
            'CMDTOT':cmdtot, 'IMMCMDSIZE':immcmdsize,
            'DLYCMDSIZE':dlycmdsize, 'CINEMASTATE':cinemastate,
            'BEACONSTATE':beaconstate, 'SRAMPAGE':srampage}

    # parse SSR_STATE
    hskpktnum = (slow_hsk_log['SSR_STATE'][0] << 16) + (slow_hsk_log['SSR_STATE'][1] << 8) + (slow_hsk_log['SSR_STATE'][2])
    datapktnum = (slow_hsk_log['SSR_STATE'][3] << 16) + (slow_hsk_log['SSR_STATE'][4] << 8) + (slow_hsk_log['SSR_STATE'][5])
    hskpktptr = (slow_hsk_log['SSR_STATE'][6] << 8) + (slow_hsk_log['SSR_STATE'][7])
    datapktptr = (slow_hsk_log['SSR_STATE'][8] << 8) + (slow_hsk_log['SSR_STATE'][9])
    # store in dictionary
    ssr_state = {
            'HSKPKTNUM':hskpktnum, 'DATAPKTNUM':datapktnum,
            'HSKPKTPTR':hskpktptr, 'DATAPKTPTR':datapktptr}

    # parse DEPLOY CONTROL
    antstat = (slow_hsk_log['DEPLOY_CONTROL'][0])
    boomstat = (slow_hsk_log['DEPLOY_CONTROL'][1])
    # store in dictionary
    deploy_control = {'ANTSTAT':antstat, 'BOOMSTAT':boomstat}

    # parse POWER CONTROL
    attselect = (slow_hsk_log['POWER_CONTROL'][0])
    atttime = (slow_hsk_log['POWER_CONTROL'][1])
    boomtime = (slow_hsk_log['POWER_CONTROL'][2])
    spare_power = (slow_hsk_log['POWER_CONTROL'][3])
    # store in dictionary
    power_control = {
            'ATTSELECT':attselect, 'ATTTIME':atttime,
            'BOOMTIME':boomtime, 'SPARE_POWER':spare_power}

    # parse ACS
    acsmode = (slow_hsk_log['ACS'][0])
    torcoils = (slow_hsk_log['ACS'][1])
    
    wb = slow_hsk_log['ACS'][2:(4*5+2)]       # working_bytes
    elevation = (wb[0] << 24) + (wb[1] << 16) + (wb[2] << 8) + (wb[3])
    spin_rate = (wb[4] << 24) + (wb[5] << 16) + (wb[6] << 8) + (wb[7])
    omega_x = (wb[8] << 24) + (wb[9] << 16) + (wb[10] << 8) + (wb[11])          # error?
    omega_y = (wb[12] << 24) + (wb[13] << 16) + (wb[14] << 8) + (wb[15])        # error?
    omega_z = (wb[16] << 24) + (wb[17] << 16) + (wb[18] << 8) + (wb[19])        # error?
    
    ephemeris_integrity_1 = (slow_hsk_log['ACS'][22])
    ephemeris_integrity_2 = (slow_hsk_log['ACS'][23])
    # store in dictionary
    acs = {
            'ACSMODE':acsmode, 'TORCOILS':torcoils,
            'ELEVATION':elevation, 'SPIN_RATE':spin_rate,
            'OMEGA_X':omega_x, 'OMEGA_Y':omega_y,
            'OMEGA_Z':omega_z,
            'EPHEMERIS_INTEGRITY_1':ephemeris_integrity_1,
            'EPHEMERIS_INTEGRITY_2':ephemeris_integrity_2}

    # parse MAG HOUSEKEEPING
    magicfalt = (slow_hsk_log['MAG_HOUSEKEEPING'][0])
    magstat = (slow_hsk_log['MAG_HOUSEKEEPING'][1])
    wb = slow_hsk_log['MAG_HOUSEKEEPING'][2:(3*3+2)]    # working_bytes
    bx = (wb[0] << 16) + (wb[1] << 8) + (wb[2])
    by = (wb[3] << 16) + (wb[4] << 8) + (wb[5])
    bz = (wb[6] << 16) + (wb[7] << 8) + (wb[8])
    spare_mag = (slow_hsk_log['MAG_HOUSEKEEPING'][11] << 8) + (slow_hsk_log['MAG_HOUSEKEEPING'][12])
    # store in dictionary
    mag_housekeeping = {
            'MAGICFALT':magicfalt, 'MAGSTAT':magstat,
            'Bx':bx, 'By':by, 'Bz':bz, 'SPARE_MAG':spare_mag}

    # parse STEIN HOUSEKEEPING
    steinflt = (slow_hsk_log['STEIN_HOUSEKEEPING'][0])
    steinhvfault = (slow_hsk_log['STEIN_HOUSEKEEPING'][1])
    sweep_integrity = (slow_hsk_log['STEIN_HOUSEKEEPING'][2])
    spare_stein_hsk = (slow_hsk_log['STEIN_HOUSEKEEPING'][3])
    # store in dictionary
    stein_housekeeping = {
            'STEINFLT':steinflt, 'STEINHVFAULT':steinhvfault,
            'SWEEP_INTEGRITY':sweep_integrity, 'SPARE_STEIN_HSK':spare_stein_hsk}
     
    # parse GENERAL SLOW  HSK SPARE
    # *NOT IMPLEMENTED*

    slowHSK_dict = {}
    slowHSK_dict.update(flightmode)
    slowHSK_dict.update(fsw_version)
    slowHSK_dict.update(devenable)
    slowHSK_dict.update(periphenable)
    slowHSK_dict.update(misc)
    slowHSK_dict.update(ssr_state)
    slowHSK_dict.update(deploy_control)
    slowHSK_dict.update(power_control)
    slowHSK_dict.update(acs)
    slowHSK_dict.update(mag_housekeeping)
    slowHSK_dict.update(stein_housekeeping)
    return slowHSK_dict

def parse_fastHSK_report(fast_hsk_log):
    # return the parsed housekeeping log as a tuple of tuples
    # 336 values of 10 bits each, comprising a 48 element sequence repeated 7 times
    # 48 element sequence = 44 housekeeping values + 4 spare (null) values
    # [arranged as 420-byte block, comprising 336 values...]
    #
    increment = 48
    n_repeats = 7

    fastHSK_labels = (
            "PANEL_X1_CURRENT", "PANEL_X2_CURRENT", "PANEL_Y1_CURRENT", "PANEL_Y2_CURRENT", 
            "PANEL_Z1_CURRENT", "PANEL_Z2_CURRENT", # end 6 EPS/SP currents 
            "PANEL_X_VOLT", "PANEL_X1_TEMP",
            "PANEL_X2_TEMP","PANEL_Y_VOLT","PANEL_Y1_TEMP","PANEL_Y2_TEMP",
            "PANEL_Z_VOLT","PANEL_Z1_TEMP","PANEL_Z2_TEMP","V5_BUS_CURRENT",
            "V3.3_CURR","BATT_BUS_CURR", # end 12 EPS/ADC measurements
            "BATT_CURR_DIR","BATT_VOLT",
            "BATT_CURR","BATT_TEMP","BATT1_CURR_DIR","BATT1_VOLT",
            "BATT1_CURR","BATT1_TEMP","BATT2_CURR_DIR","BATT2_VOLT",
            "BATT2_CURR","BATT2_TEMP","CELL_VOLT","CELL1_VOLT",
            "CELL2_VOLT", # end 15 BATT/ADC measurements
            "VMON_RAW_N","VMON_RAW_P","SENSE",
            "IMON_RAW","IIB_TEMP","VMON_MAG5V","SBAND_TEMP",
            "VMON_STEIN5V","STEIN_TEMP","VMON_STEINHV8V","OLD_SBAND_TEMP", # end 11 misc onboard measurements
            "SPARE1","SPARE2","SPARE3","SPARE4") # end 4 spare bytes
    # 6 elements; EPS currents
    # from the Clydespace EPS documentation
    #              ADC channel (1, 4, 13, 7, 10, 31)
    #              EPS nominal (+Array SA1 Current, -Array SA1 Current, 
    #                                   +Array SA2 Current, -Array SA2 Current,
    #                                           +Array SA3 Current, -Array SA3 Current)
    #              EPS gloss   (PANEL_X1_CURRENT, PANEL_X2_CURRENT, 
    #                                   PANEL_Y1_CURRENT, PANEL_Y2_CURRENT,
    #                                           PANEL_Z1_CURRENT, PANEL_Z2_CURRENT)
    eps_current_multiplier = (-1.94483, -1.955, -0.49881, -0.49928, -0.48533, -0.51985)
    eps_current_addition = (1940.8, 1953.464, 517.4618, 517.6608, 502.9626, 521.7786)
                
    # 32 elements; EPS and ADC voltages, etc
    # first 12: from the Clydespace EPS documentation
    #              ADC channel (3, 2, 
    #                                   5, 6, 
    #                                           14, 8,
    #                                                   9, 11,
    #                                                           30, 26,
    #                                                                   27, 17)
    #              EPS nominal (Array SA1 Voltage, +Array SA1 Temperature,
    #                                   -Array SA1 Temperature (error), Array SA2 Voltage,
    #                                           +Array SA2 Temperature, -Array SA2 Temperature,
    #                                                   Array SA3 Voltage, +Array SA3 Temperature,
    #                                                           -Array SA3 Temperature, 5V Bus Current,
    #                                                                  3.3V Bus Current, Battery Bus Curent) 
    #              EPS gloss   (PANEL_X_VOLT, PANEL_X1_TEMP, 
    #                                   PANEL_X2_TEMP, PANEL_Y_VOLT,
    #                                           PANEL_Y1_TEMP, PANEL_Y2_TEMP, 
    #                                                   PANEL_Z_VOLT, PANEL_Z1_TEMP,
    #                                                           PANEL_Z2_TEMP, V5_BUS_CURRENT,
    #                                                                   V3.3_CURR, BATT_BUS_CURR)
    eps_adc_multiplier = (
            -0.03532, -0.1619, -0.1619, -0.03474,  
            -0.1619, -0.1619, -0.00855, -0.1619,
            -0.1619, -4.39965, -3.20202, -4.94124)
    eps_adc_addition = (
            36.57826, 110.119, 110.119, 36.00065, 
            110.119, 110.119, 8.810868, 110.119,
            110.119, 4136.184, 2998.972, 4784.404)
    # next 35: from the Clydespace BATT documentation
    #              ADC channel (0,2,1,4,5,7,6,9,10,13,
    #                           11,14,2,7,12)
    #              BATT nominal (Battery Current Direction,Battery Voltage,
    #                                   Battery Current, Battery Temperature,
    #                                           Battery1 Current Direction, Battery1 Voltage,
    #                                                   Battery1 Current, Battery1 Temperature,
    #                                                           Battery2 Current Direction, Battery2 Voltage,
    #                           Battery2 Current, Battery2 Temperature,
    #                                   Cell Voltage, Cell1 Voltage,
    #                                           Cell2 Volage)
    #              BATT gloss   ("BATT_CURR_DIR","BATT_VOLT",
    #                                   "BATT_CURR","BATT_TEMP",
    #                                           "BATT1_CURR_DIR","BATT1_VOLT",
    #                                                   "BATT1_CURR","BATT1_TEMP",
    #                                                           "BATT2_CURR_DIR","BATT2_VOLT",
    #                           "BATT2_CURR","BATT2_TEMP",
    #                                   "CELL_VOLT","CELL1_VOLT",
    #                                           "CELL2_VOLT")
    #                                          ,"VMON_RAW_N",
    #                                                   "VMON_RAW_P","SENSE",
    #                                                           "IMON_RAW","IIB_TEMP",
    #                           "VMON_MAG5V","SBAND_TEMP",
    #                                   "VMON_STEIN5V","STEIN_TEMP",
    #                                           "VMON_STEINHV8V","SBAND_TEMP",
    #                                                   "SPARE1","SPARE2",
    #                                                           "SPARE3","SPARE4")
    
    # 32 elements; EPS and ADC voltages, etc
    # first 12: from the Clydespace EPS documentation
    
    
    batt_adc_multiplier = (
            1.0, -0.00939, -3.20, -0.163,
            1.0, -0.00939, -3.20, -0.163, 
            1.0, -0.00939, -3.20, -0.163,
            -0.00483, -0.00483, -0.00483
            )
    batt_adc_addition = (
            0.0, 9.791, 2926.22, 110.7,
            0.0, 9.791, 2926.22, 110.7, 
            0.0, 9.791, 2926.22, 110.7,
            4.852724, 4.852724, 4.852724
            )
            # , 0.0, 
            #0.0, 0.0, 0.0, 0.0)
    misc_multiplier = [1.0]*15
    misc_addition = [0.0]*15
  
    multiplier = []
    multiplier.extend(eps_current_multiplier)
    multiplier.extend(eps_adc_multiplier)
    multiplier.extend(batt_adc_multiplier)
    multiplier.extend(misc_multiplier)

    addition = []
    addition.extend(eps_current_addition)
    addition.extend(eps_adc_addition)
    addition.extend(batt_adc_addition)
    addition.extend(misc_addition)

    fastHSK = []
    for i in range(increment):
        index = range(i, increment*n_repeats+i, increment)
        transfer = tuple([fast_hsk_log[j] for j in index])
        fastHSK.append(transfer)
    
    fastHSK_dict = {}

    return fastHSK



# parse HSK frame of hex packet_bytes
def parse_hsk_frame(packet_bytes, includes_ccsds):
    """Parse a packet of HSK data, returning a dictionary structure.

    Arguments:
    packet_bytes -- iterable of bytes comprising a HSK data packet
    includes_ccsds -- Boolean argument, indicating presence of CCSDS header
    """

    # HSK DATA PACKET PARAMETERS
    # packet_size = 518         # size (BYTES) of one packet of HSK data
    # NOTE: 6-byte CCSDS header + 512-bytes housekeeping
    # 
    if includes_ccsds:
        ccsds_size = 6          # size (BYTES) of CCSDS header
    else: 
        ccsds_size = 0          # if stripped by GSEOS
    #
    packetheader_size = 0       # size (BYTES) of packet header 
                                #   (e.g. "0xAF" for STEIN)
    timestamp_size = 6          # size (BYTES) of packet timestamp
    slowHSK_size = 86         # size (BYTES) of HSK packet slow housekeeping subframe
    fastHSK_size = 420       # size (BYTES) of HSK packet fast housekeeping subframe
        # NOTE: this is 420-bytes
    fastHSKblock_size = fastHSK_size/7  
        # NOTE: this is 60-bytes
        #sparebyte_size = 2          # size (BYTES) of packet unused bytes
        # NOTE: the observed packet size is actually 514 bytes; 
        #   the final 2 bytes are spurious

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
    # HOUSEKEEPING DATA
    #   NOTE: properly, this should be protected with if/else-statements
   
    # get SLOW HSK bytes
    slowHSK_frame = packet_bytes[cursor:cursor+slowHSK_size]
    cursor += slowHSK_size
   
    # extract a SLOW HSK dictionary
    slowHSK_log = extract_slowHSK(slowHSK_frame)

    # parse SLOW HSK log into SLOW HSK
    slowHSK = parse_slowHSK_report(slowHSK_log)

    
    # get FAST HSK bytes
    fastHSK_frame = packet_bytes[cursor:cursor+fastHSK_size]
    cursor += fastHSK_size
   
    # extract a FAST HSK log
    fastHSK_log = extract_fastHSK(fastHSK_frame)
    
    # parse FAST HSK log into FAST HSK
    fastHSK = parse_fastHSK_report(fastHSK_log)
    # -----------------

    #
    # SPARE BYTES (UNIMPLEMENTED)
    
    
    # return a dictionary structure
    this_frame = {'apid':0x364,                  # 0x264 (recorded) or 0x364 (recent)
        'type':"HSK",                           # "HSK", or if known, "recordedHSK" or "recentHSK"
        'tframe_header':None,                   # to be filled in, if available
        'packet_ccsds':tuple(packet_ccsds), 
        'packet_header':tuple(packet_header),   # () for HSK paackets 
        'packet_timestamp':tuple(packet_timestamp), 
        'packet_timestamp_format':('MM','DD','HH','mm','ss','ff'),
        'refined_timestamp':None,               # to be filled in with a datetime.datetime
        'slow_hsk':slowHSK,                     # a dictionary 
        'slow_hsk_format':None,
        'fast_hsk':fastHSK,                     # a dictionary
        'fast_hsk_format':None,
        'clock_time':None,                      # this is filled in later
        'clock_time_format':"YYYY MM DD HH mm ss ffffff",
        'clock_time_quality':None,              # this is filled in later
        'source_file':None,                     # to contain filename/path
        'source_file_hash':None,                # to contain a hash
        'extraction_date':None                  # to contain the date
        }
    return this_frame


def save_data_as(data_packet_dict, type="ASCII", filename=None, overwrite=False, separator=' '):
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
            "% CINEMA[1] HSK Event List (example)\r\n",
            "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\r\n",
            "% Support Info:\r\n",
            "% SOURCE:\r\n",
            "".join(source_info),
            "% SOURCE DESCRIPTION:\r\n",
            "% -- VARIABLEs --\r\n",
            "% Generated {0}\r\n".format(datetime.datetime.now().replace(microsecond=0).isoformat()),
            "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\r\n"
            )
    
    repeat = 7          # number of times we print fast housekeeping
    
    slow_order = ['FLIGHTMODE', 'FSW_HIGH', 'FSW_LOW', 'ENA_FLASH', 'ENA_SBAND', 'ENA_TORQ','ENA_ACT','ENA_MAG','ENA_STEIN','ENA_ATT','ENA_HV','ENA_SCAN','ENA_RTC', 'ENA_IIB','ENA_UHF','TIMER2','TIMER3','TIMER4','I2C1','I2C2','UART2','ADC','UART1','SPI1','SPI2','IC1','IC5','OC4','TRIGGER','ERRCTR','ERRDATA','ERRCODE','EVTCTR','EVTCODE','CMDTOT','IMMCMDSIZE','DLYCMDSIZE','CINEMASTATE','BEACONSTATE','SRAMPAGE','HSKPKTNUM','DATAPKTNUM','HSKPKTPTR','DATAPKTPTR','ANTSTAT','BOOMSTAT','ATTSELECT','ATTTIME','BOOMTIME','SPARE_POWER','ACSMODE','TORCOILS','ELEVATION','SPIN_RATE','OMEGA_X','OMEGA_Y','OMEGA_Z','EPHEMERIS_INTEGRITY_1','EPHEMERIS_INTEGRITY_2','MAGICFALT','MAGSTAT','Bx','By','Bz','SPARE_MAG','STEINFLT','STEINHVFAULT','SWEEP_INTEGRITY']
    fast_order = (
            "PANEL_X1_CURRENT", "PANEL_X2_CURRENT", "PANEL_Y1_CURRENT", "PANEL_Y2_CURRENT", 
            "PANEL_Z1_CURRENT", "PANEL_Z2_CURRENT", "PANEL_X_VOLT", "PANEL_X1_TEMP",
            "PANEL_X2_TEMP","PANEL_Y_VOLT","PANEL_Y1_TEMP","PANEL_Y2_TEMP",
            "PANEL_Z_VOLT","PANEL_Z1_TEMP","PANEL_Z2_TEMP","V5_BUS_CURRENT",
            "V3.3_CURR","BATT_BUS_CURR","BATT_CURR_DIR","BATT_VOLT",
            "BATT_CURR","BATT_TEMP","BATT1_CURR_DIR","BATT1_VOLT",
            "BATT1_CURR","BATT1_TEMP","BATT2_CURR_DIR","BATT2_VOLT",
            "BATT2_CURR","BATT2_TEMP","CELL_VOLT","CELL1_VOLT",
            "CELL2_VOLT","VMON_RAW_N","VMON_RAW_P","SENSE",
            "IMON_RAW","IIB_TEMP","VMON_MAG5V","SBAND_TEMP",
            "VMON_STEIN5V","STEIN_TEMP","VMON_STEINHV8V","SBAND_TEMP",
            "SPARE1","SPARE2","SPARE3","SPARE4 ")



    if (filename != None) and (type=="ASCII"):
        # check to see whether a file already exists under the specified filename
        if os.path.isfile(filename) and (overwrite==False):
            print("save_data_as: file already exists.  Specify 'OVERWRITE' keyword to continue.")
            return 1
        # else, attempt to open file for writing
        with open(filename,'w') as f:
            # write out STEIN event list
            f.write("".join(header_lines))
            f.write("%{timestamp}{s}{ccsds}{s}{slow}{s}{fast}{s}MM{s}DD{s}HH{s}mm{s}ss{s}ff\r\n".format(
                timestamp="YYYY-MM-DDTHH:MM:SS.mmmmmm",
                s=separator,
                ccsds=separator.join(["apid","packet_count"]),
                slow=separator.join(slow_order),
                fast=separator.join(fast_order)*repeat))
            for i,packet in enumerate(data_packet_dict):
                
                # ccsds data
                apid = ((packet['packet_ccsds'][0] & 0b111) << 8) + packet['packet_ccsds'][1]
                packet_count = ((packet['packet_ccsds'][2] & 0b111111) << 8) + packet['packet_ccsds'][3]
                ccsds_data2 = separator.join(["{0:#05X}".format(apid), str(packet_count)])
                #print i, ccsds_data2
                #ccsds_data2 = separator.join(map(str,ccsds_data))
                
                # slow data
                #slow_data = separator.join(packet['slow_hsk'])
                slow_data = [packet['slow_hsk'][label] for label in slow_order]
                slow_data2 = separator.join(map(str,slow_data))
                #       slow_data = slow_data+" "+str(packet['slow_hsk'][label])
                
                # fast data
                fast_data = []
                for i in range(repeat):
                    fast_data.extend([value[i] for value in packet['fast_hsk']])
                fast_data2 = separator.join(map(str,fast_data)) 
               
                # assemble an unrefined datetime object from packet_timestamp 
                # (instantiate a datetime tzinfo object)
                utc = timeops.UTC()             # use UTC, without any application of DST
                
                pt_isvalid = timeops.validate_packettime(packet['packet_timestamp'])
                year = 2012
                if pt_isvalid:
                    packet_dt = datetime.datetime(year,                     # YYYY,
                            packet['packet_timestamp'][0], packet['packet_timestamp'][1],       # MM, DD,
                            packet['packet_timestamp'][2], packet['packet_timestamp'][3],           # HH, mm,
                            packet['packet_timestamp'][4], 10000*packet['packet_timestamp'][5],     # ss, us,
                            tzinfo=utc)
                    packet_dt_ascii = packet_dt.isoformat() 
                else:
                    packet_dt_ascii = "YYYY-MM-DDTHH:MM:SS.mmmmmm"


                f.write("{timestamp}{s}{ccsds}{s}{slow}{s}{fast}{s}{MM}{s}{DD}{s}{HH}{s}{mm}{s}{ss}{s}{ff}\r\n".format(
                    timestamp=packet_dt_ascii,
                    s=separator,
                    ccsds=ccsds_data2,
                    slow=slow_data2,
                    fast=fast_data2,
                    MM=packet['packet_timestamp'][0],
                    DD=packet['packet_timestamp'][1],
                    HH=packet['packet_timestamp'][2],
                    mm=packet['packet_timestamp'][3],
                    ss=packet['packet_timestamp'][4],
                    ff=packet['packet_timestamp'][5]))    
        return 0
    if (filename != None) and (type=="SLOW"):
        # check to see whether a file already exists under the specified filename
        if os.path.isfile(filename) and (overwrite==False):
            print("save_data_as: file already exists.  Specify 'OVERWRITE' keyword to continue.")
            return 1
        # else, attempt to open file for writing
        with open(filename,'w') as f:
            # write out SLOW HSK event list
            f.write("".join(header_lines))
            f.write("%{timestamp}{s}{ccsds}{s}{slow}{s}MM{s}DD{s}HH{s}mm{s}ss{s}ff\r\n".format(
                timestamp="YYYY-MM-DDTHH:MM:SS.mmmmmm",
                s=separator,
                ccsds=separator.join(["apid","packet_count"]),
                slow=separator.join(slow_order)))
            for i,packet in enumerate(data_packet_dict):
                
                # ccsds data
                apid = ((packet['packet_ccsds'][0] & 0b111) << 8) + packet['packet_ccsds'][1]
                packet_count = ((packet['packet_ccsds'][2] & 0b111111) << 8) + packet['packet_ccsds'][3]
                ccsds_data2 = separator.join(["{0:#05X}".format(apid), str(packet_count)])
                #print i, ccsds_data2
                #ccsds_data2 = separator.join(map(str,ccsds_data))
                
                # slow data
                #slow_data = separator.join(packet['slow_hsk'])
                slow_data = [packet['slow_hsk'][label] for label in slow_order]
                slow_data2 = separator.join(map(str,slow_data))
                #       slow_data = slow_data+" "+str(packet['slow_hsk'][label])
                
                # fast data

                # assemble an unrefined datetime object from packet_timestamp 
                # (instantiate a datetime tzinfo object)
                utc = timeops.UTC()             # use UTC, without any application of DST
               
                pt_isvalid = timeops.validate_packettime(packet['packet_timestamp'])
                year = 2012
                if pt_isvalid:
                    packet_dt = datetime.datetime(year,                     # YYYY,
                            packet['packet_timestamp'][0], packet['packet_timestamp'][1],           # MM, DD,
                            packet['packet_timestamp'][2], packet['packet_timestamp'][3],           # HH, mm,
                            packet['packet_timestamp'][4], 10000*packet['packet_timestamp'][5],     # ss, us,
                            tzinfo=utc)
                    packet_dt_ascii = packet_dt.isoformat() 
                else:
                    packet_dt_ascii = "YYYY-MM-DDTHH:MM:SS.mmmmmm"

                #
                f.write("{timestamp}{s}{ccsds}{s}{slow}{s}{MM}{s}{DD}{s}{HH}{s}{mm}{s}{ss}{s}{ff}\r\n".format(
                    timestamp=packet_dt_ascii,
                    s=separator,
                    ccsds=ccsds_data2,
                    slow=slow_data2,
                    MM=packet['packet_timestamp'][0],
                    DD=packet['packet_timestamp'][1],
                    HH=packet['packet_timestamp'][2],
                    mm=packet['packet_timestamp'][3],
                    ss=packet['packet_timestamp'][4],
                    ff=packet['packet_timestamp'][5]))    
        return 0

    if (filename != None) and (type=="FAST"):
        # check to see whether a file already exists under the specified filename
        if os.path.isfile(filename) and (overwrite==False):
            print("save_data_as: file already exists.  Specify 'OVERWRITE' keyword to continue.")
            return 1
        # else, attempt to open file for writing
        with open(filename,'w') as f:
            # write out FAST HSK event list
            f.write("".join(header_lines))
            f.write("%{timestamp}{s}{ccsds}{s}{fast}{s}MM{s}DD{s}HH{s}mm{s}ss{s}ff\r\n".format(
                timestamp="YYYY-MM-DDTHH:MM:SS.mmmmmm",
                s=separator,
                ccsds=separator.join(["apid","packet_count"]),
                fast=separator.join(fast_order)))
            for i,packet in enumerate(data_packet_dict):
                
                # ccsds data
                apid = ((packet['packet_ccsds'][0] & 0b111) << 8) + packet['packet_ccsds'][1]
                packet_count = ((packet['packet_ccsds'][2] & 0b111111) << 8) + packet['packet_ccsds'][3]
                ccsds_data2 = separator.join(["{0:#05X}".format(apid), str(packet_count)])
                #print i, ccsds_data2
                #ccsds_data2 = separator.join(map(str,ccsds_data))
                
                # slow data
                
                # assemble an unrefined datetime object from packet_timestamp 
                # (instantiate a datetime tzinfo object)
                utc = timeops.UTC()             # use UTC, without any application of DST
                
                pt_isvalid = timeops.validate_packettime(packet['packet_timestamp'])
                year = 2012
                if pt_isvalid:
                    packet_dt = datetime.datetime(year,                     # YYYY,
                            packet['packet_timestamp'][0], packet['packet_timestamp'][1],           # MM, DD,
                            packet['packet_timestamp'][2], packet['packet_timestamp'][3],           # HH, mm,
                            packet['packet_timestamp'][4], 10000*packet['packet_timestamp'][5],     # ss, us,
                            tzinfo=utc)
                else:
                    packet_dt = None

                
                # fast data
                for i in range(repeat):
                    fast_data = []
                    fast_data.extend([value[i] for value in packet['fast_hsk']])
                    fast_data2 = separator.join(map(str,fast_data)) 
                    
                    if packet_dt == None:
                        packet_dt_ascii = "YYYY-MM-DDTHH:MM:SS.mmmmmm"
                    else:
                        packet_dt_ascii = (packet_dt + datetime.timedelta(seconds=i*(10./7.))).isoformat()
                    
                    f.write("{timestamp}{s}{ccsds}{s}{fast}{s}{MM}{s}{DD}{s}{HH}{s}{mm}{s}{ss}{s}{ff}\r\n".format(
                        timestamp=packet_dt_ascii,
                        s=separator,
                        ccsds=ccsds_data2,
                        fast=fast_data2,
                        MM=packet['packet_timestamp'][0],
                        DD=packet['packet_timestamp'][1],
                        HH=packet['packet_timestamp'][2],
                        mm=packet['packet_timestamp'][3],
                        ss=packet['packet_timestamp'][4],
                        ff=packet['packet_timestamp'][5]))    
        return 0
    if (filename != None) and (type=="CDF"):
        pass
    if (filename != None) and (type=="pickle"):
        pass
    #else:    
    # print ASCII 
    event_number = 0
    for i in range(len(data_packet_dict)):
        #print i,j,event_number, "Invalid Timestamp"
        event_number += 1 
    return 0

