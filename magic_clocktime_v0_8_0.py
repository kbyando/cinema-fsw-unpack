# magic_clocktime.py
#
#
#    Version Information:
#       (production)
#       v0.8.0 10/02/2012 "magic_clocktime" initial production code; v0.8.x series interfaces 
#
#       (beta)
#       v0.1.7 10/02/2012 thorough pruning of old / unused code; minor algorithm tweaks
#       v0.1.7 10/02/2012 thorough pruning of old / unused code; minor algorithm tweaks
#       v0.1.6 10/01/2012 new algorithms for identifying RTC jitter & dropped frames
#       v0.1.51 10/01/2012 major bug corrected in cycles_elapsed placement
#       v0.1.5 8/02/2012 testing median-absolute-deviation based rejection
#       v0.1.4 7/23/2012 fork: reject all byte-shifted errors
#       v0.1.3 7/19/2012 timestamp fitting routine added
#       v0.1.2 7/18/2012 improved timegap checking
#       v0.1.1 7/17/2012 refined QoD handling (as magic_sampletime.py)
#       v0.1.0 7/13/2012 initial draft
#
#

import datetime
import numpy as np



# -----------------------------
# MAGIC DATA PARAMETERS
# -----------------------------
# INSTRUMENT MODE
attitudecfg = 0         # 000
attitude = 1            # 001
scienceAPr = 2          # 010
gradiometer = 3         # 011

# SENSOR
outboard = 0            # 0
inboard = 1             # 1

# MT - Magnetic Field or Temperature
mag = 0
temp = 1                # NOTE! appears unimplemented for CINEMA1!

# 128 Hz clock cycles utilized for a complete sample
temp_cycles = 0         # not implemented for CINEMA1
att_cycles = 8          # cycles: acquire 4 vectors, every other cycle (16 Hz)
sci_cycles = 16         # cycles: acquire 4 vectors, every fourth cycle (8 Hz) 
gra_cycles = 8          # cycles: acquire 4 vectors, every other cycle (16 Hz; 8 Hz whole)

# nominal and acceptable gaps (in seconds) [we split on anything greater..]
#   - a 39-sample packet filled at 8Hz takes 4.875 sec to fill
#   - if background tasks exceed their allotted 1ms, this may grow
#   - we'll split on mode changes (which clear the MAG frame buffer),
#       and it's unlikely that we should have many repeated mode inits
#   - if CINEMA has been operating in SAFE mode, we expect no MAG packets
# -----------------------------


# -----------------------------
# Quality of Data (QoD)
# -----------------------------
# We define a primary measure of quality:
# 20 BAD (should not happen; bad/corrupted data)
#   (tag with relevant HSK error byte in ones digit)
# 19 INCOMPLETE (packet_timestamp is bad; requires manual review)
# 11 INCOMPLETE (IIB buffer overflow caused dropped transfers)
# 9  DISCONTINUITY (valid data, with certain data gap)
# 8  DISCONTINUITY (valid data, with possible data gap)
# 3  IMPRECISE (valid data, with expected uncertainty 
#       [e.g. packet_timestamp automatically corrected, no frac_seconds])
# 1  PLAUSIBLE (valid data, with unexpected variation)
# 0  CREDIBLE (valid data, within expected variation)
# 
# data is considered "fully bad" if QoD >= 20
# data is considered "useful" if QoD <= 11
# data is considered "individual fit" if QoD (8,11)
# datat is considered
# -----------------------------
# -----------------------------
# Quality of Data (QoD)
# -----------------------------


# A UTC datetime.tzinfo class
class UTC(datetime.tzinfo):
    """UTC"""

    def utcoffset(self, dt):
        return None

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return datetime.timedelta(0)


def calc_fitted_sampletime(packet_list, year=2012, month=1, day=3):

    # Create a working array in which we mark QoD for 
    #   packets and packet boundaries.  Even-indexed elements 
    #   (e.g. 0,2,4,..) contain QoD for packets, while the
    #   odd-indexed elements (e.g. 1,3,5,..) contain QoD for 
    #   packet boundaries.  Initialized as QoD=0 [CREDIBLE]
    n_packets = len(packet_list)
    quality_array = np.zeros((n_packets*2), dtype=np.uint8)
    quality_array[-1] = 20      # a "break" in the very last element

    last_mode = -1              # last packet's mode
    previous_time = -1          # last (valid) packet's time
    rollover_cnt = 0            # counter for daily timetuple rollover events
    x_time = []                 # storage list for accumulated "cycle_seconds" at each packet
    y_time = []                 # list of datetime objects (for conversion to timedelta.total_seconds)
    cycle_seconds = 0           # accumulating count of elapsed cycles, in seconds  

    # ----------------------------------------------------------------
    # ----------------------------------------------------------------
    # Make initial Quality-of-Data (QoD) determinations
    for i,packet in enumerate(packet_list):
        
        # -- QoD = 19: examine packet_timestamps
        #   (if it contains invalid values, the packet can't be trusted)
        #   Work-around: if the RTC is garbage, we can manually change
        #       the packet_timestamp field, before getting this far.
        pt_isvalid = validate_packettime(packet['packet_timestamp'])

        # mark BAD packets with QoD = 19
        if (not pt_isvalid):
            quality_array[2*i] = 19             # QoD (bad timestamp)
            # pass information to user
            print("Bad Timestamp in Packet #: " + str(i))
           

        # -- QoD = 9: examine MODE switches
        #   (buffer cleared during MODE switch, so certain discontinuity)
        #   CINEMA1: individual packets only contain samples of one mode
        packet_mode = packet['magic_data'][0][0][0]     # 1st sample, status tuple, mode      
        
        if (last_mode != -1) and (last_mode != packet_mode):
            # mark the (intermediate) packet boundary
            quality_array[2*i - 1] = 9          # QoD (certain datagap)
            previous_time = -1                  # clear stored "previous_time"
        last_mode = packet_mode         # store the current packet_mode

        # examine the mode we're in (for next loop iteration) 
        #   CINEMA1: individual packets only contain samples of one mode
        if (packet_mode == attitude):
            increment = 39.*(att_cycles/128.)   # seconds
        elif (packet_mode == scienceAPr):
            increment = 39.*(sci_cycles/128.)   # seconds
        elif (packet_mode == gradiometer):
            increment = 39.*(gra_cycles/128.)   # seconds
        else: 
            increment = 0

        # -- QoD = 3: examine PACKET_TIMESTAMP for RTC rollover, jitter, and dropped frames
        #   Strict requirements for time: t2 > t1
        #   Loose requirements for time: abs(abs(t2 - t1) - increment) <= tolerance
        #   Also examine packet_timestamps for I2C byte shift.
        #   (e.g. RTC "day of month" has slipped into HH slot, bumping 
        #       all subsequent RTC tuple entries, and displacing fracsec)
        #   Preference given to rollover events; byte-shift errors not handled
        timetuple = packet['packet_timestamp']

        if (pt_isvalid):
            # store timetuple in a timedelta object
            current_time = datetime.timedelta(
                    days = rollover_cnt,
                    hours=timetuple[0],
                    minutes=timetuple[1], seconds=timetuple[2],
                    milliseconds=10*timetuple[3])

            # define tolerances
            loose_tolerance = 300       # seconds
            tight_tolerance = 0.1       # seconds

            # check for data with which to compare
            if (previous_time == -1):
                y_time.append(current_time)
                x_time.append(0)
                previous_time = current_time
            else:
                delta = (current_time - previous_time).total_seconds()
                
                # we have a strict requirement that t2 > t1
                if (current_time <= previous_time):
                    # possibilities:
                    # 1) timestamp may be *BAD*
                    # 2) RTC may have been (re)set
                    # 3) we may have just rolled over
                    # 4) data may be duplicate

                    # considering the rollover possibility
                    if abs(abs(delta) - 86400) < loose_tolerance:
                        # rollover is likely
                        current_time = current_time + datetime.timedelta(days=1)
                        rollover_cnt += 1
                        delta = (current_time - previous_time).total_seconds()
                    else:
                        # assume that timestamp is somehow bad, but that data is good
                        # mark timestamp as BAD
                        quality_array[2*i] = 19
                        
                        # increment current_time as expected
                        current_time = previous_time + datetime.timedelta(seconds=increment) 
                        delta = (current_time - previous_time).total_seconds()

                # test for:
                # 1) RTC jitter
                # 2) dropped frames
                # 3) RTC jitter *on top* of dropped frames

                # examine adjacent frames for RTC jitter (incorrect seconds field in RTC timestamp)
                f_diff = (delta - increment)    # frame difference
                jitter = abs(f_diff - round(f_diff)) < tight_tolerance

                # examine adjacent frames for evidence of a dropped frame
                f_mult = (delta/increment)    # frame multiple
                dropped = abs(f_mult - round(f_mult)) < tight_tolerance

                # examine adjacent frames for evidence of jitter+dropping
                plusminus = np.array((-1,1))
                f_mult2 = (delta+plusminus)/increment
                jittdrop = abs(f_mult2 - map(round,f_mult2)) < tight_tolerance
                
                # ----------------------------------------------------------------
                # -- DIAGNOSTIC PRINTOUT (uncomment) --
                #print (i, timetuple, delta, jitter, dropped, jittdrop[0], jittdrop[1])
                # ----------------------------------------------------------------
              

                # handle test results 
                if (jitter):
                    # expected
                    cycle_seconds += increment 
                    y_time.append(current_time) # mostly correct (possible jitter)
                    x_time.append(cycle_seconds)
                elif (dropped):
                    quality_array[2*i] = 3      # placeholder QoD
                    cycle_seconds += increment*round(f_mult) 
                    y_time.append(current_time) # correct, but with regular datagap
                    x_time.append(cycle_seconds)
                elif (jittdrop.any()):
                    # getting complicated
                    quality_array[2*i] = 3      # placeholder QoD     
                    pos = (jittdrop.tolist()).index(True)
                    cycle_seconds += increment*round(f_mult2[pos]) 
                    y_time.append(current_time) # mostly correct, but with regular datagap (and possible jitter)
                    x_time.append(cycle_seconds)
                else:
                    # yipes.. looking bad
                    quality_array[2*i-1] = 19   # placeholder QoD
                    cycle_seconds += increment 
                    y_time.append(current_time) # assume the data is good 
                    x_time.append(cycle_seconds)

            previous_time = current_time 
        else:
            y_time.append(())
            x_time.append(())
    
    # ----------------------------------------------------------------
    # -- DIAGNOSTIC PRINTOUT (uncomment) --
    # -- prints the raw packet_timestamp tuple, followed by the (Y,X) pair to fit 
    #for i,packet in enumerate(packet_list):
    #    print i, packet['packet_timestamp'], y_time[i], x_time[i]
    # ----------------------------------------------------------------

    
    # Build blocks of continuous good packet data
    #   (break on BAD data)
    #   Usage: anything above 'threshold' causes a break)
    continuous_blocks = generate_ranges(quality_array, threshold=7)    
    
    # (instantiate a datetime tzinfo object)
    utc = UTC()         # use UTC, without any application of DST
    start = datetime.datetime(year,month,day, tzinfo=utc)

    # examine blocks of continuous data
    for block in continuous_blocks:
        s = block[0]    # start packet
        f = block[1]    # finish packet

        c_time = x_time[s:f]    # cycle time (value of 128Hz clock, in seconds)
        r_time = y_time[s:f]    # RTC time
            
        # obtain a linear fit
        #   p_coeff = (m,b)
        packet_quality = quality_array[2*s:2*f]
        
        #if len(c_time) > 0:
        (p_coeff,first_timestamp) = fit_timestamps(c_time, r_time, packet_quality)
        if (p_coeff[0] > 1.1 or p_coeff[0] < 0.95):
            quality_array[2*s:2*f:2] = 17       # QoD (flag as ALGORITHM FAILED) 
        # p_coeff = (1., p_coeff[1])            # a kludge, not to be used
        #else:
        #    (p_coeff,first_timestamp) = ((1.,0), r_time[0])
        
        # ----------------------------------------------------------------
        # -- DIAGNOSTIC PRINTOUT (uncomment) --
        # -- prints PACKET_QUALITY subset for working block and fit packet_timestamp
        #print("--------------")
        #print ("PACKET_QUALITY: [" + str(s) + ":" + str(f) + "]")
        #print(packet_quality)
        #for i,packet in enumerate(packet_list[s:f]):
        #    packet_mode = packet['magic_data'][0][0][0] 
        #    temp_dt = np.polyval(p_coeff,c_time[i])
        #    tdate = (datetime.datetime(year,month,day) + first_timestamp + datetime.timedelta(seconds=temp_dt))
        #    print(s+i, packet_mode, packet['packet_timestamp'], (tdate.hour, tdate.minute, tdate.second, tdate.microsecond/10000))
        #print("--------------")
        # ----------------------------------------------------------------


        # build "clock time" from "cycle time" fit of RTC
        for i,packet in enumerate(packet_list[s:f]):
   
            # CINEMA1: individual packets only contain samples of one mode
            # NOTE: if this changes, then the following should be moved to be
            #   within the per-sample loop, such that sample[0][0] takes the
            #   place of "packet_mode" and is checked every iteration.
            packet_mode = packet['magic_data'][0][0][0] # consider the first sample

            # look to see whether the sample MAG_DATA or IB_TEMP
            if (packet['magic_data'][0][0][2] == temp):
                # IB diode temperature measurement
                # - begin sample acquisition approx. every 300 seconds
                # ** UNIMPLEMENTED IN CINEMA1 **
                raise Exception("Error! (MAGIC: TEMP Unimplemented in CINEMA1)")
            elif (packet_mode == attitude):
                # Attitude Mode
                # - vector acquisition every other (odd) interrupt
                # - complete sample effectively occupies 8 consecutive cycles
                # - 128Hz / 2 interrupts / 4 vectors = 16 Hz sample  
                increment = att_cycles
            elif (packet_mode == scienceAPr):
                # Science Mode
                # - extended settle time: vector acquisition every 4th interrupt
                # - individual vector returned in 1 cycle
                # - complete sample effectively occupies 16 cycles
                # - 128Hz / 4 interrupts / 4 vectors = 8 Hz sample
                increment = sci_cycles
            elif (packet_mode == gradiometer):
                # Gradiometer Mode
                # - vector acquisition every other (even) interrupt
                # - effectively occupies 8 consecutive cycles (half-sample)
                # - 128Hz / 2 interrupts / 4 vectors = 16 Hz sample (OB)
                # - half-sample written to data frame
                # (the same is performed for IB vectors in the next 8 cycles)
                increment = gra_cycles
            else:
                # Unrecognized sample type!
                raise Exception("Error! (MAGIC: Unrecognized mode type)")
            
            # instantiate storage list for "clock time"
            clocktime = []      # fit-derived datetime.datetime objects
           
            # begin looping over samples
            cycles_elapsed = 0  # number of cycles elapsed (per packet)
            for j, sample in enumerate(packet['magic_data']):
                
                # perform evaluation
                cycletime = c_time[i] + (cycles_elapsed/128.)
                dt = np.polyval(p_coeff,cycletime)
                fit_time = (start + first_timestamp + datetime.timedelta(seconds=dt))
                clocktime.append(fit_time)
        
                # ----------------------------------------------------------------
                # -- DIAGNOSTIC PRINTOUT (uncomment) --
                # -- prints the raw packet_timestamp tuple, followed by the fit-derived time tuple 
                ##time_tuple = packet['packet_timestamp']
                ##dd = fit_time.day
                ##hr = fit_time.hour
                ##mi = fit_time.minute
                ##sec = fit_time.second
                ##ff = fit_time.microsecond/1000
                ###if (j == 0):
                ##print(s+i, j, packet_mode, time_tuple, (hr,mi,sec,ff))
                # ----------------------------------------------------------------

                # increment cycles_elapsed
                cycles_elapsed += increment

            # stow fit-derived "clock time" in per event/sample time field
            packet['clock_time'] = clocktime

    return packet_list, quality_array


def generate_ranges(quality, threshold):
    ranges = []         # list of start/stop tuples
    series = []         # elements of successive series

    for i,q in enumerate(quality):
        # examine packet and packet_boundary quality
        if (i%2 == 0) and (q <= threshold):      # q_packet OK
            series.append(i/2)
        elif (q > threshold) and (len(series) > 0):
            # QoD not acceptable (but series accumulated) 
            # break the series, extracting start and stop indices 
            ranges.append((series[0],series[-1]+1))
            series = []
    # if we haven't yet stored the last element, do so
    if len(series) > 0:
        ranges.append((series[0],series[-1]+1))

    return ranges


def validate_packettime(packet_timestamp):
    """Check fields of packet_timestamp, return Boolean.
             
    Keyword arguments:
    packet_timestamp -- a tuple of form (HH,mm,ss,ff).
                            
    Return value:
    boolean - True (fields valid) or False (out-of-bounds values)
                                            
    Checks range of each field in packet_timestamp tuple.
    """
    if (((packet_timestamp[0] < 0) or (packet_timestamp[0] > 23)) \
            or ((packet_timestamp[1] < 0) or (packet_timestamp[1] > 59)) \
            or ((packet_timestamp[2] < 0) or (packet_timestamp[2] > 59)) \
            or ((packet_timestamp[3] < 0) or (packet_timestamp[3] > 99))):
        # then fail: there are out-of-bounds values
        return False
    else:
        # pass: all packet_timestamp values nominally OK
        return True


def fit_timestamps(cycle_time, clock_time, quality):
    """Linear fitting of RTC to ticks.  Returns fitted timestamps.
          
    Keyword arguments:
    cycle_time -- the non-absolute 128Hz cycle timing at packet start
    clock_time -- the RTC-derived packet_timestamp, as a timedelta object
    quality -- quality-of-data list

    We have two clocks available to us:
    1) the 128Hz frequency on which the MAG task is called (*not absolute*!)
    2) the RTC, and the timestamp it produces for each packet
    Using numpy.polyfit, we fit the RTC time to the 128Hz task cadence.
    """

    # convert timedelta objects in "clock_time" to seconds
    first_timestamp = clock_time[0]
    diff_secs = [(t - first_timestamp).total_seconds() for t in clock_time]

    # exclude byte-shift affected packet_times from consideration
    blocks = generate_ranges(quality, threshold=2)
    x_arraylike = []
    y_arraylike = []
    for block in blocks:
        s = block[0]    # start index 
        f = block[1]    # finish index
        x_arraylike.extend(cycle_time[s:f])
        y_arraylike.extend(diff_secs[s:f])

    # linear fitting of selected timestamps
    if len(x_arraylike) > 1:
        p_coeff = np.polyfit(x_arraylike, y_arraylike, deg=1)
    else:
        print(len(cycle_time),len(clock_time),len(quality))
        p_coeff = (1., -0.001)

    # print diagostic results
    print("m = " + str(p_coeff[0]) + ", b = " + str(p_coeff[1]))

    # evaluate the fitted timestamps
    #fit = np.polyval(p_coeff, tick_time)  # units are seconds

    # convert the fitted diff_secs ("fit") to datetime objects, and restore epoch 
    #eventtime = [first_timestamp + datetime.timedelta(seconds=dt) for dt in fit]
    #return eventtime
    return p_coeff, first_timestamp

