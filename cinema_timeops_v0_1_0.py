# cinema_timeops.py
# 
# validation, correction, and fine-tuning of CINEMA packet/event timestamps
#       Version Information:
#       (production)
#         EXPERIMENTAL (not yet complete or validated)
#
#       (beta)
#       v0.1.0 9/11/2012 time correction/"tuning" master interface
#       (based on cinema_eventtime, etc)

import datetime

# -----------------------------
# Quality of Data (QoD) definitions
# -----------------------------
# We define a primary measure of quality:
# 20 BAD (should not happen; bad/corrupted data)
#   (tag with relevant HSK error byte in ones digit)
# 19 INCOMPLETE (packet_timestamp is bad; requires manual review)
# 11 INCOMPLETE (e.g., IIB buffer overflow caused dropped transfers)
# 9  DISCONTINUITY (valid data, with certain data gap)
# 8  DISCONTINUITY (valid data, with possible data gap)
# 3  IMPRECISE (valid data, with expected uncertainty 
#       [e.g. packet_timestamp automatically corrected, no frac_seconds])
# 1  PLAUSIBLE (valid data, with unexpected variation)
# 0  CREDIBLE (valid data, within expected variation)
# -----------------------------
# -----------------------------




class UTC(datetime.tzinfo):
    """UTC
    
    A UTC datetime.tzinfo class
    """

    def utcoffset(self, dt):
        return None

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return datetime.timedelta(0)

def identify_outliers(datetime_iterable, tolerance=3*86400):
    """Identify and return outliers in a datetime iterable, relative to given tolerance (in seconds).

    Keyword arguments:
    datetime_iterable -- iterable of datetime objects
    tolerance -- maximum acceptable distance from median, in seconds

    Return value:
    outliers - the subset of datetime objects which fail a Median Absolute Deviation test

    # Easy to access other quantities given this results:
    #   inliers = [dt for dt in datetime_iterable if ((dt != ()) and (dt not in outliers))]
    #   where_inliers = [i for i,dt in enumerate(datetime_iterable) if ((dt != ()) and (dt not in outliers))]
    """

    done = False
    iterations = 0
    max_iterations = 5

    # alias datetime_iterable, and generate indices
    dt_set = datetime_iterable
    ss_indices = [i for i,dt in enumerate(dt_set) if (dt != ())]
    dt_subset = [dt for i,dt in enumerate(dt_set) if (i in ss_indices)]
    n_subset = len(dt_subset)

    # begin reducing set 
    while (done != True) and (iterations < max_iterations):

        # reduce set    
        dt_subset = [dt for i,dt in enumerate(dt_set) if ((dt != ()) and (i in ss_indices))]
        # estimate = nanmedian(dt_subset)
        # deviation = MAD(dt_subset)
        (estimate, deviation) = calc_dt_median_mad(dt_subset)
        if (deviation.total_seconds() > tolerance):
            new_ss_indices = [j for j,dt in enumerate(dt_set) if ((dt != ()) and (abs(dt - estimate) < deviation))]
            n_subset = len(new_ss_indices)
            if (n_subset > 0):
                ss_indices = new_ss_indices
            else:
                done = True
        else:
            done = True
        print(iterations, estimate.total_seconds(), deviation.total_seconds(), n_subset)
        iterations += 1

    # generate outlier list
    outliers = [dt for i,dt in enumerate(dt_set) if ((dt != ()) and (i not in ss_indices))]
    return outliers


def shift_packettime(packet_timestamp):
    """Shift elements of timestamp tuple.

    Keyword arguments:
    packet_timestamp -- a timestamp tuple, of form:
            (MM,DD,HH,mm,ss,ff) [for full-timestamped packets (e.g. HSK, STEIN, Overflow)]
                or
            (HH,mm,ss,ff)       [for half-timestamped packets (e.g. MAGIC)]
       
    Return value:
    shifted_timestamp - a timestamp tuple of shape packet_timestamp
    
    Additional possibilities:
        Depending on how the byte-shift error is occuring, the issue may occur
            - within the RTC
            - during I2C transfer
            - within FSW
        As such, it may prove sufficient to simply shift the timestamp tuple elements,
            but it may also be necessary to repack via the RTC register map
    """

    if (len(packet_timestamp) == 4) or (len(packet_timestamp) == 6):
        # the "half-timestamped" case, of form (HH,mm,ss,ff)
        shifted_timestamp = packet_timestamp[1:] + (0,)
        return shifted_timestamp
        #(packet_timestamp[1],packet_timestamp[2],packet_timestamp[3],0)
        #elif len(packet_timestamp) = 6:
        # the "full-timestamped" case, of form (MM,DD,HH,mm,ss,ff)
        #shifted_timestamp = (packet_timestamp[1],packet_timestamp[2],packet_timestamp[3],
        #    packet_timestamp[4],packet_timestamp[5],0)
        #return shifted_timestamp
    else:
        # raise an exception: packet_timestamp of unexpected size/length
        return False


def validate_packettime(packet_timestamp):
    """Check fields of packet_timestamp, return Boolean.
        
    Keyword arguments:
    packet_timestamp -- a timestamp tuple, of form:
            (MM,DD,HH,mm,ss,ff) [for full-timestamped packets (e.g. HSK, STEIN, Overflow)]
                or
            (HH,mm,ss,ff)       [for half-timestamped packets (e.g. MAGIC)]
       
    Return value:
    boolean - True (fields valid) or False (out-of-bounds values)
        
    Checks range of each field in packet_timestamp tuple.
    """
    if len(packet_timestamp) == 4:
        # the "half-timestamped" case, of form (HH,mm,ss,ff)
        if (((packet_timestamp[0] < 0) or (packet_timestamp[0] > 23)) \
                or ((packet_timestamp[1] < 0) or (packet_timestamp[1] > 59)) \
                or ((packet_timestamp[2] < 0) or (packet_timestamp[2] > 59)) \
                or ((packet_timestamp[3] < 0) or (packet_timestamp[3] > 99))):
            # then fail: there are out-of-bounds values
            return False
        else:
            # pass: all packet_timestamp values nominally OK
            return True
    elif len(packet_timestamp) == 6:
        # the "full-timestamped" case, of form (MM,DD,HH,mm,ss,ff)
        if ((packet_timestamp[0] < 1) or (packet_timestamp[0] > 12)) \
                or ((packet_timestamp[1] < 1) or (packet_timestamp[1] > 31)) \
                or ((packet_timestamp[2] < 0) or (packet_timestamp[2] > 23)) \
                or ((packet_timestamp[3] < 0) or (packet_timestamp[3] > 59)) \
                or ((packet_timestamp[4] < 0) or (packet_timestamp[4] > 59)) \
                or ((packet_timestamp[5] < 0) or (packet_timestamp[5] > 99)):
                    # then fail: there are out-of-bounds values
                    return False
        else:
            # pass: all packet_timestamp values nominally OK
            return True
    else:
        # raise an exception: packet_timestamp of unexpected size/length
        return False

def MAD(a, c=0.6745, axis=0):
    """Median Absolute Deviation along given axis of an array:

    median(abs(a - median(a))) / c
    """

    good = (a==a)
    a = np.asarray(a, np.float64)
    if a.ndim == 1:
        d = np.median(a[good])
        m = np.median(np.fabs(a[good] - d) / c)
    else:
        d = np.median(a[good], axis=axis)
        # I don't want the array to change so I have to copy it?
        if axis > 0:
            aswp = swapaxes(a[good],0,axis)
        else:            
            aswp = a[good]
            m = np.median(np.fabs(aswp - d) / c, axis=0)

    return m


def nanmedian(arr):
    """Returns median ignoring NAN
    """
    return np.median(arr[arr==arr])


def calc_dt_median_mad(dt_subset, epoch=datetime.datetime(1970,1,1, tzinfo=UTC())):
    # calculate median and MAD (median-adjusted-deviation) for list of datetime objects 

    # generate list of "seconds elapsed" relative to specified epoch
    deltas = [(dt-epoch).total_seconds() for dt in dt_subset]
    print(len(dt_subset), deltas)

    # calculate median and MAD
    estimate = nanmedian(np.array(deltas))        # (in seconds elapsed since EPOCH)
    print("estimate", estimate)
    deviation = MAD(np.array(deltas))             # (in seconds)
    #print("deltas", deltas)
    #print("deviation2", deviation)
    # make them standard datetime objects again 
    ret_estimate = datetime.timedelta(seconds=estimate)
    ret_deviation = datetime.timedelta(seconds=deviation)

    return (ret_estimate+epoch, ret_deviation)

