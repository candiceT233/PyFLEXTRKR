# Purpose: match mergedir tracked MCS with NMQ CSA and calculate radar-based statistics underneath the cloud features.

# Author: Original IDL code written by Zhe Feng (zhe.feng@pnnl.gov), Python version written by Hannah C. Barnes (hannah.barnes@pnnl.gov)


def identifypf_wrf_rain(
    mcsstats_filebase,
    cloudid_filebase,
    pfdata_filebase,
    rainaccumulation_filebase,
    stats_path,
    cloudidtrack_path,
    pfdata_path,
    rainaccumulation_path,
    startdate,
    enddate,
    geolimits,
    nmaxpf,
    nmaxcore,
    nmaxclouds,
    rr_min,
    pixel_radius,
    irdatasource,
    nmqdatasource,
    datadescription,
    datatimeresolution,
    mcs_irareathresh,
    mcs_irdurationthresh,
    mcs_ireccentricitythresh,
):
    # Input:
    # mcsstats_filebase - file header of the mcs statistics file that has the satellite data and was produced in the previous step
    # cloudid_filebase - file header of the cloudid file created in the idclouds step
    # pfdata_filebase - file header of the radar data
    # rainaccumulation_filebase - file header of the rain accumulation data
    # stats_path - directory which stores this statistics data. this is where the output from this code will be placed.
    # cloudidtrack_path - directory that contains the cloudid data created in the idclouds step
    # pfdata_path - directory that contains the radar data
    # rainaccumulation_path - directory containing the rain accumulation data
    # startdate - starting date and time of the data
    # enddate - ending date and time of the data
    # geolimits - 4-element array with plotting boundaries [lat_min, lon_min, lat_max, lon_max]
    # namxpf - maximum number of precipitation features that can exist within one satellite defined MCS at a given time
    # nmaxcore - maximum number of convective cores that can exist within one satellite defined MCS at a given time
    # nmaxclouds - maximum number of clouds allowed to be within one track
    # rr_min - minimum rain rate used when classifying precipitation features
    # pixel_radius - radius of pixels in km
    # irdatasource - source of the raw satellite data
    # nmqdatasource - source of the radar data
    # datadescription - description of the satellite data source
    # datatimeresolution - time resolution of the satellite data
    # mcs_irareathresh - satellite area threshold for MCS identificaiton
    # mcs_irdurationthresh - satellite duration threshold for MCS identification
    # mcs_ireccentricitythresh - satellite eccentricity threshold used for classifying squall lines

    # Output: (One netcdf with statistics about the satellite, radar, and rain accumulation characteristics for each satellite defined MCS)
    # mcs_length - duration of MCS portion of each track
    # length - total duration of each track (MCS and nonMCS components)
    # mcs_type - flag indicating whether this is squall line, based on satellite definition
    # status - flag indicating the evolution of each cloud in a MCS
    # startstatus - flag indicating how a MCS starts
    # endstatus - flag indicating how a MCS ends
    # trackinterruptions - flag indicating if the data used to identify this MCS is incomplete
    # boundary - flag indicating if a MCS touches the edge of the domain
    # basetime - seconds since 1970-01-01 for each cloud in a MCS
    # datetimestring - string of date and time of each cloud in a MCS
    # meanlat - mean latitude of the MCS
    # meanlon - mean longitude of the MCS
    # core_area - area of the core of MCS
    # ccs_area - area of the core and cold anvil of the MCS
    # cloudnumber - numbers indicating which clouds in the cloudid files are associated with a MCS
    # mergecloudnumber - numbers indicating which clouds in the cloudid files merge into this track
    # splitcloudnumber - numbers indicating which clouds in the cloudid files split from this track
    # nmq_frac - fraction of the cloud that exists within the radar domain
    # npf - number of precipitation features at reach time
    # pf_area - area of the precipitation features in a MCS
    # pf_lon - mean longitudes of the precipitaiton features in a MCS
    # pf_lat - mean latitudes of the precipitation features in a MCS
    # pf_rainrate - mean rainrates of the precipition features in a MCS
    # pf_skewness - skewness of the rainrains in precipitation features in a MCS
    # pf_majoraxislength - major axis lengths of the precipitation features in a MCS
    # pf_minoraxislength - minor axis lengths of the precipitation features in a MCS
    # pf_aspectratio - aspect ratios of the precipitation features in a MCS
    # pf_eccentricity - eccentricity of the precipitation features in a MCS
    # pf_orientation - angular position of the precipitation deatures in a MCS
    # pf_dbz40area - area covered by 40 dbZ echos in a precipitaiton feature in a MCS
    # pf_dbz45area - area covered by 45 dbZ echos in a precipitaiton feature in a MCS
    # pf_dbz50area - area covered by 50 dbZ echos in a precipitaiton feature in a MCS
    # pf_ccrainrate - mean rain rate of the largest few convective cores in a MCS
    # pf_sfrainrate - mean rain rate of the largest few stratiform regions in a MCS
    # pf_ccdbz10 - average height of the 10 dBZ echo top in the largest few convective cores in a MCS
    # pf_ccdbz20 - average height of the 20 dBZ echo top in the largest few convective cores in a MCS
    # pf_ccdbz30 - average height of the 30 dBZ echo top in the largest few convective cores in a MCS
    # pf_ccdbz40 - average height of the 40 dBZ echo top in the largest few convective cores in a MCS
    # pf_ncores - number of convective cores at each time during an MCS
    # pf_corelon - mean longitude of the convective cores in a MCS
    # pf_corelat - mean latitude of the convective cores in a MCS
    # pf_corearea - area of the convective cores in a MCS
    # pf_coremajoraxislength - major axis length of convective cores in a MCS
    # pf_coreminoraxislength - minor axis length of convective cores in a MCS
    # pf_coreaspectratio - aspect ratio of convective cores in a MCS
    # pf_coreccentricity - eccentricity of convective cores in a MCS
    # pf_coreorientation - angular position of convective cores in a MCS
    # pf_coremaxdbz10 - maximum height of the 10 dBZ echo contour in convective cores
    # pf_coremaxdbz20 - maximum height of the 20 dBZ echo contour in convective cores
    # pf_coremaxdbz30 - maximum height of the 30 dBZ echo contour in convective cores
    # pf_coremaxdbz40 - maximum height of the 40 dBZ echo contour in convective cores
    # pf_coreavgdbz10 - average height of the 10 dBZ echo contour in convective cores
    # pf_coreavgdbz20 - average height of the 20 dBZ echo contour in convective cores
    # pf_coreavgdbz30 - average height of the 30 dBZ echo contour in convective cores
    # pf_coreavgdbz40 - average height of the 40 dBZ echo contour in convective cores

    import numpy as np
    import logging
    import os.path
    from netCDF4 import Dataset, num2date, chartostring
    from scipy.ndimage import label, binary_dilation, generate_binary_structure
    from skimage.measure import regionprops
    from math import pi
    from scipy.stats import skew
    import xarray as xr
    import time
    import pandas as pd
    import time, datetime, calendar

    np.set_printoptions(threshold=np.inf)
    logger = logging.getLogger(__name__)
    #########################################################
    # Load MCS track stats
    logger.info("Loading IR data")
    logger.info((time.ctime()))
    mcsirstatistics_file = (
        stats_path + mcsstats_filebase + startdate + "_" + enddate + ".nc"
    )
    logger.info(mcsirstatistics_file)

    mcsirstatdata = Dataset(mcsirstatistics_file, "r")
    ir_ntracks = np.nanmax(mcsirstatdata["ntracks"]) + 1
    ir_nmaxlength = np.nanmax(mcsirstatdata["ntimes"]) + 1
    ir_basetime = mcsirstatdata["mcs_basetime"][:]
    basetime_units = mcsirstatdata["mcs_basetime"].units
    basetime_calendar = mcsirstatdata["mcs_basetime"].calendar
    ir_datetimestring = mcsirstatdata["mcs_datetimestring"][:]
    ir_cloudnumber = mcsirstatdata["mcs_cloudnumber"][:]
    ir_mergecloudnumber = mcsirstatdata["mcs_mergecloudnumber"][:]
    ir_splitcloudnumber = mcsirstatdata["mcs_splitcloudnumber"][:]
    ir_mcslength = mcsirstatdata["mcs_length"][:]
    ir_tracklength = mcsirstatdata["track_length"][:]
    ir_mcstype = mcsirstatdata["mcs_type"][:]
    ir_status = mcsirstatdata["mcs_status"][:]
    ir_startstatus = mcsirstatdata["mcs_startstatus"][:]
    ir_endstatus = mcsirstatdata["mcs_endstatus"][:]
    ir_trackinterruptions = mcsirstatdata["mcs_trackinterruptions"][:]
    ir_boundary = mcsirstatdata["mcs_boundary"][:]
    ir_meanlat = mcsirstatdata["mcs_meanlat"][:]
    ir_meanlon = mcsirstatdata["mcs_meanlon"][:]
    ir_corearea = mcsirstatdata["mcs_corearea"][:]
    ir_ccsarea = mcsirstatdata["mcs_ccsarea"][:]
    mcsirstatdata.close()

    # ir_datetimestring = ir_datetimestring[:, :, :, 0]
    ir_datetimestring = ir_datetimestring[:, :, :]

    ###################################################################
    # Intialize precipitation statistic matrices
    logger.info("Initializing matrices")
    logger.info((time.ctime()))

    # Variables for each precipitation feature
    radar_npf = np.ones((ir_ntracks, ir_nmaxlength), dtype=int) * -9999
    radar_pflon = np.ones((ir_ntracks, ir_nmaxlength, nmaxpf), dtype=float) * np.nan
    radar_pflat = np.ones((ir_ntracks, ir_nmaxlength, nmaxpf), dtype=float) * np.nan
    radar_pfnpix = np.ones((ir_ntracks, ir_nmaxlength, nmaxpf), dtype=int) * -9999
    radar_pfrainrate = (
        np.ones((ir_ntracks, ir_nmaxlength, nmaxpf), dtype=float) * np.nan
    )
    radar_pfskewness = (
        np.ones((ir_ntracks, ir_nmaxlength, nmaxpf), dtype=float) * np.nan
    )
    radar_pfmajoraxis = (
        np.ones((ir_ntracks, ir_nmaxlength, nmaxpf), dtype=float) * np.nan
    )
    radar_pfminoraxis = (
        np.ones((ir_ntracks, ir_nmaxlength, nmaxpf), dtype=float) * np.nan
    )
    radar_pfaspectratio = (
        np.ones((ir_ntracks, ir_nmaxlength, nmaxpf), dtype=float) * np.nan
    )
    radar_pforientation = (
        np.ones((ir_ntracks, ir_nmaxlength, nmaxpf), dtype=float) * np.nan
    )
    radar_pfeccentricity = (
        np.ones((ir_ntracks, ir_nmaxlength, nmaxpf), dtype=float) * np.nan
    )
    radar_pfdbz40npix = np.ones((ir_ntracks, ir_nmaxlength, nmaxpf), dtype=int) * -9999
    radar_pfdbz45npix = np.ones((ir_ntracks, ir_nmaxlength, nmaxpf), dtype=int) * -9999
    radar_pfdbz50npix = np.ones((ir_ntracks, ir_nmaxlength, nmaxpf), dtype=int) * -9999
    radar_basetime = np.empty((ir_ntracks, ir_nmaxlength), dtype="datetime64[s]")

    # Variables average for the largest few precipitation features
    radar_ccavgrainrate = np.ones((ir_ntracks, ir_nmaxlength), dtype=float) * np.nan
    radar_ccavgnpix = np.ones((ir_ntracks, ir_nmaxlength), dtype=int) * -9999
    radar_ccavgdbz10 = np.ones((ir_ntracks, ir_nmaxlength), dtype=float) * np.nan
    radar_ccavgdbz20 = np.ones((ir_ntracks, ir_nmaxlength), dtype=float) * np.nan
    radar_ccavgdbz30 = np.ones((ir_ntracks, ir_nmaxlength), dtype=float) * np.nan
    radar_ccavgdbz40 = np.ones((ir_ntracks, ir_nmaxlength), dtype=float) * np.nan
    radar_sfavgnpix = np.ones((ir_ntracks, ir_nmaxlength), dtype=int) * -9999
    radar_sfavgrainrate = np.ones((ir_ntracks, ir_nmaxlength), dtype=float) * np.nan

    # Variables for each convective core
    radar_ccncores = np.ones((ir_ntracks, ir_nmaxlength), dtype=int) * -9999
    radar_cclon = np.ones((ir_ntracks, ir_nmaxlength, nmaxcore), dtype=float) * np.nan
    radar_cclat = np.ones((ir_ntracks, ir_nmaxlength, nmaxcore), dtype=float) * np.nan
    radar_ccnpix = np.ones((ir_ntracks, ir_nmaxlength, nmaxcore), dtype=int) * -9999
    radar_ccxcentroid = (
        np.ones((ir_ntracks, ir_nmaxlength, nmaxcore), dtype=int) * -9999
    )
    radar_ccycentroid = (
        np.ones((ir_ntracks, ir_nmaxlength, nmaxcore), dtype=int) * -9999
    )
    radar_ccxweightedcentroid = (
        np.ones((ir_ntracks, ir_nmaxlength, nmaxcore), dtype=int) * -9999
    )
    radar_ccyweightedcentroid = (
        np.ones((ir_ntracks, ir_nmaxlength, nmaxcore), dtype=int) * -9999
    )
    radar_ccmajoraxis = (
        np.ones((ir_ntracks, ir_nmaxlength, nmaxcore), dtype=float) * np.nan
    )
    radar_ccminoraxis = (
        np.ones((ir_ntracks, ir_nmaxlength, nmaxcore), dtype=float) * np.nan
    )
    radar_ccaspectratio = (
        np.ones((ir_ntracks, ir_nmaxlength, nmaxcore), dtype=float) * np.nan
    )
    radar_ccorientation = (
        np.ones((ir_ntracks, ir_nmaxlength, nmaxcore), dtype=float) * np.nan
    )
    radar_ccperimeter = (
        np.ones((ir_ntracks, ir_nmaxlength, nmaxcore), dtype=float) * np.nan
    )
    radar_cceccentricity = (
        np.ones((ir_ntracks, ir_nmaxlength, nmaxcore), dtype=float) * np.nan
    )
    radar_ccmaxdbz10 = (
        np.ones((ir_ntracks, ir_nmaxlength, nmaxcore), dtype=float) * np.nan
    )
    radar_ccmaxdbz20 = (
        np.ones((ir_ntracks, ir_nmaxlength, nmaxcore), dtype=float) * np.nan
    )
    radar_ccmaxdbz30 = (
        np.ones((ir_ntracks, ir_nmaxlength, nmaxcore), dtype=float) * np.nan
    )
    radar_ccmaxdbz40 = (
        np.ones((ir_ntracks, ir_nmaxlength, nmaxcore), dtype=float) * np.nan
    )
    radar_ccdbz10mean = (
        np.ones((ir_ntracks, ir_nmaxlength, nmaxcore), dtype=float) * np.nan
    )
    radar_ccdbz20mean = (
        np.ones((ir_ntracks, ir_nmaxlength, nmaxcore), dtype=float) * np.nan
    )
    radar_ccdbz30mean = (
        np.ones((ir_ntracks, ir_nmaxlength, nmaxcore), dtype=float) * np.nan
    )
    radar_ccdbz40mean = (
        np.ones((ir_ntracks, ir_nmaxlength, nmaxcore), dtype=float) * np.nan
    )

    radar_pffrac = np.ones((ir_ntracks, ir_nmaxlength), dtype=float) * np.nan

    ##############################################################
    # Find precipitation feature in each mcs
    logger.info(("Total Number of Tracks:" + str(ir_ntracks)))

    # Loop over each track
    logger.info("Looping over each track")
    logger.info((time.ctime()))
    for it in range(0, ir_ntracks):
        logger.info(("Processing track " + str(int(it))))
        logger.info((time.ctime()))

        # Isolate ir statistics about this track
        itbasetime = np.copy(ir_basetime[it, :])
        itdatetimestring = np.copy(ir_datetimestring[it][:][:])
        itcloudnumber = np.copy(ir_cloudnumber[it, :])
        itmergecloudnumber = np.copy(ir_mergecloudnumber[it, :, :])
        itsplitcloudnumber = np.copy(ir_splitcloudnumber[it, :, :])

        statistics_outfile = (
            stats_path
            + "mcs_tracks_"
            + nmqdatasource
            + "_"
            + startdate
            + "_"
            + enddate
            + ".nc"
        )
        # Loop through each time in the track
        irindices = np.array(np.where(itcloudnumber > 0))[0, :]
        logger.info("Looping through each time step")
        logger.info(("Number of time steps: " + str(len(irindices))))
        for itt in irindices:
            logger.info(("Time step #: " + str(itt)))
            # Isolate the data at this time
            radar_basetime[it, itt] = np.array(
                [
                    pd.to_datetime(
                        num2date(
                            itbasetime[itt],
                            units=basetime_units,
                            calendar=basetime_calendar,
                        )
                    )
                ],
                dtype="datetime64[s]",
            )[0]
            ittcloudnumber = np.copy(itcloudnumber[itt])
            ittmergecloudnumber = np.copy(itmergecloudnumber[itt, :])
            ittsplitcloudnumber = np.copy(itsplitcloudnumber[itt, :])
            ittdatetimestring = np.copy(itdatetimestring[itt])
            ittdatetimestring = str(chartostring(ittdatetimestring[:, 0]))
            # import pdb; pdb.set_trace()

            if ittdatetimestring[11:12] == "0":
                # Generate date file names
                ittdatetimestring = "".join(ittdatetimestring)
                cloudid_filename = (
                    cloudidtrack_path + cloudid_filebase + ittdatetimestring + ".nc"
                )
                radar_filename = (
                    pfdata_path
                    + pfdata_filebase
                    + ittdatetimestring[0:8]
                    + "-"
                    + ittdatetimestring[9::]
                    + "00.nc"
                )
                rainaccumulation_filename = (
                    rainaccumulation_path
                    + rainaccumulation_filebase
                    + ittdatetimestring[0:8]
                    + "."
                    + ittdatetimestring[9::]
                    + "00.nc"
                )

                # statistics_outfile = stats_path + 'mcs_tracks_'  + nmqdatasource + '_' + startdate + '_' + enddate + '.nc'

                ########################################################################
                # Load data

                # Load cloudid and precip feature data
                if os.path.isfile(cloudid_filename) and os.path.isfile(radar_filename):
                    logger.info("Data Present")
                    # Load cloudid data
                    logger.info("Loading cloudid data")
                    logger.info(cloudid_filename)
                    cloudiddata = Dataset(cloudid_filename, "r")
                    cloudnumbermap = cloudiddata["cloudnumber"][:]
                    cloudiddata.close()

                    # Read precipitation data
                    logger.info("Loading radar data")
                    logger.info(radar_filename)
                    pfdata = Dataset(radar_filename, "r")
                    rawdbzmap = pfdata["dbz_convsf"][:]  # map of reflectivity
                    rawdbz10map = pfdata["dbz10_height"][:]  # map of 10 dBZ ETHs
                    rawdbz20map = pfdata["dbz20_height"][:]  # map of 20 dBZ ETHs
                    rawdbz30map = pfdata["dbz30_height"][:]  # map of 30 dBZ ETHs
                    rawdbz40map = pfdata["dbz40_height"][:]  # map of 40 dBZ ETHs
                    rawdbz45map = pfdata["dbz45_height"][:]  # map of 45 dBZ ETH
                    rawdbz50map = pfdata["dbz50_height"][:]  # map of 50 dBZ ETHs
                    rawcsamap = pfdata["csa"][
                        :
                    ]  # map of convective, stratiform, anvil categories
                    rawrainratemap = pfdata["rainrate"][:]  # map of rain rate
                    rawpfnumbermap = pfdata["pf_number"][
                        :
                    ]  # map of the precipitation feature number attributed to that pixel
                    rawdataqualitymap = pfdata["mask"][
                        :
                    ]  # map if good (1) and bad (0) data
                    lon = pfdata["lon2d"][:]
                    lat = pfdata["lat2d"][:]
                    pfdata.close()

                    # Load accumulation data is available. If not present fill array with fill value
                    logger.info("Loading accumulation data")
                    logger.info(rainaccumulation_filename)
                    if os.path.isfile(rainaccumulation_filename):
                        rainaccumulationdata = Dataset(rainaccumulation_filename, "r")
                        rawrainaccumulationmap = rainaccumulationdata["precipitation"][
                            :
                        ]
                        rainaccumulationdata.close()

                    else:
                        nt, ny, nx = np.shape(rawdbzmap)
                        rawrainaccumulationmap = (
                            np.ones((nt, ny, nx), dtype=float) * np.nan
                        )

                    ##########################################################################
                    # Get dimensions of data. Data should be preprocesses so that their latittude and longitude dimensions are the same
                    ydim, xdim = np.shape(lat)

                    #########################################################################
                    # Intialize matrices for only MCS data
                    filteredrainratemap = np.ones((ydim, xdim), dtype=float) * np.nan
                    filtereddbzmap = np.ones((ydim, xdim), dtype=float) * np.nan
                    filtereddbz10map = np.ones((ydim, xdim), dtype=float) * np.nan
                    filtereddbz20map = np.ones((ydim, xdim), dtype=float) * np.nan
                    filtereddbz30map = np.ones((ydim, xdim), dtype=float) * np.nan
                    filtereddbz40map = np.ones((ydim, xdim), dtype=float) * np.nan
                    filtereddbz45map = np.ones((ydim, xdim), dtype=float) * np.nan
                    filtereddbz50map = np.ones((ydim, xdim), dtype=float) * np.nan
                    filteredcsamap = np.zeros((ydim, xdim), dtype=int)

                    ############################################################################
                    # Find matching cloud number
                    icloudlocationt, icloudlocationy, icloudlocationx = np.array(
                        np.where(cloudnumbermap == ittcloudnumber)
                    )
                    ncloudpix = len(icloudlocationy)

                    if ncloudpix > 0:
                        logger.info("IR Clouds Present")
                        ######################################################################
                        # Check if any small clouds merge
                        logger.info("Finding mergers")
                        idmergecloudnumber = np.array(
                            np.where(ittmergecloudnumber > 0)
                        )[0, :]
                        nmergecloud = len(idmergecloudnumber)

                        if nmergecloud > 0:
                            # Loop over each merging cloud
                            for imc in idmergecloudnumber:
                                # Find location of the merging cloud
                                (
                                    imergelocationt,
                                    imergelocationy,
                                    imergelocationx,
                                ) = np.array(
                                    np.where(cloudnumbermap == ittmergecloudnumber[imc])
                                )
                                nmergepix = len(imergelocationy)

                                # Add merge pixes to mcs pixels
                                if nmergepix > 0:
                                    icloudlocationt = np.hstack(
                                        (icloudlocationt, imergelocationt)
                                    )
                                    icloudlocationy = np.hstack(
                                        (icloudlocationy, imergelocationy)
                                    )
                                    icloudlocationx = np.hstack(
                                        (icloudlocationx, imergelocationx)
                                    )

                        ######################################################################
                        # Check if any small clouds split
                        logger.info("Finding splits")
                        idsplitcloudnumber = np.array(
                            np.where(ittsplitcloudnumber > 0)
                        )[0, :]
                        nsplitcloud = len(idsplitcloudnumber)

                        if nsplitcloud > 0:
                            # Loop over each merging cloud
                            for imc in idsplitcloudnumber:
                                # Find location of the merging cloud
                                (
                                    isplitlocationt,
                                    isplitlocationy,
                                    isplitlocationx,
                                ) = np.array(
                                    np.where(cloudnumbermap == ittsplitcloudnumber[imc])
                                )
                                nsplitpix = len(isplitlocationy)

                                # Add split pixes to mcs pixels
                                if nsplitpix > 0:
                                    icloudlocationt = np.hstack(
                                        (icloudlocationt, isplitlocationt)
                                    )
                                    icloudlocationy = np.hstack(
                                        (icloudlocationy, isplitlocationy)
                                    )
                                    icloudlocationx = np.hstack(
                                        (icloudlocationx, isplitlocationx)
                                    )

                        ########################################################################
                        # Fill matrices with mcs data
                        logger.info("Fill map with data")
                        filteredrainratemap[icloudlocationy, icloudlocationx] = np.copy(
                            rawrainratemap[
                                icloudlocationt, icloudlocationy, icloudlocationx
                            ]
                        )
                        filtereddbzmap[icloudlocationy, icloudlocationx] = np.copy(
                            rawdbzmap[icloudlocationt, icloudlocationy, icloudlocationx]
                        )
                        filtereddbz10map[icloudlocationy, icloudlocationx] = np.copy(
                            rawdbz10map[
                                icloudlocationt, icloudlocationy, icloudlocationx
                            ]
                        )
                        filtereddbz20map[icloudlocationy, icloudlocationx] = np.copy(
                            rawdbz20map[
                                icloudlocationt, icloudlocationy, icloudlocationx
                            ]
                        )
                        filtereddbz30map[icloudlocationy, icloudlocationx] = np.copy(
                            rawdbz30map[
                                icloudlocationt, icloudlocationy, icloudlocationx
                            ]
                        )
                        filtereddbz40map[icloudlocationy, icloudlocationx] = np.copy(
                            rawdbz40map[
                                icloudlocationt, icloudlocationy, icloudlocationx
                            ]
                        )
                        filtereddbz45map[icloudlocationy, icloudlocationx] = np.copy(
                            rawdbz45map[
                                icloudlocationt, icloudlocationy, icloudlocationx
                            ]
                        )
                        filtereddbz50map[icloudlocationy, icloudlocationx] = np.copy(
                            rawdbz50map[
                                icloudlocationt, icloudlocationy, icloudlocationx
                            ]
                        )
                        filteredcsamap[icloudlocationy, icloudlocationx] = np.copy(
                            rawcsamap[icloudlocationt, icloudlocationy, icloudlocationx]
                        )

                        ########################################################################
                        # isolate small region of cloud data around mcs at this time
                        logger.info("Calculate new shape statistics")

                        # Set edges of boundary
                        miny = np.nanmin(icloudlocationy)
                        if miny <= 10:
                            miny = 0
                        else:
                            miny = miny - 10

                        maxy = np.nanmax(icloudlocationy)
                        if maxy >= ydim - 10:
                            maxy = ydim
                        else:
                            maxy = maxy + 11

                        minx = np.nanmin(icloudlocationx)
                        if minx <= 10:
                            minx = 0
                        else:
                            minx = minx - 10

                        maxx = np.nanmax(icloudlocationx)
                        if maxx >= xdim - 10:
                            maxx = xdim
                        else:
                            maxx = maxx + 11

                        # Isolate smaller region around cloud shield
                        subdbzmap = np.copy(filtereddbzmap[miny:maxy, minx:maxx])
                        subdbz10map = np.copy(filtereddbz10map[miny:maxy, minx:maxx])
                        subdbz20map = np.copy(filtereddbz20map[miny:maxy, minx:maxx])
                        subdbz30map = np.copy(filtereddbz30map[miny:maxy, minx:maxx])
                        subdbz40map = np.copy(filtereddbz40map[miny:maxy, minx:maxx])
                        subdbz50map = np.copy(filtereddbz50map[miny:maxy, minx:maxx])
                        subcsamap = np.copy(filteredcsamap[miny:maxy, minx:maxx])
                        subrainratemap = np.copy(
                            filteredrainratemap[miny:maxy, minx:maxx]
                        )
                        sublat = np.copy(lat[miny:maxy, minx:maxx])
                        sublon = np.copy(lon[miny:maxy, minx:maxx])

                        ########################################################
                        # Get dimensions of subsetted region
                        subdimy, subdimx = np.shape(subdbzmap)

                        ######################################################
                        # Initialize convective map
                        ccflagmap = np.zeros((subdimy, subdimx), dtype=int)

                        #####################################################
                        # Get convective core statistics
                        logger.info("Checking for convective cores")
                        icy, icx = np.array(np.where(subcsamap == 6))
                        nc = len(icy)

                        if nc > 0:
                            # Fill in convective map
                            ccflagmap[icy, icx] = 1

                            # Number convective regions
                            ccnumberlabelmap, ncc = label(ccflagmap)

                            # If convective cores exist calculate statistics
                            if ncc > 0:
                                logger.info(
                                    "Convective cores present, getting statistics"
                                )
                                # Initialize matrices
                                cclon = np.ones(ncc, dtype=float) * np.nan
                                cclat = np.ones(ncc, dtype=float) * np.nan
                                ccnpix = np.ones(ncc, dtype=int) * -9999
                                ccxcentroid = np.ones(ncc, dtype=int) * -9999
                                ccycentroid = np.ones(ncc, dtype=int) * -9999
                                ccxweightedcentroid = np.ones(ncc, dtype=int) * -9999
                                ccyweightedcentroid = np.ones(ncc, dtype=int) * -9999
                                ccmajoraxis = np.ones(ncc, dtype=float) * np.nan
                                ccminoraxis = np.ones(ncc, dtype=float) * np.nan
                                ccaspectratio = np.ones(ncc, dtype=float) * np.nan
                                ccorientation = np.ones(ncc, dtype=float) * np.nan
                                ccperimeter = np.ones(ncc, dtype=float) * np.nan
                                cceccentricity = np.ones(ncc, dtype=float) * np.nan
                                ccmaxdbz10 = np.ones(ncc, dtype=float) * np.nan
                                ccmaxdbz20 = np.ones(ncc, dtype=float) * np.nan
                                ccmaxdbz30 = np.ones(ncc, dtype=float) * np.nan
                                ccmaxdbz40 = np.ones(ncc, dtype=float) * np.nan
                                ccavgdbz10 = np.ones(ncc, dtype=float) * np.nan
                                ccavgdbz20 = np.ones(ncc, dtype=float) * np.nan
                                ccavgdbz30 = np.ones(ncc, dtype=float) * np.nan
                                ccavgdbz40 = np.ones(ncc, dtype=float) * np.nan

                                # Loop over each core
                                logger.info(
                                    (
                                        "Looping through each convective core: "
                                        + str(ncc)
                                    )
                                )
                                for cc in range(1, ncc + 1):
                                    # Isolate core
                                    iiccy, iiccx = np.array(
                                        np.where(ccnumberlabelmap == cc)
                                    )

                                    # Number of pixels in the core
                                    ccnpix[cc - 1] = len(iiccy)

                                    # Get mean latitude and longitude
                                    cclon[cc - 1] = np.nanmean(sublon[iiccy, iiccx])
                                    cclat[cc - 1] = np.nanmean(sublat[iiccy, iiccx])

                                    # Get echo top height statistics
                                    ccmaxdbz10[cc - 1] = np.nanmax(
                                        subdbz10map[iiccy, iiccx]
                                    )
                                    ccmaxdbz10[cc - 1] = np.nanmax(
                                        subdbz10map[iiccy, iiccx]
                                    )
                                    ccmaxdbz20[cc - 1] = np.nanmax(
                                        subdbz20map[iiccy, iiccx]
                                    )
                                    ccmaxdbz30[cc - 1] = np.nanmax(
                                        subdbz30map[iiccy, iiccx]
                                    )
                                    ccmaxdbz40[cc - 1] = np.nanmax(
                                        subdbz40map[iiccy, iiccx]
                                    )
                                    ccavgdbz10[cc - 1] = np.nanmean(
                                        subdbz10map[iiccy, iiccx]
                                    )
                                    ccavgdbz20[cc - 1] = np.nanmean(
                                        subdbz20map[iiccy, iiccx]
                                    )
                                    ccavgdbz30[cc - 1] = np.nanmean(
                                        subdbz30map[iiccy, iiccx]
                                    )
                                    ccavgdbz40[cc - 1] = np.nanmean(
                                        subdbz40map[iiccy, iiccx]
                                    )

                                    # Generate map of convective core
                                    iiccflagmap = np.zeros(
                                        (subdimy, subdimx), dtype=int
                                    )
                                    iiccflagmap[iiccy, iiccx] = 1

                                    # Get core geometric statistics
                                    tsubdbzmap = np.copy(subdbzmap)
                                    tsubdbzmap[np.isnan(subdbzmap)] = -9999
                                    coreproperties = regionprops(
                                        iiccflagmap, intensity_image=tsubdbzmap
                                    )
                                    cceccentricity[cc - 1] = coreproperties[
                                        0
                                    ].eccentricity
                                    ccmajoraxis[cc - 1] = (
                                        coreproperties[0].major_axis_length
                                        * pixel_radius
                                    )
                                    ccminoraxis[cc - 1] = (
                                        coreproperties[0].minor_axis_length
                                        * pixel_radius
                                    )
                                    if ccminoraxis[cc - 1] > 0:
                                        ccaspectratio[cc - 1] = np.divide(
                                            ccmajoraxis[cc - 1], ccminoraxis[cc - 1]
                                        )
                                    ccorientation[cc - 1] = (
                                        coreproperties[0].orientation
                                    ) * (180 / float(pi))
                                    ccperimeter[cc - 1] = (
                                        coreproperties[0].perimeter * pixel_radius
                                    )
                                    (
                                        ccycentroid[cc - 1],
                                        ccxcentroid[cc - 1],
                                    ) = coreproperties[0].centroid
                                    ccycentroid[cc - 1] = ccycentroid[cc - 1] + miny
                                    ccxcentroid[cc - 1] = ccxcentroid[cc - 1] + minx
                                    (
                                        ccyweightedcentroid[cc - 1],
                                        ccxweightedcentroid[cc - 1],
                                    ) = coreproperties[0].weighted_centroid
                                    ccyweightedcentroid[cc - 1] = (
                                        ccyweightedcentroid[cc - 1] + miny
                                    )
                                    ccxweightedcentroid[cc - 1] = (
                                        ccxweightedcentroid[cc - 1] + minx
                                    )

                                ####################################################
                                # Sort based on size, largest to smallest
                                logger.info("Sorting convective cores by size")
                                order = np.argsort(ccnpix)
                                order = order[::-1]

                                scclon = np.copy(cclon[order])
                                scclat = np.copy(cclat[order])
                                sccxcentroid = np.copy(ccxcentroid[order])
                                sccycentroid = np.copy(ccycentroid[order])
                                sccxweightedcentroid = np.copy(
                                    ccxweightedcentroid[order]
                                )
                                sccyweightedcentroid = np.copy(
                                    ccyweightedcentroid[order]
                                )
                                sccnpix = np.copy(ccnpix[order])
                                sccmajoraxis = np.copy(ccmajoraxis[order])
                                sccminoraxis = np.copy(ccminoraxis[order])
                                sccaspectratio = np.copy(ccaspectratio[order])
                                sccorientation = np.copy(ccorientation[order])
                                sccperimeter = np.copy(ccperimeter[order])
                                scceccentricity = np.copy(cceccentricity[order])
                                sccmaxdbz10 = np.copy(ccmaxdbz10[order])
                                sccmaxdbz20 = np.copy(ccmaxdbz20[order])
                                sccmaxdbz30 = np.copy(ccmaxdbz30[order])
                                sccmaxdbz40 = np.copy(ccmaxdbz40[order])
                                sccavgdbz10 = np.copy(ccavgdbz10[order])
                                sccavgdbz20 = np.copy(ccavgdbz20[order])
                                sccavgdbz30 = np.copy(ccavgdbz30[order])
                                sccavgdbz40 = np.copy(ccavgdbz40[order])

                                ###################################################
                                # Save convective core statisitcs
                                radar_ccncores[it, itt] = np.copy(ncc)

                                ncore_save = np.nanmin([nmaxcore, ncc])
                                radar_cclon[it, itt, 0 : ncore_save - 1] = np.copy(
                                    scclon[0 : ncore_save - 1]
                                )
                                radar_cclat[it, itt, 0 : ncore_save - 1] = np.copy(
                                    scclat[0 : ncore_save - 1]
                                )
                                radar_ccxcentroid[
                                    it, itt, 0 : ncore_save - 1
                                ] = np.copy(sccxcentroid[0 : ncore_save - 1])
                                radar_ccycentroid[
                                    it, itt, 0 : ncore_save - 1
                                ] = np.copy(sccycentroid[0 : ncore_save - 1])
                                radar_ccxweightedcentroid[
                                    it, itt, 0 : ncore_save - 1
                                ] = np.copy(sccxweightedcentroid[0 : ncore_save - 1])
                                radar_ccyweightedcentroid[
                                    it, itt, 0 : ncore_save - 1
                                ] = np.copy(sccyweightedcentroid[0 : ncore_save - 1])
                                radar_ccnpix[it, itt, 0 : ncore_save - 1] = np.copy(
                                    sccnpix[0 : ncore_save - 1]
                                )
                                radar_ccmajoraxis[
                                    it, itt, 0 : ncore_save - 1
                                ] = np.copy(sccmajoraxis[0 : ncore_save - 1])
                                radar_ccminoraxis[
                                    it, itt, 0 : ncore_save - 1
                                ] = np.copy(sccminoraxis[0 : ncore_save - 1])
                                radar_ccaspectratio[
                                    it, itt, 0 : ncore_save - 1
                                ] = np.copy(sccaspectratio[0 : ncore_save - 1])
                                radar_ccorientation[
                                    it, itt, 0 : ncore_save - 1
                                ] = np.copy(sccorientation[0 : ncore_save - 1])
                                radar_ccperimeter[
                                    it, itt, 0 : ncore_save - 1
                                ] = np.copy(sccperimeter[0 : ncore_save - 1])
                                radar_cceccentricity[
                                    it, itt, 0 : ncore_save - 1
                                ] = np.copy(scceccentricity[0 : ncore_save - 1])
                                radar_ccmaxdbz10[it, itt, 0 : ncore_save - 1] = np.copy(
                                    sccmaxdbz10[0 : ncore_save - 1]
                                )
                                radar_ccmaxdbz20[it, itt, 0 : ncore_save - 1] = np.copy(
                                    sccmaxdbz20[0 : ncore_save - 1]
                                )
                                radar_ccmaxdbz30[it, itt, 0 : ncore_save - 1] = np.copy(
                                    sccmaxdbz30[0 : ncore_save - 1]
                                )
                                radar_ccmaxdbz40[it, itt, 0 : ncore_save - 1] = np.copy(
                                    sccmaxdbz40[0 : ncore_save - 1]
                                )
                                radar_ccdbz10mean[
                                    it, itt, 0 : ncore_save - 1
                                ] = np.copy(sccavgdbz10[0 : ncore_save - 1])
                                radar_ccdbz20mean[
                                    it, itt, 0 : ncore_save - 1
                                ] = np.copy(sccavgdbz20[0 : ncore_save - 1])
                                radar_ccdbz30mean[
                                    it, itt, 0 : ncore_save - 1
                                ] = np.copy(sccavgdbz30[0 : ncore_save - 1])
                                radar_ccdbz40mean[
                                    it, itt, 0 : ncore_save - 1
                                ] = np.copy(sccavgdbz40[0 : ncore_save - 1])

                            #######################################################
                            # Compute fraction of cloud within NMQ area
                            nmqct = np.shape(
                                np.array(
                                    np.where(
                                        rawdataqualitymap[0, miny:maxy, minx:maxx] == 1
                                    )
                                )
                            )[1]
                            otherct = np.shape(
                                np.array(
                                    np.where(
                                        rawdataqualitymap[0, miny:maxy, minx:maxx] == 0
                                    )
                                )
                            )[1]

                            radar_pffrac[it, itt] = np.divide(nmqct, nmqct + otherct)

                            ######################################################   !!!!!!!!!!!!!!! Slow Step !!!!!!!!1
                            # Derive precipitation feature statistics
                            logger.info("Calculating precipitation statistics")
                            ipfy, ipfx = np.array(
                                np.where(
                                    ((filteredcsamap == 5) | (filteredcsamap == 6))
                                    & (filteredrainratemap > rr_min)
                                )
                            )
                            nrainpix = len(ipfy)

                            if nrainpix > 0:
                                ####################################################
                                # Dilate precipitation feature by one pixel. This slightly smooths the data so that very close precipitation features are connected
                                # Create binary map
                                binarypfmap = np.zeros((ydim, xdim), dtype=int)
                                binarypfmap[ipfy, ipfx] = 1

                                # Dilate (aka smooth)
                                dilationstructure = generate_binary_structure(
                                    2, 1
                                )  # Defines shape of growth. This grows one pixel as a cross

                                dilatedbinarypfmap = binary_dilation(
                                    binarypfmap,
                                    structure=dilationstructure,
                                    iterations=1,
                                ).astype(filteredrainratemap.dtype)

                                # Label precipitation features
                                pfnumberlabelmap, numpf = label(dilatedbinarypfmap)
                                logger.info(numpf)

                                if numpf > 0:
                                    logger.info("PFs present, initializing matrices")

                                    ##############################################
                                    # Initialize matrices
                                    pfnpix = np.zeros(numpf, dtype=float)
                                    pfdbz40npix = np.zeros(numpf, dtype=float)
                                    pfdbz45npix = np.zeros(numpf, dtype=float)
                                    pfdbz50npix = np.zeros(numpf, dtype=float)
                                    pfid = np.ones(numpf, dtype=int) * -9999
                                    pflon = np.ones(numpf, dtype=float) * np.nan
                                    pflat = np.ones(numpf, dtype=float) * np.nan
                                    pfxcentroid = np.ones(numpf, dtype=float) * np.nan
                                    pfycentroid = np.ones(numpf, dtype=float) * np.nan
                                    pfxweightedcentroid = (
                                        np.ones(numpf, dtype=float) * np.nan
                                    )
                                    pfyweightedcentroid = (
                                        np.ones(numpf, dtype=float) * np.nan
                                    )
                                    pfrainrate = np.ones(numpf, dtype=float) * np.nan
                                    pfskewness = np.ones(numpf, dtype=float) * np.nan
                                    pfmajoraxis = np.ones(numpf, dtype=float) * np.nan
                                    pfminoraxis = np.ones(numpf, dtype=float) * np.nan
                                    pfaspectratio = np.ones(numpf, dtype=float) * np.nan
                                    pfeccentricity = (
                                        np.ones(numpf, dtype=float) * np.nan
                                    )
                                    pfperimeter = np.ones(numpf, dtype=float) * np.nan
                                    pforientation = np.ones(numpf, dtype=float) * np.nan

                                    pfccnpix = np.ones(numpf, dtype=int) * -9999
                                    pfccrainrate = np.ones(numpf, dtype=float) * np.nan
                                    pfccdbz10 = np.ones(numpf, dtype=float) * np.nan
                                    pfccdbz20 = np.ones(numpf, dtype=float) * np.nan
                                    pfccdbz30 = np.ones(numpf, dtype=float) * np.nan
                                    pfccdbz40 = np.ones(numpf, dtype=float) * np.nan

                                    pfsfnpix = np.ones(numpf, dtype=int) * -9999
                                    pfsfrainrate = np.ones(numpf, dtype=float) * np.nan

                                    logger.info(
                                        "Looping through each feature to calculate statistics"
                                    )
                                    logger.info(("Number of PFs " + str(numpf)))
                                    ###############################################
                                    # Loop through each feature
                                    for ipf in range(1, numpf + 1):
                                        logger.info(ipf)

                                        #######################################
                                        # Find associated indices
                                        iipfy, iipfx = np.array(
                                            np.where(
                                                ((pfnumberlabelmap == ipf))
                                                & (filteredcsamap >= 5)
                                                & (filteredcsamap <= 6)
                                            )
                                        )
                                        niipfpix = len(iipfy)

                                        if niipfpix > 0:
                                            ##########################################
                                            # Compute statistics

                                            # Basic statistics
                                            pfnpix[ipf - 1] = np.copy(niipfpix)
                                            pfid[ipf - 1] = np.copy(int(ipf))
                                            pfrainrate[ipf - 1] = filteredrainratemap[
                                                iipfy, iipfx
                                            ].mean()
                                            pfskewness[ipf - 1] = skew(
                                                filteredrainratemap[iipfy, iipfx]
                                            )
                                            pflon[ipf - 1] = np.nanmean(
                                                lat[iipfy, iipfx]
                                            )
                                            pflat[ipf - 1] = np.nanmean(
                                                lon[iipfy, iipfx]
                                            )

                                            # Generate map of convective core
                                            iipfflagmap = np.zeros(
                                                (ydim, xdim), dtype=int
                                            )
                                            iipfflagmap[iipfy, iipfx] = 1

                                            # Geometric statistics
                                            tfilteredrainratemap = np.copy(
                                                filteredrainratemap
                                            )
                                            tfilteredrainratemap[
                                                np.isnan(tfilteredrainratemap)
                                            ] = -9999
                                            pfproperties = regionprops(
                                                iipfflagmap,
                                                intensity_image=tfilteredrainratemap,
                                            )
                                            pfeccentricity[ipf - 1] = pfproperties[
                                                0
                                            ].eccentricity
                                            pfmajoraxis[ipf - 1] = (
                                                pfproperties[0].major_axis_length
                                                * pixel_radius
                                            )
                                            # Need to treat minor axis length with an error except since the python algorthim occsionally throws an error.
                                            try:
                                                pfminoraxis[ipf - 1] = (
                                                    pfproperties[0].minor_axis_length
                                                    * pixel_radius
                                                )
                                            except ValueError:
                                                pass
                                            if ~np.isnan(
                                                pfminoraxis[ipf - 1]
                                            ) or ~np.isnan(pfmajoraxis[ipf - 1]):
                                                pfaspectratio[ipf - 1] = np.divide(
                                                    pfmajoraxis[ipf - 1],
                                                    pfminoraxis[ipf - 1],
                                                )
                                            pforientation[ipf - 1] = (
                                                pfproperties[0].orientation
                                            ) * (180 / float(pi))
                                            pfperimeter[ipf - 1] = (
                                                pfproperties[0].perimeter * pixel_radius
                                            )
                                            [
                                                pfycentroid[ipf - 1],
                                                pfxcentroid[ipf - 1],
                                            ] = pfproperties[0].centroid
                                            [
                                                pfyweightedcentroid[ipf - 1],
                                                pfxweightedcentroid[ipf - 1],
                                            ] = pfproperties[0].weighted_centroid

                                            # Convective precipitation statistics
                                            iipfccy, iipfccx = np.array(
                                                np.where(
                                                    (pfnumberlabelmap == ipf)
                                                    & (filteredcsamap == 6)
                                                )
                                            )
                                            niipfcc = len(iipfccy)

                                            if niipfcc > 0:
                                                pfccnpix[ipf - 1] = np.copy(niipfcc)
                                                pfccrainrate[
                                                    ipf - 1
                                                ] = filteredrainratemap[
                                                    iipfccy, iipfccx
                                                ].mean()

                                                pfccdbz10[ipf - 1] = filtereddbz10map[
                                                    iipfccy, iipfccx
                                                ].mean()
                                                pfccdbz20[ipf - 1] = filtereddbz20map[
                                                    iipfccy, iipfccx
                                                ].mean()
                                                pfccdbz30[ipf - 1] = filtereddbz30map[
                                                    iipfccy, iipfccx
                                                ].mean()
                                                pfccdbz40[ipf - 1] = filtereddbz40map[
                                                    iipfccy, iipfccx
                                                ].mean()

                                            # Stratiform precipitation feature statistics
                                            iipfsy, iipfsx = np.array(
                                                np.where(
                                                    (pfnumberlabelmap == ipf)
                                                    & (filteredcsamap == 5)
                                                )
                                            )
                                            niipfs = len(iipfsy)

                                            if niipfs > 0:
                                                pfsfnpix[ipf - 1] = np.copy(niipfs)
                                                pfsfrainrate[ipf - 1] = np.nanmean(
                                                    filteredrainratemap[iipfsy, iipfsx]
                                                )

                                            # Find area exceeding echo top threshold
                                            pfdbz40npix[ipf - 1] = len(
                                                np.array(
                                                    np.where(
                                                        filtereddbz40map[iipfy, iipfx]
                                                        > 0
                                                    )
                                                )[0, :]
                                            )
                                            pfdbz45npix[ipf - 1] = len(
                                                np.array(
                                                    np.where(
                                                        filtereddbz45map[iipfy, iipfx]
                                                        > 0
                                                    )
                                                )[0, :]
                                            )
                                            pfdbz50npix[ipf - 1] = len(
                                                np.array(
                                                    np.where(
                                                        filtereddbz50map[iipfy, iipfx]
                                                        > 0
                                                    )
                                                )[0, :]
                                            )

                                    logger.info("Loop done")
                                    ##############################################################
                                    # Sort precipitation features by size, large to small
                                    logger.info("Sorting PFs by size")
                                    pforder = np.argsort(pfnpix)
                                    pforder = pforder[::-1]

                                    spfnpix = pfnpix[pforder]
                                    spfid = pfid[pforder]
                                    spfrainrate = pfrainrate[pforder]
                                    spfskewness = pfskewness[pforder]
                                    spflon = pflon[pforder]
                                    spflat = pflat[pforder]
                                    spfeccentricity = pfeccentricity[pforder]
                                    spfmajoraxis = pfmajoraxis[pforder]
                                    spfminoraxis = pfminoraxis[pforder]
                                    spfaspectratio = pfaspectratio[pforder]
                                    spforientation = pforientation[pforder]
                                    spfycentroid = pfycentroid[pforder]
                                    spfxcentroid = pfxcentroid[pforder]
                                    spfxweightedcentroid = pfxweightedcentroid[pforder]
                                    spfyweightedcentroid = pfyweightedcentroid[pforder]
                                    spfccnpix = pfccnpix[pforder]
                                    spfccrainrate = pfccrainrate[pforder]
                                    spfccdbz10 = pfccdbz10[pforder]
                                    spfccdbz20 = pfccdbz20[pforder]
                                    spfccdbz30 = pfccdbz30[pforder]
                                    spfccdbz40 = pfccdbz40[pforder]
                                    spfsfnpix = pfsfnpix[pforder]
                                    spfsfrainrate = pfsfrainrate[pforder]
                                    spfdbz40npix = pfdbz40npix[pforder]
                                    spfdbz45npix = pfdbz45npix[pforder]
                                    spfdbz50npix = pfdbz50npix[pforder]

                                    ###################################################
                                    # Save precipitation feature statisitcs
                                    radar_npf[it, itt] = np.copy(numpf)

                                    nradar_save = np.nanmin([nmaxpf, numpf])
                                    radar_pflon[it, itt, 0:nradar_save] = spflon[
                                        0:nradar_save
                                    ]
                                    radar_pflat[it, itt, 0:nradar_save] = spflat[
                                        0:nradar_save
                                    ]
                                    radar_pfnpix[it, itt, 0:nradar_save] = spfnpix[
                                        0:nradar_save
                                    ]
                                    radar_pfrainrate[
                                        it, itt, 0:nradar_save
                                    ] = spfrainrate[0:nradar_save]
                                    radar_pfskewness[
                                        it, itt, 0:nradar_save
                                    ] = spfskewness[0:nradar_save]
                                    radar_pfmajoraxis[
                                        it, itt, 0:nradar_save
                                    ] = spfmajoraxis[0:nradar_save]
                                    radar_pfminoraxis[
                                        it, itt, 0:nradar_save
                                    ] = spfminoraxis[0:nradar_save]
                                    radar_pfaspectratio[
                                        it, itt, 0:nradar_save
                                    ] = spfaspectratio[0:nradar_save]
                                    radar_pforientation[
                                        it, itt, 0:nradar_save
                                    ] = spforientation[0:nradar_save]
                                    radar_pfeccentricity[
                                        it, itt, 0:nradar_save
                                    ] = spfeccentricity[0:nradar_save]
                                    radar_pfdbz40npix[
                                        it, itt, 0:nradar_save
                                    ] = spfdbz40npix[0:nradar_save]
                                    radar_pfdbz45npix[
                                        it, itt, 0:nradar_save
                                    ] = spfdbz45npix[0:nradar_save]
                                    radar_pfdbz50npix[
                                        it, itt, 0:nradar_save
                                    ] = spfdbz50npix[0:nradar_save]

                                    ####################################################
                                    # Average the first two largest precipitation features to represent the cloud system
                                    logger.info(
                                        "Calculating statistics for the few largest convective and stratiform components"
                                    )
                                    radar_ccavgnpix[it, itt] = np.nansum(
                                        spfccnpix[0:nradar_save]
                                    )
                                    if radar_ccavgnpix[it, itt] > 0:
                                        radar_ccavgrainrate[it, itt] = np.nanmean(
                                            spfccrainrate[
                                                np.where(
                                                    ~np.isnan(
                                                        spfccrainrate[0:nradar_save]
                                                    )
                                                )
                                            ]
                                        )
                                        radar_ccavgdbz10[it, itt] = np.nanmean(
                                            spfccdbz10[0:nradar_save]
                                        )
                                        radar_ccavgdbz20[it, itt] = np.nanmean(
                                            spfccdbz20[0:nradar_save]
                                        )
                                        radar_ccavgdbz30[it, itt] = np.nanmean(
                                            spfccdbz30[0:nradar_save]
                                        )
                                        radar_ccavgdbz40[it, itt] = np.nanmean(
                                            spfccdbz40[0:nradar_save]
                                        )

                                    radar_sfavgnpix[it, itt] = np.nansum(
                                        spfsfnpix[0:nradar_save]
                                    )
                                    if radar_sfavgnpix[it, itt] != -9999:
                                        radar_sfavgrainrate[it, itt] = np.nanmean(
                                            spfsfrainrate[
                                                np.where(
                                                    ~np.isnan(
                                                        spfrainrate[0:nradar_save]
                                                    )
                                                )
                                            ]
                                        )

                else:
                    logger.info(
                        (
                            "One or both files do not exist: "
                            + cloudid_filename
                            + ", "
                            + radar_filename
                        )
                    )

            else:
                logger.info(ittdatetimestring)
                logger.info(("Half-hourly data ?!:" + str(ittdatetimestring)))

    ###################################
    # Convert number of pixels to area
    logger.info("Converting pixels to area")
    logger.info((time.ctime()))

    radar_pfdbz40area = np.multiply(radar_pfdbz40npix, np.square(pixel_radius))
    radar_pfdbz45area = np.multiply(radar_pfdbz45npix, np.square(pixel_radius))
    radar_pfdbz50area = np.multiply(radar_pfdbz50npix, np.square(pixel_radius))

    radar_pfarea = np.multiply(radar_pfnpix, np.square(pixel_radius))
    radar_ccavgarea = np.multiply(radar_ccavgnpix, np.square(pixel_radius))
    radar_sfavgarea = np.multiply(radar_sfavgnpix, np.square(pixel_radius))

    radar_ccarea = np.multiply(radar_ccnpix, np.square(pixel_radius))

    ##################################
    # Save output to netCDF file
    logger.info("Saving data")
    logger.info((time.ctime()))

    # Check if file already exists. If exists, delete
    if os.path.isfile(statistics_outfile):
        os.remove(statistics_outfile)

    # import pdb; pdb.set_trace()
    # Definte xarray dataset
    output_data = xr.Dataset(
        {
            "mcs_length": (["track"], np.squeeze(ir_mcslength)),
            "length": (["track"], ir_tracklength),
            "mcs_type": (["track"], ir_mcstype),
            "status": (["track", "time"], ir_status),
            "startstatus": (["track"], ir_startstatus),
            "endstatus": (["track"], ir_endstatus),
            "interruptions": (["track"], ir_trackinterruptions),
            "boundary": (["track"], ir_boundary),
            "basetime": (["track", "time"], radar_basetime),
            "datetimestring": (
                ["track", "time", "characters"],
                ir_datetimestring[:, :, :, 0],
            ),
            "meanlat": (["track", "time"], ir_meanlat),
            "meanlon": (["track", "time"], ir_meanlon),
            "core_area": (["track", "time"], ir_corearea),
            "ccs_area": (["track", "time"], ir_ccsarea),
            "cloudnumber": (["track", "time"], ir_cloudnumber),
            "mergecloudnumber": (["track", "time", "mergesplit"], ir_mergecloudnumber),
            "splitcloudnumber": (["track", "time", "mergesplit"], ir_splitcloudnumber),
            "nmq_frac": (["track", "time"], radar_pffrac),
            "npf": (["track", "time"], radar_npf),
            "pf_area": (["track", "time", "pfs"], radar_pfarea),
            "pf_lon": (["track", "time", "pfs"], radar_pflon),
            "pf_lat": (["track", "time", "pfs"], radar_pflat),
            "pf_rainrate": (["track", "time", "pfs"], radar_pfrainrate),
            "pf_skewness": (["track", "time", "pfs"], radar_pfskewness),
            "pf_majoraxislength": (["track", "time", "pfs"], radar_pfmajoraxis),
            "pf_minoraxislength": (["track", "time", "pfs"], radar_pfminoraxis),
            "pf_aspectratio": (["track", "time", "pfs"], radar_pfaspectratio),
            "pf_eccentricity": (["track", "time", "pfs"], radar_pfeccentricity),
            "pf_orientation": (["track", "time", "pfs"], radar_pforientation),
            "pf_dbz40area": (["track", "time", "pfs"], radar_pfdbz40area),
            "pf_dbz45area": (["track", "time", "pfs"], radar_pfdbz45area),
            "pf_dbz50area": (["track", "time", "pfs"], radar_pfdbz50area),
            "pf_ccrainrate": (["track", "time"], radar_ccavgrainrate),
            "pf_sfrainrate": (["track", "time"], radar_sfavgrainrate),
            "pf_ccarea": (["track", "time"], radar_ccavgarea),
            "pf_sfarea": (["track", "time"], radar_sfavgarea),
            "pf_ccdbz10": (["track", "time"], radar_ccavgdbz10),
            "pf_ccdbz20": (["track", "time"], radar_ccavgdbz20),
            "pf_ccdbz30": (["track", "time"], radar_ccavgdbz30),
            "pf_ccdbz40": (["track", "time"], radar_ccavgdbz40),
            "pf_ncores": (["track", "time"], radar_ccncores),
            "pf_corelon": (["track", "time", "cores"], radar_cclon),
            "pf_corelat": (["track", "time", "cores"], radar_cclat),
            "pf_corearea": (["track", "time", "cores"], radar_ccarea),
            "pf_coremajoraxislength": (["track", "time", "cores"], radar_ccmajoraxis),
            "pf_coreminoraxislength": (["track", "time", "cores"], radar_ccmajoraxis),
            "pf_coreaspectratio": (["track", "time", "cores"], radar_ccaspectratio),
            "pf_coreeccentricity": (["track", "time", "cores"], radar_cceccentricity),
            "pf_coreorientation": (["track", "time", "cores"], radar_ccorientation),
            "pf_coremaxdbz10": (["track", "time", "cores"], radar_ccmaxdbz10),
            "pf_coremaxdbz20": (["track", "time", "cores"], radar_ccmaxdbz20),
            "pf_coremaxdbz30": (["track", "time", "cores"], radar_ccmaxdbz30),
            "pf_coremaxdbz40": (["track", "time", "cores"], radar_ccmaxdbz40),
            "pf_coreavgdbz10": (["track", "time", "cores"], radar_ccdbz10mean),
            "pf_coreavgdbz20": (["track", "time", "cores"], radar_ccdbz20mean),
            "pf_coreavgdbz30": (["track", "time", "cores"], radar_ccdbz30mean),
            "pf_coreavgdbz40": (["track", "time", "cores"], radar_ccdbz40mean),
        },
        coords={
            "track": (["track"], np.arange(1, ir_ntracks + 1)),
            "time": (["time"], np.arange(0, ir_nmaxlength)),
            "pfs": (["pfs"], np.arange(0, nmaxpf)),
            "cores": (["cores"], np.arange(0, nmaxcore)),
            "mergesplit": (
                ["mergesplit"],
                np.arange(0, np.shape(ir_mergecloudnumber)[2]),
            ),
            "characters": (["characters"], np.ones(13) * -9999),
        },
        attrs={
            "title": "File containing ir and precpitation statistics for each track",
            "source1": irdatasource,
            "source2": nmqdatasource,
            "description": datadescription,
            "startdate": startdate,
            "enddate": enddate,
            "time_resolution_hour": str(int(datatimeresolution)),
            "mergdir_pixel_radius": pixel_radius,
            "MCS_IR_area_thresh_km2": str(int(mcs_irareathresh)),
            "MCS_IR_duration_thresh_hr": str(int(mcs_irdurationthresh)),
            "MCS_IR_eccentricity_thres": str(int(mcs_ireccentricitythresh)),
            "max_number_pfs": str(int(nmaxpf)),
            "contact": "Hannah C Barnes: hannah.barnes@pnnl.gov",
            "created_on": time.ctime(time.time()),
        },
    )

    # Specify variable attributes
    output_data.track.attrs["description"] = "Total number of tracked features"
    output_data.track.attrs["units"] = "unitless"

    output_data.time.attrs["dscription"] = "Maximum number of features in a given track"
    output_data.time.attrs["units"] = "unitless"

    output_data.pfs.attrs[
        "long_name"
    ] = "Maximum number of precipitation features in one cloud feature"
    output_data.pfs.attrs["units"] = "unitless"

    output_data.cores.attrs[
        "long_name"
    ] = "Maximum number of convective cores in a precipitation feature at one time"
    output_data.cores.attrs["units"] = "unitless"

    output_data.mergesplit.attrs[
        "long_name"
    ] = "Maximum number of mergers / splits at one time"
    output_data.mergesplit.attrs["units"] = "unitless"

    output_data.length.attrs["long_name"] = "Length of track containing each track"
    output_data.length.attrs["units"] = "Temporal resolution of orginal data"

    output_data.mcs_length.attrs["long_name"] = "Length of each MCS in each track"
    output_data.mcs_length.attrs["units"] = "Temporal resolution of orginal data"

    output_data.mcs_type.attrs["long_name"] = "Type of MCS"
    output_data.mcs_type.attrs["values"] = "1 = MCS, 2 = Squall line"
    output_data.mcs_type.attrs["units"] = "unitless"

    output_data.status.attrs[
        "long_name"
    ] = "Flag indicating the status of each feature in MCS"
    output_data.status.attrs[
        "values"
    ] = "-9999=missing cloud or cloud removed due to short track, 0=track ends here, 1=cloud continues as one cloud in next file, 2=Biggest cloud in merger, 21=Smaller cloud(s) in merger, 13=Cloud that splits, 3=Biggest cloud from a split that stops after the split, 31=Smaller cloud(s) from a split that stop after the split. The last seven classifications are added together in different combinations to describe situations."
    output_data.status.attrs["min_value"] = 0
    output_data.status.attrs["max_value"] = 52
    output_data.status.attrs["units"] = "unitless"

    output_data.startstatus.attrs[
        "long_name"
    ] = "Flag indicating the status of first feature in MCS track"
    output_data.startstatus.attrs[
        "values"
    ] = "-9999=missing cloud or cloud removed due to short track, 0=track ends here, 1=cloud continues as one cloud in next file, 2=Biggest cloud in merger, 21=Smaller cloud(s) in merger, 13=Cloud that splits, 3=Biggest cloud from a split that stops after the split, 31=Smaller cloud(s) from a split that stop after the split. The last seven classifications are added together in different combinations to describe situations."
    output_data.startstatus.attrs["min_value"] = 0
    output_data.startstatus.attrs["max_value"] = 52
    output_data.startstatus.attrs["units"] = "unitless"

    output_data.endstatus.attrs[
        "long_name"
    ] = "Flag indicating the status of last feature in MCS track"
    output_data.endstatus.attrs[
        "values"
    ] = "-9999=missing cloud or cloud removed due to short track, 0=track ends here, 1=cloud continues as one cloud in next file, 2=Biggest cloud in merger, 21=Smaller cloud(s) in merger, 13=Cloud that splits, 3=Biggest cloud from a split that stops after the split, 31=Smaller cloud(s) from a split that stop after the split. The last seven classifications are added together in different combinations to describe situations."
    output_data.endstatus.attrs["min_value"] = 0
    output_data.endstatus.attrs["max_value"] = 52
    output_data.endstatus.attrs["units"] = "unitless"

    output_data.interruptions.attrs["long_name"] = "flag indicating if track incomplete"
    output_data.interruptions.attrs[
        "values"
    ] = "0 = full track available, good data. 1 = track starts at first file, track cut short by data availability. 2 = track ends at last file, track cut short by data availability"
    output_data.interruptions.attrs["min_value"] = 0
    output_data.interruptions.attrs["max_value"] = 2
    output_data.interruptions.attrs["units"] = "unitless"

    output_data.boundary.attrs[
        "long_name"
    ] = "Flag indicating whether the core + cold anvil touches one of the domain edges."
    output_data.boundary.attrs["values"] = "0 = away from edge. 1= touches edge."
    output_data.boundary.attrs["min_value"] = 0
    output_data.boundary.attrs["max_value"] = 1
    output_data.boundary.attrs["units"] = "unitless"

    output_data.basetime.attrs["standard_name"] = "time"
    output_data.basetime.attrs["long_name"] = "basetime of cloud at the given time"

    output_data.datetimestring.attrs["long_name"] = "date-time"
    output_data.datetimestring.attrs[
        "long_name"
    ] = "date_time for each cloud in the mcs"

    output_data.meanlon.attrs["standard_name"] = "longitude"
    output_data.meanlon.attrs[
        "long_name"
    ] = "mean longitude of the core + cold anvil for each feature at the given time"
    output_data.meanlon.attrs["min_value"] = geolimits[1]
    output_data.meanlon.attrs["max_value"] = geolimits[3]
    output_data.meanlon.attrs["units"] = "degrees"

    output_data.meanlat.attrs["standard_name"] = "latitude"
    output_data.meanlat.attrs[
        "long_name"
    ] = "mean latitude of the core + cold anvil for each feature at the given time"
    output_data.meanlat.attrs["min_value"] = geolimits[0]
    output_data.meanlat.attrs["max_value"] = geolimits[2]
    output_data.meanlat.attrs["units"] = "degrees"

    output_data.core_area.attrs["long_name"] = "area of the cold core at the given time"
    output_data.core_area.attrs["units"] = "km^2"

    output_data.ccs_area.attrs[
        "long_name"
    ] = "area of the cold core and cold anvil at the given time"
    output_data.ccs_area.attrs["units"] = "km^2"

    output_data.cloudnumber.attrs[
        "long_name"
    ] = "cloud number in the corresponding cloudid file of clouds in the mcs"
    output_data.cloudnumber.attrs[
        "usage"
    ] = "to link this tracking statistics file with pixel-level cloudid files, use the cloudidfile and cloudnumber together to identify which cloud this current track and time is associated with"
    output_data.cloudnumber.attrs["units"] = "unitless"

    output_data.mergecloudnumber.attrs[
        "long_name"
    ] = "cloud number of small, short-lived clouds merging into the MCS"
    output_data.mergecloudnumber.attrs[
        "usage"
    ] = "to link this tracking statistics file with pixel-level cloudid files, use the cloudidfile and cloudnumber together to identify which cloud this current track and time is associated with"
    output_data.mergecloudnumber.attrs["units"] = "unitless"

    output_data.splitcloudnumber.attrs[
        "long_name"
    ] = "cloud number of small, short-lived clouds splitting from the MCS"
    output_data.splitcloudnumber.attrs[
        "usage"
    ] = "to link this tracking statistics file with pixel-level cloudid files, use the cloudidfile and cloudnumber together to identify which cloud this current track and time is associated with"
    output_data.splitcloudnumber.attrs["units"] = "unitless"

    output_data.nmq_frac.attrs[
        "long_name"
    ] = "fraction of cold cloud shielf covered by NMQ mask"
    output_data.nmq_frac.attrs["units"] = "unitless"
    output_data.nmq_frac.attrs["min_value"] = 0
    output_data.nmq_frac.attrs["max_value"] = 1
    output_data.nmq_frac.attrs["units"] = "unitless"

    output_data.npf.attrs[
        "long_name"
    ] = "number of precipitation features at a given time"
    output_data.npf.attrs["units"] = "unitless"

    output_data.pf_area.attrs[
        "long_name"
    ] = "area of each precipitation feature at a given time"
    output_data.pf_area.attrs["units"] = "km^2"

    output_data.pf_lon.attrs["standard_name"] = "longitude"
    output_data.pf_lon.attrs[
        "long_name"
    ] = "mean longitude of each precipitaiton feature at a given time"
    output_data.pf_lon.attrs["units"] = "degrees"

    output_data.pf_lat.attrs["standard_name"] = "latitude"
    output_data.pf_lat.attrs[
        "long_name"
    ] = "mean latitude of each precipitaiton feature at a given time"
    output_data.pf_lat.attrs["units"] = "degrees"

    output_data.pf_rainrate.attrs[
        "long_name"
    ] = "mean precipitation rate (from rad_hsr_1h) pf each precipitation feature at a given time"
    output_data.pf_rainrate.attrs["units"] = "mm/hr"

    output_data.pf_skewness.attrs[
        "long_name"
    ] = "skewness of each precipitation feature at a given time"
    output_data.pf_skewness.attrs["units"] = "unitless"

    output_data.pf_majoraxislength.attrs[
        "long_name"
    ] = "major axis length of each precipitation feature at a given time"
    output_data.pf_majoraxislength.attrs["units"] = "km"

    output_data.pf_minoraxislength.attrs[
        "long_name"
    ] = "minor axis length of each precipitation feature at a given time"
    output_data.pf_minoraxislength.attrs["units"] = "km"

    output_data.pf_aspectratio.attrs[
        "long_name"
    ] = "aspect ratio (major axis / minor axis) of each precipitation feature at a given time"
    output_data.pf_aspectratio.attrs["units"] = "unitless"

    output_data.pf_eccentricity.attrs[
        "long_name"
    ] = "eccentricity of each precipitation feature at a given time"
    output_data.pf_eccentricity.attrs["min_value"] = 0
    output_data.pf_eccentricity.attrs["max_value"] = 1
    output_data.pf_eccentricity.attrs["units"] = "unitless"

    output_data.pf_orientation.attrs[
        "long_name"
    ] = "orientation of the major axis of each precipitation feature at a given time"
    output_data.pf_orientation.attrs["units"] = "degrees clockwise from vertical"
    output_data.pf_orientation.attrs["min_value"] = 0
    output_data.pf_orientation.attrs["max_value"] = 360

    output_data.pf_dbz40area.attrs[
        "long_name"
    ] = "area of the precipitation feature with column maximum reflectivity >= 40 dBZ at a given time"
    output_data.pf_dbz40area.attrs["units"] = "km^2"

    output_data.pf_dbz45area.attrs[
        "long_name"
    ] = "area of the precipitation feature with column maximum reflectivity >= 45 dBZ at a given time"
    output_data.pf_dbz45area.attrs["units"] = "km^2"

    output_data.pf_dbz50area.attrs[
        "long_name"
    ] = "area of the precipitation feature with column maximum reflectivity >= 50 dBZ at a given time"
    output_data.pf_dbz50area.attrs["units"] = "km^2"

    output_data.pf_ccrainrate.attrs[
        "long_name"
    ] = "mean rain rate of the largest several the convective cores at a given time"
    output_data.pf_ccrainrate.attrs["units"] = "mm/hr"

    output_data.pf_sfrainrate.attrs[
        "long_name"
    ] = "mean rain rate in the largest several statiform regions at a given time"
    output_data.pf_sfrainrate.attrs["units"] = "mm/hr"

    output_data.pf_ccarea.attrs[
        "long_name"
    ] = "total area of the largest several convective cores at a given time"
    output_data.pf_ccarea.attrs["units"] = "km^2"

    output_data.pf_sfarea.attrs[
        "long_name"
    ] = "total area of the largest several stratiform regions at a given time"
    output_data.pf_sfarea.attrs["units"] = "km^2"

    output_data.pf_ccdbz10.attrs[
        "long_name"
    ] = "mean 10 dBZ echo top height of the largest several convective cores at a given time"
    output_data.pf_ccdbz10.attrs["units"] = "km"

    output_data.pf_ccdbz20.attrs[
        "long_name"
    ] = "mean 20 dBZ echo top height of the largest several convective cores at a given time"
    output_data.pf_ccdbz20.attrs["units"] = "km"

    output_data.pf_ccdbz30.attrs[
        "long_name"
    ] = "mean 30 dBZ echo top height the largest several convective cores at a given time"
    output_data.pf_ccdbz30.attrs["units"] = "km"

    output_data.pf_ccdbz40.attrs[
        "long_name"
    ] = "mean 40 dBZ echo top height of the largest several convective cores at a given time"
    output_data.pf_ccdbz40.attrs["units"] = "km"

    output_data.pf_ncores.attrs[
        "long_name"
    ] = "number of convective cores (radar identified) in a precipitation feature at a given time"
    output_data.pf_ncores.attrs["units"] = "unitless"

    output_data.pf_corelon.attrs["standard_name"] = "longitude"
    output_data.pf_corelon.attrs[
        "long_name"
    ] = "mean longitude of each convective core in a precipitation features at the given time"
    output_data.pf_corelon.attrs["units"] = "degrees"

    output_data.pf_coreeccentricity.attrs[
        "long_name"
    ] = "eccentricity of each convective core in the precipitation feature at a given time"
    output_data.pf_coreeccentricity.attrs["min_value"] = 0
    output_data.pf_coreeccentricity.attrs["max_value"] = 1
    output_data.pf_coreeccentricity.attrs["units"] = "unitless"

    output_data.pf_orientation.attrs[
        "long_name"
    ] = "orientation of the major axis of each precipitation feature at a given time"
    output_data.pf_orientation.attrs["units"] = "degrees clockwise from vertical"
    output_data.pf_orientation.attrs["min_value"] = 0
    output_data.pf_orientation.attrs["max_value"] = 360

    output_data.pf_corelat.attrs["standard_name"] = "latitude"
    output_data.pf_corelat.attrs[
        "long_name"
    ] = "mean latitude of each convective core in a precipitation features at the given time"
    output_data.pf_corelat.attrs["units"] = "degrees"

    output_data.pf_corearea.attrs[
        "long_name"
    ] = "area of each convective core in the precipitatation feature at the given time"
    output_data.pf_corearea.attrs["units"] = "km^2"

    output_data.pf_coremajoraxislength.attrs[
        "long_name"
    ] = "major axis length of each convective core in the precipitation feature at a given time"
    output_data.pf_coremajoraxislength.attrs["units"] = "km"

    output_data.pf_coreminoraxislength.attrs[
        "long_name"
    ] = "minor axis length of each convective core in the precipitation feature at a given time"
    output_data.pf_coreminoraxislength.attrs["units"] = "km"

    output_data.pf_coreaspectratio.attrs[
        "long_name"
    ] = "aspect ratio (major / minor axis length) of each convective core in the precipitation feature at a given time"
    output_data.pf_coreaspectratio.attrs["units"] = "unitless"

    output_data.pf_coreeccentricity.attrs[
        "long_name"
    ] = "eccentricity of each convective core in the precipitation feature at a given time"
    output_data.pf_coreeccentricity.attrs["min_value"] = 0
    output_data.pf_coreeccentricity.attrs["max_value"] = 1
    output_data.pf_coreeccentricity.attrs["units"] = "unitless"

    output_data.pf_coreorientation.attrs[
        "long_name"
    ] = "orientation of the major axis of each convective core in the precipitation feature at a given time"
    output_data.pf_coreorientation.attrs["units"] = "degrees clockwise from vertical"
    output_data.pf_coreorientation.attrs["min_value"] = 0
    output_data.pf_coreorientation.attrs["max_value"] = 360

    output_data.pf_coremaxdbz10.attrs[
        "long_name"
    ] = "maximum 10-dBZ echo top height in each convective core in the precipitation features at a given time"
    output_data.pf_coremaxdbz10.attrs["units"] = "km"

    output_data.pf_coremaxdbz20.attrs[
        "long_name"
    ] = "maximum 20-dBZ echo top height in each convective core in the precipitation features at a given time"
    output_data.pf_coremaxdbz20.attrs["units"] = "km"

    output_data.pf_coremaxdbz30.attrs[
        "long_name"
    ] = "maximum 30-dBZ echo top height in each convective core in the precipitation features at a given time"
    output_data.pf_coremaxdbz30.attrs["units"] = "km"

    output_data.pf_coremaxdbz40.attrs[
        "long_name"
    ] = "maximum 40-dBZ echo top height in each convective core in the precipitation features at a given time"
    output_data.pf_coremaxdbz40.attrs["units"] = "km"

    output_data.pf_coreavgdbz10.attrs[
        "long_name"
    ] = "mean 10-dBZ echo top height in each convective core in the precipitation features at a given time"
    output_data.pf_coreavgdbz10.attrs["units"] = "km"

    output_data.pf_coreavgdbz20.attrs[
        "long_name"
    ] = "mean 20-dBZ echo top height in each convective core in the precipitation features at a given time"
    output_data.pf_coreavgdbz20.attrs["units"] = "km"

    output_data.pf_coreavgdbz30.attrs[
        "long_name"
    ] = "mean 30-dBZ echo top height in each convective core in the precipitation features at a given time"
    output_data.pf_coreavgdbz30.attrs["units"] = "km"

    output_data.pf_coreavgdbz40.attrs[
        "long_name"
    ] = "mean 40-dBZ echo top height in each convective core in the precipitation features at a given time"
    output_data.pf_coreavgdbz40.attrs["units"] = "km"

    # Write netcdf file
    logger.info("")
    logger.info(statistics_outfile)
    output_data.to_netcdf(
        path=statistics_outfile,
        mode="w",
        engine="h5netcdf", invalid_netcdf=True,
        format="NETCDF4_CLASSIC",
        unlimited_dims="track",
        encoding={
            "mcs_length": {"dtype": "int", "zlib": True, "_FillValue": -9999},
            "mcs_type": {"dtype": "int", "zlib": True, "_FillValue": -9999},
            "status": {"dtype": "int", "zlib": True, "_FillValue": -9999},
            "startstatus": {"dtype": "int", "zlib": True, "_FillValue": -9999},
            "endstatus": {"dtype": "int", "zlib": True, "_FillValue": -9999},
            "basetime": {"zlib": True, "units": "seconds since 1970-01-01"},
            "datetimestring": {"zlib": True},
            "boundary": {"dtype": "int", "zlib": True, "_FillValue": -9999},
            "interruptions": {"dtype": "int", "zlib": True, "_FillValue": -9999},
            "meanlat": {"zlib": True, "_FillValue": np.nan},
            "meanlon": {"zlib": True, "_FillValue": np.nan},
            "core_area": {"zlib": True, "_FillValue": np.nan},
            "ccs_area": {"zlib": True, "_FillValue": np.nan},
            "cloudnumber": {"dtype": "int", "zlib": True, "_FillValue": -9999},
            "mergecloudnumber": {"dtype": "int", "zlib": True, "_FillValue": -9999},
            "splitcloudnumber": {"dtype": "int", "zlib": True, "_FillValue": -9999},
            "nmq_frac": {"zlib": True, "_FillValue": np.nan},
            "pf_area": {"zlib": True, "_FillValue": np.nan},
            "pf_lon": {"zlib": True, "_FillValue": np.nan},
            "pf_lat": {"zlib": True, "_FillValue": np.nan},
            "pf_rainrate": {"zlib": True, "_FillValue": np.nan},
            "pf_skewness": {"zlib": True, "_FillValue": np.nan},
            "pf_majoraxislength": {"zlib": True, "_FillValue": np.nan},
            "pf_minoraxislength": {"zlib": True, "_FillValue": np.nan},
            "pf_aspectratio": {"zlib": True, "_FillValue": np.nan},
            "pf_orientation": {"zlib": True, "_FillValue": np.nan},
            "pf_eccentricity": {"zlib": True, "_FillValue": np.nan},
            "pf_dbz40area": {"zlib": True, "_FillValue": np.nan},
            "pf_dbz45area": {"zlib": True, "_FillValue": np.nan},
            "pf_dbz50area": {"zlib": True, "_FillValue": np.nan},
            "pf_ccarea": {"zlib": True, "_FillValue": np.nan},
            "pf_sfarea": {"zlib": True, "_FillValue": np.nan},
            "pf_ccrainrate": {"zlib": True, "_FillValue": np.nan},
            "pf_sfrainrate": {"zlib": True, "_FillValue": np.nan},
            "pf_ccdbz10": {"zlib": True, "_FillValue": np.nan},
            "pf_ccdbz20": {"zlib": True, "_FillValue": np.nan},
            "pf_ccdbz30": {"zlib": True, "_FillValue": np.nan},
            "pf_ccdbz40": {"zlib": True, "_FillValue": np.nan},
            "pf_ncores": {"dtype": "int", "zlib": True, "_FillValue": -9999},
            "pf_corelon": {"zlib": True, "_FillValue": np.nan},
            "pf_corelat": {"zlib": True, "_FillValue": np.nan},
            "pf_corearea": {"zlib": True, "_FillValue": np.nan},
            "pf_coremajoraxislength": {"zlib": True, "_FillValue": np.nan},
            "pf_coreminoraxislength": {"zlib": True, "_FillValue": np.nan},
            "pf_coreaspectratio": {"zlib": True, "_FillValue": np.nan},
            "pf_coreorientation": {"zlib": True, "_FillValue": np.nan},
            "pf_coreeccentricity": {"zlib": True, "_FillValue": np.nan},
            "pf_coremaxdbz10": {"zlib": True, "_FillValue": np.nan},
            "pf_coremaxdbz20": {"zlib": True, "_FillValue": np.nan},
            "pf_coremaxdbz30": {"zlib": True, "_FillValue": np.nan},
            "pf_coremaxdbz40": {"zlib": True, "_FillValue": np.nan},
            "pf_coreavgdbz10": {"zlib": True, "_FillValue": np.nan},
            "pf_coreavgdbz20": {"zlib": True, "_FillValue": np.nan},
            "pf_coreavgdbz30": {"zlib": True, "_FillValue": np.nan},
            "pf_coreavgdbz40": {"zlib": True, "_FillValue": np.nan},
        },
    )
