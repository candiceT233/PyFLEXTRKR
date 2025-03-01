import numpy as np
import os
import sys
import xarray as xr
import pandas as pd
import time
import logging

def trackclouds(
        cloudid_filepairs,
        cloudid_basetimepairs,
        config,
):
    """
    Track clouds in successive pairs of cloudid files.

    Arguments:
        cloudid_filepairs: tuple
            Cloudid filename pairs
        cloudid_basetimepairs: tuple
            Cloudid basetime pairs
        config: dictionary
            Dictionary containing config parameters

    Returns:
        track_outfile: string
            Track file name.
    """

    logger = logging.getLogger(__name__)

    # Separate inputs
    firstcloudidfilename, secondcloudidfilename = cloudid_filepairs[0], cloudid_filepairs[1]
    firstbasetime, secondbasetime = cloudid_basetimepairs[0], cloudid_basetimepairs[1]
    firstdatestring = pd.to_datetime(firstbasetime, unit="s").strftime("%Y%m%d")
    firsttimestring = pd.to_datetime(firstbasetime, unit="s").strftime("%H%M")
    seconddatestring = pd.to_datetime(secondbasetime, unit="s").strftime("%Y%m%d")
    secondtimestring = pd.to_datetime(secondbasetime, unit="s").strftime("%H%M")
    dataoutpath = config["tracking_outpath"]
    feature_varname = config.get("feature_varname", "feature_number")
    nfeature_varname = config.get("nfeature_varname", "nfeatures")
    timegap = config["timegap"]
    nmaxlinks = config["nmaxlinks"]
    othresh = config["othresh"]
    fillval = config["fillval"]

    # firstcloudidfilename = zipped_inputs[0]
    logger.debug(("firstcloudidfilename: ", firstcloudidfilename))
    # secondcloudidfilename = zipped_inputs[1]
    logger.debug(("secondcloudidfilename: ", secondcloudidfilename))

    ########################################################
    # Set constants
    # Version information
    # outfilebase = "track" + track_version + "_"
    outfilebase = "track_"
    ########################################################
    # Isolate new and reference file and base times
    new_file = secondcloudidfilename
    new_datestring = seconddatestring
    new_timestring = secondtimestring
    new_basetime = secondbasetime
    logger.debug(f"new basetime: {new_basetime}")
    new_filedatetime = str(new_datestring) + "_" + str(new_timestring)

    reference_file = firstcloudidfilename
    reference_datestring = firstdatestring
    reference_timestring = firsttimestring
    reference_basetime = firstbasetime
    logger.debug(f"ref basetime: {reference_basetime}")
    reference_filedatetime = str(reference_datestring) + "_" + str(reference_timestring)

    # Check that new and reference files differ by less than timegap in hours.
    # Use base time (which is the seconds since 01-Jan-1970 00:00:00).
    # Divide base time difference between the files by 3600 to get difference in hours
    hour_diff = (np.subtract(new_basetime, reference_basetime)) / float(3600)
    if hour_diff < timegap and hour_diff > 0:
        logger.debug("Linking:")

        ##############################################################
        # Load cloudid file from before, called reference file
        logger.debug(reference_filedatetime)

        # Open file
        reference_data = xr.open_dataset(reference_file, mask_and_scale=False)
        reference_convcold_cloudnumber = reference_data[feature_varname].data
        nreference = reference_data[nfeature_varname].data
        reference_data.close()

        ##########################################################
        # Load next cloudid file, called new file
        logger.debug(f"new_filedattime: {new_filedatetime}")

        # Open file
        new_data = xr.open_dataset(new_file, mask_and_scale=False)
        new_convcold_cloudnumber = new_data[feature_varname].data
        nnew = new_data[nfeature_varname].data
        new_data.close()

        # Convert float type to int, missing value to 0
        # This should not be needed when setting mask_and_scale=False
        reference_convcold_cloudnumber[np.isnan(reference_convcold_cloudnumber)] = 0
        reference_convcold_cloudnumber = reference_convcold_cloudnumber.astype("int")
        new_convcold_cloudnumber[np.isnan(new_convcold_cloudnumber)] = 0
        new_convcold_cloudnumber = new_convcold_cloudnumber.astype("int")

        ############################################################
        # Get size of data
        times, ny, nx = np.shape(new_convcold_cloudnumber)

        # Add 1 to nclouds for both reference and new cloudid files to account for files that have 0 clouds
        nreference = nreference + 1
        nnew = nnew + 1

        #######################################################
        # Initialize matrices
        reference_forward_index = (
            np.ones((1, int(nreference), int(nmaxlinks)), dtype=int) * fillval
        )
        reference_forward_size = (
            np.ones((1, int(nreference), int(nmaxlinks)), dtype=int) * fillval
        )
        new_backward_index = (
            np.ones((1, int(nnew), int(nmaxlinks)), dtype=int) * fillval
        )
        new_backward_size = np.ones((1, int(nnew), int(nmaxlinks)), dtype=int) * fillval

        ######################################################
        # Loop through each cloud / feature in reference time and look for overlaping clouds / features in the new file
        for refindex in np.arange(1, nreference + 1):
            # Locate where the cloud in the reference file overlaps with any cloud in the new file
            forward_matchindices = np.where(
                (reference_convcold_cloudnumber == refindex)
                & (new_convcold_cloudnumber != 0)
            )

            # Get the convcold_cloudnumber of the clouds in the new file that overlap the cloud in the reference file
            forward_newindex = new_convcold_cloudnumber[forward_matchindices]
            unique_forwardnewindex = np.unique(forward_newindex)

            # Calculate size of reference cloud in terms of number of pixels
            sizeref = len(
                np.extract(
                    reference_convcold_cloudnumber == refindex,
                    reference_convcold_cloudnumber,
                )
            )

            # Loop through the overlapping clouds in the new file, determining if they statisfy the overlap requirement
            forward_nmatch = 0  # Initialize overlap counter
            for matchindex in unique_forwardnewindex:
                sizematch = len(
                    np.extract(forward_newindex == matchindex, forward_newindex)
                )

                if sizematch / float(sizeref) > othresh:
                    if forward_nmatch > nmaxlinks:
                        logger.debug(
                            ("reference: " + number_filepath + files[ifile - 1])
                        )
                        logger.debug(("new: " + number_filepath + files[ifile]))
                        sys.exit(
                            "More than "
                            + str(int(nmaxlinks))
                            + " clouds in new file match with reference cloud?!"
                        )
                    else:
                        reference_forward_index[
                            0, int(refindex) - 1, forward_nmatch
                        ] = matchindex
                        reference_forward_size[
                            0, int(refindex) - 1, forward_nmatch
                        ] = len(
                            np.extract(
                                new_convcold_cloudnumber == matchindex,
                                new_convcold_cloudnumber,
                            )
                        )

                        forward_nmatch = forward_nmatch + 1

        ######################################################
        # Loop through each cloud / feature at new time and look for overlaping clouds / features in the reference file
        for newindex in np.arange(1, nnew + 1):
            # Locate where the cloud in the new file overlaps with any cloud in the reference file
            backward_matchindices = np.where(
                (new_convcold_cloudnumber == newindex)
                & (reference_convcold_cloudnumber != 0)
            )

            # Get the convcold_cloudnumber of the clouds in the reference file that overlap the cloud in the new file
            backward_refindex = reference_convcold_cloudnumber[backward_matchindices]
            unique_backwardrefindex = np.unique(backward_refindex)

            # Calculate size of reference cloud in terms of number of pixels
            sizenew = len(
                np.extract(
                    new_convcold_cloudnumber == newindex, new_convcold_cloudnumber
                )
            )

            # Loop through the overlapping clouds in the new file, determining if they statisfy the overlap requirement
            backward_nmatch = 0  # Initialize overlap counter
            for matchindex in unique_backwardrefindex:
                sizematch = len(
                    np.extract(backward_refindex == matchindex, backward_refindex)
                )

                if sizematch / float(sizenew) > othresh:
                    if backward_nmatch > nmaxlinks:
                        logger.debug(
                            ("reference: " + number_filepath + files[ifile - 1])
                        )
                        logger.debug(("new: " + number_filepath + files[ifile]))
                        sys.exit(
                            "More than "
                            + str(int(nmaxlinks))
                            + " clouds in reference file match with new cloud?!"
                        )
                    else:
                        new_backward_index[
                            0, int(newindex) - 1, backward_nmatch
                        ] = matchindex
                        new_backward_size[0, int(newindex) - 1, backward_nmatch] = len(
                            np.extract(
                                reference_convcold_cloudnumber == matchindex,
                                reference_convcold_cloudnumber,
                            )
                        )

                        backward_nmatch = backward_nmatch + 1

        #########################################################
        # Save forward and backward indices and linked sizes in netcdf file

        # create filename
        track_outfile = dataoutpath + outfilebase + new_filedatetime + ".nc"

        # Check if file already exists. If exists, delete
        if os.path.isfile(track_outfile):
            os.remove(track_outfile)

        logger.debug("Writing single tracks")

        # Define output variables dictionary
        varlist = {
            "basetime_new": (
                ["time"],
                np.array(
                    [pd.to_datetime(new_data["base_time"].data, unit="s")],
                    dtype="datetime64[s]",
                )[0],
            ),
            "basetime_ref": (
                ["time"],
                np.array(
                    [pd.to_datetime(reference_data["base_time"].data, unit="s")],
                    dtype="datetime64[s]",
                )[0],
            ),
            "newcloud_backward_index": (
                ["time", "nclouds_new", "nlinks"],
                new_backward_index,
            ),
            "newcloud_backward_size": (
                ["time", "nclouds_new", "nlinks"],
                new_backward_size,
            ),
            "refcloud_forward_index": (
                ["time", "nclouds_ref", "nlinks"],
                reference_forward_index,
            ),
            "refcloud_forward_size": (
                ["time", "nclouds_ref", "nlinks"],
                reference_forward_size,
            ),
        }
        coordlist = {
            "time": (["time"], np.arange(0, 1)),
            "nclouds_new": (["nclouds_new"], np.arange(0, nnew)),
            "nclouds_ref": (["nclouds_ref"], np.arange(0, nreference)),
            "nlinks": (["nlinks"], np.arange(0, nmaxlinks)),
        }
        gattrlist = {
            "title": "Indices linking clouds in two consecutive files forward and backward in time and the size of the linked cloud",
            "Conventions": "CF-1.6",
            "Institution": "Pacific Northwest National Laboratoy",
            "Contact": "Zhe Feng, zhe.feng@pnnl.gov",
            "Created_on": time.ctime(time.time()),
            "new_date": new_filedatetime,
            "ref_date": reference_filedatetime,
            "new_file": new_file,
            "ref_file": reference_file,
            # "tracking_version_number": track_version,
            "overlap_threshold": str(int(othresh * 100)) + "%",
            "maximum_gap_allowed": str(timegap) + " hr",
        }
        # Define xarray dataset
        output_data = xr.Dataset(varlist, coords=coordlist, attrs=gattrlist)

        # Specify variable attributes
        output_data.nclouds_new.attrs["long_name"] = "number of cloud in new file"
        output_data.nclouds_new.attrs["units"] = "unitless"

        output_data.nclouds_ref.attrs["long_name"] = "number of cloud in reference file"
        output_data.nclouds_ref.attrs["units"] = "unitless"

        output_data.nlinks.attrs[
            "long_name"
        ] = "maximum number of clouds that can be linked to a given cloud"
        output_data.nlinks.attrs["units"] = "unitless"

        output_data.basetime_new.attrs[
            "long_name"
        ] = "epoch time (seconds since 01/01/1970 00:00) of new file"
        output_data.basetime_new.attrs["standard_name"] = "time"

        output_data.basetime_ref.attrs[
            "long_name"
        ] = "epoch time (seconds since 01/01/1970 00:00) of reference file"
        output_data.basetime_ref.attrs["standard_name"] = "time"

        output_data.newcloud_backward_index.attrs["long_name"] = "reference cloud index"
        output_data.newcloud_backward_index.attrs[
            "usage"
        ] = "each row represents a cloud in the new file and the numbers in that row provide all reference cloud indices linked to that new cloud"
        output_data.newcloud_backward_index.attrs["units"] = "unitless"
        output_data.newcloud_backward_index.attrs["valid_min"] = 1
        output_data.newcloud_backward_index.attrs["valid_max"] = nreference

        output_data.refcloud_forward_index.attrs["long_name"] = "new cloud index"
        output_data.refcloud_forward_index.attrs[
            "usage"
        ] = "each row represents a cloud in the reference file and the numbers provide all new cloud indices linked to that reference cloud"
        output_data.refcloud_forward_index.attrs["units"] = "unitless"
        output_data.refcloud_forward_index.attrs["valid_min"] = 1
        output_data.refcloud_forward_index.attrs["valid_max"] = nnew

        output_data.newcloud_backward_size.attrs["long_name"] = "reference cloud area"
        output_data.newcloud_backward_size.attrs[
            "usage"
        ] = "each row represents a cloud in the new file and the numbers provide the area of all reference clouds linked to that new cloud"
        output_data.newcloud_backward_size.attrs["units"] = "km^2"

        output_data.refcloud_forward_size.attrs["long_name"] = "new cloud area"
        output_data.refcloud_forward_size.attrs[
            "usage"
        ] = "each row represents a cloud in the reference file and the numbers provide the area of all new clouds linked to that reference cloud"
        output_data.refcloud_forward_size.attrs["units"] = "km^2"

        # Write netcdf files
        output_data.to_netcdf(
            path=track_outfile,
            mode="w",
            engine="h5netcdf", invalid_netcdf=True, 
            format="NETCDF4_CLASSIC",
            unlimited_dims="time",
            encoding={
                "basetime_new": {
                    "dtype": "int64",
                    "zlib": True,
                    "units": "seconds since 1970-01-01",
                },
                "basetime_ref": {
                    "dtype": "int64",
                    "zlib": True,
                    "units": "seconds since 1970-01-01",
                },
                "newcloud_backward_index": {
                    "dtype": "int",
                    "zlib": True,
                    "_FillValue": fillval,
                },
                "newcloud_backward_size": {
                    "dtype": "int",
                    "zlib": True,
                    "_FillValue": fillval,
                },
                "refcloud_forward_index": {
                    "dtype": "int",
                    "zlib": True,
                    "_FillValue": fillval,
                },
                "refcloud_forward_size": {
                    "dtype": "int",
                    "zlib": True,
                    "_FillValue": fillval,
                },
            },
        )
        logger.info(track_outfile)
        return track_outfile