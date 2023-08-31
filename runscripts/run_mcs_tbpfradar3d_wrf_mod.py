import os
import sys
import logging
import dask
from dask.distributed import Client, LocalCluster
from pyflextrkr.ft_utilities import load_config, setup_logging
from pyflextrkr.preprocess_wrf_tb_rainrate_reflectivity import preprocess_wrf
from pyflextrkr.idfeature_driver import idfeature_driver
from pyflextrkr.tracksingle_driver import tracksingle_driver
from pyflextrkr.gettracks import gettracknumbers
from pyflextrkr.trackstats_driver import trackstats_driver
from pyflextrkr.identifymcs import identifymcs_tb
from pyflextrkr.matchtbpf_driver import match_tbpf_tracks
from pyflextrkr.robustmcs_radar import define_robust_mcs_radar
from pyflextrkr.mapfeature_driver import mapfeature_driver
from pyflextrkr.movement_speed import movement_speed


import subprocess
import time
FLUSH_MEM = os.environ.get("FLUSH_MEM")
INVALID_OS_CACHE = os.environ.get("INVALID_OS_CACHE")

def flush_os_cache(logger):
    logger.info(f"Flushing OS Cahces ...")
    command = "sudo /sbin/sysctl vm.drop_caches=3"
    subprocess.call(command, shell=True)

def set_curr_task(task):
    os.environ['CURR_TASK'] = task
    command = "echo running task [$CURR_TASK]"
    command2 = "export CURR_TASK=$CURR_TASK"
    subprocess.call(command, shell=True)
    subprocess.call(command2, shell=True)


if __name__ == '__main__':

    # Set the logging message level
    setup_logging()
    logger = logging.getLogger(__name__)

    # Load configuration file
    config_file = sys.argv[1]
    config = load_config(config_file)

    # Specify track statistics file basename and pixel-level output directory
    # for mapping track numbers to pixel files
    trackstats_filebase = config['trackstats_filebase']  # All Tb tracks
    mcstbstats_filebase = config['mcstbstats_filebase']  # MCS tracks defined by Tb-only
    mcsrobust_filebase = config['mcsrobust_filebase']   # MCS tracks defined by Tb+PF
    mcstbmap_outpath = 'mcstracking_tb'     # Output directory for Tb-only MCS
    alltrackmap_outpath = 'ccstracking'     # Output directory for all Tb tracks

    # Step 0 - Preprocess wrfout files to get Tb, rainrate, reflectivity
    if config['run_preprocess']:
        preprocess_wrf(config)
        if FLUSH_MEM == "TRUE":
            start_time = time.perf_counter()
            flush_os_cache(logger)
            elapsed_time = (time.perf_counter() - start_time) * 1000
            
    ################################################################################################
    # Parallel processing options
    if config['run_parallel'] == 1:
        # Set Dask temporary directory for workers
        dask_tmp_dir = config.get("dask_tmp_dir", "./")
        dask.config.set({'temporary-directory': dask_tmp_dir})
        # Local cluster
        cluster = LocalCluster(n_workers=config['nprocesses'], threads_per_worker=1)
        client = Client(cluster)
        client.run(setup_logging)
    elif config['run_parallel'] == 2:
        # Dask-MPI
        scheduler_file = os.path.join(os.environ["SCRATCH"], "scheduler.json")
        client = Client(scheduler_file=scheduler_file)
        client.run(setup_logging)
    else:
        logger.info(f"Running in serial.")
    
    flush_os_cache_time = []
    
    # Step 1 - Identify features
    if config['run_idfeature']:
        set_curr_task('run_idfeature')
        idfeature_driver(config)
        if FLUSH_MEM == "TRUE":
            start_time = time.perf_counter()
            flush_os_cache(logger)
            flush_os_cache_time.append((time.perf_counter() - start_time) * 1000)
            

    # Step 2 - Link features in time adjacent files
    if config['run_tracksingle']:        
        set_curr_task('run_tracksingle')
        tracksingle_driver(config)
        if FLUSH_MEM == "TRUE":
            start_time = time.perf_counter()
            flush_os_cache(logger)
            flush_os_cache_time.append((time.perf_counter() - start_time) * 1000)

    # Step 3 - Track features through the entire dataset
    if config['run_gettracks']:
        set_curr_task('run_gettracks')
        tracknumbers_filename = gettracknumbers(config)
        if FLUSH_MEM == "TRUE":
            start_time = time.perf_counter()
            flush_os_cache(logger)
            flush_os_cache_time.append((time.perf_counter() - start_time) * 1000)

    # Step 4 - Calculate track statistics
    if config['run_trackstats']:
        set_curr_task('run_trackstats')
        trackstats_filename = trackstats_driver(config)
        if FLUSH_MEM == "TRUE":
            start_time = time.perf_counter()
            flush_os_cache(logger)
            flush_os_cache_time.append((time.perf_counter() - start_time) * 1000)

    # Step 5 - Identify MCS using Tb
    if config['run_identifymcs']:
        set_curr_task('run_identifymcs')
        mcsstats_filename = identifymcs_tb(config)
        if FLUSH_MEM == "TRUE":
            start_time = time.perf_counter()
            flush_os_cache(logger)
            flush_os_cache_time.append((time.perf_counter() - start_time) * 1000)

    # Step 6 - Match PF to MCS
    if config['run_matchpf']:
        set_curr_task('run_matchpf')
        pfstats_filename = match_tbpf_tracks(config)
        if FLUSH_MEM == "TRUE":
            start_time = time.perf_counter()
            flush_os_cache(logger)
            flush_os_cache_time.append((time.perf_counter() - start_time) * 1000)

    # Step 7 - Identify robust MCS
    if config['run_robustmcs']:
        set_curr_task('run_robustmcs')
        robustmcsstats_filename = define_robust_mcs_radar(config)
        if FLUSH_MEM == "TRUE":
            start_time = time.perf_counter()
            flush_os_cache(logger)
            flush_os_cache_time.append((time.perf_counter() - start_time) * 1000)

    # Step 8 - Map tracking to pixel files
    if config['run_mapfeature']:
        # Map robust MCS track numbers to pixel files (default)
        set_curr_task('run_mapfeature')
        mapfeature_driver(config, trackstats_filebase=mcsrobust_filebase)
        # # Map Tb-only MCS track numbers to pixel files (provide outpath_basename keyword)
        # mapfeature_driver(config, trackstats_filebase=mcstbstats_filebase, outpath_basename=mcstbmap_outpath)
        # # Map all Tb track numbers to pixel level files (provide outpath_basename keyword)
        # mapfeature_driver(config, trackstats_filebase=trackstats_filebase, outpath_basename=alltrackmap_outpath)
        if FLUSH_MEM == "TRUE":
            start_time = time.perf_counter()
            flush_os_cache(logger)
            flush_os_cache_time.append((time.perf_counter() - start_time) * 1000)

    # Step 9 - Movement speed calculation
    if config['run_speed']:
        set_curr_task('run_speed')
        movement_speed(config)
        if FLUSH_MEM == "TRUE":
            start_time = time.perf_counter()
            flush_os_cache(logger)
            flush_os_cache_time.append((time.perf_counter() - start_time) * 1000)
    
    if FLUSH_MEM == "TRUE":
        logger.info("OS cache flush overhead : {:.2f} milliseconds".format(sum(flush_os_cache_time)))