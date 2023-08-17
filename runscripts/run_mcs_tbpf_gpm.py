import os
import sys
import logging
import dask
from dask.distributed import Client, LocalCluster
from pyflextrkr.ft_utilities import load_config, setup_logging
from pyflextrkr.idfeature_driver import idfeature_driver
from pyflextrkr.tracksingle_driver import tracksingle_driver
from pyflextrkr.gettracks import gettracknumbers
from pyflextrkr.trackstats_driver import trackstats_driver
from pyflextrkr.identifymcs import identifymcs_tb
from pyflextrkr.matchtbpf_driver import match_tbpf_tracks
from pyflextrkr.robustmcspf import define_robust_mcs_pf
from pyflextrkr.mapfeature_driver import mapfeature_driver
from pyflextrkr.movement_speed import movement_speed

# Added for multi-node parallel processing
from dask_mpi import initialize

# Added for flushing memory
import subprocess
import time
try:
    FLUSH_MEM = os.environ.get("FLUSH_MEM")
    SLURM_JOB_NUM_NODES = os.environ.get("SLURM_JOB_NUM_NODES")
    SLURM_JOB_NUM_NODES = int(SLURM_JOB_NUM_NODES)
    HOSTLIST = os.environ.get("HOSTLIST")
except:
    FLUSH_MEM = "FALSE"
    SLURM_JOB_NUM_NODES = 1
    HOSTLIST = "localhost"


def flush_os_cache(logger):
    logger.info(f"Flushing OS Cahces ...")
    # command = "sudo /sbin/sysctl vm.drop_caches=3"
    if SLURM_JOB_NUM_NODES <= 1:
        command = "sudo /sbin/sysctl vm.drop_caches=3"
    else:
        command = f"srun -n{SLURM_JOB_NUM_NODES} -w {HOSTLIST} --oversubscribe sudo /sbin/sysctl vm.drop_caches=3"
    
    print(f"cmd: {command}")
    subprocess.call(command, shell=True)


def flush_os_cache(logger):
    logger.info(f"Flushing OS Cahces ...")
    # command = "sudo /sbin/sysctl vm.drop_caches=3"
    if SLURM_JOB_NUM_NODES <= 1:
        command = "sudo /sbin/sysctl vm.drop_caches=3"
    else:
        command = f"srun -n{SLURM_JOB_NUM_NODES} -w {HOSTLIST} --oversubscribe sudo /sbin/sysctl vm.drop_caches=3"
    
    print(f"cmd: {command}")
    subprocess.call(command, shell=True)
    
    
if __name__ == '__main__':

    # Set the logging message level
    setup_logging()
    logger = logging.getLogger(__name__)

    # Load configuration file
    config_file = sys.argv[1]
    config = load_config(config_file)
    year = config["startdate"][0:4]
    # Update path names by adding a year
    config["clouddata_path"] = f"{config['clouddata_path']}{year}/"
    config["tracking_outpath"] = f"{config['tracking_outpath']}{year}/"
    os.makedirs(config["tracking_outpath"], exist_ok=True)

    # Specify track statistics file basename and pixel-level output directory
    # for mapping track numbers to pixel files
    trackstats_filebase = config['trackstats_filebase']  # All Tb tracks
    mcstbstats_filebase = config['mcstbstats_filebase']  # MCS tracks defined by Tb-only
    mcsrobust_filebase = config['mcsrobust_filebase']   # MCS tracks defined by Tb+PF
    mcstbmap_outpath = 'mcstracking_tb'     # Output directory for Tb-only MCS
    alltrackmap_outpath = 'ccstracking'     # Output directory for all Tb tracks

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
        initialize(dashboard=False) # ,protocol="ucx",interface="ib0" scheduler_port=9000
        client = Client()
        client.wait_for_workers(timeout=120)
        client.run(setup_logging)
        print("Client scheduler:", client.scheduler)
        
        # # Dask-MPI
        # # Get the scheduler filename from input argument
        # scheduler_file = sys.argv[2]
        # n_workers = int(sys.argv[3])
        # timeout = config.get("timeout", 120)
        # client = Client(scheduler_file=scheduler_file)
        # client.wait_for_workers(n_workers=n_workers, timeout=timeout)
        # client.run(setup_logging)
    else:
        logger.info(f"Running in serial.")
    
    
    flush_os_cache_time = []

    # Step 1 - Identify features
    if config['run_idfeature']:
        idfeature_driver(config)
        if FLUSH_MEM == "TRUE":
            start_time = time.perf_counter()
            flush_os_cache(logger)
            flush_os_cache_time.append((time.perf_counter() - start_time) * 1000)


    # Step 2 - Link features in time adjacent files
    if config['run_tracksingle']:
        tracksingle_driver(config)
        if FLUSH_MEM == "TRUE":
            start_time = time.perf_counter()
            flush_os_cache(logger)
            flush_os_cache_time.append((time.perf_counter() - start_time) * 1000)

    # Step 3 - Track features through the entire dataset
    if config['run_gettracks']:
        tracknumbers_filename = gettracknumbers(config)
        if FLUSH_MEM == "TRUE":
            start_time = time.perf_counter()
            flush_os_cache(logger)
            flush_os_cache_time.append((time.perf_counter() - start_time) * 1000)


    # Step 4 - Calculate track statistics
    if config['run_trackstats']:
        trackstats_filename = trackstats_driver(config)
        if FLUSH_MEM == "TRUE":
            start_time = time.perf_counter()
            flush_os_cache(logger)
            flush_os_cache_time.append((time.perf_counter() - start_time) * 1000)


    # Step 5 - Identify MCS using Tb
    if config['run_identifymcs']:
        mcsstats_filename = identifymcs_tb(config)
        if FLUSH_MEM == "TRUE":
            start_time = time.perf_counter()
            flush_os_cache(logger)
            flush_os_cache_time.append((time.perf_counter() - start_time) * 1000)


    # Step 6 - Match PF to MCS
    if config['run_matchpf']:
        pfstats_filename = match_tbpf_tracks(config)
        if FLUSH_MEM == "TRUE":
            start_time = time.perf_counter()
            flush_os_cache(logger)
            flush_os_cache_time.append((time.perf_counter() - start_time) * 1000)


    # Step 7 - Identify robust MCS
    if config['run_robustmcs']:
        robustmcsstats_filename = define_robust_mcs_pf(config)
        if FLUSH_MEM == "TRUE":
            start_time = time.perf_counter()
            flush_os_cache(logger)
            flush_os_cache_time.append((time.perf_counter() - start_time) * 1000)


    # Step 8 - Map tracking to pixel files
    if config['run_mapfeature']:
        # Map robust MCS track numbers to pixel files (default)
        mapfeature_driver(config, trackstats_filebase=mcsrobust_filebase)
        # Map Tb-only MCS track numbers to pixel files (provide outpath_basename keyword)
        # mapfeature_driver(config, trackstats_filebase=mcstbstats_filebase, outpath_basename=mcstbmap_outpath)
        # Map all Tb track numbers to pixel level files (provide outpath_basename keyword)
        # mapfeature_driver(config, trackstats_filebase, outpath_basename=alltrackmap_outpath)
        if FLUSH_MEM == "TRUE":
            start_time = time.perf_counter()
            flush_os_cache(logger)
            flush_os_cache_time.append((time.perf_counter() - start_time) * 1000)


    # Step 9 - Movement speed calculation
    if config['run_speed']:
        movement_speed(config)
        if FLUSH_MEM == "TRUE":
            start_time = time.perf_counter()
            flush_os_cache(logger)
            flush_os_cache_time.append((time.perf_counter() - start_time) * 1000)

    if FLUSH_MEM == "TRUE":
        logger.info("OS cache flush overhead : {:.2f} milliseconds".format(sum(flush_os_cache_time)))
        
    client.close()
