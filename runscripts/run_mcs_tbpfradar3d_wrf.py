import os, pathlib
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

# from dask_jobqueue import SLURMCluster
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

    if SLURM_JOB_NUM_NODES <= 1:
        command = "sudo /sbin/sysctl vm.drop_caches=3"
    else:
        command = f"srun -n{SLURM_JOB_NUM_NODES} -w {HOSTLIST} --oversubscribe sudo /sbin/sysctl vm.drop_caches=3"
    
    print(f"cmd: {command}")

    subprocess.call(command, shell=True)

def set_curr_task(logger,task):
    os.environ['CURR_TASK'] = task
    command = "export CURR_TASK=$CURR_TASK"
    subprocess.run(command, shell=True)
    logger.info(f"Running task: {task}")

def set_curr_task_file(logger,task):
    
    workflow_name = os.environ.get("WORKFLOW_NAME")
    path_for_task_files = os.environ.get("PATH_FOR_TASK_FILES")
    vfd_task_file = None
    vol_task_file = None
    
    if workflow_name and path_for_task_files:
        vfd_task_file = os.path.join(path_for_task_files, f"{workflow_name}_vfd.curr_task")
        vol_task_file = os.path.join(path_for_task_files, f"{workflow_name}_vol.curr_task")

        # Make path for task files if it does not exist
        pathlib.Path(path_for_task_files).mkdir(parents=True, exist_ok=True)

        with open(vfd_task_file, "w") as file:
            file.write(task)
        logger.info(f"Overwrote: {vfd_task_file} with {task}")

        with open(vol_task_file, "w") as file:
            file.write(task)
        logger.info(f"Overwrote: {vol_task_file} with {task}")
    else:
        logger.info("Invalid or missing WORKFLOW_NAME, PATH_FOR_TASK_FILES environment variable.")    

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
        mem_limit = 1048576 * 1024 * 8 # 32 GiB
        # initialize(dashboard=False,memory_limit=mem_limit) # ,protocol="ucx",interface="ib0" scheduler_port=9000
        initialize(dashboard=False)

        client = Client()
        client.run(setup_logging)
        print("Client scheduler:", client.scheduler)
        # Dask-MPI
        # scheduler_file = os.path.join(os.environ["SCRATCH"], "scheduler.json")
        # client = Client(scheduler_file=scheduler_file)
        # client.run(setup_logging)

        
    else:
        client = None
        logger.info(f"Running in serial.")
    
    
    flush_os_cache_time = []


            
    # Step 1 - Identify features
    if config['run_idfeature']:
        # os.environ['CURR_TASK'] = 'run_idfeature'
        set_curr_task(logger, 'run_idfeature')
        idfeature_driver(config)
        if FLUSH_MEM == "TRUE":
            start_time = time.perf_counter()
            flush_os_cache(logger)
            flush_os_cache_time.append((time.perf_counter() - start_time) * 1000)
            

    # Step 2 - Link features in time adjacent files
    if config['run_tracksingle']:        
        # os.environ['CURR_TASK'] = 'run_tracksingle'
        set_curr_task(logger, 'run_tracksingle')
        tracksingle_driver(config)
        if FLUSH_MEM == "TRUE":
            start_time = time.perf_counter()
            flush_os_cache(logger)
            flush_os_cache_time.append((time.perf_counter() - start_time) * 1000)

    # Step 3 - Track features through the entire dataset
    if config['run_gettracks']:
        # os.environ['CURR_TASK'] = 'run_gettracks'
        set_curr_task(logger, 'run_gettracks')
        tracknumbers_filename = gettracknumbers(config)
        if FLUSH_MEM == "TRUE":
            start_time = time.perf_counter()
            flush_os_cache(logger)
            flush_os_cache_time.append((time.perf_counter() - start_time) * 1000)

    # Step 4 - Calculate track statistics
    if config['run_trackstats']:
        # os.environ['CURR_TASK'] = 'run_trackstats'
        set_curr_task(logger, 'run_trackstats')
        trackstats_filename = trackstats_driver(config)
        if FLUSH_MEM == "TRUE":
            start_time = time.perf_counter()
            flush_os_cache(logger)
            flush_os_cache_time.append((time.perf_counter() - start_time) * 1000)

    # Step 5 - Identify MCS using Tb
    if config['run_identifymcs']:
        # os.environ['CURR_TASK'] = 'run_identifymcs'
        set_curr_task(logger, 'run_identifymcs')
        mcsstats_filename = identifymcs_tb(config)
        if FLUSH_MEM == "TRUE":
            start_time = time.perf_counter()
            flush_os_cache(logger)
            flush_os_cache_time.append((time.perf_counter() - start_time) * 1000)

    # Step 6 - Match PF to MCS
    if config['run_matchpf']:
        # os.environ['CURR_TASK'] = 'run_matchpf'    
        set_curr_task(logger, 'run_matchpf')
        pfstats_filename = match_tbpf_tracks(config)
        if FLUSH_MEM == "TRUE":
            start_time = time.perf_counter()
            flush_os_cache(logger)
            flush_os_cache_time.append((time.perf_counter() - start_time) * 1000)

    # Step 7 - Identify robust MCS
    if config['run_robustmcs']:
        # os.environ['CURR_TASK'] = 'run_robustmcs'
        set_curr_task(logger, 'run_robustmcs')
        robustmcsstats_filename = define_robust_mcs_radar(config)
        if FLUSH_MEM == "TRUE":
            start_time = time.perf_counter()
            flush_os_cache(logger)
            flush_os_cache_time.append((time.perf_counter() - start_time) * 1000)

    # Step 8 - Map tracking to pixel files
    if config['run_mapfeature']:
        # Map robust MCS track numbers to pixel files (default)
        # os.environ['CURR_TASK'] =  'run_mapfeature'
        set_curr_task(logger, 'run_mapfeature')
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
        # os.environ['CURR_TASK'] = 'run_speed'
        set_curr_task(logger, 'run_speed')
        movement_speed(config)
        if FLUSH_MEM == "TRUE":
            start_time = time.perf_counter()
            flush_os_cache(logger)
            flush_os_cache_time.append((time.perf_counter() - start_time) * 1000)
    
    if FLUSH_MEM == "TRUE":
        logger.info("OS cache flush overhead : {:.2f} milliseconds".format(sum(flush_os_cache_time)))
    
    if client:
        client.close()