#!/bin/bash
#SBATCH --job-name=demo_wrf_tbradar_0
#SBATCH --partition=short
#SBATCH --time=01:30:00
#SBATCH -N 2
#SBATCH --ntasks=80
#SBATCH --ntasks-per-node=40
#SBATCH --output=./R_%x.out
#SBATCH --error=./R_%x.err
# --cpus-per-task=4
# --mem=8G

###############################################################################################
# This script demonstrates running cell tracking on NEXRAD data (KHGX)
# To run this demo script:
# 1. Modify the dir_demo to a directory on your computer to download the sample data
# 2. Run the script: bash demo_cell_nexrad.sh
# 
# By default the demo config uses 4 processors for parallel processing, 
#    assuming most computers have at least 4 CPU cores. 
#    If your computer has more than 4 processors, you may modify 'nprocesses' 
#    in config_nexrad500m_example.yml to reduce the run time.
###############################################################################################

# export SCRATCH=/scratch/tang584
# export SCRATCH=.
# export PMIX_MCA_gds=hash

# # Specify directory for the demo data
# dir_demo='/qfs/people/tang584/scripts/PyFLEXTRKR/hm_nexrad_demo' 
# # Example config file name
# config_demo='config_nexrad_cell_demo.yml'
# # Demo input data directory
# dir_input='/qfs/people/tang584/scripts/PyFLEXTRKR/input_data/nexrad_reflectivity1' #data downloaded

## Prepare Test Directories
TEST_NAME='wrf_tbradar'
# FS_PREFIX="/qfs/projects/oddite/$USER" # NFS
FS_PREFIX="/rcfs/projects/chess/$USER" # PFS

# Specify directory for the demo data
dir_demo="${FS_PREFIX}/flextrkr_runs/${TEST_NAME}" # NFS
mkdir -p $dir_demo
rm -rf $dir_demo/*
# Example config file name
# config_example='config_wrf_mcs_tbradar_example.yml'
config_example='config_wrf_mcs_tbradar_short.yml'
# config_example='config_wrf_mcs_tbradar_seq.yml'
config_demo='config_wrf_mcs_tbradar_demo.yml'
cp ./$config_demo $dir_demo
# Demo input data directory
dir_input="${FS_PREFIX}/flextrkr_runs/input_data/${TEST_NAME}"


# dir_script="/people/tang584/scripts/PyFLEXTRKR"

## Prepare Slurm Host Names and IPs
NODE_NAMES=`echo $SLURM_JOB_NODELIST|scontrol show hostnames`

hostlist=$(echo "$NODE_NAMES" | tr '\n' ',')
echo "hostlist: $hostlist"
export HOSTLIST=$hostlist


PREPARE_CONFIG () {

    # Add '\' to each '/' in directory names
    dir_raw1=$(echo ${dir_input} | sed 's_/_\\/_g')
    dir_input1=$(echo ${dir_input} | sed 's_/_\\/_g')
    dir_demo1=$(echo ${dir_demo} | sed 's_/_\\/_g')
    # Replace input directory names in example config file
    sed 's/INPUT_DIR/'${dir_input1}'/g;s/TRACK_DIR/'${dir_demo1}'/g;s/RAW_DATA/'${dir_raw1}'/g' ${config_example} > ${config_demo}
    # sed 's/INPUT_DIR/'${dir_input1}'/g;s/TRACK_DIR/'${dir_demo1}'/g' ${config_example} > ${config_demo}
    echo 'Created new config file: '${config_demo}
}

RUN_TRACKING () {
    # Creating a scheduler file
    rm -rf $SCRATCH/scheduler.json
    set -x 
    # mpirun -np 4 -host $hostlist dask-mpi --scheduler-file $SCRATCH/scheduler.json &

    # Run tracking
    echo 'Running PyFLEXTRKR ...'
    # mpirun --host $hostlist --npernode 2 python ../runscripts/run_mcs_tbpfradar3d_wrf.py ${config_demo} &> ${FUNCNAME[0]}-demo.log
    
    if [[ $SLURM_JOB_NUM_NODES -gt 1 ]]; then
        srun -n$SLURM_NTASKS -w $hostlist python ../runscripts/run_mcs_tbpfradar3d_wrf.py ${config_demo} &> ${FUNCNAME[0]}-demo.log
        # srun ${SLURM_NTASKS} --ntasks-per-node=${SLURM_NTASKS_PER_NODE} -w $hostlist python ../runscripts/run_mcs_tbpfradar3d_wrf.py ${config_demo} &> ${FUNCNAME[0]}-demo.log
    else
        python ../runscripts/run_mcs_tbpfradar3d_wrf.py ${config_demo} &> ${FUNCNAME[0]}-demo.log
    fi

    # srun -n ${SLURM_NTASKS} --ntasks-per-node=${SLURM_NTASKS_PER_NODE} python ../runscripts/run_mcs_tbpfradar3d_wrf.py ${config_demo} &> ${FUNCNAME[0]}-demo.log

    #--scheduler-file /scratch/tang584/scheduler.json 
    
    #&> ${FUNCNAME[0]}-demo.log
    
    SCRATCH

    echo 'Tracking is done.'

}

MAKE_QUICKLOOK_PLOTS () {
    # Make quicklook plots
    echo 'Making quicklook plots ...'
    quicklook_dir=${dir_demo}'/quicklooks_trackpaths/'
    python ../Analysis/plot_subset_tbze_mcs_tracks_demo.py -s '2015-05-06T00' -e '2015-05-10T00' \
        -c ${config_demo} -o horizontal -p 1 --figsize 8 12 --output ${quicklook_dir}
    echo 'View quicklook plots here: '${quicklook_dir}
}

MAKE_ANIMATION () {
    # Make animation using ffmpeg
    echo 'Making animations from quicklook plots ...'
    ffmpeg -framerate 2 -pattern_type glob -i ${quicklook_dir}'*.png' -c:v libx264 -r 10 -crf 20 -pix_fmt yuv420p \
        -y ${quicklook_dir}quicklook_animation.mp4
    echo 'View animation here: '${quicklook_dir}quicklook_animation.mp4
}

MON_MEM () {
    srun -n$SLURM_JOB_NUM_NODES -w $hostlist killall free

    log_name=wrf_tbpf_mem_usage
    log_file="${log_name}-demo.log"
    echo "Logging mem usage to $log_file"

    index=0  # Initialize the index variable

    free -h | awk -v idx="$index" 'BEGIN{OFS="\t"} NR==1{print "Index\t","Type\t" $0} NR==2{print idx, $0}' > "$log_file"

    free -h -s 1 | grep --line-buffered Mem | sed --unbuffered = | paste - - >> "$log_file"

}

date

# MON_MEM &

# spack load ior
# timeout 45 srun -N1 -n10 ior -w -r -t 1m -b 30g 

# # Activate PyFLEXTRKR conda environment
echo 'Activating PyFLEXTRKR environment ...'
source activate pyflextrkr_copy # pyflextrkr flextrkr

export FLUSH_MEM=TRUE # TRUE for flush, FALSE for no flush
export CURR_TASK=""

PREPARE_CONFIG


# ulimit -v $((10 * 1024 * 1024)) # in KB

srun -n$SLURM_JOB_NUM_NODES -w $hostlist --oversubscribe sudo /sbin/sysctl vm.drop_caches=3

start_time=$(($(date +%s%N)/1000000))
RUN_TRACKING
duration=$(( $(date +%s%N)/1000000 - $start_time))
echo "RUN_TRACKING done... $duration milliseconds elapsed." | tee -a RUN_TRACKING-demo.log




echo 'MCS_WRF_TBRADAR Demo completed!'
date

sacct -j $SLURM_JOB_ID --format="JobID,JobName,Partition,CPUTime,AllocCPUS,State,ExitCode,MaxRSS,MaxVMSize"
