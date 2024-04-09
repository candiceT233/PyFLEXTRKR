# First Evaluation
## run_get_tracks & run_trackstats & run_identifymcs
1. run_get_tracks
    - collects:
        - data stage-in to node local time (move from PFS)
        - stage runtime
        - data stage-out time (stage out tracknumbers.nc to all nodes local for run_trackstats)
    - input: 
        - last cloudid.nc file
        - all track.nc files
    - output:
        - tracknumbers.nc
2. run_trackstats
    - collects:
        - previous stage included stagout time
        - stage runtime
        - next task in same node, no stage out time
    - input:
        - tracknumbers.nc
    - output:
        - trackstats.nc
        - trackstats_sparse.nc
3. run_identifymcs
    - collects:
        - run run_identifymcs same as run_trackstats
        - stage-out time (move to PFS)
    - input:
        - trackstats_sparse.nc
    - output:
        - mcs_tracks.nc


