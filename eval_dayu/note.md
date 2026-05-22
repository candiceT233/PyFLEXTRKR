# First Evaluation
## run_tracksingle (stage2)
1. run_tracksingle (all-to-all)
    - collects:
        - data stage-in to node local time (move from PFS)
        - stage runtime
        - data stage-out time (to PFS)
    - input:
        - cloudid_xxx.nc
    - output:
        - track_xxx.nc

## run_get_tracks & run_trackstats & run_identifymcs
1. stage 3: run_get_tracks (fan-in)
    - collects:
        - data stage-in to node local time (move from PFS)
        - stage runtime
        - data stage-out time (stage out tracknumbers.nc to all nodes local for run_trackstats)
    - input: 
        - last cloudid.nc file
        - all track.nc files
    - output:
        - tracknumbers.nc
2. stage 4: run_trackstats (fan-out)
    - collects:
        - previous stage included stagout time
        - stage runtime
        - next task in same node, no stage out time
    - input:
        - tracknumbers.nc
    - output:
        - trackstats.nc
        - trackstats_sparse.nc
3. stage 5: run_identifymcs (one-to-one)
    - collects:
        - run run_identifymcs same as run_trackstats
        - stage-out time (move to PFS)
    - input:
        - trackstats_sparse.nc
    - output:
        - mcs_tracks.nc


