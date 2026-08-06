#! /usr/bin/bash -l
#SBATCH --job-name="02_FORHYX_aggregate_predictors"
#SBATCH --time=04:00:00

#SBATCH --account=invest          ### invest           ### FOR DEVELOPMENT: gratis
#SBATCH --qos=job_icpu-stocker    ### job_icpu-stocker ### FOR DEVELOPMENT: job_debug (AND ensure not to use capacity storage!)
#SBATCH --ntasks=1             # nr of tasks (processes), used for MPI jobs that may run distributed on multiple compute nodes
#SBATCH --cpus-per-task=48     # nr of threads, used for shared memory jobs that run locally on a single compute node (default: 1)
#SBATCH --mem-per-cpu=6G

#SBATCH --mail-user=fabian.bernhard@unibe.ch
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --chdir=GitHub/fabern/forhyx/data-raw  # ensure to omit tilde ~/. Defines the working directory (relative to working directory where you submitted job via `sbatch`). This directory contains your script, and where the --output will be written
#SBATCH --output=slurm-%x_%j.out        # --output relative to working directory (takes into account --chdir)

# Taken from Github/fabern/aggregate-era5land-daily on 2026-07-19: proc.sh
echo "Started on: $(date --rfc-3339=seconds)"
echo "Hostname: $(hostname)"

echo "SLURM Job ID: ${SLURM_JOBID}"

module load cURL/8.11.1-GCCcore-14.2.0 OpenSSL/3  # on UBELIX HPC
module load Anaconda3                             # on UBELIX HPC
eval "$(conda shell.bash hook)"                   # on UBELIX HPC

echo "Current directory:"
pwd

cd ~/GitHub/fabern/forhyx/data-raw

# NOTE: ensure manually the out-path exists:
echo "Start python script: $(date --rfc-3339=seconds)"
conda activate forhyx

# python3 02_aggregate_predictors.py --dataset-type era5 \
#     --input-path /storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5_1996-2025_run2026-07-14/aggregated_UTCDaily \
#     --out-path /storage/capacity/occr_geco/data_2/scratch/fbernhard/FORHYX/

python3 02_aggregate_predictors.py --dataset-type era5-land \
    --input-path /storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5Land-T_2000-2026_run2026-07-16/aggregated_UTCDaily \
    --out-path /storage/capacity/occr_geco/data_2/scratch/fbernhard/FORHYX/

conda deactivate

echo "Finished on: $(date --rfc-3339=seconds)"

# NOTE: if we want to follow the progress, we could do so with the dask scheduler.
#  - run another command on ubelix to forward the corresponding port from worker to submit node: fb24k097@submit04:~$ ssh -L 8787:localhost:8787 cnode16
#  - and also from the submit node forward that port to your local machine (e.g. using the Ports tab in VSCode)








# TO CHECK FOR INPUT:
#input_path = '/storage/scratch/giub_geco/fbernhard/ERA5_aggregated_newCodeNewDownload/' # era5-single and era5-pressure
#input_path = '/storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5Land-T_2000-2026_run2026-07-16/aggregated_global' # era5land
# Check current download and aggregation inventory (if files missing, run the corrsponding download slurm scripts)
# # ERA5-Land:
# python3 ~/GitHub/fabern/download-ECMWF-data/inventory_ECMWF.py --download-path /storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5Land-T_2000-2026_run2026-07-16/
# ### NOTE: for predictors: For 2020-2026 with 't2m', 'd2m', 'swvl1', 'snowc':  nothing missing
# ### NOTE: for targets:    For 2020-2026 with 'ssr','str','tp','sp','t2m'   :  nothing missing (have all of this from earlier download)
# python3 ~/GitHub/fabern/download-ECMWF-data/inventory_ECMWF.py --aggregate-path /storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5Land-T_2000-2026_run2026-07-16/aggregated_UTCDaily
# ### NOTE: for predictors: For 2016-2025 with 'mean_t2m', 'mean_d2m', 'mean_swvl1', 'mean_snowc':  nothing missing
# ### ### NOTE: for targets:    For 2024-2026 with 'tot_ssr','tot_str','tot_tp','mean_sp','mean_t2m' :  all of 'tot_ssr','tot_str','tot_tp','mean_sp' missing

# # ERA5:
# python3 ~/GitHub/fabern/download-ECMWF-data/inventory_ECMWF.py --download-path  /storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5_1996-2025_run2026-07-14
# ### NOTE: for predictors: For 2008-2025 with z.500, sst, siconc: nothing missing
# ### NOTE: for targets:    For 2008-2026 with cape, cp, 500.u,500.v,925.u,925.v: cape.2026.01-07, cp.2026.06-07
# python3 ~/GitHub/fabern/download-ECMWF-data/inventory_ECMWF.py --aggregate-path /storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5_1996-2025_run2026-07-14/aggregated_UTCDaily
# cd /storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5_1996-2025_run2026-07-14/aggregated_UTCDaily
# rsync -avz --include="*/" --include="*era5-pressure-levels_UTCDaily.500.mean_z*" --exclude="*" /storage/scratch/giub_geco/fbernhard/ERA5_aggregated_newCodeNewDownload/ /storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5_1996-2025_run2026-07-14/aggregated_UTCDaily
# rsync -avz --include="*/" --include="*era5-single-level_UTCDaily.mean_siconc*" --exclude="*" /storage/scratch/giub_geco/fbernhard/ERA5_aggregated_newCodeNewDownload/ /storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5_1996-2025_run2026-07-14/aggregated_UTCDaily
# rsync -avz --include="*/" --include="*era5-single-level_UTCDaily.mean_sst*" --exclude="*" /storage/scratch/giub_geco/fbernhard/ERA5_aggregated_newCodeNewDownload/ /storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5_1996-2025_run2026-07-14/aggregated_UTCDaily
# rsync -avz --include="*/" --include="*era5-pressure-levels_UTCDaily.500_900_925.mean_v*" --exclude="*" /storage/scratch/giub_geco/fbernhard/ERA5_aggregated_newCodeNewDownload/ /storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5_1996-2025_run2026-07-14/aggregated_UTCDaily
# rsync -avz --include="*/" --include="*era5-single-level_UTCDaily.max_cape*" --exclude="*" /storage/scratch/giub_geco/fbernhard/ERA5_aggregated_newCodeNewDownload/ /storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5_1996-2025_run2026-07-14/aggregated_UTCDaily
# rsync -avz --include="*/" --include="*era5-single-level_UTCDaily.tot_cp*" --exclude="*" /storage/scratch/giub_geco/fbernhard/ERA5_aggregated_newCodeNewDownload/ /storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5_1996-2025_run2026-07-14/aggregated_UTCDaily
# TODO: rm -r /storage/scratch/giub_geco/fbernhard/ERA5_aggregated_newCodeNewDownload/
# TODO: change yearly aggregation SLURM script to use new path instead of the rm-r path (line above)

# ### NOTE: for predictors: For 2010-2026 with mean_z.500, mean_sst, mean_siconc: nothing missing
# ### NOTE: for targets:    For 2010-2026 with max_cape, mean_cp, mean_u500,mean_v500,mean_u925,mean_v925: missing mean_cp, max_cape
