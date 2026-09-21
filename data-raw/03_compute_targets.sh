#! /usr/bin/bash -l
#SBATCH --job-name="03_FORHYX_compute_SCE_1974-2025"
#SBATCH --time=18:00:00

#SBATCH --account=invest          ### invest           ### FOR DEVELOPMENT: gratis
#SBATCH --qos=job_icpu-stocker    ### job_icpu-stocker ### FOR DEVELOPMENT: job_debug (AND ensure not to use capacity storage!)
#SBATCH --ntasks=1             # nr of tasks (processes), used for MPI jobs that may run distributed on multiple compute nodes
#SBATCH --cpus-per-task=36     # nr of threads, used for shared memory jobs that run locally on a single compute node (default: 1)
#SBATCH --mem-per-cpu=5G

#SBATCH --mail-user=fabian.bernhard@unibe.ch
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --chdir=GitHub/fabern/forhyx/data-raw  # ensure to omit tilde ~/. Defines the working directory (relative to working directory where you submitted job via `sbatch`). This directory contains your script, and where the --output will be written
#SBATCH --output=slurm-%x_%j.out        # --output relative to working directory (takes into account --chdir)

# HOWTO: ssh ubelix; cd ~; sbatch ~/GitHub/fabern/forhyx/data-raw/03_compute_targets.sh

# python3 ~/GitHub/fabern/download-ECMWF-data/inventory_ECMWF.py --download-path  /storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5_1996-2025_run2026-07-14 # TODO: 
# ### NOTE: for predictors: For 1960-2026 with z.500, sst, siconc: nothing missing
# ### NOTE: for targets:    For 1960-2026 with cape, cp, 500.u,500.v,925.u,925.v: cape.2026.01-07, cp.2026.06-07
# python3 ~/GitHub/fabern/download-ECMWF-data/inventory_ECMWF.py --aggregate-path /storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5_1996-2025_run2026-07-14/aggregated_UTCDaily
# ### NOTE: for predictors: For 1960-2025 with mean_z.500, mean_sst, mean_siconc: nothing missing
# ### NOTE: for targets:    For 1960-2025 with max_cape, tot_cp, mean_u500,mean_v500,mean_u925,mean_v925 (except for 1960-1973 mean_u500,mean_v500)


# Taken from Github/fabern/aggregate-era5land-daily on 2026-07-19: proc.sh
echo "Started on: $(date --rfc-3339=seconds)"
echo "Hostname: $(hostname)"

echo "SLURM Job ID: ${SLURM_JOBID}"

module purge
module load cURL/8.11.1-GCCcore-14.2.0 OpenSSL/3  # on UBELIX HPC
module load Anaconda3                             # on UBELIX HPC
eval "$(conda shell.bash hook)"                   # on UBELIX HPC

echo "Current directory:"
pwd

cd ~/GitHub/fabern/forhyx/data-raw

rm -r /storage/scratch/giub_geco/fbernhard/FORHYX/03* # delete old outputs

echo "Start python script: $(date --rfc-3339=seconds)"
conda activate forhyx
python3 03_compute_targets.py \
    --start-year 1974 \
    --end-year 2025
conda deactivate

echo "Finished on: $(date --rfc-3339=seconds)"

# NOTE: if we want to follow the progress, we could do so with the dask scheduler.
#  - run another command on ubelix to forward the corresponding port from worker to submit node: fb24k097@submit04:~$ ssh -L 8787:localhost:8787 cnode16
#  - and also from the submit node forward that port to your local machine (e.g. using the Ports tab in VSCode)