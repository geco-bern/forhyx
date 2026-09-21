# Download and preprocess data

## Download hourly and aggregate to daily
Download data by using code from the `fabern/download-ECMWF-data` repository:

```bash
# step 0) download the neede ERA5 and ERA5-Land files
ssh ubelix; cd ~; sbatch ~/GitHub/fabern/download-ECMWF-data/download_era5_hersbach_2020-pressure-level_u-v.sh
ssh ubelix; cd ~; sbatch ~/GitHub/fabern/download-ECMWF-data/download_era5_hersbach_2020-pressure-level_z500.sh
ssh ubelix; cd ~; sbatch ~/GitHub/fabern/download-ECMWF-data/download_era5_hersbach_2020-single-level_2d-2t-sst-asn-swvl1-cape-z-ci.sh
ssh ubelix; cd ~; sbatch ~/GitHub/fabern/download-ECMWF-data/download_era5_hersbach_2020-single-level_cp_cape_sst_ci.sh
ssh ubelix; cd ~; sbatch ~/GitHub/fabern/download-ECMWF-data/download_era5land_munoz-sabater_2021_swvl1-d2m-snowc.sh
ssh ubelix; cd ~; sbatch ~/GitHub/fabern/download-ECMWF-data/download_era5land_munoz-sabater_2021_tp-pev-t2m-sp-ssr-str.sh

# Restart downloads
#   If the download script is interrupted it can be restarted, any missing *.nc will be downloaded.
#   NOTE: there can be corrupt *.nc files. Therefore, first clean the partly downloaded files with
    #   find . -name '*.grib' -type f -exec sh -c '
    #       for grib do
    #           base=${grib%.grib}
    #           printf "\nMatching %s:\n" "$base"
    #           find "${grib%/*}" -maxdepth 1 -type f -name "${base##*/}.*" -print
    #           # rm -f -- "$grib" "$base.nc" "$base.grib" "$base.grib.*.idx" # TODO: activate this for deletion
    #       done
    #       ' sh {} +
#   (Optionally, also delete the remaining, undownloaded files in the download queue at the CDS website: https://cds.climate.copernicus.eu/requests)
```

Postprocess hourly downloaded data
```bash
# step 1) aggregate hourly to daily with code from fabern/download-ECMWF-data repository
# 1a get current situation:
python3 /storage/homefs/fb24k097/GitHub/fabern/download-ECMWF-data/inventory_ECMWF.py --check-file-validity TRUE --download-path /storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5_1996-2025_run2026-07-14/
python3 /storage/homefs/fb24k097/GitHub/fabern/download-ECMWF-data/inventory_ECMWF.py --check-file-validity TRUE --download-path /storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5Land-T_2000-2026_run2026-07-16


# # current result:
# i) ERA5
# available years 1960-2025 for:
# directory: /storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5_1996-2025_run2026-07-14/
# to compute: sce and some predictors:
# variables: 500.z      500_900_925.u  500_900_925.v       cape            ci             cp            sst 

# ii) ERA5-Land
# available years 1961-1982 and 1982-2025 for:
# directory: /storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5Land-T_2000-2026_run2026-07-16
# to compute: other-predictors:
# variables: d2m           snowc          swvl1           t2m

# available years 1950-2024 for:
# directory: /storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5Land_1950-2024
# to compute: cwd_global:
# variables: d2m            pev             sp            ssr            ssrd           str            t2m             tp            u10            v10
# and new download: directory: /storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5Land-T_2026-2026_run2026-08-10

# 1b compute aggregation
# i) ERA5
ssh ubelix; cd ~; sbatch ~/GitHub/fabern/download-ECMWF-data/aggregate_ERA5single_UTCdaily.sh   # ERA5 single
ssh ubelix; cd ~; sbatch ~/GitHub/fabern/download-ECMWF-data/aggregate_ERA5pressure_UTCdaily.sh # ERA5 pressure
# ii) ERA5-Land
ssh ubelix; cd ~; sbatch ~/GitHub/fabern/download-ECMWF-data/aggregate_ERA5land_UTCdaily.sh # ERA5-Land

# 1c get current situation of aggregation
python3 /storage/homefs/fb24k097/GitHub/fabern/download-ECMWF-data/inventory_ECMWF.py --check-file-validity TRUE --aggregate-path /storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5_1996-2025_run2026-07-14/aggregated_UTCDaily
# result: 1960-2025  for # ERA5 single
# result: 1974-2025  for # ERA5 pressure
python3 /storage/homefs/fb24k097/GitHub/fabern/download-ECMWF-data/inventory_ECMWF.py --check-file-validity TRUE --aggregate-path /storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5Land-T_2000-2026_run2026-07-16/aggregated_UTCDaily
# result: 1962-2026  for # ERA5Land predictors
python3 /storage/homefs/fb24k097/GitHub/fabern/download-ECMWF-data/inventory_ECMWF.py --check-file-validity TRUE --aggregate-path /storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5Land_2025-2026_forPCWDUpdate/aggregated_UTCDaily
# result: 2025-2026  for newly downloaded # ERA5Land targets (PCWD)
python3 /storage/homefs/fb24k097/GitHub/fabern/download-ECMWF-data/inventory_ECMWF.py --check-file-validity TRUE --aggregate-path /storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5Land_2025-2026_forPCWDUpdate/aggregated_UTCDaily
```


## Derive preidction targets and aggregate to coarser spatial and temporal resolution for the ML workflow
```bash
# step 2) compute targets and compute aggregated (forhyx) with code from fabern/forhyx repository

# 2a) PREDICTORS:
# GO TO FORHYX REPOSITORY AND DO:
# check available years for ERA5-Land:
python3 ~/GitHub/fabern/download-ECMWF-data/inventory_ECMWF.py --aggregate-path /storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5Land-T_2000-2026_run2026-07-16/aggregated_UTCDaily
# check available years for ERA5-Land:
python3 ~/GitHub/fabern/download-ECMWF-data/inventory_ECMWF.py --aggregate-path /storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5_1996-2025_run2026-07-14/aggregated_UTCDaily

# run aggregation to predictors
cd ~; sbatch ~/GitHub/fabern/forhyx/data-raw/02_aggregate_predictors.sh


# 2b) TARGETS:
# i) SCE:
python3 ~/GitHub/fabern/download-ECMWF-data/inventory_ECMWF.py --download-path  /storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5_1996-2025_run2026-07-14
python3 ~/GitHub/fabern/download-ECMWF-data/inventory_ECMWF.py --aggregate-path /storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5_1996-2025_run2026-07-14/aggregated_UTCDaily
cd ~; sbatch ~/GitHub/fabern/forhyx/data-raw/03_compute_targets.sh

# ii) PCWD see separate repository geco-bern/cwd_global

# iii) aggregate to lower temporal resolution
cd ~; sbatch ~/GitHub/fabern/forhyx/data-raw/04_aggregate_targets.sh
```
