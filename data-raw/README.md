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
python3 /storage/homefs/fb24k097/GitHub/fabern/download-ECMWF-data/inventory_ECMWF.py --check-file-validity FALSE --download-path /storage/capacity/occr_geco/data_2/archive/era5_hersbach_2020/data
python3 /storage/homefs/fb24k097/GitHub/fabern/download-ECMWF-data/inventory_ECMWF.py --check-file-validity FALSE --download-path /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data
#result: daily downloaded: 1950-2026(Oct):     500.z,500_900_925.u,500_900_925.v,cape,ci,cp,sst
#result: daily downloaded: 1950-2026(Oct):     d2m,pev,snowc,sp,ssr,ssrd,str,swvl1,t2m,tp,u10,v10                                                      
#result: daily downloaded: 1960-1999(missing): e,evabs,evavt

#result: daily aggregated: 1950-2026(oct): 500.mean_z,500_900_925.mean_u,500_900_925.mean_v,max_cape,mean_siconc,mean_sst,tot_cp
#result: daily aggregated: 1950-2026(oct): max_t2m,mean_d2m,mean_snowc,mean_sp,mean_swvl1,mean_t2m,mean_wind10,min_t2m,tot_pev,tot_ssr,tot_ssrd,tot_str,tot_tp
#result: daily aggregated: missing: mean_u10,mean_v10,tot_e,tot_evabs,tot_evavt   

# 1b compute aggregation
# i) ERA5
ssh ubelix; cd ~; sbatch ~/GitHub/fabern/download-ECMWF-data/aggregate_ERA5single_UTCdaily.sh   # ERA5 single
ssh ubelix; cd ~; sbatch ~/GitHub/fabern/download-ECMWF-data/aggregate_ERA5pressure_UTCdaily.sh # ERA5 pressure
# ii) ERA5-Land
ssh ubelix; cd ~; sbatch ~/GitHub/fabern/download-ECMWF-data/aggregate_ERA5land_UTCdaily.sh # ERA5-Land

# 1c get current situation of aggregation
conda activate ecmwf_download
python3 /storage/homefs/fb24k097/GitHub/fabern/download-ECMWF-data/inventory_ECMWF.py --check-file-validity FALSE --aggregate-path /storage/capacity/occr_geco/data_2/archive/era5_hersbach_2020/data_derived_00_dailyUTC_v3
python3 /storage/homefs/fb24k097/GitHub/fabern/download-ECMWF-data/inventory_ECMWF.py --check-file-validity FALSE --aggregate-path /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_00_dailyUTC_v3
# result: daily aggregated: 1950-2026(oct): 500.mean_z,500_900_925.mean_u,500_900_925.mean_v,max_cape,mean_siconc,mean_sst,tot_cp
# result: daily aggregated: 1950-2026(oct): max_t2m,mean_d2m,mean_snowc,mean_sp,mean_swvl1,mean_t2m,mean_wind10,min_t2m,tot_pev,tot_ssr,tot_ssrd,tot_str,tot_tp
# result: daily aggregated: missing: mean_u10,mean_v10,tot_e,tot_evabs,tot_evavt   

```
## Derive prediction targets and aggregate to coarser spatial and temporal resolution for the ML workflow
```bash
# step 2) compute targets and compute aggregated (forhyx) with code from fabern/forhyx repository

# 2a) PREDICTORS:
# GO TO FORHYX REPOSITORY AND DO:
# First, check available years for ERA5-Land and ERA5 with the above python3 script using `--aggregate-path`
# Second, run aggregation to predictors
cd ~; sbatch ~/GitHub/fabern/forhyx/data-raw/02_aggregate_predictors.sh

# 2b) TARGETS:
# i) SCE:
cd ~; sbatch ~/GitHub/fabern/forhyx/data-raw/03_compute_targets.sh
# This took about 15h for 25 years (code data-raw/old-logs/slurm-03_FORHYX_compute_SCE_12468414.out). (was before performance improvement in 28b06e7)
# This took about 40min for 76 years (code data-raw/slurm-03_FORHYX_compute_SCE_1974-2025_20839493.out)

# ii) PCWD 
# see separate repository geco-bern/cwd_global

# iii) aggregate to lower temporal resolution
cd ~; sbatch ~/GitHub/fabern/forhyx/data-raw/04_aggregate_targets.sh
```
