# aggregate predictors over week and coarsen grid to reduce complexity

### Step 1: Import Libraries 
# Import the required libraries:
import argparse
import os, fnmatch
import pprint
import time
import xarray as xr
import numpy as np
import pandas as pd
from dask.distributed import Client, LocalCluster
import dask

dask.config.set({
    "distributed.comm.timeouts.connect": "60s",
    "distributed.comm.timeouts.tcp": "120s",
})

# FOR SDSC project we need for predictors
# for ERA5:
#   ds_weekly[['z.500']]
#   ds_weekly[['mean_sst', 'mean_siconc']]
# for ERA5-Land:
#   ds_weekly[['mean_t2m', 'mean_d2m', 'mean_swvl1', 'mean_snowc']]

def parse_args():
    parser = argparse.ArgumentParser(
        description='Download ERA5 / ERA5Land data from the Climate Data Store (CDS).',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--input-path', required=True,
                         help="Root input directory for aggregated daily files.")
    parser.add_argument('--out-path', required=True,
                             help="Root output directory.")
    parser.add_argument('--dataset-type', choices=['era5','era5-land'],
                         help="Controls data-set specifics; choose 'era5-land' or 'era5'.")
    return parser.parse_args()

def list_netcdf_files(root_dir, pattern):
    netcdf_files = []
    for root, dirs, files in os.walk(root_dir):
        for filename in fnmatch.filter(files, pattern):
            if filename.endswith('.nc'):
                netcdf_files.append(os.path.join(root, filename))
    return netcdf_files

if __name__ == "__main__":
    args = parse_args(); 

    # TODO: remove input_path =    '/storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5Land-T_2000-2026_run2026-07-16/aggregated_UTCDaily'
    # TODO: remove out_directory = '/storage/capacity/occr_geco/data_2/scratch/fbernhard/FORHYX/'
    # TODO: remove dataset_type =  'era5-land'
    
    input_path = args.input_path     # input_path =    '/storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5_1996-2025_run2026-07-14/aggregated_UTCDaily'
    out_directory = args.out_path    # out_directory = '/storage/capacity/occr_geco/data_2/scratch/fbernhard/FORHYX/'
    dataset_type = args.dataset_type # dataset_type =  'era5'


    # FOR INTERACTIVE SCRIPTING: cluster = LocalCluster(n_workers=min(64, int(os.environ['SLURM_NPROCS'])), threads_per_worker=1)
    # FOR INTERACTIVE SCRIPTING: client = Client(address=cluster)
    cluster = LocalCluster(n_workers=min(64, int(os.environ['SLURM_CPUS_PER_TASK'])), threads_per_worker=1)
    with Client(address=cluster) as client:
        print("Cluster setup: ", flush=True)
        print(client, flush=True)
        print(client.dashboard_link, flush=True)

        ### Step 2: Loop over different predictor variables
        if dataset_type == 'era5':
            vars = ['500.mean_z', 'mean_sst', 'mean_siconc'] # era5
        elif dataset_type == 'era5-land':
            vars = ['mean_swvl1'] # , 'mean_snowc', 'max_t2m', 'min_t2m', 'mean_t2m', 'mean_d2m']

        fname_prefix_map = {
            # era5-pressure:
            "500.mean_z":  "FORHYX.predictors.era5pressure",
            # era5-single:
            "mean_siconc": "FORHYX.predictors.era5single",
            "mean_sst":    "FORHYX.predictors.era5single",
            # era5-land:
            "min_t2m":     "FORHYX.predictors.era5land",
            "max_t2m":     "FORHYX.predictors.era5land",
            "mean_t2m":    "FORHYX.predictors.era5land",
            "mean_d2m":    "FORHYX.predictors.era5land",
            "mean_swvl1":  "FORHYX.predictors.era5land",
            "mean_snowc":  "FORHYX.predictors.era5land",
        }

        for curr_var in vars: # curr_var = vars[0]

            ### Step 3: Check presence/absence and validity of NetCDF Files
            # List all NetCDF files in path
            print(input_path, flush=True)
            file_pattern = f'*_UTCDaily.{curr_var}*.nc'
            netcdf_files = list_netcdf_files(input_path, file_pattern)
            netcdf_files.sort()

            print("All files to treat:")
            pprint.pp(netcdf_files)

            ### Step 4: Open Multiple NetCDF Files
            # Use `xarray.open_mfdataset` to open multiple NetCDF files as a single dataset:
            ds_daily = xr.open_mfdataset(
                netcdf_files,
                combine='by_coords', 
                engine="h5netcdf", 
                parallel =True, 
                compat='no_conflicts',
                join='exact',
                chunks={"valid_time": "auto", "latitude": "auto", "longitude": "auto"})

            print("\n\nFULL DAILY INPUT DATA SET TO AGGREGATE: ########################", flush=True)
            print(ds_daily, flush=True)

            ### Step 5: Aggregate in time (weekly) and in space (1 deg resolution)

            # 1. Coarsen spatially FIRST (100x (or 16x) data reduction before temporal ops)
            target_resolution = 1.0 # degrees

            # get current resolution (either 0.25 (ERA5) or 0.1 (ERA5-Land)) 
            curr_grid_resolution = np.diff(ds_daily.longitude[0:2].values)[0] # NOTE: assumes lon-lat grid is homogenous
            factor = int(target_resolution/curr_grid_resolution)
            ds_coarse = ds_daily.coarsen(latitude=factor, longitude=factor, boundary="trim").mean()

            # 2. Resample temporally LATER using built-in Dask-optimized resample
            ds_weekly = ds_coarse.resample(valid_time="7D").mean()

            print("Aggregated weekly:", flush=True)
            print(ds_weekly, flush=True)

            # NOTE: For rigorous spatial aggregation, use cosine weighting to account for grid-area on sphere:
            # UNUSED: # Cosine-latitude weighting prior to coarsening or regridding
            # UNUSED: weights = np.cos(np.deg2rad(ds_daily.latitude))
            # UNUSED: weights.name = "weights"
            # UNUSED: ds_weighted = ds_daily.weighted(weights)
            # UNUSED: ds_weekly2 = (
            # UNUSED:     ds_weighted.coarsen(latitude=factor, longitude=factor, boundary="trim")
            # UNUSED:     .mean()
            # UNUSED:     .resample(valid_time="7D")
            # UNUSED:     .mean()
            # UNUSED: )

            # variable specific steps
            if curr_var == "500.mean_z":
                ds_weekly = (
                    ds_weekly
                    .sel(pressure_level=500)
                    .reset_coords("pressure_level", drop=True)
                    .rename({"mean_z": "500_mean_z"}) # NOTE: get rid of "."
                )

            ### Step 6: Output as single netCDF file:
            print("\n\nSTORE AS SINGLE netCDF FILE: ########################", flush=True)

            # setup filename
            start_year = pd.to_datetime(min(ds_weekly['valid_time'].values)).year
            end_year = pd.to_datetime(max(ds_weekly['valid_time'].values)).year
            year_range = f"{start_year}-{end_year}"

            fname_prefix = fname_prefix_map[curr_var]
            var_name = curr_var.replace(".", "_")
            new_fpath = f"{out_directory}/{fname_prefix}.{var_name}.{year_range}.nc"
            print(new_fpath, flush=True)

            # If a previous output exists, rename it with a timestamped _old_ suffix
            # new_fpath = '/storage/capacity/occr_geco/data_2/scratch/fbernhard/FORHYX/FORHYX.predictors.era5land.mean_t2m.2016-2025.nc'
            if os.path.exists(new_fpath):
                backup_path = f"{new_fpath}_old_{time.strftime('%Y%m%d_T%H%M%S')}.nc"
                print(f"Found existing file {new_fpath}, renaming to {backup_path}", flush=True)
                os.rename(new_fpath, backup_path)

            # setup NetCDF format
            compr_level = 5
            chunk_size_definition = {
                'valid_time': 4,   # in weekly resolution = 4 weeks
                'latitude':   18,  # if 1-deg resolution = 18 deg
                'longitude':  36,
                'pressure_level': 3, # NOTE: only used for pressure level ERA5 (UNUSED FOR PREDICTORS)
            }
            curr_chunk_size = tuple(            # this ensures correct order of coords and limit maximum chunk size
                min(chunk_size_definition[k],ds_weekly.sizes[k])
                for k in ds_weekly.dims)
            #print("Target chunk size: ", curr_chunk_size, flush=True)
            
            # Define the new NetCDF encoding
            coord_encoding = {coord: ds_weekly[coord].encoding for coord in ds_weekly.coords}
            var_encoding   = {var:   ds_weekly[var].encoding   for var   in ds_weekly.data_vars}
            # Remove incompatible encoding parameters:
            for coord in coord_encoding:
                coord_encoding[coord].pop('szip', None) # None results in removal if it exists
                coord_encoding[coord].pop('zstd', None) # None results in removal if it exists
                coord_encoding[coord].pop('bzip2', None) # None results in removal if it exists
                coord_encoding[coord].pop('blosc', None) # None results in removal if it exists
            for var in var_encoding:
                var_encoding[var].pop('coordinates', None) # remove error in field 'coordinates'
                var_encoding[var].pop('szip', None)
                var_encoding[var].pop('zstd', None)
                var_encoding[var].pop('bzip2', None)
                var_encoding[var].pop('blosc', None)
                var_encoding[var].pop('preferred_chunks', None)
            # Set 'complevel', and 'chunksizes' of all variables # https://stackoverflow.com/a/66333685
            for var in var_encoding:
                var_encoding[var]['shuffle']    = True                               # update compression level
                var_encoding[var]['complevel']  = compr_level #9                     # update compression level
                var_encoding[var]['chunksizes'] = curr_chunk_size
                var_encoding[var]['zlib'] = True
            
            # Output
            encoding = {}
            encoding.update(coord_encoding)
            encoding.update(var_encoding)

            print("To save:", flush=True)
            print(ds_weekly, flush=True)

            # Save the dataset to a new NetCDF file
            start_time = time.time()
            ds_weekly.to_netcdf(new_fpath, encoding=encoding)
            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f" Compression level: {compr_level}: Elapsed time: {elapsed_time} seconds", flush=True)
