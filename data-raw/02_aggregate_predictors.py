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
    parser.add_argument('--start-year', type=int,
                         help="Start year for filtering input files (inclusive).")
    parser.add_argument('--end-year', type=int,
                         help="End year for filtering input files (inclusive).")
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
    # TODO: remove start_year_requested =  2016
    # TODO: remove end_year_requested =  2017
    
    input_path = args.input_path     # input_path =    '/storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5_1996-2025_run2026-07-14/aggregated_UTCDaily'
    out_directory = args.out_path    # out_directory = '/storage/capacity/occr_geco/data_2/scratch/fbernhard/FORHYX/'
    dataset_type = args.dataset_type # dataset_type =  'era5'

    start_year_requested = args.start_year
    end_year_requested = args.end_year

    print('input_path: ', input_path, flush=True)
    print('out_directory: ', out_directory, flush=True)
    print('dataset_type: ', dataset_type, flush=True)
    print('start_year_requested: ', start_year_requested, flush=True)
    print('end_year_requested: ', end_year_requested, flush=True)

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
            vars = ['mean_sst', 'mean_siconc'] # era5 # TODO: remove again
        elif dataset_type == 'era5-land':
            vars = ['mean_snowc', 'max_t2m', 'min_t2m', 'mean_t2m', 'mean_swvl1', 'mean_d2m'] # era5-land

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

        print(f"\nLoop over variables {vars}: ########################", flush=True)
        for curr_var in vars: # curr_var = vars[0]
            print(f"\n\nStart with variable {curr_var}: ########################", flush=True)

            ### Step 3: Check presence/absence and validity of NetCDF Files
            # List all NetCDF files in path
            print(input_path, flush=True)
            file_pattern = f'*_UTCDaily.{curr_var}*.nc'
            netcdf_files = list_netcdf_files(input_path, file_pattern)
            netcdf_files.sort()

            # filter files by requested year range
            filtered_netcdf_files = []
            for fpath in netcdf_files:
                # Extract the year from the filename (example filename: 'era5-pressure-levels_UTCDaily.500.mean_z.2016.nc')
                filename = os.path.basename(fpath)
                year_str = filename.split('.')[-2]  # Assuming the year is the second last
                try:
                    if start_year_requested <= int(year_str) <= end_year_requested:
                        filtered_netcdf_files.append(fpath)
                except ValueError:  
                    print(f"Warning: Could not extract year from filename {filename}. Skipping this file.", flush=True)

            netcdf_files = filtered_netcdf_files

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

            # 1. Aggregate the aligned 0.25-degree ERA5 and 0.1-degree
            # ERA5-Land grids into common, fixed 1-degree cells. Coarsening
            # retains useful Dask chunks, unlike groupby_bins(), which creates
            # one chunk for every output cell.
            target_resolution = 1.0
            longitude_resolution = float(
                np.abs(ds_daily.longitude.values[1] - ds_daily.longitude.values[0])
            )
            latitude_resolution = float(
                np.abs(ds_daily.latitude.values[1] - ds_daily.latitude.values[0])
            )
            longitude_factor = int(round(target_resolution / longitude_resolution))
            latitude_factor = int(round(target_resolution / latitude_resolution))

            if not (
                np.isclose(longitude_factor * longitude_resolution, target_resolution)
                and np.isclose(latitude_factor * latitude_resolution, target_resolution)
            ):
                raise ValueError(
                    "The source grid resolution must divide evenly into the "
                    f"{target_resolution}-degree target grid; got "
                    f"{latitude_resolution} x {longitude_resolution} degrees."
                )

            # Treat 360 degrees as a duplicate of 0 degrees if it is present.
            ds_daily = ds_daily.where(ds_daily.longitude < 360.0, drop=True)
            expected_longitudes = 360 * longitude_factor
            expected_latitudes = 180 * latitude_factor + 1
            if not (
                np.isclose(ds_daily.longitude.values[0], 0.0)
                and np.isclose(ds_daily.latitude.values[0], 90.0)
                and ds_daily.sizes["longitude"] == expected_longitudes
                and ds_daily.sizes["latitude"] == expected_latitudes
            ):
                raise ValueError(
                    "The source grid is not aligned with the fixed 1-degree "
                    "cells (expected longitude to start at 0 degrees and "
                    "descending latitude to start at 90 degrees)."
                )

            # Grid-cell area is proportional to cos(latitude). Calculate the
            # weighted mean as separate numerator and denominator datasets so
            # missing values receive no weight on a variable-by-variable basis.
            # Float32 weights keep float32 inputs from being promoted to float64.
            latitude_weights = xr.DataArray(
                np.cos(np.deg2rad(ds_daily.latitude.values)).astype(np.float32),
                coords={"latitude": ds_daily.latitude},
                dims=("latitude",),
                name="latitude_weights",
            )
            coarsen_kwargs = {
                "latitude": latitude_factor,
                "longitude": longitude_factor,
                "boundary": "trim",
            }
            weighted_sum = (
                (ds_daily * latitude_weights)
                .coarsen(**coarsen_kwargs)
                .sum(skipna=True)
            )
            weight_sum = (
                (ds_daily.notnull() * latitude_weights)
                .coarsen(**coarsen_kwargs)
                .sum(skipna=True)
            )
            # Empty source cells have zero total weight. Mask their denominator
            # before division so they remain NaN without evaluating 0 / 0.
            valid_weight_sum = weight_sum.where(weight_sum > 0)
            ds_coarse = weighted_sum / valid_weight_sum

            # coarsen() derives coordinates from the source point locations;
            # assign the geometric centers of the fixed cells explicitly.
            longitude_attrs = ds_daily.longitude.attrs.copy()
            latitude_attrs = ds_daily.latitude.attrs.copy()
            ds_coarse = ds_coarse.assign_coords(
                longitude=np.arange(0.5, 360.0, target_resolution),
                latitude=np.arange(89.5, -90.0, -target_resolution),
            )
            ds_coarse.longitude.attrs = longitude_attrs
            ds_coarse.latitude.attrs = latitude_attrs

            # 2. Build 52 calendar-anchored weekly means for each year.  Each
            # year contains Jan 1--Dec 30 (364 days); remaining year-end days
            # are intentionally excluded so all years have exactly 52 values.
            ds_coarse = ds_coarse.assign_coords(
                valid_time=ds_coarse.valid_time.dt.floor("D")
            ).sortby("valid_time")
            years = np.unique(ds_coarse.valid_time.dt.year.values)
            weekly_by_year = []
            weekly_frequency = pd.Timedelta(days=7)
            for year in years:
                year_start = pd.Timestamp(year=int(year), month=1, day=1)
                daily_index = pd.date_range(year_start, periods=52 * 7, freq="D")
                weekly_by_year.append(
                    ds_coarse
                    .reindex(valid_time=daily_index)
                    .resample(valid_time=weekly_frequency, origin=year_start)
                    .mean()
                )
            ds_weekly = xr.concat(weekly_by_year, dim="valid_time")

            print("Aggregated weekly:", flush=True)
            print(ds_weekly, flush=True)

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
