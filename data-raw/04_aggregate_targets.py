# aggregate targets over week and aggregate over region

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
import geopandas as gpd
import regionmask
dask.config.set({
    'distributed.comm.timeouts.connect': '60s',
    'distributed.comm.timeouts.tcp': '120s',
})

# # for plotting
# import matplotlib.pyplot as plt

pcwd_input_path = '/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_03_daily_pcwd.narm_v2-doy-reset_netcdf'
sce_input_path = '/storage/scratch/giub_geco/fbernhard/FORHYX/03_era5-pressure-levels_UTCDaily.SCE.all-steps.nc'
#sce_input_path = '/storage/scratch/giub_geco/fbernhard/FORHYX/daily-03_era5-pressure-levels_UTCDaily.SCE.all-steps.nc' # backup rename
regions = gpd.read_file('/storage/homefs/fb24k097/GitHub/fabern/forhyx/data/regions/shapefile/regions.shp')

def parse_args():
    parser = argparse.ArgumentParser(
        description='Download ERA5 / ERA5Land data from the Climate Data Store (CDS).',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--out-path', default = '/storage/capacity/occr_geco/data_2/scratch/fbernhard/FORHYX/',
                             help="Root output directory.")
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

if __name__ == '__main__':
    args = parse_args();

    # out_directory = '/storage/capacity/occr_geco/data_2/scratch/fbernhard/FORHYX'
    # start_year_requested = 1954
    # start_year_requested = 2003
    # end_year_requested = 2025
    out_directory = args.out_path
    # os.makedirs(out_directory, exist_ok=True)

    start_year_requested = args.start_year
    end_year_requested = args.end_year

    # cluster = LocalCluster(n_workers=min(64, int(os.environ['SLURM_NPROCS'])), threads_per_worker=1)
    # cluster = LocalCluster(n_workers=4, threads_per_worker=1)

    cluster = LocalCluster(n_workers=min(64, int(os.environ['SLURM_CPUS_PER_TASK'])), threads_per_worker=1)
    # client = Client(address=cluster)
    with Client(address=cluster) as client:
        print('Cluster setup: ', flush=True)
        print(client, flush=True)
        print(client.dashboard_link, flush=True)

        ### Step 2: Loop over different target variables
        vars = ['pcwd', 'sce']
        vars = ['pcwd'] # only aggregate pcwd
        #vars = ['sce']   # only aggregate sce
        #curr_var = vars[0]
        #curr_var = vars[1]
        for curr_var in vars:

            ### Step 3: Check presence/absence and validity of NetCDF Files
            if curr_var == 'pcwd':
                # List all NetCDF files in path
                print(pcwd_input_path, flush=True)
                file_pattern = f'*data_derived_03_daily_pcwd_v2-doy_*_r-generated.nc'
                netcdf_files = list_netcdf_files(pcwd_input_path, file_pattern)
                netcdf_files.sort()

                print('All files to treat:')
                pprint.pp(netcdf_files)

                # Use `xarray.open_mfdataset` to open multiple NetCDF files as a single dataset:
                if not netcdf_files:
                    raise FileNotFoundError(f'No files found for {curr_var}')

                ### Step 4: Open Multiple NetCDF Files
                ds_daily = xr.open_mfdataset(
                    netcdf_files,
                    combine='by_coords',
                    #engine='h5netcdf', 
                    parallel =True, 
                    compat='no_conflicts',
                    join='exact',
                    chunks="auto" # {'time': 28, 'lat': 256, 'lon': 256}
                )

            if curr_var == 'sce':
                ### Step 4: Open Single NetCDF Files
                ds_daily = xr.open_dataset(sce_input_path)
                if 'SCE' in ds_daily.variables:
                    ds_daily = ds_daily.rename_vars({"SCE":"SCE_ID"})
                SCE_boolean = ds_daily != 0
                SCE_boolean = SCE_boolean.rename_vars({"SCE_ID":"SCE_bool"})

                # #import matplotlib.pyplot as plt
                # plt.figure()
                # #ds_daily.sel(longitude = slice(-3,8), latitude = slice(30,25)).isel(valid_time=181)['SCE_ID'].plot();
                # ds_daily.sel(valid_time="2022-06-27")['SCE_ID'].plot();
                # plt.savefig('figure_SCE.png')
                # plt.figure()
                # SCE_boolean.sel(valid_time="2022-06-27")['SCE_bool'].plot();
                # plt.savefig('figure_SCE_bool.png')
                
                ds_daily = SCE_boolean.rename({"longitude":"lon", "latitude":"lat", "valid_time":"time"})
                # NOTE that SCE_boolean has already longitudes between -180 and +180

            # In order to subset region Europe:
            # - Switch longitudes from between 0 and 360 to between -180 and +179.9.
            # - Remove the duplicate 360° endpoint and wrap to [-180, 180).
            # NOTE: both operations have no effect is data set is already from -180 to 180
            if np.isclose(ds_daily.lon[-1], 360.0):
                ds_daily = ds_daily.isel(lon=slice(None, -1))
            ds_daily = (
                ds_daily
                .assign_coords(lon=((ds_daily.lon + 180) % 360) - 180)
                .sortby('lon')
            )
            print('\n\nFULL DAILY INPUT DATA SET TO AGGREGATE: ########################', flush=True)
            print(ds_daily, flush=True)

            ### Step 5: Aggregate in time (weekly) and in space (to regions)
            # ensure there is a human-readable name column if you want one
            if 'region_name' not in regions.columns:
                regions['region_name'] = regions['region_id'].astype(str)

            min_lon, min_lat, max_lon, max_lat = regions.total_bounds # [-7.625 32.625 30.125 54.625]
            
            ## Prepare ERA5Land data coordinates:
            # Crop before constructing a region × latitude × longitude array.
            lat_is_decreasing = np.diff(ds_daily.lat[range(0,2)])[0] < 0
            lat_slice = slice(max_lat, min_lat) if lat_is_decreasing else slice(min_lat, max_lat)
            ds_daily2 = ds_daily.sel(
                lon=slice(min_lon, max_lon),
                lat=lat_slice
            )
            
            # Create mask with dimensions: region, lat, lon
            mask = regionmask.mask_3D_geopandas(
                regions, 
                ds_daily2, 
                numbers='region_id', 
                drop=True)

            # Append regional mask to xarray
            ds_daily3 = ds_daily2.where(mask)

            # # Check output visually:
            # plt.figure()
            # ds_daily.sel(lon = slice(0,30), lat = slice(35,65)).isel(time=181)['pcwd_mm'].plot();
            # plt.savefig('figure.png')
            # plt.figure()
            # # mask.sel(lon = slice(0,30), lat = slice(35,65), region = 2).plot();
            # # mask.sel(lon = slice(0,30), lat = slice(35,65), region = 3).plot();
            # mask.sel(lon = slice(0,30), lat = slice(35,65), region = 10).plot();
            # plt.savefig('figure_regions.png')
            # plt.figure()
            # ds_daily3.sel(region = 10, lon = slice(0,30), lat = slice(35,65)).isel(time=181)['pcwd_mm'].plot();
            # plt.savefig('figure_pcwd_region10.png')
            # plt.figure()
            # ds_daily3.sel(region = 9, lon = slice(0,30), lat = slice(35,65)).isel(time=181)['pcwd_mm'].plot();
            # plt.savefig('figure_pcwd_region9.png')

            # 1. Resample spatially
            # Compute mean per region using area-based weights
            #   For boolean SCE taking the mean gives the (weighted) area fraction.
            #   For numeric PCWD taking the mean gives the (weighted) mean across the region.
            latitude_weights = np.cos(np.deg2rad(ds_daily3.lat))
            regional_daily = (
                ds_daily3
                .weighted(latitude_weights)
                .mean(dim=('lat', 'lon'), skipna=True)
            )

            ## old, unused approach: # 2. Resample temporally using built-in Dask-optimized resample
            ## old, unused approach: # Compute mean per week
            ## old, unused approach: regional_weekly = regional_daily.resample(time='7D').mean()
            # 2. Build 52 calendar-anchored weekly means for each year, using
            # the same temporal aggregation as the predictor aggregation.
            # Each year starts on Jan 1 and contains 52 complete seven-day
            # periods; remaining year-end days are intentionally excluded.
            regional_daily = regional_daily.assign_coords(
                time=regional_daily.time.dt.floor('D')
            ).sortby('time')
            years = np.unique(regional_daily.time.dt.year.values)
            if start_year_requested is not None:
                years = years[years >= start_year_requested]
            if end_year_requested is not None:
                years = years[years <= end_year_requested]
            if years.size == 0:
                raise ValueError(
                    'No target data falls within the requested year range.'
                )
            weekly_by_year = []
            weekly_frequency = pd.Timedelta(days=7)
            for year in years:
                year_start = pd.Timestamp(year=int(year), month=1, day=1)
                daily_index = pd.date_range(year_start, periods=52 * 7, freq='D')
                weekly_by_year.append(
                    regional_daily
                    .reindex(time=daily_index)
                    .resample(time=weekly_frequency, origin=year_start)
                    .mean()
                )
            regional_weekly = xr.concat(weekly_by_year, dim='time')

            # 3. Output as CSV:
            if curr_var == 'pcwd':
                target_weekly_df = regional_weekly['pcwd_mm'].to_pandas()
                target_weekly_df.columns.name = 'region'
                target_weekly_df = target_weekly_df.rename(
                    columns=lambda region_id: f"region_{region_id}"
                )
                
                # # Check output visually:
                # fig, ax = plt.subplots(figsize=(12, 6))
                # regional_daily['pcwd_mm'].plot.line(
                #     x='time', hue='region', ax=ax, add_legend=True,
                # )
                # ax.set(
                #     title='Daily regional mean potential cumulative water deficit',
                #     xlabel='Date', ylabel='PCWD (mm)',
                # )
                # ax.grid(visible=True, alpha=0.3)
                # fig.tight_layout()
                # fig.savefig('figure_pcwd_regional_daily_timeseries.png', dpi=150)
                # plt.close(fig)

            if curr_var == 'sce':
                target_weekly_df = regional_weekly['SCE_bool'].to_pandas()
                target_weekly_df.columns.name = 'region'
                target_weekly_df = target_weekly_df.rename(
                    columns=lambda region_id: f"region_{region_id}"
                )

            start_year = pd.to_datetime(min(regional_weekly['time'].values)).year
            end_year = pd.to_datetime(max(regional_weekly['time'].values)).year
            year_range = f"{start_year}-{end_year}"
            csv_path = os.path.join(out_directory, f'FORHYX.targets.{curr_var}_regional_weekly.{year_range}.csv')
            target_weekly_df.to_csv(csv_path, index_label='time')
            print(f'\n\nWrote weekly regional {curr_var} to {csv_path}', flush=True)

                # # Check output visually:
                # fig, ax = plt.subplots(2,1, figsize=(24, 6), sharex='all')
                # regional_daily['SCE_bool'].sel(region=[10]).plot.line(
                #     x='time', hue='region', ax=ax[0], add_legend=True,
                # )
                # regional_weekly['SCE_bool'].sel(region=[10]).plot.line(
                #     x='time', hue='region', ax=ax[1], add_legend=True,
                # )
                # ax[0].set(
                #     title='Daily regional area fraction experiencing a SCE (avg SCE_bool)',
                #     xlabel=None, ylabel='Area fraction (-)',
                # )
                # ax[1].set(
                #     title='Weekly regional area fraction experiencing a SCE (avg SCE_bool)',
                #     xlabel='Date', ylabel='Area fraction (-)',
                # )
                # ax[1].grid(visible=True, alpha=0.3)
                # fig.tight_layout()
                # fig.savefig('figure_sce_regional_daily_timeseries.png', dpi=150)
                # plt.close(fig)


            print('########################\n\n', flush=True)


