# compute targets (PCWD and SCE)

# PCWD done already: see geco-bern/cwd_global at https://github.com/geco-bern/cwd_global/tree/main/src/ERA5Land-fullResNoNA

# SCE: done hereafter


# aggregate targets over week and aggregate over region

### Step 1: Import Libraries 
# Import the required libraries:
import argparse
import fnmatch
import os
import re
import timeit
from datetime import datetime

import dask
import numpy as np
import pandas as pd
import xarray as xr
from dask.distributed import Client, LocalCluster
from skimage.measure import label, regionprops

input_path    = '/storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5_1996-2025_run2026-07-14/aggregated_UTCDaily'
out_directory = '/storage/scratch/giub_geco/fbernhard/FORHYX/'

# ll -h /storage/scratch/giub_geco/fbernhard/FORHYX/03*
# rm -r /storage/scratch/giub_geco/fbernhard/FORHYX/03*


dask.config.set({
    "distributed.comm.timeouts.connect": "60s",
    "distributed.comm.timeouts.tcp": "120s",
})

def parse_args():
    parser = argparse.ArgumentParser(
        description='Compute daily severe convective environments from ERA5 data.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--start-year', type=int, required=True,
                         help="Start year for filtering input files (inclusive).")
    parser.add_argument('--end-year', type=int, required=True,
                         help="End year for filtering input files (inclusive).")
    return parser.parse_args()

def list_netcdf_files(root_dir, pattern):
    netcdf_files = []
    for root, dirs, files in os.walk(root_dir):
        for filename in fnmatch.filter(files, pattern):
            if filename.endswith('.nc'):
                netcdf_files.append(os.path.join(root, filename))
    return netcdf_files


def select_yearly_files(netcdf_files, variable, start_year, end_year):
    """Select exactly one yearly NetCDF file per requested year and variable."""
    year_pattern = re.compile(r"\.(\d{4})\.nc$")
    files_by_year = {}

    for path in netcdf_files:
        filename = os.path.basename(path)
        if f".{variable}." not in filename:
            continue

        match = year_pattern.search(filename)
        if match is None:
            continue

        year = int(match.group(1))
        if not start_year <= year <= end_year:
            continue
        if year in files_by_year:
            raise ValueError(
                f"More than one input file found for {variable} in {year}: "
                f"{files_by_year[year]} and {path}"
            )
        files_by_year[year] = path

    missing_years = sorted(set(range(start_year, end_year + 1)) - files_by_year.keys())
    if missing_years:
        raise FileNotFoundError(
            f"Missing {variable} input files for years: {missing_years}"
        )

    return [files_by_year[year] for year in sorted(files_by_year)]

if __name__ == "__main__":
    args = parse_args()
    
    start_year_requested = args.start_year
    end_year_requested = args.end_year

    if start_year_requested > end_year_requested:
        raise ValueError(
            f"start year ({start_year_requested}) must not be after "
            f"end year ({end_year_requested})"
        )

    start_date_str = str(start_year_requested) + "-01-01"
    end_date_str   = str(end_year_requested) + "-12-31"
    run_tag = f"{start_year_requested:04d}-{end_year_requested:04d}"
    os.makedirs(out_directory, exist_ok=True)

    n_workers = min(64, int(os.environ.get("SLURM_CPUS_PER_TASK", os.cpu_count() or 1)))
    # os.environ['SLURM_NTASKS'] # TODO: determine what to use in the next line
    # os.environ['SLURM_NPROCS'] # TODO: determine what to use in the next line
    # cluster = LocalCluster(n_workers=min(64, int(os.environ['SLURM_NPROCS'])), threads_per_worker=1)
    with LocalCluster(n_workers=n_workers, threads_per_worker=1) as cluster, Client(cluster) as client:
        print("Cluster setup: ", flush=True)
        print(client, flush=True)
        print(client.dashboard_link, flush=True)

        shear_fname = os.path.join(
            out_directory,
            f'03_era5-pressure-levels_UTCDaily.500_925_shear.{run_tag}.nc',
        )
        conv_fname = os.path.join(
            out_directory,
            f'03_era5-pressure-levels_UTCDaily.conv.{run_tag}.nc',
        )
        verbose = True
        
        #all_needed_vars_pressure_level = ['500.mean_u','500.mean_v','925.mean_u','925.mean_v'] Not named like this
        all_needed_vars_pressure_level = ['500_900_925.mean_u','500_900_925.mean_v']
        all_needed_vars_single_level   = ['max_cape', 'tot_cp']

        ### Step 3: Check presence/absence and validity of NetCDF Files
        # List all NetCDF files in path
        print(input_path, flush=True)
        file_pattern = f'*_UTCDaily.*.nc'
        netcdf_files = list_netcdf_files(input_path, file_pattern)
        netcdf_files.sort()

        files_by_variable = {
            variable: select_yearly_files(
                netcdf_files,
                variable,
                start_year_requested,
                end_year_requested,
            )
            for variable in all_needed_vars_pressure_level + all_needed_vars_single_level
        }
        print(
            f"Input years: {run_tag}; "
            + ", ".join(
                f"{variable}: {len(paths)} files"
                for variable, paths in files_by_variable.items()
            ),
            flush=True,
        )

        ### Step 4: Compute severe convective environments (SCE) (code adapted from https://github.com/feldmann-m/EU_conv)
        #### Step 4a: compute shear
        netcdf_files_on_pressure_level = sorted(
            path
            for variable in all_needed_vars_pressure_level
            for path in files_by_variable[variable]
        )
        
        # open pressure level data
        pl_data = xr.open_mfdataset(
            netcdf_files_on_pressure_level,
            combine='by_coords', 
            engine="h5netcdf", 
            parallel =True, 
            compat='no_conflicts',
            join='inner',
            chunks={"valid_time": "auto", "latitude": "auto", "longitude": "auto"})

        pl_data = pl_data.sel(valid_time = slice(start_date_str, end_date_str))
        # subset the European regions extent: [-7.625 32.625 30.125 54.625]
        pl_data = pl_data.sel(latitude  = slice(60,25))
        # pl_data = xr.concat([
        #     pl_data.sel(longitude=slice(350, 360)),
        #     pl_data.sel(longitude=slice(0, 35))
        #     ], dim='longitude')
        pl_data = pl_data.assign_coords(longitude=(((pl_data.longitude + 180) % 360) - 180))
        pl_data = pl_data.sortby('longitude')
        pl_data = pl_data.sel(longitude=slice(-10, 35))  # 10°W..35°E

        if verbose: print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),': computing shear', flush=True)
        # sfc_data_2['shear'] = ((pl_data.u.sel(level=500)**2 + pl_data.v.sel(level=500)**2)**0.5 - (pl_data.u.sel(level=900)**2 + pl_data.v.sel(level=900)**2)**0.5).squeeze()
        du = pl_data.mean_u.sel(pressure_level=500) - pl_data.mean_u.sel(pressure_level=925)
        dv = pl_data.mean_v.sel(pressure_level=500) - pl_data.mean_v.sel(pressure_level=925)

        sfc_data_2={}
        sfc_data_2['shear'] = (du**2 + dv**2)**0.5                # TODO(fabian): is this the same as 3 lines prior? shear = (u5^2 + v5^2)^0.5 - (u9^2 + v9^2)^0.5 = A - B
                                                                #                                                  shear^2 = A^2 - 2AB + B^2 
                                                                #                                                          = (u5^2 + v5^2) - 2*(A*B) + (u9^2 + v9^2)
                                                                #                                                          = (u5^2 + v5^2) - 2*((u5^2 + v5^2)*(u9^2 + v9^2))^0.5 + (u9^2 + v9^2)
                                                                #                                                  shear   = sqrt(u5^2 + v5^2 + u9^2 + v9^2 - 2*()^0.5) # ????
        for key, da in sfc_data_2.items():
            da.name = key
        if verbose: print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),': converting array to xarray dataset', flush=True)
        #sfc_data_2 = xr.Dataset(sfc_data_2,compat='override')
        sfc_data_2_dataset = xr.merge(list(sfc_data_2.values()), compat='override')
        if verbose: print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),': writing shear dataset', flush=True)
        print(sfc_data_2_dataset, flush = True)
        sfc_data_2_dataset.to_netcdf(shear_fname, mode='w') # mode='w' leads to overwriting
        pl_data.close()

        #### Step 4b: compute gridpoint-wise boolean if convective
        netcdf_files_on_single_level = sorted(
            path
            for variable in all_needed_vars_single_level
            for path in files_by_variable[variable]
        )
        
        # open single level data
        sfc_data_1 = xr.open_mfdataset(
            netcdf_files_on_single_level,
            combine='by_coords', 
            engine="h5netcdf", 
            parallel =True, 
            compat='no_conflicts',
            join='exact',
            chunks={"valid_time": "auto", "latitude": "auto", "longitude": "auto"})
        sfc_data_1 = sfc_data_1.sel(valid_time=slice(start_date_str, end_date_str))
        # subset the European regions extent: [-7.625 32.625 30.125 54.625]
        sfc_data_1 = sfc_data_1.sel(latitude  = slice(60,25))
        # sfc_data_1 = xr.concat([
        #     sfc_data_1.sel(longitude=slice(350, 360)),
        #     sfc_data_1.sel(longitude=slice(0, 35))
        #     ], dim='longitude')
        sfc_data_1 = sfc_data_1.assign_coords(longitude=(((sfc_data_1.longitude + 180) % 360) - 180))
        sfc_data_1 = sfc_data_1.sortby('longitude')
        sfc_data_1 = sfc_data_1.sel(longitude=slice(-10, 35))  # 10°W..35°E

        cape = sfc_data_1.max_cape
        with xr.open_dataset(shear_fname, engine="h5netcdf") as shear_dataset:
            cape, shear = xr.align(cape, shear_dataset.shear, join="exact")
            conv_dataset = xr.Dataset({
                'conv_EU': (cape > 500) & (shear > 10)
            }) # US was: (cape > 1000) & (shear > 20) # cape [], shear [m/s]
            if verbose: print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),': writing conv dataset', flush=True)
            print(conv_dataset, flush = True)
            conv_dataset.to_netcdf(conv_fname, mode='w') # mode='w' leads to overwriting


        #### Step 5c: analyse spatial coherence (blobs) to define SCE
        if verbose: print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),': reading data', flush=True)
        ticc = timeit.default_timer()
        conv_source = xr.open_dataset(conv_fname, engine="h5netcdf")
        cp_r, conv = xr.align(sfc_data_1.tot_cp, conv_source.conv_EU, join="exact")

        if verbose: print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),': retrieving object properties', flush=True)
        maxtime=len(conv.valid_time)
        if maxtime == 0:
            raise ValueError(f"No timesteps found for requested years {run_tag}")

        chunk_size = 100
        min_object_size = 50
        property_columns = [
            'time', 'itime', 'label', 'size', 'precip', 'a_precip',
            'ilat', 'ilon', 'lat', 'lon',
        ]
        sce_prefix = os.path.join(
            out_directory,
            f'03_era5-pressure-levels_UTCDaily.SCE.{run_tag}',
        )
        csv_chunk_files = []
        nc_chunk_files = []
        tracked_regions_chunk = np.zeros(
            (
                min(chunk_size, maxtime),
                conv.sizes['latitude'],
                conv.sizes['longitude'],
            ),
            dtype=np.int32,
        )
        latitude_values = conv.latitude.values
        longitude_values = conv.longitude.values
        valid_time_values = conv.valid_time.values

        for chunk_start in range(0, maxtime, chunk_size):
            chunk_tic = timeit.default_timer()
            chunk_stop = min(chunk_start + chunk_size, maxtime)
            chunk_length = chunk_stop - chunk_start
            tracked_regions_chunk.fill(0)
            tracked_property_records = []

            # Read 100 days in one scheduler call. This avoids both the former
            # per-object reads and one Dask round trip per field and day.
            conv_chunk, cp_chunk = dask.compute(
                conv.isel(valid_time=slice(chunk_start, chunk_stop)).data,
                cp_r.isel(valid_time=slice(chunk_start, chunk_stop)).data,
            )
            conv_chunk = np.asarray(conv_chunk, dtype=bool)
            cp_chunk = np.asarray(cp_chunk)

            for chunk_t in range(chunk_length):
                t = chunk_start + chunk_t
                conv_obj = label(conv_chunk[chunk_t])
                cp_t = cp_chunk[chunk_t]

                for region in regionprops(conv_obj):
                    size = int(region.area)
                    if size < min_object_size:
                        continue

                    # Work inside the object's bounding box instead of constructing
                    # a full-domain boolean mask for every object.
                    object_mask = region.image
                    cp_values = cp_t[region.slice][object_mask]
                    precip = float(np.nansum(cp_values))
                    if precip == 0:
                        continue

                    tracked_region = tracked_regions_chunk[chunk_t][region.slice]
                    tracked_region[object_mask] = region.label

                    ilat, ilon = region.centroid
                    ilat2 = int(np.clip(np.rint(ilat), 0, len(latitude_values) - 1))
                    ilon2 = int(np.clip(np.rint(ilon), 0, len(longitude_values) - 1))

                    tracked_property_records.append({
                        'time': valid_time_values[t],
                        'itime': t,
                        'label': int(region.label),
                        'size': size,
                        'precip': precip,
                        'a_precip': int(np.count_nonzero(cp_values > 0)),
                        'ilat': float(ilat),
                        'ilon': float(ilon),
                        'lat': latitude_values[ilat2],
                        'lon': longitude_values[ilon2],
                    })

            # Flush the current 100-day buffers to CSV and NetCDF.
            chunk_end = chunk_stop - 1
            csv_chunk_fname = f'{sce_prefix}.{chunk_end:05d}.csv'
            nc_chunk_fname = f'{sce_prefix}.{chunk_end:05d}.nc'

            tracked_properties = pd.DataFrame.from_records(
                tracked_property_records,
                columns=property_columns,
            )
            tracked_properties.to_csv(csv_chunk_fname, index=False)
            tracked_regions_xr = xr.DataArray(
                tracked_regions_chunk[:chunk_length],
                coords=conv.isel(valid_time=slice(chunk_start, chunk_stop)).coords,
                dims=conv.dims,
                name="SCE_ID",
            )
            tracked_regions_xr.to_netcdf(nc_chunk_fname, mode='w')
            csv_chunk_files.append(csv_chunk_fname)
            nc_chunk_files.append(nc_chunk_fname)

            chunk_toc = timeit.default_timer()
            print(
                f'Timesteps {chunk_start}-{chunk_end} out of {maxtime}, '
                f'duration {(chunk_toc - chunk_tic) / 60:.2f} min',
                flush=True,
            )
            print(csv_chunk_fname, flush=True)
            print(nc_chunk_fname, flush=True)

        if verbose: print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),': saving final data', flush=True)
        tocc = timeit.default_timer()
        print('total script time '+str((tocc-ticc)/60)+' min', flush=True)



        # MERGE DATA
        print('merging data', flush=True)
        
        tracked_properties = pd.concat(
            [pd.read_csv(path) for path in csv_chunk_files],
            ignore_index=True,
        )
        print(tracked_properties.shape, flush=True)
        tracked_properties.to_csv(f'{sce_prefix}.all-steps.csv', index=False)

        expected_valid_time = valid_time_values.copy()
        conv_source.close()
        sfc_data_1.close()

        with xr.open_mfdataset(
            nc_chunk_files,
            combine='nested',
            concat_dim='valid_time',
            join='exact',
        ) as data:
            # Check that every requested timestep occurs exactly once and in order.
            if not data.indexes["valid_time"].is_monotonic_increasing:
                raise ValueError("Output valid_time is not monotonic")
            if data.indexes["valid_time"].has_duplicates:
                raise ValueError("Output contains duplicate valid_time values")
            if not np.array_equal(data.valid_time.values, expected_valid_time):
                raise ValueError("Output valid_time does not match the requested input period")
            print(data, flush=True)
            data.to_netcdf(f'{sce_prefix}.all-steps.nc', mode='w')

        # plt.figure()
        # data.sel(longitude = slice(0,30), latitude = slice(65,25)).isel(valid_time=181)
        # data.sel(longitude = slice(0,30), latitude = slice(65,25)).isel(valid_time=181)['SCE'].plot();
        # data.sel(longitude = slice(0,30), latitude = slice(65,25)
        #                 ).sel(valid_time="2022-06-27")['SCE'].plot();
        # plt.savefig('figure_SCE.png')

                        # # For output..... TODO
                        # compr_level = 5
                        # chunk_size_definition = {
                        # 'valid_time': 4,   # in weekly resolution = 4 weeks
                        # 'latitude':   18,  # if 1-deg resolution = 18 deg
                        # 'longitude':  36,
                        # 'pressure_level': 3, # NOTE: only used for pressure level ERA5 (UNUSED FOR PREDICTORS)
                        # }
