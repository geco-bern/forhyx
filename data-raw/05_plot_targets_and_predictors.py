# aggregate targets over week and aggregate over region

### Step 1: Import Libraries 
# Import the required libraries:
import os, fnmatch
import pprint
import xarray as xr
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import cartopy.crs as ccrs
# from dask.distributed import Client, LocalCluster
# import dask
import glob

if __name__ == '__main__':

    # predictors: '/storage/capacity/occr_geco/data_2/scratch/fbernhard/FORHYX/FORHYX.predictors.era5land.max_t2m.2016-2025.nc'
    # predictors: '/storage/capacity/occr_geco/data_2/scratch/fbernhard/FORHYX/FORHYX.predictors.era5land.mean_d2m.2016-2025.nc'
    # predictors: '/storage/capacity/occr_geco/data_2/scratch/fbernhard/FORHYX/FORHYX.predictors.era5land.mean_t2m.2016-2025.nc'
    # predictors: '/storage/capacity/occr_geco/data_2/scratch/fbernhard/FORHYX/FORHYX.predictors.era5land.min_t2m.2016-2025.nc'
    # predictors: TODO: era5-land: snowc
    # predictors: TODO: era5: 500.z
    # predictors: TODO: era5: si, sst

    # targets: '/storage/capacity/occr_geco/data_2/scratch/fbernhard/FORHYX/FORHYX.targets.pcwd_regional_weekly.csv'
    # targets: '/storage/capacity/occr_geco/data_2/scratch/fbernhard/FORHYX/FORHYX.targets.sce_regional_weekly.csv'

    input_dir = '/storage/capacity/occr_geco/data_2/scratch/fbernhard/FORHYX/'

    ### Step 1: Open predictors
    era5land_predictor_files = glob.glob(input_dir+'/FORHYX.predictors.era5land*')
    # pprint.pp(era5land_predictor_files)
    ds_predictors_era5land = xr.open_mfdataset(
        era5land_predictor_files,
        combine='by_coords',
        compat='no_conflicts',
        join='exact',
        # join='inner',
        chunks="auto", parallel =True
    )

    # TODO: ensure the dates are the same!
    era5_predictor_files = glob.glob(input_dir+'/FORHYX.predictors.era5p*') + glob.glob(input_dir+'/FORHYX.predictors.era5s*')
    ds_predictors_era5 = xr.open_mfdataset( 
        era5_predictor_files[1],
        combine='by_coords',
        compat='no_conflicts',
        # join='exact',
        join='outer', # TODO: currently outer needed because of different dates due to 7-day average with different starting date
        chunks="auto", parallel =True
    )
    # TODO: ensure coordinates are the same:
    # TODO: xr.concat([ds_predictors_era5, ds_predictors_era5land])

    # TODO: add additional predictors (NAO) to the xarray.DataSet

    ### Step 2: Read targets
    csv_path_sce  = input_dir+'FORHYX.targets.sce_regional_weekly.csv'
    csv_path_pcwd = input_dir+'FORHYX.targets.pcwd_regional_weekly.csv'

    # Read CSV as a time-indexed DataFrame
    df_sce  = pd.read_csv(csv_path_sce, parse_dates=['time'], index_col='time')
    df_pcwd = pd.read_csv(csv_path_pcwd, parse_dates=['time'], index_col='time')
    

    ### Step 3: Create and save plot
    #### Step 3a: plot maps for a chosen date and show that date on CSV time series
    
    date_to_plot = pd.to_datetime('2022-07-01') # choose a date to visualise



    # TODO: issue with mean_swvl1,min_t2m: fig = plt.figure()
    # TODO: issue with mean_swvl1,min_t2m: #ds_predictors["mean_swvl1"].sel(valid_time=date_to_plot, method='nearest').plot() # TODO: issue with mean_swvl1 and min_t2m
    # TODO: issue with mean_swvl1,min_t2m: #ds_predictors["min_t2m"].sel(valid_time=date_to_plot, method='nearest').plot() # TODO: issue with mean_swvl1 and min_t2m
    # TODO: issue with mean_swvl1,min_t2m: #ds_predictors["mean_d2m"].sel(valid_time=date_to_plot, method='nearest').plot()
    # TODO: issue with mean_swvl1,min_t2m: # ds_predictors["mean_swvl1"].isel(valid_time=1).values # TODO: issue with mean_swvl1 and min_t2m: only nan!
    # TODO: issue with mean_swvl1,min_t2m: # ds_predictors["min_t2m"].isel(valid_time=1).plot() # works
    # TODO: issue with mean_swvl1,min_t2m: ds_predictors["min_t2m"].isel(valid_time=126).plot() # works
    # TODO: issue with mean_swvl1,min_t2m: fig.savefig('test.png', dpi=150, bbox_inches='tight')


    # select nearest time in the xarray dataset
    sel_time = ds_predictors.valid_time.sel(valid_time=date_to_plot, method='nearest')

    # pick predictor variables to map (first 3 variables if available)
    vars_to_map = list(ds_predictors.data_vars)

    n_maps = len(vars_to_map)
    ncols_maps = 3
    nrows_maps = int(np.ceil(n_maps/ncols_maps))

    fig = plt.figure(figsize=(5 * ncols_maps, (nrows_maps+2)*2.5))
    gs = gridspec.GridSpec(
        nrows_maps+2, ncols_maps, 
        height_ratios=[7.5 for it in range(nrows_maps)] + [2.5, 2.5], hspace=0.3)
    
    # i) top row: maps
    for i, var in enumerate(vars_to_map):
        # use Cartopy PlateCarree projection to preserve lon/lat aspect
        ax_pos = (i//ncols_maps, i%ncols_maps) #  for i in [0,1,2,3,4]]
        ax = fig.add_subplot(gs[*ax_pos], projection=ccrs.PlateCarree())
        try:
            # select by time coordinate
            da = ds_predictors[var].sel(valid_time=date_to_plot, method='nearest')
            # use pcolormesh for correct georeferenced plotting and no aspect distortion
            pcm = da.plot.pcolormesh(ax=ax, transform=ccrs.PlateCarree(), cmap='viridis', add_colorbar=False)
            ax.coastlines(resolution='50m')
            try:
                ax.set_extent([
                    float(da['longitude'].min()), float(da['longitude'].max()), 
                    float(da['latitude'].min()), float(da['latitude'].max())
                    ], crs=ccrs.PlateCarree())
            except Exception:
                pass
            ax.set_title(f"{var} — {pd.to_datetime(date_to_plot).date()}")
            ax.set_xlabel(None); ax.set_ylabel(None)
            # add horizontal colorbar beneath this axis
            # cbar_label = "%s [%s]" % (ds_predictors[var].attrs["long_name"], ds_predictors[var].attrs["units"])
            cbar_label = "[" + ds_predictors[var].attrs["units"] + "]"
            cbar = fig.colorbar(pcm, ax=ax, label = cbar_label, orientation='horizontal', pad=0.06, fraction=0.05)
        except Exception as e:
            ax.text(0.5, 0.5, f'Error plotting {var}: {e}', ha='center')

    # ii) bottom rows: CSV time series spanning all columns
    ax_ts_sce  = fig.add_subplot(gs[nrows_maps,   :])
    ax_ts_pcwd = fig.add_subplot(gs[nrows_maps+1, :], sharex=ax_ts_sce)

    # plot specified regions from CSV with discrete colors
    region_cols = ['region_10'] # use df_sce.columns.tolist() for all regions
    colors = plt.get_cmap('tab20')(np.linspace(0, 1, max(1, len(region_cols))))

    for i, col in enumerate(region_cols):
        ax_ts_sce.plot(df_sce.index, df_sce[col], color=colors[i], label=col)
    for i, col in enumerate(region_cols):
        ax_ts_pcwd.plot(df_pcwd.index, df_pcwd[col], color=colors[i], label=col)

    
    # vertical red line at the selected date
    ax_ts_sce.axvline( date_to_plot, color='red', linestyle='--', linewidth=2, label=f'selected date: {pd.to_datetime(date_to_plot).date()}')
    ax_ts_pcwd.axvline(date_to_plot, color='red', linestyle='--', linewidth=2, label=f'selected date: {pd.to_datetime(date_to_plot).date()}')

    ax_ts_sce.set_title( 'SCE weekly area fraction by region')
    ax_ts_pcwd.set_title('PCWD weekly average by region')
    #ax_ts_sce.set_xlabel('Time')
    ax_ts_sce.set_ylabel( 'Area fraction')
    ax_ts_pcwd.set_ylabel('PCWD (mm)')
    ax_ts_sce.grid( alpha=0.3)
    ax_ts_pcwd.grid(alpha=0.3)
    ax_ts_sce.legend( title='region', ncol=1, loc='upper left', bbox_to_anchor=(0.02, 0.98))

    #fig.tight_layout()
    fig.savefig('maps_and_timeseries_date_to_plot.png', dpi=150, bbox_inches='tight')
    plt.close(fig)
