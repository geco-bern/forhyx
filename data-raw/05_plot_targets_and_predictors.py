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
    era5land_predictor_files = sorted(glob.glob(input_dir+'/FORHYX.predictors.era5land.*.2008-2008.nc')) # TODO: update years
    ds_predictors_era5land = xr.open_mfdataset(
        era5land_predictor_files,
        combine='by_coords',
        compat='no_conflicts',
        join='exact',
        chunks="auto", parallel =True
    )

    era5_predictor_files = glob.glob(input_dir+'/FORHYX.predictors.era5p*.2009-2009.nc' # TODO: update years
        ) + glob.glob(input_dir+'/FORHYX.predictors.era5s*.2009-2009.nc')               # TODO: update years
    ds_predictors_era5 = xr.open_mfdataset( 
        era5_predictor_files,
        combine='by_coords',
        compat='no_conflicts',
        join='exact',
        chunks="auto", parallel =True
    )
    # TODO: ensure coordinates are the same:
    # TODO: xr.concat([ds_predictors_era5, ds_predictors_era5land])

    # Read the monthly Jones/CRU NAO scalar predictor.
    nao_csv_path = os.path.join(
        input_dir, 'FORHYX.predictors.nao_monthly.csv'
    )
    df_nao = pd.read_csv(
        nao_csv_path, parse_dates=['time'], index_col='time'
    ).sort_index()

    ### Step 2: Read targets
    csv_path_sce  = input_dir+'FORHYX.targets.sce_regional_weekly.csv'
    csv_path_pcwd = input_dir+'FORHYX.targets.pcwd_regional_weekly.csv'

    # Read CSV as a time-indexed DataFrame
    df_sce  = pd.read_csv(csv_path_sce, parse_dates=['time'], index_col='time')
    df_pcwd = pd.read_csv(csv_path_pcwd, parse_dates=['time'], index_col='time')

    ### Step 3: Create and save plot
    #### Step 3a: plot maps for a chosen date and show that date on CSV time series
    
    date_to_plot = pd.to_datetime('2022-07-01') # choose a date to visualise

    # Crop the NAO index to the same time extent
    plot_start = min(df_sce.index.min(), df_pcwd.index.min())
    plot_end = max(df_sce.index.max(), df_pcwd.index.max())
    df_nao_plot = df_nao.loc[plot_start:plot_end]




    # TODO: issue with mean_swvl1,min_t2m: fig = plt.figure()
    # TODO: issue with mean_swvl1,min_t2m: #ds_predictors_era5land["mean_swvl1"].sel(valid_time=date_to_plot, method='nearest').plot() # TODO: issue with mean_swvl1 and min_t2m
    # TODO: issue with mean_swvl1,min_t2m: #ds_predictors_era5land["min_t2m"].sel(valid_time=date_to_plot, method='nearest').plot() # TODO: issue with mean_swvl1 and min_t2m
    # TODO: issue with mean_swvl1,min_t2m: #ds_predictors_era5land["mean_d2m"].sel(valid_time=date_to_plot, method='nearest').plot()
    # TODO: issue with mean_swvl1,min_t2m: # ds_predictors_era5land["mean_swvl1"].isel(valid_time=1).values # TODO: issue with mean_swvl1 and min_t2m: only nan!
    # TODO: issue with mean_swvl1,min_t2m: # ds_predictors_era5land["min_t2m"].isel(valid_time=1).plot() # works
    # TODO: issue with mean_swvl1,min_t2m: ds_predictors_era5land["min_t2m"].isel(valid_time=126).plot() # works
    # TODO: issue with mean_swvl1,min_t2m: fig.savefig('test.png', dpi=150, bbox_inches='tight')


    # select nearest time in the xarray dataset
    # sel_time_era5     = ds_predictors_era5.valid_time.sel(valid_time=date_to_plot, method='nearest')
    # sel_time_era5land = ds_predictors_era5land.valid_time.sel(valid_time=date_to_plot, method='nearest')

    # pick predictor variables to map (first 3 variables if available)
    vars_to_map = list(ds_predictors_era5land.data_vars) + list(ds_predictors_era5.data_vars)

    n_maps = len(vars_to_map)
    ncols_maps = 3
    nrows_maps = int(np.ceil(n_maps/ncols_maps))
    nrows_ts = 3 # number of rows for time series plots

    fig = plt.figure(figsize=(5 * ncols_maps, (nrows_maps+nrows_ts)*2.5))
    gs = gridspec.GridSpec(
        nrows_maps+nrows_ts, ncols_maps,
        height_ratios=(
            [7.5 for it in range(nrows_maps)] + [2.5, 2.5, 2.5]
        ),
        hspace=0.3,
    )

    # i) map rows
    for i, var in enumerate(vars_to_map):
        # use Cartopy PlateCarree projection to preserve lon/lat aspect
        ax_pos = (0+i//ncols_maps, i%ncols_maps) #  for i in [0,1,2,3,4]]
        ax = fig.add_subplot(gs[*ax_pos], projection=ccrs.PlateCarree())
        try:
            # select by time coordinate
            if var == "mean_sst" or var == "mean_siconc":
                da = ds_predictors_era5[var].sel(valid_time=date_to_plot, method='nearest')
            else:
                da = ds_predictors_era5land[var].sel(valid_time=date_to_plot, method='nearest')
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
            # cbar_label = "%s [%s]" % (da.attrs["long_name"], da.attrs["units"])
            cbar_label = "[" + da.attrs["units"] + "]"
            cbar = fig.colorbar(pcm, ax=ax, label = cbar_label, orientation='horizontal', pad=0.06, fraction=0.05)
        except Exception as e:
            ax.text(0.5, 0.5, f'Error plotting {var}: {e}', ha='center')

    # ii) middle row: monthly scalar predictor
    ax_ts_nao = fig.add_subplot(gs[nrows_maps+0, :])
    ax_ts_nao.plot(
        df_nao_plot.index, df_nao_plot['nao'],
        color='tab:purple', linewidth=1.2, label='Jones/CRU NAO',
    )
    ax_ts_nao.axhline(0, color='black', linewidth=0.8, alpha=0.6)

    # iii) bottom rows: target time series spanning all columns
    ax_ts_sce  = fig.add_subplot(gs[nrows_maps+1, :], sharex=ax_ts_nao)
    ax_ts_pcwd = fig.add_subplot(gs[nrows_maps+2, :], sharex=ax_ts_sce)

    # plot specified regions from CSV with discrete colors
    region_cols = ['region_10'] # use df_sce.columns.tolist() for all regions
    colors = plt.get_cmap('tab20')(np.linspace(0, 1, max(1, len(region_cols))))

    for i, col in enumerate(region_cols):
        ax_ts_sce.plot(df_sce.index, df_sce[col], color=colors[i], label=col)
    for i, col in enumerate(region_cols):
        ax_ts_pcwd.plot(df_pcwd.index, df_pcwd[col], color=colors[i], label=col)

    
    # vertical red line at the selected date
    ax_ts_nao.axvline(date_to_plot, color='red', linestyle='--', linewidth=2, label=f'selected date: {pd.to_datetime(date_to_plot).date()}')
    ax_ts_sce.axvline( date_to_plot, color='red', linestyle='--', linewidth=2, label=f'selected date: {pd.to_datetime(date_to_plot).date()}')
    ax_ts_pcwd.axvline(date_to_plot, color='red', linestyle='--', linewidth=2, label=f'selected date: {pd.to_datetime(date_to_plot).date()}')

    ax_ts_nao.set_title('Monthly Jones/CRU North Atlantic Oscillation index')
    ax_ts_sce.set_title( 'SCE weekly area fraction by region')
    ax_ts_pcwd.set_title('PCWD weekly average by region')
    #ax_ts_sce.set_xlabel('Time')
    ax_ts_nao.set_ylabel('NAO index')
    ax_ts_sce.set_ylabel( 'Area fraction')
    ax_ts_pcwd.set_ylabel('PCWD (mm)')
    ax_ts_nao.grid(alpha=0.3)
    ax_ts_sce.grid( alpha=0.3)
    ax_ts_pcwd.grid(alpha=0.3)
    ax_ts_nao.legend(ncol=2, loc='upper left', bbox_to_anchor=(0.02, 0.98))
    ax_ts_sce.legend( title='region', ncol=1, loc='upper left', bbox_to_anchor=(0.02, 0.98))

    #fig.tight_layout()
    fig.savefig('maps_and_timeseries_date_to_plot.png', dpi=150, bbox_inches='tight')
    plt.close(fig)
