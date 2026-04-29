import xarray as xr
import matplotlib.pyplot
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import matplotlib.gridspec as gridspec
import numpy as np
import pickle
import dask
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--cyc_data', dest='c_data', default=0)
parser.add_argument('--ac_data', dest='ac_data', default=0)
parser.add_argument('--outfile', dest='outfile', default=0)
args = parser.parse_args()

dataset_c_long = xr.open_dataset(args.c_data)
dataset_ac_long = xr.open_dataset(args.ac_data)

dataset_c_long["longitude"] = xr.where(dataset_c_long.longitude > 360, dataset_c_long.longitude -360, dataset_c_long.longitude)
dataset_c_long["longitude"] = xr.where(dataset_c_long.longitude < 0, dataset_c_long.longitude +360, dataset_c_long.longitude)

dataset_ac_long["longitude"] = xr.where(dataset_ac_long.longitude > 360, dataset_ac_long.longitude -360, dataset_ac_long.longitude)
dataset_ac_long["longitude"] = xr.where(dataset_ac_long.longitude < 0, dataset_ac_long.longitude +360, dataset_ac_long.longitude)

adt_cyc_data = dataset_c_long.sel(obs=(dataset_c_long.longitude >= 275) & (dataset_c_long.latitude >=25) & (dataset_c_long.latitude <=60) & (dataset_c_long.observation_number ==0))
adt_cyc_data = dataset_c_long.isel(obs=np.in1d(dataset_c_long.track, adt_cyc_data.track))
adt_ac_data = dataset_ac_long.sel(obs=(dataset_ac_long.longitude >= 275) & (dataset_ac_long.latitude >=25) & (dataset_ac_long.latitude <=60) & (dataset_ac_long.observation_number ==0))
adt_ac_data = dataset_ac_long.isel(obs=np.in1d(dataset_ac_long.track, adt_ac_data.track))

for i in range(5):
    subset_cyc = adt_cyc_data.sel(obs=(adt_cyc_data.observation_number + 1 >= (i+1)*30))
    subset_cyc = adt_cyc_data.isel(obs=np.in1d(adt_cyc_data.track, subset_cyc.track))
    
    subset_ac = adt_ac_data.sel(obs=(adt_ac_data.observation_number + 1 >= (i+1)*30))
    subset_ac = adt_ac_data.isel(obs=np.in1d(adt_ac_data.track, subset_ac.track))

    subset_cyc = subset_cyc.chunk({"obs":10**5})
    subset_ac = subset_ac.chunk({"obs":10**5})
    subset_cyc.to_netcdf(args.outfile + "/Cyclonic_" + str(i+1) + "MB.nc",encoding={"time": {}})
    subset_ac.to_netcdf(args.outfile + "/Anticyclonic_" + str(i+1) + "MB.nc",encoding={"time": {}})