import xarray as xr 
import numpy as np 
import matplotlib.pyplot as plt
import argparse
import json
import pickle
import dask
from scipy.spatial import cKDTree

def build_tree(lon, lat):
    coords = np.column_stack((lon, lat))
    return cKDTree(coords)

def compute_box_stats(tree, dataset, query_point, lon_list, lat_list, box_radius = np.sqrt(8)):
    idx = tree.query_ball_point(query_point, box_radius)
    q_lon, q_lat = query_point
    filt_idx = [i for i in idx if ( q_lon - 1 <= lon_list[i] <= q_lon+2 and q_lat-1 <= lat_list[i] <= q_lat + 2)]
    
    if not filt_idx:
        return [np.nan,np.nan,np.nan,np.nan], [np.nan,np.nan,np.nan,np.nan]
    #values = np.array([dataset[i] for i in filt_idx])

    box = np.array([dataset.amplitude.values[i] for i in filt_idx])
    amp_mean = np.nanmean(box)
    amp_var = np.nanvar(box)

    box = np.array([dataset.speed_contour_shape_error.values[i] for i in filt_idx])
    shape_mean = np.nanmean(box)
    shape_var = np.nanvar(box)
    
    box = np.array([np.sqrt(dataset.speed_area.values[i]/np.pi) for i in filt_idx])
    area_mean = np.nanmean(box)
    area_var = np.nanvar(box)

    box = np.array([dataset.speed_average.values[i] for i in filt_idx])
    speed_mean = np.nanmean(box)
    speed_var = np.nanvar(box)

    return [amp_mean,shape_mean,area_mean,speed_mean], [amp_var,shape_var,area_var,speed_var]

@dask.delayed
def process_grid_point(lon, lat):
    query_point = (lon,lat)
    c_mean, c_var = compute_box_stats(tree_c, dataset_c, query_point, c_lon, c_lat)
    ac_mean, ac_var = compute_box_stats(tree_ac, dataset_ac, query_point, ac_lon, ac_lat)

    return (lon, lat, c_mean,c_var,ac_mean,ac_var)

@dask.delayed
def get_ratio_params(lon,lat):
    box_c_amp = []
    box_c_area = []
    box_c_shape = []
    box_c_speed = []
    box_ac_amp = []
    box_ac_area = []
    box_ac_shape = []
    box_ac_speed = []
    for i in range(len(dataset_c)):
        if lon == 0:
            if (359 <= c_lon[i] or c_lon[i]<= 2) and lat-1 <= c_lat[i] <= lat +2:
                box_c_amp.append(dataset_c.amplitude.values[i])
                box_c_area.append(np.sqrt(dataset_c.speed_area.values[i]/np.pi))
                box_c_shape.append(dataset_c.speed_contour_shape_error.values[i])
                box_c_speed.append(dataset_c.speed_average.values[i])
        elif lon == 359: 
            if (358 <= c_lon[i] or c_lon[i]<= 1) and lat-1 <= c_lat[i] <= lat +2:
                box_c_amp.append(dataset_c.amplitude.values[i])
                box_c_area.append(np.sqrt(dataset_c.speed_area.values[i]/np.pi))
                box_c_shape.append(dataset_c.speed_contour_shape_error.values[i])
                box_c_speed.append(dataset_c.speed_average.values[i])
        else:
            if lon-1 <= c_lon[i] <= lon+2 and lat-1 <= c_lat[i] <=lat+2:
                box_c_amp.append(dataset_c.amplitude.values[i])
                box_c_area.append(np.sqrt(dataset_c.speed_area.values[i]/np.pi))
                box_c_shape.append(dataset_c.speed_contour_shape_error.values[i])
                box_c_speed.append(dataset_c.speed_average.values[i])
    
    for i in range(len(dataset_ac)):
        if lon == 0:
            if (359 <= ac_lon[i] or ac_lon[i]<= 2) and lat-1 <= ac_lat[i] <= lat +2:
                box_ac_amp.append(dataset_ac.amplitude.values[i])
                box_ac_area.append(np.sqrt(dataset_ac.speed_area.values[i] / np.pi))
                box_ac_shape.append(dataset_ac.speed_contour_shape_error.values[i])
                box_ac_speed.append(dataset_ac.speed_average.values[i])
        elif lon == 359: 
            if (358 <= ac_lon[i] or ac_lon[i]<= 1) and lat-1 <= ac_lat[i] <= lat +2:
                box_ac_amp.append(dataset_ac.amplitude.values[i])
                box_ac_area.append(np.sqrt(dataset_ac.speed_area.values[i] / np.pi))
                box_ac_shape.append(dataset_ac.speed_contour_shape_error.values[i])
                box_ac_speed.append(dataset_ac.speed_average.values[i])
        else:
            if lon-1 <= ac_lon[i] <= lon+2 and lat-1 <= ac_lat[i] <=lat+2:
                box_ac_amp.append(dataset_ac.amplitude.values[i])
                box_ac_area.append(np.sqrt(dataset_ac.speed_area.values[i]/np.pi))
                box_ac_shape.append(dataset_ac.speed_contour_shape_error.values[i])
                box_ac_speed.append(dataset_ac.speed_average.values[i])

    c_mean = [np.mean(box_c_amp),np.mean(box_c_shape),np.mean(box_c_area),np.mean(box_c_speed)]
    c_var = [np.var(box_c_amp),np.var(box_c_shape),np.var(box_c_area),np.var(box_c_speed)]
    ac_mean = [np.mean(box_ac_amp),np.mean(box_ac_shape),np.mean(box_ac_area),np.mean(box_ac_speed)]
    ac_var = [np.var(box_ac_amp),np.var(box_ac_shape),np.var(box_ac_area),np.var(box_ac_speed)]
    return (lon, lat, c_mean, c_var, ac_mean, ac_var)
    

parser = argparse.ArgumentParser()
parser.add_argument('--cyc_data', dest='cyc', default=0)
parser.add_argument('--ac_data', dest='ac', default=0)
parser.add_argument('--outfile', dest='outfile', default="global")
args = parser.parse_args()

# Getting Cyclonic Data
dataset_c = xr.open_dataset(args.cyc)
dataset_c = dataset_c.drop_vars(['effective_contour_latitude','effective_contour_longitude','uavg_profile','speed_contour_latitude','speed_contour_longitude'])
dataset_c["longitude"] = xr.where(dataset_c.longitude > 360, dataset_c.longitude -360, dataset_c.longitude)
dataset_c["longitude"] = xr.where(dataset_c.longitude < 0, dataset_c.longitude +360, dataset_c.longitude)

# Getting Anticyclonic Data
dataset_ac = xr.open_dataset(args.ac)
dataset_ac = dataset_ac.drop_vars(['effective_contour_latitude','effective_contour_longitude','uavg_profile','speed_contour_latitude','speed_contour_longitude'])
dataset_ac["longitude"] = xr.where(dataset_ac.longitude > 360, dataset_ac.longitude -360, dataset_ac.longitude)
dataset_ac["longitude"] = xr.where(dataset_ac.longitude < 0, dataset_ac.longitude +360, dataset_ac.longitude)

c_lon = dataset_c.longitude.values
c_lat = dataset_c.latitude.values
ac_lon = dataset_ac.longitude.values
ac_lat = dataset_ac.latitude.values

print("cyc")
tree_c = build_tree(dataset_c.longitude.values, dataset_c.latitude.values)
print("ac")
tree_ac = build_tree(dataset_ac.longitude.values, dataset_ac.latitude.values)

print("Trees Built")

map_c_amp_mean = np.zeros((360,180))
map_c_amp_var = np.zeros((360,180))
map_ac_amp_mean = np.zeros((360,180))
map_ac_amp_var = np.zeros((360,180))

map_c_shape_mean = np.zeros((360,180))
map_c_shape_var = np.zeros((360,180))
map_ac_shape_mean = np.zeros((360,180))
map_ac_shape_var = np.zeros((360,180))

map_c_area_mean = np.zeros((360,180))
map_c_area_var = np.zeros((360,180))
map_ac_area_mean = np.zeros((360,180))
map_ac_area_var = np.zeros((360,180))

map_c_speed_mean = np.zeros((360,180))
map_c_speed_var = np.zeros((360,180))
map_ac_speed_mean = np.zeros((360,180))
map_ac_speed_var = np.zeros((360,180))

lon_center = np.arange(1,359)
lat_center = np.arange(-90,90)
tasks = [process_grid_point(lon,lat) 
        for lon in lon_center
        for lat in lat_center
        ]

print("Assigned Tasks")

results = dask.compute(*tasks)

for lon, lat, c_mean, c_var, ac_mean, ac_var in results:
    map_c_amp_mean[lon][lat+90] = c_mean[0]
    map_ac_amp_mean[lon][lat+90] = ac_mean[0]
    map_c_amp_var[lon][lat+90] = c_var[0]
    map_ac_amp_var[lon][lat+90] = ac_var[0]

    map_c_shape_mean[lon][lat+90] = c_mean[1]
    map_ac_shape_mean[lon][lat+90] = ac_mean[1]
    map_c_shape_var[lon][lat+90] = c_var[1]
    map_ac_shape_var[lon][lat+90] = ac_var[1]

    map_c_area_mean[lon][lat+90] = c_mean[2]
    map_ac_area_mean[lon][lat+90] = ac_mean[2]
    map_c_area_var[lon][lat+90] = c_var[2]
    map_ac_area_var[lon][lat+90] = ac_var[2]

    map_c_speed_mean[lon][lat+90] = c_mean[3]
    map_ac_speed_mean[lon][lat+90] = ac_mean[3]
    map_c_speed_var[lon][lat+90] = c_var[3]
    map_ac_speed_var[lon][lat+90] = ac_var[3]

tasks = [get_ratio_params(lon,lat) 
        for lon in [0,359]
        for lat in lat_center
        ]

print("Assigned edge cases")
results = dask.compute(*tasks)

for lon, lat, c_mean, c_var, ac_mean, ac_var in results:
    map_c_amp_mean[lon][lat+90] = c_mean[0]
    map_ac_amp_mean[lon][lat+90] = ac_mean[0]
    map_c_amp_var[lon][lat+90] = c_var[0]
    map_ac_amp_var[lon][lat+90] = ac_var[0]

    map_c_shape_mean[lon][lat+90] = c_mean[1]
    map_ac_shape_mean[lon][lat+90] = ac_mean[1]
    map_c_shape_var[lon][lat+90] = c_var[1]
    map_ac_shape_var[lon][lat+90] = ac_var[1]

    map_c_area_mean[lon][lat+90] = c_mean[2]
    map_ac_area_mean[lon][lat+90] = ac_mean[2]
    map_c_area_var[lon][lat+90] = c_var[2]
    map_ac_area_var[lon][lat+90] = ac_var[2]

    map_c_speed_mean[lon][lat+90] = c_mean[3]
    map_ac_speed_mean[lon][lat+90] = ac_mean[3]
    map_c_speed_var[lon][lat+90] = c_var[3]
    map_ac_speed_var[lon][lat+90] = ac_var[3] 


np.save(args.outfile + "_amp_cyc_mean.npy",map_c_amp_mean)
np.save(args.outfile + "_amp_ac_mean.npy",map_ac_amp_mean)
np.save(args.outfile + "_amp_cyc_var.npy",map_c_amp_var)
np.save(args.outfile + "_amp_ac_var.npy",map_ac_amp_var)

np.save(args.outfile + "_area_cyc_mean.npy",map_c_area_mean)
np.save(args.outfile + "_area_ac_mean.npy",map_ac_area_mean)
np.save(args.outfile + "_area_cyc_var.npy",map_c_area_var)
np.save(args.outfile + "_area_ac_var.npy",map_ac_area_var)

np.save(args.outfile + "_shape_cyc_mean.npy",map_c_shape_mean)
np.save(args.outfile + "_shape_ac_mean.npy",map_ac_shape_mean)
np.save(args.outfile + "_shape_cyc_var.npy",map_c_shape_var)
np.save(args.outfile + "_shape_ac_var.npy",map_ac_shape_var)

np.save(args.outfile + "_speed_cyc_mean.npy",map_c_speed_mean)
np.save(args.outfile + "_speed_ac_mean.npy",map_ac_speed_mean)
np.save(args.outfile + "_speed_cyc_var.npy",map_c_speed_var)
np.save(args.outfile + "_speed_ac_var.npy",map_ac_speed_var)
