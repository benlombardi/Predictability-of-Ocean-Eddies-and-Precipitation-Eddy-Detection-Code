import xarray as xr 
import numpy as np 
import matplotlib.pyplot as plt
import argparse
import json
import pickle
import dask
from scipy.spatial import cKDTree
import scipy

def build_tree(lon, lat):
    coords = np.column_stack((lon, lat))
    return cKDTree(coords)

def compute_box(tree, dataset, query_point, lon_list, lat_list, box_radius = np.sqrt(8)):
    idx = tree.query_ball_point(query_point, box_radius)
    q_lon, q_lat = query_point
    filt_idx = [i for i in idx if ( q_lon - 1 <= lon_list[i] <= q_lon+2 and q_lat-1 <= lat_list[i] <= q_lat + 2)]
    
    if not filt_idx:
        return [[],[],[],[]]
    #values = np.array([dataset[i] for i in filt_idx])

    box_amp = np.array([dataset.amplitude.values[i] for i in filt_idx])

    box_shape = np.array([dataset.speed_contour_shape_error.values[i] for i in filt_idx])
    
    box_area = np.array([np.sqrt(dataset.speed_area.values[i]/np.pi) for i in filt_idx])

    box_speed = np.array([dataset.speed_average.values[i] for i in filt_idx])

    return [box_amp, box_shape, box_area, box_speed]

@dask.delayed
def process_grid_point(lon, lat):
    pvals = [0 for i in range(4)]
    query_point = (lon,lat)
    box1s = compute_box(tree_c, dataset_c_long, query_point, c_lon, c_lat)
    box2s = compute_box(tree_ac, dataset_ac_long, query_point, ac_lon, ac_lat)
    for i in range(4):
        if len(box1s[i]) ==0 or len(box2s[i])==0:
            pvals[i] = np.nan
        else:
            t_stat, pvals[i] = scipy.stats.ttest_ind(box1s[i],box2s[i],equal_var=False)
    return (lon, lat, pvals)

@dask.delayed
def get_pvals(lon,lat):
    box_c_amp = []
    box_c_area = []
    box_c_shape = []
    box_c_speed = []
    box_ac_amp = []
    box_ac_area = []
    box_ac_shape = []
    box_ac_speed = []
    pvals = [0 for i in range(4)]
    for i in range(len(dataset_c_long)):
        if lon == 0:
            if (359 <= c_lon[i] or c_lon[i]< 2) and lat-1 <= c_lat[i] <= lat +2:
                box_c_amp.append(dataset_c_long.amplitude.values[i])
                box_c_area.append(np.sqrt(dataset_c_long.speed_area.values[i]/np.pi))
                box_c_shape.append(dataset_c_long.speed_contour_shape_error.values[i])
                box_c_speed.append(dataset_c_long.speed_average.values[i])
        elif lon == 359: 
            if (358 <= c_lon[i] or c_lon[i]< 1) and lat-1 <= c_lat[i] <= lat +2:
                box_c_amp.append(dataset_c_long.amplitude.values[i])
                box_c_area.append(np.sqrt(dataset_c_long.speed_area.values[i]/np.pi))
                box_c_shape.append(dataset_c_long.speed_contour_shape_error.values[i])
                box_c_speed.append(dataset_c_long.speed_average.values[i])
        else:
            if lon-1 <= c_lon[i] <= lon+2 and lat-1 <= c_lat[i] <=lat+2:
                box_c_amp.append(dataset_c_long.amplitude.values[i])
                box_c_area.append(np.sqrt(dataset_c_long.speed_area.values[i]/np.pi))
                box_c_shape.append(dataset_c_long.speed_contour_shape_error.values[i])
                box_c_speed.append(dataset_c_long.speed_average.values[i])
    
    for i in range(len(dataset_ac_long)):
        if lon == 0:
            if (359 <= ac_lon[i] or ac_lon[i]< 2) and lat-1 <= ac_lat[i] <= lat +2:
                box_ac_amp.append(dataset_ac_long.amplitude.values[i])
                box_ac_area.append(np.sqrt(dataset_ac_long.speed_area.values[i]/np.pi))
                box_ac_shape.append(dataset_ac_long.speed_contour_shape_error.values[i])
                box_ac_speed.append(dataset_ac_long.speed_average.values[i])
        elif lon == 359: 
            if (358 <= ac_lon[i] or ac_lon[i]< 1) and lat-1 <= ac_lat[i] <= lat +2:
                box_ac_amp.append(dataset_ac_long.amplitude.values[i])
                box_ac_area.append(np.sqrt(dataset_ac_long.speed_area.values[i]/np.pi))
                box_ac_shape.append(dataset_ac_long.speed_contour_shape_error.values[i])
                box_ac_speed.append(dataset_ac_long.speed_average.values[i])
        else:
            if lon-1 <= ac_lon[i] <= lon+2 and lat-1 <= ac_lat[i] <=lat+2:
                box_ac_amp.append(dataset_ac_long.amplitude.values[i])
                box_ac_area.append(np.sqrt(dataset_ac_long.speed_area.values[i]/np.pi))
                box_ac_shape.append(dataset_ac_long.speed_contour_shape_error.values[i])
                box_ac_speed.append(dataset_ac_long.speed_average.values[i])

    box1s = [box_c_amp, box_c_shape, box_c_area, box_c_speed]
    box2s = [box_ac_amp, box_ac_shape, box_ac_area, box_ac_speed]

    for i in range(4):
        if len(box1s[i]) ==0 or len(box2s[i])==0:
            pvals[i] = np.nan
        else:
            t_stat, pvals[i] = scipy.stats.ttest_ind(box1s[i],box2s[i],equal_var=False)
    return (lon, lat, pvals)
    

parser = argparse.ArgumentParser()
parser.add_argument('--data1', dest='cyc', default=0)
parser.add_argument('--data2', dest='ac', default=0)
parser.add_argument('--outfile', dest='outfile', default="global")
args = parser.parse_args()

# Getting Cyclonic Data
dataset_c_long = xr.open_dataset(args.cyc)
dataset_c_long = dataset_c_long.drop_vars(['effective_contour_latitude','effective_contour_longitude','uavg_profile','speed_contour_latitude','speed_contour_longitude'])
dataset_c_long["longitude"] = xr.where(dataset_c_long.longitude > 360, dataset_c_long.longitude -360, dataset_c_long.longitude)
dataset_c_long["longitude"] = xr.where(dataset_c_long.longitude < 0, dataset_c_long.longitude +360, dataset_c_long.longitude)

# Getting Anticyclonic Data
dataset_ac_long = xr.open_dataset(args.ac)
dataset_ac_long = dataset_ac_long.drop_vars(['effective_contour_latitude','effective_contour_longitude','uavg_profile','speed_contour_latitude','speed_contour_longitude'])
dataset_ac_long["longitude"] = xr.where(dataset_ac_long.longitude > 360, dataset_ac_long.longitude -360, dataset_ac_long.longitude)
dataset_ac_long["longitude"] = xr.where(dataset_ac_long.longitude < 0, dataset_ac_long.longitude +360, dataset_ac_long.longitude)

c_lon = dataset_c_long.longitude.values
c_lat = dataset_c_long.latitude.values
ac_lon = dataset_ac_long.longitude.values
ac_lat = dataset_ac_long.latitude.values

print("cyc")
tree_c = build_tree(dataset_c_long.longitude.values, dataset_c_long.latitude.values)
print("ac")
tree_ac = build_tree(dataset_ac_long.longitude.values, dataset_ac_long.latitude.values)

print("Trees Built")

map_amp_p = np.zeros((360,180))

map_area_p = np.zeros((360,180))

map_speed_p = np.zeros((360,180))

map_shape_p = np.zeros((360,180))

c_lonenter = np.arange(0,360)
c_latenter = np.arange(-90,90)
tasks = [process_grid_point(lon,lat) 
        for lon in c_lonenter
        for lat in c_latenter
        ]

print("Assigned Tasks")

results = dask.compute(*tasks)

for lon, lat, pvals in results:
    map_amp_p[lon][lat+90] = pvals[0]
    map_shape_p[lon][lat+90] = pvals[1]
    map_area_p[lon][lat+90] = pvals[2]
    map_speed_p[lon][lat+90] = pvals[3]


print("Doing Edge cases")
tasks = [get_pvals(lon,lat) 
        for lon in [0,359]
        for lat in c_latenter
        ]

print("Assigned Tasks")

results = dask.compute(*tasks)
print("Doing Edge cases")
for lon, lat, pvals in results:
    map_amp_p[lon][lat+90] = pvals[0]
    map_shape_p[lon][lat+90] = pvals[1]
    map_area_p[lon][lat+90] = pvals[2]
    map_speed_p[lon][lat+90] = pvals[3]

np.save(args.outfile + "_amp_pval.npy",map_amp_p)
np.save(args.outfile + "_shape_pval.npy",map_shape_p)
np.save(args.outfile + "_area_pval.npy",map_area_p)
np.save(args.outfile + "_speed_pval.npy",map_speed_p)

