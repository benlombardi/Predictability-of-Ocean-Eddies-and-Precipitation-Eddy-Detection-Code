import netCDF4
from netCDF4 import Dataset
import os
import datetime
from py_eddy_tracker.dataset.grid import UnRegularGridDataset, RegularGridDataset
import time
import argparse

def daily_detection(path,outfile_list,in_dir,out_dir):
    date = datetime.datetime(int(path[7:11]),int(path[12:14]),int(path[15:17]))
    if date.strftime("ac_%Y%m%d.nc") in outfile_list:
        return
    else:
        print(str(date))
        g = RegularGridDataset(in_dir + "/" + path,"lon","lat")
        g.bessel_high_filter("ssh",700,order=1)

        # a, c = g.eddy_identification("ssh", "u", "v", date, 0.002, shape_error=70,pixel_limit=(5, 1000),sampling=20,presampling_multiplier=10,
        #                              sampling_method="visvalingam",nb_step_min=2, nb_step_to_be_mle=0)
        a, c = g.eddy_identification("ssh", "u", "v", date, 0.002, shape_error=70,pixel_limit=(31, 6250),sampling=20,presampling_multiplier=10,
                                     sampling_method="visvalingam",nb_step_min=2, nb_step_to_be_mle=0) 
        with Dataset(date.strftime(out_dir + "/ac/ac_%Y%m%d.nc"), "w") as h:
            a.to_netcdf(h)
        with Dataset(date.strftime(out_dir + "/cyc/cyc_%Y%m%d.nc"), "w") as h:
            c.to_netcdf(h)
    return

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', dest='num', default=0)
    parser.add_argument('--in_dir',dest='in_dir',default=0)
    parser.add_argument('--out_dir',dest='out_dir',default=0)
    
    args = parser.parse_args()
    
    file_num = int(args.num)
    start = time.time()
    file_list = os.listdir(args.in_dir)
    file_list = file_list[file_num:]
    print(len(file_list))
    outfile_list = os.listdir(args.out_dir + "/ac")
    for path in file_list:
        daily_detection(path,outfile_list,args.in_dir,args.out_dir)
