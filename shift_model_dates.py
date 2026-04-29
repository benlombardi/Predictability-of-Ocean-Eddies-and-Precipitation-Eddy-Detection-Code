import os
import xarray as xr
import argparse
import datetime


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--in_dir", dest="in_dir",default=0)
    parser.add_argument("--out_dir", dest="out_dir",default=0)
    args = parser.parse_args()

    in_dir = args.in_dir
    out_dir = args.out_dir 

    cyc_dir = in_dir + "/cyc/"
    ac_dir = in_dir + "/ac/"

    cyc_out = out_dir + "/cyc/"
    ac_out = out_dir + "/ac/"


    file_list_cyc = os.listdir(cyc_dir)
    file_list_cyc.sort()

    file_list_ac = os.listdir(ac_dir)
    file_list_ac.sort()

    N = len(file_list_cyc)
    start_date = datetime.datetime(1993, 1, 1)
    print(N)

    for i in range(N):
        new_date = start_date + datetime.timedelta(days=i)
        cyc_data = xr.open_dataset(cyc_dir + file_list_cyc[i])
        cyc_data.to_netcdf(new_date.strftime(cyc_out + 'cyc_%Y%m%d.nc'))
        ac_data = xr.open_dataset(ac_dir + file_list_ac[i])
        ac_data.to_netcdf(new_date.strftime(ac_out + 'ac_%Y%m%d.nc'))
        
        if i % 500 == 0:
            print("Files Shifted: " + str(i))