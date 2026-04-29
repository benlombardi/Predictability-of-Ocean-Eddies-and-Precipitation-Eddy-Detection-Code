import xesmf as xe
import numpy as np
import xarray as xr
import os
import argparse 
from datetime import datetime

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', dest='start',default=0)
    parser.add_argument('--end', dest='end',default=-1)
    parser.add_argument('--out_dir', dest='out_dir',default=0)
    args = parser.parse_args()

    file_list = os.listdir("YOUR_IN_DIR")

    infile = file_list[0]

    data = xr.open_dataset("YOUR_IN_DIR/" + infile)
    data = data[["adt","ugos","vgos"]]
    data = data.sel(time=data.time.values[0])

    data = data.rename({"latitude":"lat","longitude":"lon"})

    lat_out = np.arange(-89.95, 90.0, 0.1)  # centers of 0.25° grid cells
    lon_out = np.arange(-179.95,180,0.1)   # 0 to 360 if your data uses 0-360

    ds_out = xr.Dataset({
        'lat': (['lat'], lat_out.data),
        'lon': (['lon'], lon_out.data)
        })

    regridder = xe.Regridder(data["adt"],ds_out,method="bilinear", periodic=True,weights="adt_regrid_weights.nc")

    outfile_list = os.listdir(args.out_dir)

    for infile in file_list:
        data = xr.open_dataset("YOUR_IN_DIR/" + infile)
        date = data.time.values[0]
        if "regrid_" +str(date)[0:10] + ".nc" in outfile_list:
            pass
        else:
            data = data[["adt","ugos","vgos"]]
            data = data.sel(time=data.time.values[0])
            data = data.rename({"latitude":"lat","longitude":"lon"})

            ssh_regridded = regridder(data["adt"])
            u_regridded = regridder(data["ugos"])
            v_regridded = regridder(data["vgos"])

            regridded_data = xr.Dataset({"ssh": ssh_regridded,"u": u_regridded,"v": v_regridded},coords={"lon":ds_out.lon,"lat":ds_out.lat},attrs={'description':'Cartesian Regridding of OM4','author':'Ben Lombardi (blombardi@ucar.edu)','time': str(date)})
            regridded_data.ssh.attrs["units"] = "meter"
            regridded_data.u.attrs["units"] = "m/s"
            regridded_data.v.attrs["units"] = "m/s"

            encoding = {
                        "ssh": {"zlib": True, "complevel": 6, "dtype": "float32"},
                        "u":   {"zlib": True, "complevel": 6, "dtype": "float32"},
                        "v":   {"zlib": True, "complevel": 6, "dtype": "float32"},
                    }

            regridded_data.to_netcdf(args.out_dir + "/regrid_" +str(date)[0:10] + ".nc",encoding=encoding,engine="netcdf4")

