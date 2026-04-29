import xarray as xr
import xesmf as xe
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
import os
import cartopy.crs as ccrs
import cftime
from dateutil.relativedelta import relativedelta


if __name__ == "__main__":
    print("Making regridders")
    path="/glade/campaign/cgd/oce/projects/FOSI/HR/g.e21.GIAF.TL319_t13.5thCyc.ice.001/ocn/hist/"
    dataset = xr.open_dataset("/glade/campaign/cgd/oce/projects/FOSI/HR/g.e21.GIAF.TL319_t13.5thCyc.ice.001/ocn/hist/g.e21.GIAF.TL319_t13.5thCyc.ice.001.pop.h.nday1.0257-11-01.nc")
    dataset = dataset[["SSH_2","V1_1","U1_1"]]
    ds = dataset.sel(time=dataset.time.values[0])
    ds = ds.rename({"nlon":"x","nlat":"y"})
    ssh = ds["SSH_2"]
    u_vel = ds["U1_1"]
    v_vel = ds["V1_1"]
    
    # U grid coordinates
    lon_U= ds['ULONG']
    lat_U = ds['ULAT']
    
    # T grid coordinates
    lon_T= ds['TLONG']
    lat_T = ds['TLAT']
    
    # Make sure the dimensions match (e.g., ('j', 'i'))
    ssh = ssh.assign_coords({
        'lon': (('y', 'x'), lon_T.data),
        'lat': (('y', 'x'), lat_T.data)
    })
    
    u_vel = u_vel.assign_coords({
        'lon': (('y', 'x'), lon_U.data),
        'lat': (('y', 'x'), lat_U.data)
    })
    
    v_vel = v_vel.assign_coords({
        'lon': (('y', 'x'), lon_U.data),
        'lat': (('y', 'x'), lat_U.data)
    })
    
    # Regular grid: 0.25 degree spacing
    lat_out = np.arange(-89.875, 90.0, 0.1)  # centers of 0.25° grid cells
    lon_out = np.arange(-179.875,180,0.1)   # 0 to 360 if your data uses 0-360
    
    ds_out = xr.Dataset({
        'lat': (['lat'], lat_out.data),
        'lon': (['lon'], lon_out.data)
    })
    
    ds_in_ssh = xr.Dataset({
        'lat': (('y', 'x'), ssh['lat'].data),
        'lon': (('y', 'x'), ssh['lon'].data)
    })
    
    ds_in_vel = xr.Dataset({
        'lat': (('y', 'x'), u_vel['lat'].data),
        'lon': (('y', 'x'), u_vel['lon'].data)
    })
    
    regridder_bi_ssh = xe.Regridder(ds_in_ssh, ds_out, method='bilinear',periodic=True)
    regridder_bi_vel = xe.Regridder(ds_in_vel, ds_out, method='bilinear',periodic=True)
    print("Regridders done")

    dir_path = "/glade/campaign/cgd/oce/projects/FOSI/HR/g.e21.GIAF.TL319_t13.5thCyc.ice.001/ocn/hist/"
    files = os.listdir(dir_path)
    files_clean = []
    for file in files:
        if "nday1" in file:
            if int(file[-12:-9])>= 281 and int(file[-12:-9]) <= 309:
                files_clean.append(file)
    print("Files prepped")
    print("Starting")

    outfile_list = os.listdir("YOUR_OUT_DIR")
    k = 0
    for file in files_clean:
        dataset = xr.open_dataset(path +file)
        dataset = dataset[["SSH_2","V1_1","U1_1"]]
        print(file)
        print(dataset.time.values)
        for day in dataset.time.values:
            print(day)
            date = datetime.fromisoformat(day.isoformat())
            date = date + relativedelta(years=1712)
            if "regrid_" +str(date)[0:10] + ".nc" in outfile_list:
                pass
            else:
                print(day)
                ds = dataset.sel(time=day)
                ds = ds.rename({"nlon":"x","nlat":"y"})
                ssh = ds["SSH_2"]
                u_vel = ds["U1_1"]
                v_vel = ds["V1_1"]
                ssh = ssh.assign_coords({
                    'lon': (('y', 'x'), lon_T.data),
                    'lat': (('y', 'x'), lat_T.data)
                })
                
                u_vel = u_vel.assign_coords({
                    'lon': (('y', 'x'), lon_U.data),
                    'lat': (('y', 'x'), lat_U.data)
                })
                
                v_vel = v_vel.assign_coords({
                    'lon': (('y', 'x'), lon_U.data),
                    'lat': (('y', 'x'), lat_U.data)
                })
    
                ssh_regridded = regridder_bi_ssh(ssh,skipna=True)
                u_regridded = regridder_bi_vel(u_vel,skipna=True)
                v_regridded = regridder_bi_vel(v_vel,skipna=True)
                regridded_data = xr.Dataset({"ssh": ssh_regridded/100,"u": u_regridded/100,"v": v_regridded/100},coords={"lon":ds_out.lon,"lat":ds_out.lat},attrs={'description':'Cartesian Regridding of OM4','author':'Ben Lombardi (blombardi@ucar.edu)','time': str(date)})
                regridded_data.ssh.attrs["units"] = "meter"
                regridded_data.u.attrs["units"] = "m/s"
                regridded_data.v.attrs["units"] = "m/s"
                regridded_data.to_netcdf("YOUR_OUT_DIR/regrid_" +str(date)[0:10] + ".nc")
