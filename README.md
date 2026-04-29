# Predictability-of-Ocean-Eddies-and-Precipitation-Eddy-Detection-Code

This repository contains the code used to generate the plot found in (link paper here).

There are three main steps to running the code. 

1) Running the Detection and Tracking Algorithm:
    - Before we can run the detection and tracking algorithm we first need to regrid the model data to a regular lat-lon grid. This is accomplished by the regrid.py script. We only used data post 01/01/1993 for a fair comparison with the Satellite era. We also run shift_model_dates.py after regridding for the model data. The model data does not contain leap years. As the tracking algorithm has problems with the irregular time-stepping caused by this lack of leap years, this script shifts the dates to account for this.  
    - Also you need to regrid the ssh data to the same 0.1 degree grid used for the model data. This is accomplished by running the regrid_ssh.py script. We use run this on the ADT field of the Sea Level DT 2024 dataset.
    - Once the daily ssh files have been regridded, you can run the dets.py script. This script uses the [py-eddy-tracker](https://py-eddy-tracker.readthedocs.io/en/v3.3.1/) algorithm version 3.6.1 and the same detection and tracking parameters of the META 3.2 DT datasets.
    - To run the tracking code you can run EddyTracking conf_*.yaml via the command line. This will build to the eddy datasets.
2) Subset the data and build meshes:
    - First run iso_obs_data.py to subset the eddy datasets into subsets containing eddies that live longer than 1-5 months.
    - Then run build_att_mesh.py on both the model data and adt data. For this paper we ran this script on the datasets that contain eddies that live longer than 2 months.
    - Then run att_sig.py passing in both the ADT and model dataset to test for the significance of these differences.
3) Create the figure
    - run na_paper_plots.py
    - Currently this script is conf
