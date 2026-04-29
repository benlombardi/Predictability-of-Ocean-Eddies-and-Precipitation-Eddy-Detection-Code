import matplotlib.pyplot as plt
from matplotlib import ticker
import numpy as np
import json
import pandas as pd
import pickle
import xarray as xr
import cartopy.crs as ccrs
import matplotlib.gridspec as gridspec
import matplotlib.colors as mcolors

def compute_max(data):
    max_list = []
    for entry in data: 
        q1 = np.nanpercentile(entry,25)
        q3 = np.nanpercentile(entry,75)
        iqr = q3 - q1
        max_list.append(q3 + 1.5* iqr)
    return np.nanmax(max_list)

def compute_min(data):
    min_list = []
    for entry in data: 
        q1 = np.nanpercentile(entry,25)
        q3 = np.nanpercentile(entry,75)
        iqr = q3 - q1
        min_list.append(q1 - 1.5* iqr)
    return np.nanmin(min_list)

def compute_fdr(pvals,alpha_fdr):
    mask = ~np.isnan(pvals)
    pvals_post = pvals[mask]
    pvals_post.sort()
    N = len(pvals_post)

    p_max_list =[]

    for i in range(N):
        if pvals_post[i] <= (i+1)/N *alpha_fdr:
            p_max_list.append(pvals_post[i])

    p_fdr = np.max(p_max_list)
    return p_fdr

def make_paper_plot(mean_meshes,pvals,title_quant,cmaps,cbar_units,outfile,alpha_fdr = 0.1):
    proj = ccrs.PlateCarree()
    fig = plt.figure(figsize=(16, 8))
    gs = gridspec.GridSpec(2, 5, height_ratios=[1, 1], width_ratios=[1,1,0.15,1,0.15],wspace=0.5)

    # First row: histograms

    # Second row: cartopy maps
    axes_map = [fig.add_subplot(gs[0, 0], projection=proj),fig.add_subplot(gs[0, 1], projection=proj),fig.add_subplot(gs[1,0], projection=proj),fig.add_subplot(gs[1,1], projection=proj)]

    # Third row: Significance
    axes_sig = [fig.add_subplot(gs[0, 3], projection=proj),fig.add_subplot(gs[1, 3], projection=proj)]


    labels = ["ADT","POP","ADT","POP"]
    for i in range(4):
        if i < 2:
            cmap = cmaps[0]
            c_max = compute_max([mean_meshes[0],mean_meshes[1]])
            c_max = 0.5
        else:
            cmap = cmaps[1]
            c_max = compute_max([mean_meshes[2],mean_meshes[3]])
        # Add new subplot with Cartopy projection in the same position
        pos =axes_map[i].pcolormesh(np.arange(0,360)+0.5,np.arange(-90,90)+0.5,
            mean_meshes[i].T,
            transform=proj,
            linewidth=0,
            cmap=cmap,
            vmax = c_max
            )


        #ax_map.set_title(title_quant, fontsize=22)
        # Active meridian/parallel
        g1 = axes_map[i].gridlines(draw_labels=True)
        g1.xlabel_style = {'size':8}
        g1.ylabel_style = {'size':8}
        axes_map[i].set_extent([-85, 0, 25, 60], crs=ccrs.PlateCarree())
        # Active coastline
        axes_map[i].coastlines()
        axes_map[i].set_title(labels[i],fontsize=15)

        if i == 0:
            colorbar_ax = plt.subplot(gs[0,2]) 
            box = colorbar_ax.get_position()
            new_height = 0.5*box.height

            # Compute the new y0 to center the shorter bar vertically
            new_y0 = box.y0 + (box.height - new_height) / 2

            # Apply the new position
            colorbar_ax.set_position([0.99*box.x0, new_y0, 0.5*box.width, new_height])

            cb = fig.colorbar(pos, cax=colorbar_ax, orientation='vertical',label=cbar_units[0],extend="max")
            tick_locator = ticker.MaxNLocator(nbins=5)
            cb.locator = tick_locator
            cb.update_ticks()

        if i == 2:
            colorbar_ax = plt.subplot(gs[1,2]) 
            box = colorbar_ax.get_position()
            new_height = 0.4*box.height

            # Compute the new y0 to center the shorter bar vertically
            new_y0 = box.y0 + (box.height - new_height) / 2

            # Apply the new position
            colorbar_ax.set_position([0.99*box.x0, new_y0, 0.5*box.width, new_height])

            cb = fig.colorbar(pos, cax=colorbar_ax, orientation='vertical',label=cbar_units[1],extend="max")
            tick_locator = ticker.MaxNLocator(nbins=5)
            cb.locator = tick_locator
            cb.update_ticks()

    ################### Making sig figs

    c_max = compute_max([mean_meshes[0] - mean_meshes[1]])
    c_min = compute_min([mean_meshes[0] - mean_meshes[1]])

    print(c_min)
    print(c_max)


    p_fdr = compute_fdr(pvals[0],alpha_fdr)

    divnorm = mcolors.TwoSlopeNorm(vmin=c_min, vcenter=0, vmax=c_max)

    pos = axes_sig[0].pcolormesh(np.arange(0, 360) + 0.5, np.arange(-90, 90) + 0.5,
                        mean_meshes[0].T - mean_meshes[1].T,
                        transform=proj,
                        linewidth=0,
                        cmap="coolwarm",norm=divnorm)

    hatch_mask = np.where(pvals[0] > p_fdr,1,np.nan)

    axes_sig[0].contour(np.arange(0, 360) + 0.5,np.arange(-90, 90) + 0.5,pvals[0].T,levels=[p_fdr],colors="k",transform=proj,)
    axes_sig[0].contourf(np.arange(0, 360) + 0.5,np.arange(-90, 90) + 0.5,hatch_mask.T,levels=[0.5,1.5],transform=proj,hatches=["..."],colors='darkgrey')

    g1 = axes_sig[0].gridlines(draw_labels=True)
    g1.xlabel_style = {'size': 8}
    g1.ylabel_style = {'size': 8}
    axes_sig[0].coastlines()
    axes_sig[0].set_extent([-85, 0, 25, 60], crs=ccrs.PlateCarree())
    axes_sig[0].set_title("E) ADT and FOSI Difference",fontsize=15)
    colorbar_ax = plt.subplot(gs[0, -1]) 
    box = colorbar_ax.get_position()
    new_height = box.height

    # Compute the new y0 to center the shorter bar vertically
    new_y0 = box.y0 + (box.height - new_height) / 2

    # Apply the new position
    colorbar_ax.set_position([0.99*box.x0, 1.2*new_y0, 0.5*box.width, 0.5*new_height])

    cb = fig.colorbar(pos, cax=colorbar_ax, orientation='vertical',label=cbar_units[0],extend="both")
    tick_locator = ticker.MaxNLocator(nbins=5)
    cb.locator = tick_locator
    cb.update_ticks()

    
    p_fdr = compute_fdr(pvals[1],alpha_fdr)
    c_max = compute_max([mean_meshes[2] - mean_meshes[3]])
    c_min = compute_min([mean_meshes[2] - mean_meshes[3]])
    print(c_min)
    print(c_max)

    divnorm = mcolors.TwoSlopeNorm(vmin=c_min, vcenter=0, vmax=c_max)

    pos = axes_sig[1].pcolormesh(np.arange(0, 360) + 0.5, np.arange(-90, 90) + 0.5,
                        mean_meshes[2].T - mean_meshes[3].T,
                        transform=proj,
                        linewidth=0,
                        cmap="coolwarm",norm=divnorm)

    hatch_mask = np.where(pvals[1] > p_fdr,1,np.nan)

    axes_sig[1].contour(np.arange(0, 360) + 0.5,np.arange(-90, 90) + 0.5,pvals[1].T,levels=[p_fdr],colors="k",transform=proj,)
    axes_sig[1].contourf(np.arange(0, 360) + 0.5,np.arange(-90, 90) + 0.5,hatch_mask.T,levels=[0.5,1.5],transform=proj,hatches=["..."],colors='darkgrey')

    g1 = axes_sig[1].gridlines(draw_labels=True)
    g1.xlabel_style = {'size': 8}
    g1.ylabel_style = {'size': 8}
    axes_sig[1].coastlines()
    axes_sig[1].set_extent([-85, 0, 25, 60], crs=ccrs.PlateCarree())
    axes_sig[1].set_title("F) ADT and FOSI Difference",fontsize=15)

    colorbar_ax = plt.subplot(gs[1, -1]) 
    box = colorbar_ax.get_position()
    new_height = box.height

    # Compute the new y0 to center the shorter bar vertically
    new_y0 = box.y0 + (box.height - new_height) / 2

    # Apply the new position
    colorbar_ax.set_position([0.99*box.x0, 1.8*new_y0, 0.5*box.width, 0.5*new_height])

    cb = fig.colorbar(pos, cax=colorbar_ax, orientation='vertical',label=cbar_units[1],extend="both")

    tick_locator = ticker.MaxNLocator(nbins=5)
    cb.locator = tick_locator
    cb.update_ticks()

    # fig.text(0.095, 0.33, 'Local Cyc Mean Maps', va='center', rotation='vertical', fontsize=12)
    fig.suptitle(title_quant,fontsize=30,y=0.93)
    plt.savefig(outfile,bbox_inches="tight")
    plt.close('all')


make_paper_plot([np.load("PATH_TO_OBS_AMP_MEAN"),np.load("PATH_TO_MODEL_AMP_MEAN"),np.load("PATH_TO_OBS_RADIUS_MEAN")/1000,np.load("PATH_TO_MODEL_RADIUS_MEAN")/1000],
                [np.load("PATH_TO_AMP_PVALS"),np.load("PATH_TO_RADIUS_PVALS")],
                "Amplitude (fist row) and Radius (second row)",["plasma","viridis"], ["m","km"], "na_test3.png")
