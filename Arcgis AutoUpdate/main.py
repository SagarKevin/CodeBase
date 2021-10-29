# -*- coding: utf-8 -*-
"""
Created on Thu Oct 14 10:35:50 2021

@author: skevin
"""

import arcgis
import os 

arcgis_username = 'Sagar.Kevin_MarineInstitute'
arcgis_password = 'Marine200!*'
gis = arcgis.GIS(profile='learn_user', username= arcgis_username, password= arcgis_password)

os.system('python OverwriteFS.py learn_user 6560252a638a4bf59aed4afe4f9287c8 compass_mace_head_csv compass_mace_head.csv')

