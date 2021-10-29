# -*- coding: utf-8 -*-
"""
Created on Tue Mar  9 09:50:41 2021

@author: skevin
"""

import pandas as pd
import math as m
import xml.etree.ElementTree as ET
import xmltodict
import glob
import os
from datetime import datetime, timedelta

def round_minutes(dt):
    resolution = timedelta(minutes=10)
    dt = datetime.strptime(dt.split('.')[0], '%d-%b-%Y %H:%M:%S')
    dt = dt + (datetime.min - dt) % resolution
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')

def fetchDataFromERDDAP(start_datetime,end_datetime):
    # Set ERDDAP server details
    s = 'https://erddap.marine.ie/erddap' #'http://10.11.1.82/erddap'
    p = 'tabledap'
    r = 'csv'
    dataset_id = 'compass_mace_head'
    
    param = ['sbe_temp_avg','sbe_salinity_avg']

    # Generate parameter component of URL
    plist = ''
    metadata = ['time']
    for item in metadata + param:
        plist = plist+item+'%2C'
    plist = plist[0:-3]

    # Create dataframe for population
    df = pd.DataFrame()

    # Create ERDDAP url and load data for selected dates  
    url = s+"/"+p+"/"+dataset_id+"."+r+"?"+plist+"&time%3E="+start_datetime+"&time%3C"+end_datetime
    print(url)
    df= pd.read_csv(url,header=[0],skiprows=[1],infer_datetime_format=True)

    return df

#Functions to calculate pH
def internal_ph(t,V,k0,k2):
    
    t = float(t)
    V = float(V)
    #Convert to K
    T = t + 273.15
    
    ## Nernst Equation
    sn = (R*T*m.log(10))/F
    
    pH = (V-k0-(k2*T))/sn
    
    return round(pH,4)

def external_ph(t,V,k0,k2,S):
    
    t = float(t)
    #t = round(t,2)
    T = t + 273.15 #Convert to K
    V = float(V)
    
    ## Nernst Equation
    sn = (R*T*m.log(10))/F
    
    ## Sample Iconic Strength
    I = (19.924*S)/(1000-(1.005*S))
    
    ##Debye-Huckel Constant
    ADH = (0.00000343*m.pow(t,2)) + (0.00067524*t) + 0.49172143
    
    ##Total Chloride in seawater
    CLT = (0.99889/35.453)*(S/1.80655)*(1000/(1000-1.005*S))
    
    ##Logarithm of HCL activity coefficient
    logHCL = (-ADH*m.sqrt(I))/(1+(1.394*m.sqrt(I))) + (0.08885-(0.000111*t))*I
    
    ##Acid dissociation constant of HSO4
    p1 = -4276.1/T
    p2 = 141.328
    p3 = 23.093*m.log(T)
    p4 = ((-13856/T)+324.57-(47.986*m.log(T)))*m.sqrt(I)
    p5 = ((35474/T)-771.54+(114.723)*m.log(T))*I
    p6 = (2698/T)*m.pow(I,1.5)
    p7 = (1776/T)*m.pow(I,2)
    
    power = p1+p2-p3+p4+p5-p6+p7
    
    KS = (1-(0.001005*S))*m.exp(power)
    
    ##Total sulfate in seawater
    ST = (0.1400/96.062)*(S/1.80655)
    
    
    pH = (V-k0-(k2*T))/sn + m.log(CLT,10) + 2*logHCL - m.log((1+(ST/KS)),10) - m.log10((1000-(1.005*S))/1000)
    
    return round(pH,4)

#filename = '19011000.csv'
#try:
    
# Step 1: get a list of all file(s) in target directory
path = r'Archived\Combined\*'
allFiles = glob.glob(path + ".csv")

fileList = []

for files in allFiles:
    fileList.append(files)
    
combined_df = pd.DataFrame()
dfs = []
    
for file in fileList:

    f = open(file)
    filename = os.path.splitext(file)[0]
    
    metaDataList = []
    xmlList = []
    dataList = []
    counter = 0
    
    for row in f:
        if counter < 8:
            metaDataList.append(row.rstrip())
        elif counter >= 8 and counter < 297:
            xmlList.append(row.rstrip())
        else:
            dataList.append(row.rstrip())
        counter+=1
    
    
    dfMetaData = pd.DataFrame([row.split(",") for row in metaDataList])
    dfMetaData.columns = ['FrameHeader', 'Coeff', 'Value']
    
    xmlstring = ''.join(xmlList)
    myxml = ET.fromstring(xmlstring.lower())
    
    xml_dict = xmltodict.parse(xmlstring.lower())
    
    colNames = []
    
    for i in range(len(xml_dict['instrumentpackage']['instrument']['varasciiframe']['sensorfieldgroup'])):
        colNames.append(xml_dict['instrumentpackage']['instrument']['varasciiframe']['sensorfieldgroup'][i]['name'])
    
    dfData = pd.DataFrame([row.split(",") for row in dataList])
    
    colNames.insert(0,'FrameHeader')
    dfData.columns = [c.upper() for c in colNames]
    
    dfData = dfData.drop(columns = ['TIME'])
    dfData = dfData.rename(columns={"DATE":"TIME"})
    dfData["TIME"] = dfData.TIME.apply(lambda row: round_minutes(row))
    #dfData.to_csv('rawData.csv',index=False)
    
    #Caliberation Coeeficients
    k0i = float(dfMetaData[dfMetaData['Coeff']=='CAL_PHINT_OFFSET_COEFF']['Value'])
    k2i = float(dfMetaData[dfMetaData['Coeff']=='CAL_PHINT_SLOPE_COEFF']['Value'])
    
    k0e = float(dfMetaData[dfMetaData['Coeff']=='CAL_PHEXT_OFFSET_COEFF']['Value'])
    k2e = float(dfMetaData[dfMetaData['Coeff']=='CAL_PHEXT_SLOPE_COEFF']['Value'])
    
    #Onboard Salinity
    S = float(dfMetaData[dfMetaData['Coeff']=='ONBOARD_SALINITY_PSU']['Value'])
    #S = 37.385
    
    ## Universal Constants
    R = 8.3144621 #Gas Constant
    F = 96485.365 #Faraday Constant
    
    #Get Seabird Temperature and Salinity data
    minDate = dfData.TIME.min()
    maxDate = dfData.TIME.max()
    sbeData = fetchDataFromERDDAP(minDate, maxDate)
    sbeData = sbeData.rename(columns={"time":"TIME","sbe_temp_avg":"SBE_TEMP","sbe_salinity_avg":"SBE_SALINITY"})
    
    
    dfUpdated = pd.merge(dfData,sbeData,on=["TIME"])
    
    dfUpdated.to_csv('rawData.csv',index=False)
    
    dfProcessed = dfUpdated.iloc[:,:5].copy()
    dfProcessed['NEW_PH_INT'] = dfUpdated.apply(lambda row: internal_ph(row['SBE_TEMP'], row['VOLT_INT'], k0i, k2i), axis=1)
    dfProcessed['NEW_PH_EXT'] = dfUpdated.apply(lambda row: external_ph(row['SBE_TEMP'], row['VOLT_EXT'], k0e, k2e, row['SBE_SALINITY']), axis=1)
    dfProcessed['PRO_TEMP'] = dfUpdated['SBE_TEMP']
    dfProcessed['PRO_SAL'] = dfUpdated['SBE_SALINITY']
    dfProcessed['CTD_OXYGEN'] = dfUpdated['CTD_OXYGEN']
    dfProcessed['STATUS'] = dfUpdated['STATUS']
    dfProcessed['CHECK'] = dfUpdated['CHECK']
    
    dfs.append(dfProcessed)
    

#Save final 
combined_dataFrames = pd.concat(dfs, ignore_index=True)
combined_dataFrames.to_csv('Processed/Combined_all.csv', index=False)
