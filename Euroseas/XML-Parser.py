# -*- coding: utf-8 -*-
"""
Created on Mon May 10 16:59:22 2021

@author: skevin
"""

import pandas as pd
#import xml.etree.ElementTree as ET
import xmltodict
#import json
  
# Load XML
filename = 'Mongstad buoy east Record 7469.xml'
with open(filename) as fd:
    xml = xmltodict.parse(fd.read())

# Load Sensor Data
time = xml['Device']['Data']['Time']
sensorDataList = xml['Device']['Data']['SensorData']

sensorRaw = pd.DataFrame();
df = pd.DataFrame();

for i in range(len(sensorDataList)):
    sensorID = sensorDataList[i]['@ID']
    sensorName = sensorDataList[i]['@ProdName']
    lat = ''
    long = ''
    if '@GeoPosition' in sensorDataList[i]:
        lat = sensorDataList[i]['@GeoPosition'].split(',')[0]
        long = sensorDataList[i]['@GeoPosition'].split(',')[1]
    
    df = pd.DataFrame(sensorDataList[i]['Parameters']['Point'])
    df.insert(0,'Sensor_ID',sensorID)
    df.insert(1,'Sensor_Name',sensorName)
    df.insert(2,'Time',time)
    df.insert(3,'Latitude',lat)
    df.insert(4,'Longitude',long)
    if sensorDataList[i]['@ProdName']=='Gill MaxiMet':
        df['Latitude'] = df.loc[df['@Descr']=='GPS Latitude'].Value.values[0]
        df['Longitude'] = df.loc[df['@Descr']=='GPS longitude'].Value.values[0]
    sensorRaw = sensorRaw.append(df)

sensorRaw.columns = sensorRaw.columns.str.lstrip('@')
#print(sensorRaw)
#sensorRaw.to_csv("sensorCombined.csv", index = False)

# Split info and data

## Sensor Info
sensorInfo = sensorRaw[['Sensor_ID', 'Sensor_Name', 'ID', 'Descr', 'Type', 'Unit', 'RangeMin', 'RangeMax']]
#print(sensorInfo)
sensorInfo.to_csv("sensorInfo.csv", index = False)

## Sensor Data
sensorData = sensorRaw[['Sensor_ID', 'Time', 'Latitude', 'Longitude','Sensor_Name', 'Descr', 'Value']]
sensorData['Sensor_Column'] = sensorData['Sensor_Name'] + ' ' + sensorData['Descr']
sensorData['Sensor_Column'] = sensorData['Sensor_Column'].replace(' ', '_', regex=True)
sensorData = sensorData[['Sensor_ID', 'Time', 'Latitude', 'Longitude','Sensor_Column', 'Value']]
#sensorData = sensorData.pivot_table(index=['Sensor_ID', 'Time', 'Latitude', 'Longitude'], 
#                                    columns='Sensor_Column', values='Value', aggfunc='first').reset_index()
print(sensorData)
sensorData.to_csv("sensorData.csv", index = False)

# Load System Data

systemData = xml['Device']['Data']['SystemData']

#print(systemData['@Descr'])
systemDf = pd.DataFrame(systemData['Parameters']['Point'])
#systemDf.to_csv("systemData.csv", index = False)