#!/usr/bin/python

'''
  This program allows to generate a dataset in FIWARE-NGSI v2 format
  containing Traffic Incident data adapted to the data models
  defined by the GSMA IDE project (IoT and Big Data Ecosystem)

  The input data must be an XML file as described by: 

  http://opendata.euskadi.eus/catalogo/-/incidencias-trafico-euskadi/

  Here you can find a full description of the data fields (in Spanish):

  http://opendata.euskadi.eus/contenidos/ds_general/incidencias_trafico/es_trafico/adjuntos/tutorial_trafico.pdf

  The original data source is the Open Data portal of the Basque Country Government (Spain)
  and is subject to the following license:

  http://creativecommons.org/licenses/by/3.0/es/

  Additional legal terms (in Spanish) can be found at:

  http://opendata.euskadi.eus/informacion-legal/r42-440/es/

  Disclaimer: Data content has not been normalized (TODO)

  Usage: traffic-incidences.py <XML input file>

'''

import sys
import xml.etree.ElementTree as ET
import uuid
import json

tree = ET.parse(sys.argv[1])
root = tree.getroot()

def value(obj, item, val):
  if type(val) is str:
    obj[item] = val
    return

  if val is not None and hasattr(val, 'text'):
    if val.text is not None:
      obj[item] = val.text

def number(obj, item, val):
 if val is not None and hasattr(val, 'text'):
    if val.text is not None:
      obj[item] = float(val.text)

def date(obj, item, val):
  obj[item] = { 'type': 'date', 'value': val }

dataset = []

for child in root:
  obj = { 'id': str(uuid.uuid1()), 'type': 'TrafficIncidence' }
  
  value(obj, 'category',        child.find('tipo'))
  value(obj, 'cause',           child.find('causa'))
  value(obj, 'severity',        child.find('nivel'))
  value(obj, 'roadId',          child.find('carretera'))
  value(obj, 'travelDirection', child.find('sentido'))

  value(obj, 'description',     child.find('nombre'))

  number(obj, 'initialKilometer', child.find('pk_inicial'))
  number(obj, 'endKilometer',     child.find('pk_final'))

  latitude =  child.find('latitud').text
  longitude = child.find('longitud').text
  if latitude and longitude and latitude != '0.0' and longitude != '0.0':
    obj['location'] = { 'type': 'geo:point', 'crs': 'WGS84' }
    value(obj['location'], 'value', latitude + ',' + longitude)

  datesNode = child.find('fechahora_ini')
  if datesNode is not None:
    datesStr = datesNode.text
    dates = datesStr.split(' - ', 2)
    if len(dates) == 2:
      date(obj, 'startDate', dates[0])
      date(obj, 'endDate',   dates[1])
    else:
      date(obj, 'startDate', datesStr.replace(' ','T') + '+02:00')

  dataset.append(obj)


print json.dumps(dataset, indent=2, separators=(',', ': '))
