# -*- coding: utf-8 -*-
"""
Gets weather forecast from the portuguese meteorological service, ipma.pt
"""

import urllib2
import json
from dateutil import parser
import datetime

iptma_codes = {
  'Lisboa': '1110600',
  'Porto':  '1131200',
  'Aveiro': '1010500'
}

weather_type_dict = {
  '1': 'Clear',
  '2': 'Slightly cloudy',
  '3': 'Partly Cloudy',
  '4': 'Overcast',
  '5': 'High clouds',
  '6': 'Light rain',
  '7': 'Drizzle',
  '11': 'Heavy rain',
  '9':  'Rain'
}

ipma_url = 'https://api.ipma.pt/json/alldata/{}.json'

def get_weather_forecasted_pt(locality):
  source = ipma_url.format(iptma_codes[locality])
  
  req = urllib2.Request(url=source)
  f = urllib2.urlopen(req)
  data = json.loads(f.read())
  
  print data
  out = []
    
  for forecast in data:
    obj = {
      'type': 'WeatherForecast',
      'dayMaximum': {
        'temperature': get_data(forecast, 'tMax'),
      },
      'dayMinimum': {
        'temperature': get_data(forecast, 'tMin')
      },
      'feelsLikeTemperature': get_data(forecast, 'utci'),
      'temperature': get_data(forecast, 'tMed'),
    }
    hr = get_data(forecast, 'hR')
    if hr <> None:
      hr = hr / 100
    else:
      hr = None
    
    valid_from = parser.parse(forecast['dataPrev'])
    period = int(forecast['idPeriodo'])
    valid_to =   valid_from + datetime.timedelta(hours=period)
      
    obj['relativeHumidity'] = hr
    obj['created'] = forecast['dataUpdate']
    obj['validity'] = {
      'from': forecast['dataPrev'],
      'to':   valid_to.isoformat()
    }
    obj['address'] = {
      'addressCountry': 'PT',
      'addressLocality': locality
    }
    
    obj['windDirection'] = forecast['ddVento']
    
    weather_type_id = str(forecast['idTipoTempo'])
    if weather_type_id in weather_type_dict:
      obj['weatherType'] = weather_type_dict[weather_type_id]
      
    out.append(obj)
  
  return out


def get_data(forecast, item):
  value = float(forecast[item])
  if value == -99.0:
    value = None
  
  return value