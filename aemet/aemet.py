#!bin/python
# -*- coding: utf-8 -*-

from flask import Flask, jsonify, request, Response
import urllib2
import xml.dom.minidom
import datetime
import json
import copy
from dateutil import parser
import csv
import StringIO
import re

postal_codes = {
  '47001': '47186',
  '28001': '28079',
  '39001': '39075',
  '34001': '34120',
  '34200': '34023',
  '05194': '05123'
}

awareness_type_dict = {
  '1':  'Wind',
  '2':  'Snow/Ice',
  '3':  'Thunderstorms',
  '4':  'Fog',
  '5':  'Extreme High Temperature',
  '6':  'Extreme Low Temperature',
  '7':  'Coastal Event',
  '8':  'Forest Fire',
  '9':  'Avalanches',
  '10': 'Rain',
  '11': 'Flood',
  '12': 'Rain-Flood'
}

awareness_level_dict = {
  '':  'White',
  '1': 'Green',
  '2': 'Yellow',
  '3': 'Orange',
  '4': 'Red'
}

app = Flask(__name__)

aemet_service    = "http://www.aemet.es/xml/municipios/localidad_{}.xml"
weather_observed = "http://www.aemet.es/es/eltiempo/observacion/ultimosdatos_{}_datos-horarios.csv?k=cle&l={}&datos=det&w=0&f=temperatura&x=h6"
weather_alarms   = "http://www.meteoalarm.eu/documents/rss/{}.rss"

reg_exp = re.compile('<img(?P<group>.*?)>')

@app.route('/')
def index():
    return "Hello, World!"

  
@app.route('/v2/entities',  methods=['GET'])
def get_weather():
    entity_type = request.args.get('type')
      
    if entity_type == 'WeatherForecast':
      return get_weather_forecasted(request)
    elif entity_type == 'WeatherObserved':
      return get_weather_observed(request)
    elif entity_type == 'WeatherAlarm':
      return get_weather_alarms(request)
    else:
      return Response(json.dumps([]), mimetype='application/json')
    
def get_weather_alarms(request):
  query = request.args.get('q')

  if not query:
      return Response(json.dumps([]), mimetype='application/json')
  
  tokens  = query.split(';')
  
  country = ''
  
  for token in tokens:
    items = token.split(':')
    if items[0] == 'country':
      country = items[1].lower()
  
  source = weather_alarms.format(country)
  req = urllib2.Request(url=source)
  f = urllib2.urlopen(req)
  
  xml_data = f.read()
  DOMTree = xml.dom.minidom.parseString(xml_data).documentElement
  
  out = []
  
  items = DOMTree.getElementsByTagName('item')[1:]
  
  for item in items:
    description = item.getElementsByTagName('description')[0].firstChild.nodeValue
    # Enable description parsing
    description = description.replace('&nbsp;', '')
    description = re.sub(reg_exp,'<img\g<group>></img>',description)
    
    zone = item.getElementsByTagName('title')[0].firstChild.nodeValue.strip()
    uid = item.getElementsByTagName('guid')[0].firstChild.nodeValue
    pub_date_str = item.getElementsByTagName('pubDate')[0].firstChild.nodeValue
    pub_date = datetime.datetime.strptime(pub_date_str[:-6], '%a, %d %b %Y %H:%M:%S').isoformat()
    
    parsed_content = xml.dom.minidom.parseString(description).documentElement
    rows = parsed_content.getElementsByTagName('tr')[1:2]
    
    row_number = 0
    for row in rows:
      columns = row.getElementsByTagName('td')
      for column in columns:
        # img column contains the awareness level and type
        img_aux = column.getElementsByTagName('img')
        if img_aux.length > 0:
          awareness_str = img_aux[0].getAttribute('alt')
          
          alarm_data = parse_alarm(awareness_str)
          if alarm_data['level'] > 1:
            if row_number == 0:
              v_from = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
            else:
              v_from =  datetime.datetime.today().replace(hour=0, minute=0, second=0,microsecond=0) + datetime.timedelta(days=1)
            obj = {
              'type': 'WeatherAlarm',
              'id':   'WeatherAlarm' + '-' + uid + '-' + str(row_number),
              'validity': {
                'from': v_from.isoformat(),
                'to':   (v_from + datetime.timedelta(days=1)).isoformat()
              },
              'awarenessType':  alarm_data['awt'],
              'awarenessLevel': alarm_data['levelColor'],
              'address': {
                'addressCountry': country.upper(),
                'addressLocality': zone
              },
              'source': 'http://www.meteoalarm.eu',
              'created': pub_date
            }
            out.append(obj)
      row_number += 1
  
  return Response(json.dumps(out), mimetype='application/json')

def parse_alarm(alarm_string):
  elements = alarm_string.split(' ')
  awt = elements[0].split(':')[1]
  level = elements[1].split(':')[1]
  
  return {
    'level':      int(level),
    'levelColor': awareness_level_dict[level],
    'awt':        awareness_type_dict[awt]
  }

def get_weather_observed(request):    
    query = request.args.get('q')
    
    if not query:
      return Response(json.dumps([]), mimetype='application/json')
    
    tokens  = query.split(';')
    
    station_code = ''
    country = ''
    
    for token in tokens:
      items = token.split(':')
      if items[0] == 'stationCode':
        station_code = items[1]
      elif items[0] == 'country':
        country = items[1]
    
    if not station_code or not country or country <> 'ES':
      return Response(json.dumps([]), mimetype='application/json')
    
    source = weather_observed.format(station_code, station_code)
    
    req = urllib2.Request(url=source)
    f = urllib2.urlopen(req)
    csv_data = f.read()
    
    csv_file = StringIO.StringIO(csv_data)
    reader = csv.reader(csv_file, delimiter=',')
    
    out = []
    index = 0
    for row in reader:
      if index == 0:
        address = row[0]
        
      if index < 4:
        index += 1
        continue
      
      print row
      
      observation = {
        'type': 'WeatherObserved'
      }
      if len(row) < 2:
        continue
      
      observation['temperature'] = get_data(row, 1)
      observation['windSpeed'] = get_data(row, 2, int)
      observation['windDirection'] = row[3] or None
      observation['precipitation'] = get_data(row, 6)
      observation['pressure'] = get_data(row, 7)
      observation['pressureTendency'] =  get_data(row, 8)
      observation['relativeHumidity'] = get_data(row, 9, factor=100.0)
      
      observation['observed'] = datetime.datetime.strptime(row[0], '%d/%m/%Y %H:%M').isoformat()
      observation['source'] = 'http://www.aemet.es'
      observation['address'] = {
        'addressLocality': address.decode('latin-1'),
        'addressCountry': country
      }
      
      out.append(observation)
    
    print out
    return Response(json.dumps(out), mimetype='application/json')
  
def get_data(row, index, conversion=float, factor=1.0):
  out = None
  
  value = row[index]
  if(value <> ''):
    out = conversion(value) / factor
    
  return out
    
def get_weather_forecasted(request):    
    country = ''
    postal_code = ''
    
    query = request.args.get('q')
    
    if not query:
      return Response(json.dumps([]), mimetype='application/json')
    
    tokens  = query.split(';')
    for token in tokens:
      items = token.split(':')
      if items[0] == 'postalCode':
        postal_code = items[1]
      elif items[0] == 'country':
        country = items[1]
        
    print country, postal_code
    
    if not country or not postal_code or not postal_code in postal_codes or country <> 'ES':
      return Response(json.dumps([]), mimetype='application/json')
    
    source = aemet_service.format(postal_codes[postal_code])
    req = urllib2.Request(url=source)
    f = urllib2.urlopen(req)
    xml_data = f.read()
    DOMTree = xml.dom.minidom.parseString(xml_data).documentElement
    
    address_locality = DOMTree.getElementsByTagName('nombre')[0].firstChild.nodeValue
    address = { }
    address['addressCountry'] = country
    address['postalCode'] = postal_code
    address['addressLocality'] = address_locality
    
    created =  DOMTree.getElementsByTagName('elaborado')[0].firstChild.nodeValue
    
    forecasts = DOMTree.getElementsByTagName('prediccion')[0].getElementsByTagName('dia')
    
    out = []
    for forecast in forecasts:
      date = forecast.getAttribute('fecha')
      normalizedForecast = parse_aemet_forecast(forecast, date)
      counter = 1
      for f in normalizedForecast:
        f['type'] = 'WeatherForecast'
        f['id'] = generate_id(postal_code, country, date) + '_' + str(counter)
        f['address'] = address
        f['created'] = created
        f['source'] = source
        counter+=1
        out.append(f)
    
    return Response(json.dumps(out), mimetype='application/json')
    

def parse_aemet_forecast(forecast, date):
  periods = { }
  out = []
  
  parsed_date = parser.parse(date)
  
  pops = forecast.getElementsByTagName('prob_precipitacion')
  for pop in pops:
    period = pop.getAttribute('periodo')
    if not period:
      period = '00-24'
    if pop.firstChild and pop.firstChild.nodeValue:
      insert_into_period(periods, period,
                  'precipitationProbability', float(pop.firstChild.nodeValue) / 100.0)
  
  period = None
  weather_types = forecast.getElementsByTagName('estado_cielo')
  for weather_type in weather_types:
    period = weather_type.getAttribute('periodo')
    if not period:
      period = '00-24'
    if weather_type.firstChild and weather_type.firstChild.nodeValue:
      insert_into_period(periods, period, 'weatherType',
                         weather_type.getAttribute('descripcion'))
  
  period = None
  wind_data = forecast.getElementsByTagName('viento')
  for wind in wind_data:
    period = wind.getAttribute('periodo')
    if not period:
      period = '00-24'
    wind_direction = wind.getElementsByTagName('direccion')[0]
    wind_speed = wind.getElementsByTagName('velocidad')[0]
    if wind_speed.firstChild and wind_speed.firstChild.nodeValue:
      insert_into_period(periods, period, 'windSpeed',
                         int(wind_speed.firstChild.nodeValue))
    if wind_direction.firstChild and wind_direction.firstChild.nodeValue:
      insert_into_period(periods, period, 'windDirection',
                         wind_direction.firstChild.nodeValue)
  
  temperature_node = forecast.getElementsByTagName('temperatura')[0]
  max_temp = float(temperature_node.getElementsByTagName('maxima')[0].firstChild.nodeValue)
  min_temp = float(temperature_node.getElementsByTagName('minima')[0].firstChild.nodeValue)
  get_parameter_data(temperature_node, periods, 'temperature')
  
  temp_feels_node = forecast.getElementsByTagName('sens_termica')[0]
  max_temp_feels = float(temp_feels_node.getElementsByTagName('maxima')[0].firstChild.nodeValue)
  min_temp_feels = float(temp_feels_node.getElementsByTagName('minima')[0].firstChild.nodeValue)
  get_parameter_data(temp_feels_node, periods, 'feelsLikeTemperature')
  
  humidity_node = forecast.getElementsByTagName('humedad_relativa')[0]
  max_humidity = float(humidity_node.getElementsByTagName('maxima')[0].firstChild.nodeValue) / 100.0
  min_humidity = float(humidity_node.getElementsByTagName('minima')[0].firstChild.nodeValue) / 100.0
  get_parameter_data(humidity_node, periods, 'relativeHumidity', 100.0)
  
  for period in periods:
    print period
  
  for period in periods:
    period_items = period.split('-')
    period_start = period_items[0]
    period_end = period_items[1]
    end_hour = int(period_end)
    end_date = copy.deepcopy(parsed_date)
    if end_hour > 23:
      end_hour = 0
      end_date = parsed_date + datetime.timedelta(days=1)
    
    start_date = parsed_date.replace(hour=int(period_start), minute=0, second=0)
    end_date = end_date.replace(hour=end_hour,minute=0,second=0)   
    
    objPeriod = periods[period]
    objPeriod['validity'] = { }
    objPeriod['validity']['from'] = start_date.isoformat()
    objPeriod['validity']['to'] = end_date.isoformat()
    
    maximum = { }
    objPeriod['dayMaximum'] = maximum
    minimum = { }
    objPeriod['dayMinimum'] = minimum
    
    maximum['temperature'] = max_temp
    minimum['temperature'] = min_temp
    
    maximum['relativeHumidity'] = max_humidity
    minimum['relativeHumidity'] = min_humidity
    
    maximum['feelsLikeTemperature'] = max_temp_feels
    minimum['feelsLikeTemperature'] = min_temp_feels
    
    out.append(objPeriod)
  
  return out


def get_parameter_data(node, periods, parameter, factor=1.0):
  param_periods = node.getElementsByTagName('dato')
  for param in param_periods:
    hour_str = param.getAttribute('hora')
    hour = int(hour_str)
    interval_start = hour - 6
    interval_start_str = str(interval_start)
    if interval_start < 10:
      interval_start_str = '0' + str(interval_start)
      
    period = interval_start_str + '-' + hour_str
    if param.firstChild and param.firstChild.nodeValue:
      param_val = float(param.firstChild.nodeValue)
      insert_into_period(periods, period, parameter, param_val / factor)


def insert_into_period(periods, period, attribute, value):
  if not period in periods:
    periods[period] = { }
  
  periods[period][attribute] = value

def generate_id(postal_code, country, date):
  return postal_code + '_' + country + '_' + date

if __name__ == '__main__':
    app.run(host='0.0.0.0',port=1028,debug=True)