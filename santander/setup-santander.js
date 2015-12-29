'use strict';

/* Sets up all the data needed for smart parking in Santander */

const ORION_SERVER = 'http://130.206.83.68:1026/v1';
const SANTANDER_SERVER = 'http://mu.tlmat.unican.es:8099/v1'

var csv = require('ya-csv');

var Orion = require('fiware-orion-client'),
    OrionClient = new Orion.Client({
      url: ORION_SERVER,
      userAgent: 'Test'
    }),
    SantanderClient = new Orion.Client({
      url: SANTANDER_SERVER,
      userAgent: 'Test',
      service: 'smartsantander'
    });

function setupConfig() {
  return new Promise(function(resolve, reject) {
    var sensor2Polygon = Object.create(null);
    var polygon2Sensor = Object.create(null);
  
    var obj = null;
    readCsv('sensors_polygons.csv').then(function(data) {
      data.forEach(function(aRecord) {
        var sensorId = aRecord.id;
        var polygonId = aRecord['sensor_id'];
        sensor2Polygon[sensorId] = polygonId;
        if (!polygon2Sensor[polygonId]) {
          polygon2Sensor[polygonId] = [];
        }
        polygon2Sensor[polygonId].push(sensorId);
      });
      
      obj = {
        'sensor2Polygon': sensor2Polygon,
        'polygon2Sensor': polygon2Sensor
      };
      
      // console.log('Data: ', JSON.stringify(obj));
      
      return OrionClient.updateContext({
        type: 'santander:smartparking:config',
        id:   'parking_config_1',
        data: obj
      });
    }, reject).then(function() {
        resolve(obj); },
      reject).catch(reject);
  });
}

function readCsv(file) {
  return new Promise(function(resolve, reject) {
    var collection = [];
    
    var reader = csv.createCsvFileReader(file, {
      'columnsFromHeader': true,
      'separator': ','
    });
    
    reader.addListener('data', function(data) {
      collection.push(data);
    });
    
    reader.addListener('end', function() {
      resolve(collection);
    });
    
    reader.addListener('error', function() {
      reject();
    });
  });
}

function readJson(file) {
  var fs = require('fs');
  var obj = JSON.parse(fs.readFileSync(file, 'utf8'));
  
  return obj;
}

// Iterates over the street parking list (polygon GeoJSON)

setupConfig().then(function(config) {
  console.log('Data updated properly');
  
  var data = readJson('polygons_geojson.geojson');
  // Array to hold the list of entities to be created
  var entitiesToCreate = [];
  // Array fo promises for querying sensor status
  var querySensorStatus = [];
  
  data.features.forEach(function(aFeature) {
    var properties = aFeature.properties;
    var polygonId = properties['sensor_id'];
    
    var obj = {
      id:   'santander' + ':' + polygonId,
      type: 'StreetParking',
      allowedVehicles: ['Car'],
      totalSpotNumber: config.polygon2Sensor[polygonId].length
    };
    
    var centroid = properties['centroid_YCOORD'] + ', ' + properties['centroid_XCOORD'];
    obj.centroid = new Orion.Attribute(centroid, 'geo:point');
    
    var coordinates = aFeature.geometry.coordinates[0];
    var polygonCoords = '';
    coordinates.forEach(function(aCoordinate) {
      polygonCoords += aCoordinate[1] + ',' + aCoordinate[0]
      polygonCoords += ','
    });
    
    obj.location = new Orion.Attribute(polygonCoords.substring(0, polygonCoords.length - 1),
                                       'geo:polygon');
    entitiesToCreate.push(obj);
    
    var sensors = config.polygon2Sensor[polygonId];
    var queryData = [];
    sensors.forEach(function(aSensor) {
      queryData.push({
        id: aSensor
      })
    });
    
    // Enquee a promise to get status
    querySensorStatus.push(SantanderClient.queryContext(queryData,{
      path: '/parking/#'
    }));
  });
  
  Promise.all(querySensorStatus).then(function(results) {
    console.log('Promise.all finished: ', results.length, entitiesToCreate.length);
    entitiesToCreate.forEach(function(aEntity, index) {
      console.log(index);
      if (index == 27) {
        console.log(aEntity);
      }
      var freeSpots = aEntity.totalSpotNumber;
      var sensorData = results[index];
      sensorData && sensorData.forEach(function(aSensor) {
        if (aSensor['presenceStatus:parking'] === 'true') {
          freeSpots--;
        }
      });
      aEntity.availableSpotNumber = freeSpots;
      aEntity.updated = new Date();
    });
    
    console.log('Going to create entities');
    
    return OrionClient.updateContext(entitiesToCreate);
  
  }).then(function() {
      console.log('Santander street parking created OK');
    }).catch(function(err) {
        console.error('Error while setting up Santander data', err);
    });
}).catch(function(err) {
    console.error('Error while setting up configuration: ', err);
});