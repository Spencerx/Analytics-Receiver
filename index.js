var AWS = require('aws-sdk')
  , docClient = new AWS.DynamoDB.DocumentClient({region: 'us-west-2'})
  , https = require('https')
  , async = require('async')
  , fs = require('fs')
  , configJSON = fs.readFileSync('config.json', {encoding: 'utf-8'})
  , config = JSON.parse(configJSON);

exports.handler = function (event, context, callback) {
  var body = event['body-json'];
  var requestContext = event.context;
  var currentDate = new Date();
  var todaysStartDate = new Date();
  todaysStartDate.setUTCHours(0,0,0,0);
  var todaysEndDate = new Date();
  todaysEndDate.setUTCHours(0,0,0,0);
  todaysEndDate.setDate(todaysEndDate.getDate() + 1);
  if (Object.keys(body).length === 0) {
    var params = {
      TableName: config.dynamodb.tables.counts,
      Key: { 'start_date': todaysStartDate.toISOString(), 'end_date': todaysEndDate.toISOString() },
      UpdateExpression: 'ADD opt_out_count :updateVal, total_request_count :totalVal SET start_epoch = :startEpoch, end_epoch = :endEpoch',
      ExpressionAttributeValues: {
        ':updateVal': 1,
        ':totalVal': 1,
        ':startEpoch': (todaysStartDate.getTime() / 1000),
        ':endEpoch': (todaysEndDate.getTime() / 1000)
      },
      ReturnValues:'NONE'
    };
    docClient.update(params, function(updateErr, updateData) {
      if(updateErr){
        console.error('Error when updating opt out count:', updateErr);
      }
      callback(null, config.version);
    });
  } else {
    async.waterfall([
      function(cb){
        if(requestContext.test_mode) {
          cb(null, {'Item': requestContext.testDoc});
        } else {
          docClient.get({TableName: config.dynamodb.tables.instances, Key: {'instance-id': body.uuid}}, cb);
        }
      },
      function(gotDoc, cb){
        if(Object.keys(gotDoc).length === 0) {
          getGeoIP(requestContext['source-ip'], function(err, geoIp){
            cb(err, gotDoc, geoIp);
          });
        } else {
          cb(null, gotDoc, {});
        }
      },
      function(gotDoc, geoip, cb){
        if (body.arch.toLowerCase() == 'amd64') {
          body.arch = 'x86_64'; // stupid windows
        }
        var params = {
          'arch': body.arch,
          'dev': body.dev || false,
          'distribution': body.distribution || null,
          'docker': body.docker || false,
          'instance-id': body.uuid,
          'os_name': body.os_name,
          'os_version': body.os_version,
          'python_version': body.python_version,
          'time_zone': body.timezone,
          'user_agent': requestContext['user-agent'],
          'version': body.version,
          'virtualenv': body.virtualenv || false
        };
        params.first_seen_datetime = (gotDoc.Item && gotDoc.Item.first_seen_datetime) ? gotDoc.Item.first_seen_datetime : currentDate.toISOString();
        params.last_seen_datetime = currentDate.toISOString();
        for (var key in geoip) {
          if(key === 'ip') continue;
          if(geoip[key] === '') continue;
          if(key === 'metro_code' && geoip[key] === 0) continue;
          params['geo_'+key] = geoip[key];
        }
        if (gotDoc.Item) {
          for (var key in gotDoc.Item) {
            if(key.indexOf('geo_') === 0) {
              params[key] = gotDoc.Item[key];
            }
          }
        }
        for (var key in params) {
          if(params[key] === '') {
            params[key] = null;
          }
        }
        cb(null, params, (gotDoc && gotDoc.Item));
      },
      function(params, is_new_instance, cb){
        if(requestContext.test_mode) {
          console.warn('Test mode active, not submitting data to DynamoDB or Keen!');
          return cb(null, params);
        }
        async.parallel([
          function(cbb){
            docClient.put({TableName: config.dynamodb.tables.instances, Item: params}, cbb);
          },
          function(cbb){
            publishEventToKeenIO(params, cbb);
          },
          function(cbb){
            var updateParameter = (is_new_instance) ? 'new_instance_count' : 'existing_instance_count';
            var params = {
              TableName: config.dynamodb.tables.counts,
              Key: { 'start_date': todaysStartDate.toISOString(), 'end_date': todaysEndDate.toISOString() },
              UpdateExpression: 'ADD '+updateParameter+' :updateVal, total_request_count :totalVal  SET start_epoch = :startEpoch, end_epoch = :endEpoch',
              ExpressionAttributeValues: {
                ':updateVal': 1,
                ':totalVal': 1,
                ':startEpoch': (todaysStartDate.getTime() / 1000),
                ':endEpoch': (todaysEndDate.getTime() / 1000)
              },
              ReturnValues:'NONE'
            };
            docClient.update(params, cbb);
          }
        ], function(err){
          cb(err, params);
        });
      }
    ], function(err, doc){
      if(err){
        console.error('ERROR at end:', err);
      }
      if(requestContext.test_mode) {
        console.warn('Test mode: Inserted doc would have been:', doc);
      }
      callback(null, config.version);
    });
  }
};

function getGeoIP(ipAddress, callback){
  var req = https.request({
    hostname: config.freegeoip_source,
    port: 443,
    path: '/json/'+ipAddress,
    method: 'GET'
  }, function(res) {
    res.on('data', function(d) {
      callback(null, JSON.parse(d));
    });
  });
  req.end();

  req.on('error', function(e){
    console.error('GeoIP error:', e);
    callback(e, null);
  });
}

function publishEventToKeenIO(params, callback){
  var keen_params = {
    'first_seen_datetime': params.first_seen_datetime,
    'last_seen_datetime': params.last_seen_datetime,
    'system_info': {
      'arch': params.arch,
      'docker': params.docker,
      'distribution': params.distribution,
      'os_name': params.os_name,
      'os_version': params.os_version,
      'python_version': params.python_version,
      'virtualenv': params.virtualenv,
    },
    'ip_geo_info': {
      'city': params['geo_city'],
      'country_code': params['geo_country_code'],
      'country_name': params['geo_country_name'],
      'latitude': params['geo_latitude'],
      'longitude': params['geo_longitude'],
      'metro_code': params['geo_metro'],
      'region_code': params['geo_region_code'],
      'region_name': params['geo_region_name'],
      'time_zone': params['geo_time_zone'],
      'zip_code': params['geo_zip_code']
    },
    'homeassistant': {
      'dev': params.dev,
      'instance-id': params['instance-id'],
      'time_zone': params.time_zone,
      'version': params.version
    },
    'keen': {
      'location': {
        'coordinates': [params['geo_longitude'], params['geo_latitude']]
      },
      'addons': [
        {
          'name' : 'keen:date_time_parser',
          'input' : { 'date_time': 'first_seen_datetime' },
          'output' : 'first_seen_timestamp_info'
        },
        {
          'name' : 'keen:date_time_parser',
          'input' : { 'date_time': 'last_seen_datetime' },
          'output' : 'last_seen_timestamp_info'
        }
      ]
    }
  };
  var req = https.request({
    hostname: 'api.keen.io',
    port: 443,
    path: '/3.0/projects/'+config.keen.project+'/events/'+config.keen.event_name,
    method: 'POST',
    headers: {
      'Authorization': config.keen.write_key,
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(JSON.stringify(keen_params))
    }
  }, function(res) {
    res.on('data', function(d) {
      callback(null, JSON.parse(d));
    });
  });
  req.end(JSON.stringify(keen_params));

  req.on('error', function(e){
    console.error('Keen.io error:', e);
    callback(e, null);
  });
}

function addZ(n){
  return n<10? '0'+n:''+n;
}
