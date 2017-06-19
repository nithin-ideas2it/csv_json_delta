
let oldCsvFilePath = './pets_orginal_old.csv';
let newCsvFilePath = './pets_orginal.csv';
let Async = require('async');
let diffJson = require('diff-json');
let _ = require('lodash');
let csv = require('csvtojson');

module.exports = {
 getJSON: getJSON,
 getDelta: getDelta
};

var newJson;
function getJSON(path, cb) {
  let dataList = [];
  csv()
    .fromFile(path)
    .on('json', function(rec) {
      dataList.push(rec);
    })
    .on('done', function(error) {
      if (error) {
        console.log('Error while process csv to json on file path: ' + path);
        console.log(error);
        cb(error);
      }
      let csvJson = {};
      csvJson.data = _.sortBy(dataList, 'ID');
      console.log(' csvJson.data length ' + csvJson.data.length);
      cb(null, csvJson);
    });
}

function getDeltaKey(callback) {
  Async.waterfall([
    function (cb) {
      getJSON(oldCsvFilePath, function(error, oldCsvJson){
        console.log('Old CSV');
        if (error) {
          console.log('Error while process old csv');
          console.log(error);
          cb(error);
        }
        cb(null, oldCsvJson);
      });
    },
    function (oldCsvJson, cb) {
      getJSON(newCsvFilePath, function(error, newCsvJson){
        console.log('New CSV');
        if (error) {
          console.log('Error while process new csv');
          console.log(error);
          cb(error);
        }
        newJson = newCsvJson;
        let jsonDiff = diffJson.diff(oldCsvJson, newCsvJson, {data: 'ID'});
        cb(null, jsonDiff);
      });
    }
  ], function (error, result) {
      if (error) {
        console.log('Error while process getDeltaKey()');
        console.log(error);
        callback(error);
      }
      callback(null, result[0]);
  });
}


function getDelta() {
  getDeltaKey(function(error, deltaKeys) {
    var delta = [];
    if (deltaKeys && deltaKeys.changes) {
        console.log(' deltaKeys.changes length');
        console.log(deltaKeys.changes.length);
        Async.eachSeries(deltaKeys.changes, function iterator(rec, cb) {
        var row = {};
        row.action = rec.type;
        row.id = rec.key;
        console.log(' Changes ');
        console.log(rec);
        if (rec.type === 'add' || rec.type === 'update') {
          row.data = _.find(newJson.data, {'ID': rec.key});
        }
        delta.push(row);
        cb();
        
      }, function done(error) {
        if (error) {
          console.log('Error while process getDelta()');
          console.log(error);
          return error;
        } else {
          console.log(' delta ');
          console.log(delta);
          return delta;
        }
      });
    } else {
       console.log(' No delta ');
       console.log(delta);
       return delta;
    }

  });
}
