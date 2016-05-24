var express = require('express');
var router = express.Router();
var partitionData;
var queue = require('queue-async');
var fs = require('fs');
var q = queue();

/*
var partitionCount = 32;

for (var m = 0; m < partitionCount; m++){
  q.defer(fs.readFile, __dirname + "/../DataGeneration/partition" + m + ".json", 'utf8');
}
q.awaitAll(function(error, results) {
  for (var i = 0; i < results.length; i++) {
    results[i] = JSON.parse(results[i]);
  }
  partitionData = results;
});

var PartitionSize = fs.readFileSync( __dirname + "/../DataGeneration/size.json", 'utf8');
PartitionSize = JSON.parse(PartitionSize);

*/

/* GET home page. */
/*
router.get('/', function(req, res, next) {
  res.render('index', { partitionData: partitionData, PartitionSize: PartitionSize, title: "The RUM Conjecture"});
});
*/


/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index', {title: "The RUM Conjecture"});
});

/*router.get('/', function(req, res, next) {
  res.render('index', { partitionData: partitionData, PartitionSize: PartitionSize, title: "RUM Visual"});
});*/

module.exports = router;
