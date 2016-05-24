SOCKET_PORT = 8080;
DB_TELNET_PORT = 8088;
DB_TELNET_HOST = "localhost";

var express = require('express');
var path = require('path');
var logger = require('morgan');
var cookieParser = require('cookie-parser');
var bodyParser = require('body-parser');
var queue = require('queue-async');
var fs = require('fs');
var routes = require('./routes/index');
var binParser = require('binary-parser').Parser;
var app = express();

var server = app.listen(SOCKET_PORT);
var io = require('socket.io').listen(server);
var net = require('net');
var current_connection = 0;
var database_client = 0;

// Run Python using Nodejs...Soooo sad
var arr00 = [1,2,3,4,5];
var name = "hello";
var PythonShell = require('python-shell');



/*
//var sys = require('sys')
var exec = require('child_process').exec;
var child;
// executes `pwd`
execute("pwd");
execute("ls -l");

function execute(command) {
    child = exec(command, function (error, stdout) {
        console.log('stdout: ' + stdout);
        if (error !== null) {
            console.log('exec error: ' + error);
        }
    });
    return child;
}
*/


// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'ejs');

console.log("Hello! Start the node!");

var selDataSet = [];
var sortedData = [];
var results = [];
var workPath;
var pivots;
var sizes;
var size;


var parseData = new binParser()
    .endianess('little')
    .array('dataset', {
        type: 'int32le',
        readUntil: 'eof'
    });

// communicate with server
function printOutput(server, output) {
    if (current_connection) {

        //console.log("received_event: " + output);
        output = JSON.parse(output);
        if (output.event == 'dsl_result') {
          console.log(output.res);
          current_connection.emit("message", {msg: output.res});
        }
        else if (output.event == "message") {
          current_connection.emit("message", {msg: output.msg});
        }
        else if (output.event == "visualize") {

            console.log("Now visualize the partition Info - RUM and Graph");

/*          pivots = test(1000000000);
            pivots.sort(function (a, b) {
                return a - b
            });
            pivots.pop();
            pivots.push(1000000000);*/

            //sizes = test(1000000);

            var pivots = output.pivots;

            var sizes = output.sizes;

            var infoName = ['rq','pq','up','d','i','size'];
            var partitionArr = partitionCost(sortedData, results, pivots);
            partitionArr.push(sizes);
            var partitionInfo = merge2json(infoName, partitionArr);

            partitionInfo = JSON.parse(partitionInfo);

            current_connection.emit("currentPartition", {partitionInfo: partitionInfo, serverRUM: [0, 40, 60]});

        }
        else if (output.event == "performance") {
            console.log("PERFORMANCE");
            //Test message

            //current_connection.emit("message", {msg: "PERFORMANCE"});


            // TODO: Add a bar chart, which coponents could be incrementally added
            // Y-aix: integer numbers indicating the throughput which is number of operation per second
            // X-aix: incrementally added, should be Algorithm types, each type should have two bars on graph

            // For the database perfomance basic information would be read and write opertion throughput
            // Do not know if algorithm should be specified, depending on the front end
            // output.readThroughput
            // output.writeThroughput
        }

        // TODO: And for CPU utilization and Memory consumption, try to execute a bash cammand in nodejs
        // and parse the output, the command would be 'top' or 'htop' or some other monitoring tools
    }
}

function send_command(command) {
    if (!database_client) {
        database_client = net.connect(parseInt(DB_TELNET_PORT), DB_TELNET_HOST, function () {
            database_client.setEncoding('utf8');
            console.log('Connected to Database server.');
            var output = "";
            database_client.on('data', function (chunk) {
                for (var i = 0; i < chunk.length; i++) {
                    if (chunk[i] == '\n') {
                        // TODO: parse the message (data) accordingly
                        // process_output("db", output);	// "db" indicates the source of data
                        // Display the response on screen
                        printOutput("db", output);
                        output = "";
                    }
                    else {
                        output += chunk[i];
                    }
                }
            });
            database_client.on('end', function () {
                console.log('database server hanged up!');
                client = 0;
            });
            database_client.write(command + "\n");
        });
    }
    else {
        database_client.write(command + "\n");
    }
}


function test(domain) {
    var arr = [];
    while (arr.length < 4000) {
        var randomnumber = Math.ceil(Math.random() * domain);
        var found = false;
        for (var i = 0; i < arr.length; i++) {
            if (arr[i] == randomnumber) {
                found = true;
                break
            }
        }
        if (!found)arr[arr.length] = randomnumber;
    }
    //document.write(arr);
    return arr;
}

// For dataSet, Use sortedData
function partitionCost(dataSet, avgCost, pivots) {

    var cost = new Array(5);
    for (var i = 0; i < cost.length; i++) {
        cost[i] = new Array();
    }
    var sum = [0,0,0,0,0];
    var j = 0;

    for (var p = 0; p < dataSet.length; p++) {
        if (dataSet[p] > pivots[j]) {
            ++j;
            for (var k = 0; k < cost.length; k++){
                cost[k].push(sum[k]);
                sum[k] = 0;
            }
        }
        var num = Math.floor(dataSet[p]/10000);//100
        for (var k = 0; k < cost.length; k++){
            sum[k] = sum[k] + avgCost[k][num];
        }
    }
    return cost;
}


/*---------------------------------------------------*/
/*-------------Functions of Data Processing----------*/
/*---------------------------------------------------*/


// Quick Sort Functions
function quickSort(a, low, high){

    if(high > low){
        var index = getRandomInt(low,high);
        //console.log(low,high,index);
        var pivot  = a[index];
        //console.log("pivot",pivot);
        a = partition(a,pivot);
        //console.log(a);
        quickSort(a,low,index-1);
        quickSort(a,index+1,high);
    }

    return a;
}

function  partition(a,pivot){

    var i = 0;
    for( var j=0; j < a.length; j++ ){
        if( a[j]!= pivot && a[j] < pivot ){
            var temp = a[i];
            a[i] = a[j];
            a[j] = temp;
            i++;
        }
    }
    return a;
}

function getRandomInt (min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}





// Todo: rewrite merge function
// merge function requires the length of arrays are same
// exp. merge2json(['a','b','c'],[[1,2,3,4],[4,5,6,7],[3,4,5,6]])
function merge2json(attributes, arr){
    var len = arr[0].length;
    var content = "{";
    var result = "[";
    for (var k = 0; k < len; k++) {
        for (var j = 0; j < arr.length; j++){
            if (j == (arr.length - 1)){
                content = content + "\"" + attributes[j] + "\":" + arr[j][k] + "";
            }
            else{
                content = content + "\"" + attributes[j] + "\":" + arr[j][k] + ",";
            }
        }
        if (k == (len - 1))
            content = content + "}";
        else
            content = content + "},";
        result = result + content;
        content = "{";
    }
    result = result + "]";
    //console.log(result);
    return result;

}



//WorkLoad Sampling

function workLoadSample(sampleSize, workDetail) {

    var sample = new Array(5);
    for (var i = 0; i < sample.length; i++) {
        sample[i] = new Array();
    }
    var subsum = [0, 0, 0, 0, 0];
    var j = 1;
    console.log("LLLLLENGTH + " + workDetail[0].length);
    for (var i = 0; i < workDetail[0].length; i++) {

        for (var k = 0; k < workDetail.length; k++) {
            subsum[k] = subsum[k] + workDetail[k][i];
        }

        if ((i == (sampleSize * j - 1)) && (i < (workDetail[0].length - 1))) {
            j = j + 1;
            for (var k = 0; k < sample.length; k++) {
                sample[k].push(subsum[k]);
                subsum[k] = 0;
            }
        }
        if (i == workDetail[0].length - 1) {
            for (var k = 0; k < sample.length; k++) {
                sample[k].push(subsum[k]);
            }
        }

    }
    return sample;
}


// Todo: double check!!! Histogram of Data
function histogramSample(low, high, ticksNum, dataSet) {

    var hisData = [];
    var range = high - low;
    var block = range / ticksNum;
    for (var i = 0; i < ticksNum; i++) {
        hisData.push(0);
    }
    for (var i = 0; i < dataSet.length; i++) {
        var gap = dataSet[i] - low;
        var num = Math.floor(gap / block);
        hisData[num] = hisData[num] + 1;
        if (num == ticksNum) {
            hisData[ticksNum - 1] = hisData[ticksNum - 1] + 1;
        }
    }
    return hisData;

}


function costSample(blockSize, dataSet, avgCost) {

    var sample = new Array(5);
    for (var i = 0; i < sample.length; i++) {
        sample[i] = new Array();
    }
    var sum = [0, 0, 0, 0, 0];
    var j = 1;
    console.log("This is local!!!");
    console.log("Now Dataset!!!!!!!" + dataSet.length);
    for (var i = 0; i < dataSet.length; i++) {
        var num = Math.floor(dataSet[i]/10000); //100
        //console.log(num)
        for (var k = 0; k < sample.length; k++) {
            sum[k] = sum[k] + avgCost[k][num];
        }

        if (i == (blockSize * j - 1)) {
            j = j + 1;
            for (var k = 0; k < sample.length; k++) {
                sum[k] = sum[k].toFixed(6);
                sample[k].push(sum[k]);
                sum[k] = 0;
            }
        }
        if (i == dataSet.length - 1) {
            for (var k = 0; k < sample.length; k++) {
                sum[k] = sum[k].toFixed(6);
                sample[k].push(sum[k]);
            }
        }

    }
    console.log("sample[0] length is " + sample[0].length);
    return sample;

}


// Socket.IO
//app.listen(SOCKET_PORT);
io.on('connection', function (socket) {
    current_connection = socket;
    console.log("socketIO connected");
    socket.on("sendConnect", function () {
            console.log("sendConnect received");
            send_command("connect");
        }
    );

    socket.on("showFile", function (data) {

            console.log("dataSet " + data.filename);
            selDataSet = [];
            fs.readFile(__dirname + "/../data/dataset" + data.filename + "/foo.tb1.a", function (err, data) {
                if (err) {
                    return console.log(err);
                }
                var dataSetInfo = parseData.parse(data);
                selDataSet = dataSetInfo.dataset;
                console.log("ddddddataset[0] -- " + selDataSet[selDataSet.length-1]);
                size= selDataSet.length;
                console.log(size);
                sortedData = selDataSet.slice();
/*                var l = selDataSet.length - 1;
                while (l--){
                    sortedData.push(selDataSet[l]);
                }*/
                sortedData.sort(function (a, b) {
                    return a - b
                });
                //sortedData = quickSort(sortedData, 0, sortedData.length);
                console.log("sortedSize" + sortedData.length);
                var numOfticks = 200;
                var domain = [0, 1000000000];
                // Data for drawing histogram of dataset
                var histogram = histogramSample(domain[0], domain[1], numOfticks, selDataSet);
                histogram = histogram.filter(Boolean);
                // Trans processed dataset to browser
                current_connection.emit("datasetView", {dataSetInfo: histogram});
            });
        }
    );

    //Todo: speed up... Choose a workLoad
    socket.on("showWorkLoad", function (data) {
            // If selDataSet is empty, then alarm
            console.log("LENGTH" + selDataSet.length);
            if (!selDataSet.length) {
                console.log("SORRY YOU SHOULD SELECT FILE OF DATASET FIRST!");
            }
            else {
                // Read workLoad
                var q = queue();
                workPath = __dirname + "/../data/workload" + data.workLoad;
                var sampleLength = [];
                //sampleSize is num of data for visualizing the distribution of workload
                var sampleSize = 100;

                // Read sampling data for each partition
                q.defer(fs.readFile, workPath + "/rq")
                    .defer(fs.readFile, workPath + "/pq")
                    .defer(fs.readFile, workPath + "/up")
                    .defer(fs.readFile, workPath + "/de")
                    .defer(fs.readFile, workPath + "/in")
                    .awaitAll(function (error, workLoadData) {

                        results = workLoadData;

                        // Sum array for each operation
                        var sum = [0, 0, 0, 0, 0];
                        for (var i = 0; i < results.length; i++) {
                            results[i] = parseData.parse(results[i]);
                            results[i] = results[i].dataset;
                            sampleLength.push(results[i][0]);   //sampleLength store the length of each workLoad
                            results[i].shift();
                            sum[i] = results[i].reduce(function (prev, cur) {
                                return prev + cur;
                            });
                        }
                        console.log(sum);
                        console.log("result length " + results[0].length);
                        for (var i = 0; i < results.length; i++) {
                            for (var k = 0; k < results[i].length; k++) {
                                results[i][k] = results[i][k] / sum[i];
                            }
                        }

                        var workLength = sampleLength[0];
                        // workLoad distribution sampling
                        var sampleInterval = workLength / sampleSize;
                        console.log("sampleInterval " + sampleInterval);
                        var opName = ['rq','pq','up','d','i'];


                        var workSample = workLoadSample(sampleInterval, results);
                        var wlInfo = merge2json(opName,workSample);
                        wlInfo = JSON.parse(wlInfo);

                        var blockNum = 10000;
                        console.log("workLength " + workLength);
                        var blockSize = size / blockNum;
                        console.log("blockSize - " + blockSize);


                        var storedOp = costSample(blockSize, selDataSet, results);
                        var storedCost = merge2json(opName, storedOp);
                        console.log("storedCost - " + storedCost.length);
                        storedCost = JSON.parse(storedCost);


                        var sortedOp = costSample(blockSize, sortedData, results);
                        var sortedCost = merge2json(opName, sortedOp);
                        sortedCost = JSON.parse(sortedCost);

                        current_connection.emit("workloadView", {
                            workLoadInfo: wlInfo,
                            stored: storedCost,
                            sorted: sortedCost
                        });

                    });

            }
        }
    );

    // apply DataSet
    socket.on("applyFile", function (data) {
            send_command("dataSet " + data.filename);
        }
    );


    // apply workload
    socket.on("applyWorkLoad", function (data) {
            send_command("workload " + data.workLoad);
        }
    );

    // Run partition algorithm to get expected cost 
    // and basic info of resulted partitions,
    // i.e. partition count and size of each partition
    socket.on("part_algo", function (data) {
            var part_algo = data.part_algo;
            send_command("part_algo " + data.part_algo);
        }
    );

    // User define RUM
    socket.on("userRUM", function (data) {

            var rePartitionInfo = data.part_algo;
            rePartitionInfo.push(data.userRUM);
            send_command("re_parti_algo " + rePartitionInfo);
        }
    );

    // User satisfy
    socket.on("applyPartition", function (data) {
            // The next line should actually be to apply physical partition
            // send_command("exec_work " + data.status);
            // Not sure if data.status is in need
            send_command("phys_part " + data.status);
        }
    );

    socket.on("exec_work", function (data) {
            // no need for parameter to execute the workload, 
            // might need some in the future
            console.log("exe work!");
            send_command("exec_work ");
        }
    );

    socket.on("show_Cost", function (data) {


            // no need for parameter to execute the workload,
            // might need some in the future
            console.log("show_Cost! -- " + data.rrCost);
            var c_pram = [data.srCost,data.rrCost,data.swCost,data.rwCost,data.lgCost1,data.lgCost2];

            var type = ["uniform",0,1000000000];
                var arr = [1,2,3];
                var options = {
                    mode: 'text',
                    args: [type,workPath,pivots,sizes,size,c_pram]
                };

            PythonShell.run('costcal.py', options, function (err, results) {
                if (err) throw err;
                // results is an array consisting of messages collected during execution
                console.log("results " + results);

                var stored = results[0].slice(1, results[0].length-1);
                var stored = JSON.parse("[" + stored + "]");
                var sorted = results[1].slice(1, results[1].length-1);
                var sorted = JSON.parse("[" + sorted + "]");
                var p = results[2].slice(1, results[2].length-1);
                var p = JSON.parse("[" + p + "]");
                var str1 = "{\"Name\":\"Stored Order\",\"Read Cost\":"+ stored[0]+ ",\"Write Cost\":" + stored[1]+"}";
                var str2 = "{\"Name\":\"Sorted Order\",\"Read Cost\":"+ sorted[0]+ ",\"Write Cost\":" + sorted[1]+"}";
                var str3 = "{\"Name\":\"Partition\",\"Read Cost\":"+ p[0]+ ",\"Write Cost\":" + p[1]+"}";
                var str = "[" + str1 + "," + str2 + "," + str3 + "]";
                var costInfo = JSON.parse(str);
                console.log(costInfo);
                current_connection.emit("cost", {costInfo: costInfo});

            });



        }
    );



});

app.use(logger('dev'));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended: false}));
app.use(cookieParser());
app.use(express.static(__dirname));
app.use('/', routes);

// catch 404 and forward to error handler
app.use(function (req, res, next) {
    var err = new Error('Not Found');
    err.status = 404;
    next(err);
});

// error handlers

// development error handler
// will print stacktrace
if (app.get('env') === 'development') {
    app.use(function (err, req, res, next) {
        res.status(err.status || 500);
        res.render('error', {
            message: err.message,
            error: err
        });
    });
}

// production error handler
// no stacktraces leaked to user
app.use(function (err, req, res, next) {
    res.status(err.status || 500);
    res.render('error', {
        message: err.message,
        error: {}
    });
});


module.exports = app;
