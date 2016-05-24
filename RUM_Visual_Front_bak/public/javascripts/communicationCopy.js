/**
 * Created by logicRabbit on 1/10/16.
 */
// Add EventListener to the button to send command
SOCKET_PORT = 8080;
var socket = io('http://localhost:' + SOCKET_PORT);

console.log("hello there");

document.getElementById('sendCommand').addEventListener("click", function () {
    console.log("button clicked");
    socket.emit("sendConnect");
});

document.getElementById('fileSelectButton').addEventListener("click", function () {
    console.log("file choose button clicked");
    var sel = document.getElementById('fileSelect');
    console.log("dataset chosen as " + sel.options[sel.selectedIndex].value);
    socket.emit("datasetOpen", {filename: sel.options[sel.selectedIndex].value});

});

socket.on('data', function (data) {
    //console.log("hihi");
    var outputText = document.getElementById('serverResponse');
    outputText.innerHTML = outputText.innerHTML + data;
    console.log("data from middleware received");
});

var margin0 = {top: 20, right: 30, bottom: 30, left: 40},
    width0 = 960 - margin0.left - margin0.right,
    height0 = 250 - margin0.top - margin0.bottom;

var svg1 = d3.select("#overview").append("svg")
    .attr("width", width0 + margin0.left + margin0.right)
    .attr("height", height0 + margin0.top + margin0.bottom)
    .append("g")
    .attr("transform", "translate(" + margin0.left + "," + margin0.top + ")");


socket.on('fileContent', function (dataContent) {

    var faithful = dataContent.content;
    //faithful = JSON.parse(dataContent.content);


    var x0 = d3.scale.linear()
        .domain([d3.min(faithful), d3.max(faithful)])
        .range([0, width0]);

    var histogram = d3.layout.histogram()
        .frequency(true);
    // .bins(x.ticks());

    var data0 = histogram(faithful);

    var m = -1;
    var maxLength = d3.max(data0.map(function(num){
        m++;
        console.log("dataLength..." + data0[m].length);
        return data0[m].length;
    }));

    console.log("maxLength===" + maxLength);
    // console.log("data0++++" + data[0].length + "ssss" + data[11].length + data.length);


    var xAxis0 = d3.svg.axis()
        .scale(x0)
        .orient("bottom");


    var initial = 0;

    var value =  data0.map(function(number){
        console.log("sssss2232323232"  + data0[initial].y);
        number = data0[initial].y;
        initial++;
        return number;
    });

    //console.log("hi" + data0[0].y);


    var maxValue = d3.max(value);
    console.log("maxValue" + maxValue);

    var y0 = d3.scale.linear()
        .domain([0, maxLength])
        .range([height0, 0]);

    var yAxis0 = d3.svg.axis()
        .scale(y0)
        .orient("left");
    //.tickFormat(d3.format("%"));



    svg1.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + height0 + ")")
        .call(xAxis0)
        .append("text")
        .attr("class", "label")
        .attr("x", width0)
        .attr("y", -6)
        .style("text-anchor", "end");
    // .text("Time between Eruptions (min.)");

    svg1.append("g")
        .attr("class", "y axis")
        .call(yAxis0);


    svg1.selectAll(".bar")
        .data(data0)
        .enter().insert("rect", ".axis")
        .attr("class", "bar")
        .attr("x", function(d) { return x0(d.x) + 1; })
        .attr("y", function(d) { return y0(d.y); })
        .attr("width", x0(data0[0].dx + data0[0].x) - x0(data0[0].x) - 1)
        .attr("height", function(d) { console.log("lslslsll" + y0(d.y)); return height0 - y0(d.y); });


    /*************************/



    // Define svg attr
    var margin = {top: 10, right: 20, bottom: 500, left: 30},
        margin2 = {top: 140, right: 480, bottom: 10, left: 30},
        margin3 = {top: 140, right: 20, bottom: 10, left: 490},
        width = 960 - margin.left - margin.right,
        width2 = 960 - margin2.left - margin2.right,
        width3 = 960 - margin3.left - margin3.right,
        height = 600 - margin.top - margin.bottom,
        height2 = 600 - margin2.top - margin2.bottom;


    var svg = d3.select("#brushmap").append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom);


    var x1 = d3.scale.linear(),
        y1 = d3.scale.linear().range([height, 0]);

    // A formatter for counts - for Histogram
    var formatCount = d3.format(",.0f");

    var results = [];
    results = dataContent.partitionData;

        var data = [];
        data = dataContent.PartitionSize;


        /***************************************************************/
        /*--------------- show overview partition and brush------------*/
        /***************************************************************/

        // partition number
                     var partitionNum = data.length;

        // max partition size
                     var maxPartitionSize = d3.max(data);

        // partitionEachLength
                     var barWidth = width/partitionNum;

        x1.range([0, width + barWidth/2]);

        var xAxis = d3.svg.axis().scale(x1).orient("bottom").ticks(partitionNum);

      x1.domain([0, partitionNum]);
      y1.domain([0,maxPartitionSize]);

      svg.append("g")
              .attr("class", "x axis")
              .attr("transform", "translate(0," + height + ")")
        .call(xAxis);

        svg.selectAll(".bar")
              .data(data)
              .enter().append("rect")
              .attr("class", "bar")
              .attr("x", function(d,i) {return x1(i+1) - barWidth / 2; })
              .attr("width", barWidth)
              .attr("y", function(d) { return  y1(d);})
              .attr("height", function(d) { return height - y1(d); });

      // TODO Brush
      // Detailed partition Number
      var ShowPartitionNum = 1;

      var brush = d3.svg.brush()
              .x(x1)
              .extent([0.5,0.5 + ShowPartitionNum])
              .on("brush", brushed);

      svg.append("g")
              .attr("class", "x brush")
              .call(brush)
              .selectAll("rect")
              .attr("y", -6)
              .attr("height", height + 7);


      /***************************************************************/
      /*--------------------- Show Detailed Partition----------------*/
      /***************************************************************/

      var maxNum = d3.max(results[0]);
      var minNum = d3.min(results[0]);


      //Show HeatMap for each Partition
      showHeatMap(0, minNum, maxNum);

      //Show Histogram for each Partition
      showHistogram(0, minNum, maxNum);


      function brushed() {

        extent0 = brush.extent();
        extent0[0] = Math.floor(extent0[0]) + 0.5;
        extent0[1] = extent0[0] + ShowPartitionNum;
        var selectPartition = extent0[0] - 0.5;
        d3.select(this).transition().duration(50)
                .call(brush.extent(extent0));

        var maxNum = d3.max(results[selectPartition]);
        var minNum = d3.min(results[selectPartition]);

        var delHeap = svg.select("#heatmap").remove();
        var delHeap = svg.select("#histogram").remove();

        //Show HeatMap for each Partition
        showHeatMap(selectPartition, minNum, maxNum);

        //Show Histogram for each Partition
        showHistogram(selectPartition, minNum, maxNum);

      }

      //Show HeatMap for each Partition
      function showHeatMap(selectPartition, minNum, maxNum){

        var perColumnNum = Math.ceil(Math.sqrt(maxPartitionSize));
        var perSize = width2/perColumnNum;

        // Define the ColorZone
        var colorZone = d3.scale.linear().range(["white", "orange"]);
        colorZone.domain([minNum,maxNum]);  // calculate color range

        // Apend heatMap
        var heatMap = svg.append("g").attr("id","heatmap").selectAll("#heatmap")
                .data(results[selectPartition])
                .enter()
                .append("rect")
                .attr("width", perSize)
                .attr("height",perSize)
                .attr("transform",function(d,i){
                  var x = calX(i, perSize, perColumnNum);
                  var y = calY(i, perSize, perColumnNum);
                  return "translate(" + x + "," + y + ")";}
        )
        .attr("fill", function(d){ return colorZone(d); });

      }

      //Show Histogram for each Partition
      function showHistogram(selectPartition, minNum, maxNum){


        var x3 = d3.scale.linear()
                .domain([minNum, maxNum])
                .range([0, width3]);

        // Generate a histogram using 20 uniformly-spaced bins.
        var data = d3.layout.histogram()
                .bins(20)
                (results[selectPartition]);

        var y3 = d3.scale.linear()
                .domain([0, d3.max(data, function(d) { return d.y; })])
                .range([0, height2]);


        var xAxis3 = d3.svg.axis()
                .scale(x3)
                .orient("top");

        var bar = svg.append("g")
                .attr("id","histogram").selectAll("#histogram")
                .data(data)
                .enter().append("g")
                .attr("class", "bar")
                .attr("transform", function(d) { return "translate(" + (x3(d.x) + margin3.left)  + "," + (margin3.top) + ")"; });

        bar.append("rect")
                .attr("x", 1)
                .attr("width", x3(data[0].dx + minNum) - 1)
                .attr("height", function(d) {  return y3(d.y); });

        bar.append("text")
                .attr("dy", ".75em")
                .attr("y", 6)
                .attr("x", x3(data[0].dx + minNum) / 2)
                .attr("text-anchor", "middle")
                .text(function(d) { return formatCount(d.y); });

        svg.selectAll("#histogram").append("g")
                .attr("class", "x3 axis")
                .attr("transform", "translate(" + margin3.left + "," + (margin3.top) + ")")
        .call(xAxis3);
        }


        function calX(innerNum, perSize, perColumnNum){
        var x = margin2.left + (innerNum % perColumnNum) * perSize;
        return x;
    }
                     function calY(innerNum, perSize, perColumnNum){
        var y = margin2.top  + perSize * (Math.floor(innerNum/perColumnNum) + 1);
        return y;
    }




});