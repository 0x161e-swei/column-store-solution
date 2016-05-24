/**
 * Created by logicRabbit on 1/21/16.
 */
var d3 = require('d3');
var queue = require('queue-async');
var fs = require('fs');

var brushMap = module.exports = function() {

   var data = [];

   var interactionMap = function(container) {
       console.log("results is " + data.length);

       console.log("container is " + container);



    // Define svg attr
    var margin = {top: 10, right: 20, bottom: 500, left: 30},
        margin2 = {top: 140, right: 480, bottom: 10, left: 30},
        margin3 = {top: 140, right: 20, bottom: 10, left: 490},
        width = 960 - margin.left - margin.right,
        width2 = 960 - margin2.left - margin2.right,
        width3 = 960 - margin3.left - margin3.right,
        height = 600 - margin.top - margin.bottom,
        height2 = 600 - margin2.top - margin2.bottom;


    var x1 = d3.scale.linear(),
        y1 = d3.scale.linear().range([height, 0]);

    // A formatter for counts - for Histogram
    var formatCount = d3.format(",.0f");

       var size = fs.readFileSync( __dirname + "/../../DataGeneration/size.json", 'utf8');


       size = JSON.parse(size);

            console.log("size" + size);

           // if (error) throw error;

            /***************************************************************/
            /*--------------- show overview partition and brush------------*/
            /***************************************************************/

            // partition number
            var partitionNum = size.length;

            console.log("partitionNum 233333333" + partitionNum);

            // max partition size
            var maxPartitionSize = d3.max(size);

            // partitionEachLength
            var barWidth = width/partitionNum;

            x1.range([0, width + barWidth/2]);

            var xAxis = d3.svg.axis().scale(x1).orient("bottom").ticks(partitionNum);

            x1.domain([0, partitionNum]);
            y1.domain([0,maxPartitionSize]);

            var svg = container.append("svg")
                  .attr("width", width + margin.left + margin.right)
                  .attr("height", height + margin.top + margin.bottom);


            svg.append("g")
                .attr("class", "x axis")
                .attr("transform", "translate(0," + height + ")")
                .call(xAxis);

            svg.selectAll(".bar")
                .data(size)
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

            var maxNum = d3.max(data[0]);
            var minNum = d3.min(data[0]);


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

                var maxNum = d3.max(data[selectPartition]);
                var minNum = d3.min(data[selectPartition]);

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
                    .data(data[selectPartition])
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
                var size = d3.layout.histogram()
                    .bins(20)
                    (data[selectPartition]);

                var y3 = d3.scale.linear()
                    .domain([0, d3.max(size, function(d) { return d.y; })])
                    .range([0, height2]);


                var xAxis3 = d3.svg.axis()
                    .scale(x3)
                    .orient("top");

                var bar = svg.append("g")
                    .attr("id","histogram").selectAll("#histogram")
                    .data(size)
                    .enter().append("g")
                    .attr("class", "bar")
                    .attr("transform", function(d) { return "translate(" + (x3(d.x) + margin3.left)  + "," + (margin3.top) + ")"; });

                bar.append("rect")
                    .attr("x", 1)
                    .attr("width", x3(size[0].dx + minNum) - 1)
                    .attr("height", function(d) {  return y3(d.y); });

                bar.append("text")
                    .attr("dy", ".75em")
                    .attr("y", 6)
                    .attr("x", x3(size[0].dx + minNum) / 2)
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


   };

    interactionMap.data = function(value) {
        if (!arguments.length) {
            return data;
        }
        data = value;
        return interactionMap;
    };

    return interactionMap;
};



