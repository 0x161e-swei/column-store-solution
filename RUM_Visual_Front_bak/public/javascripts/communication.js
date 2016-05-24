/**
 * Created by logicRabbit on 1/10/16.
 */
// Add EventListener to the button to send command
SOCKET_PORT = 8080;
//var socket = io('http://localhost:' + SOCKET_PORT);
var nodejs_server = 'http://' + document.domain + ':' + location.port;
var socket = io(nodejs_server);

var op = ["Range Query","Point Query","Update","Delete","Insert"];


document.getElementById('applyFileButton').addEventListener("click", function () {
    var sel = document.getElementById('fileSelect');
    socket.emit("applyFile", {filename: sel.options[sel.selectedIndex].value} );
});

document.getElementById('applyWorkLoadButton').addEventListener("click", function () {
    var workLoad = document.getElementById('workLoadSelect');
    socket.emit("applyWorkLoad", {workLoad: workLoad.options[workLoad.selectedIndex].value} );
});

document.getElementById('exe').addEventListener("click", function () {
    socket.emit("exec_work", {status: "1"} );
});

document.getElementById('userConfirm').addEventListener("click", function () {
    var alg = document.getElementById('algorithmSel');
    var ghost = 0;
    if(document.getElementById("ghostCheck").checked) ghost = document.getElementById("ghostInput").value;
    alg = alg.options[alg.selectedIndex].value;
    part_algoArr = [alg, ghost];
    $( document ).ready(function() {
        $('#loading').modal('show');
    });
    socket.emit("part_algo", {part_algo: part_algoArr} );
});


document.getElementById('setRUM').addEventListener("click", function () {

    var alg = document.getElementById('algorithmSel');
    var check = document.getElementsByName('ghost');
    var ghost;
    for (var i = 0; i < check.length; i++){
        if(check[i].checked){
            if(i == 0)   ghost = 0;
            if(i == 1)   ghost = document.getElementById("ghostInput").value;
        }
    }
    alg = alg.options[alg.selectedIndex].value;
    part_algoArr = [alg, ghost];

    var R = document.getElementById('nR').value;
    var U = document.getElementById('nU').value;
    var M = document.getElementById('nM').value;
    var userRUM = [R, U, M];
    //d3.select("eleTriangle").remove();
    d3.select("#changeT");
    console.log("R" + R + " U" + U + " M" + M);

    socket.emit("userRUM", {part_algo: part_algoArr, userRUM: userRUM} );

});

// Apply partition and ask for the info
document.getElementById('phy_Partition').addEventListener("click", function () {
    socket.emit("applyPartition", {status: "1"} );
});

document.getElementById('showCost').addEventListener("click", function () {
    var srCost = document.getElementById('srCost').value;
    var rrCost = document.getElementById('rrCost').value;
    var swCost = document.getElementById('swCost').value;
    var rwCost = document.getElementById('rwCost').value;
    var lgCost1 = document.getElementById('lgCost1').value;
    var lgCost2 = document.getElementById('lgCost2').value;
    socket.emit("show_Cost", {srCost: srCost, rrCost: rrCost, swCost: swCost, rwCost: rwCost,lgCost1:lgCost1,lgCost2:lgCost2} );
});

socket.on('message', function (dataContent) {
    var msg = dataContent.msg;
    tempAlert(msg,1000);
});


socket.on('datasetView', function (dataContent) {
    var dataSet = dataContent.dataSetInfo;
    console.log("dataSet is " + dataSet);
    overviewShow(dataSet);
});

socket.on('workloadView', function (dataContent) {

    //overviewShow(dataContent.dataSetData);
    console.log("hello here is the workload!!!!");
    var workLoad = dataContent.workLoadInfo;
    showWorkLoad(workLoad);
    showStackedbar(dataContent.stored,"stored");
    showStackedbar(dataContent.sorted,"sorted");
    $('#loading').modal('hide');


});

socket.on('currentPartition', function (dataContent) {

    var value = dataContent.serverRUM;
    evaluateRUM(value[0],value[1],value[2]);
    showPartitionInfo(dataContent.partitionInfo);
    $('#loading').modal('hide');
});

socket.on('evaluate', function (dataContent) {

    var value = dataContent.serverRUM;
    evaluateRUM(value[0],value[1],value[2]);

});

socket.on('cost', function (dataContent) {

    var value = dataContent.costInfo;
    costBar(value);
});


function showAlert(hideAfter) {
    //$('#loading').modal('show');
    if(hideAfter) {
        setTimeout(fadeItOut,500);
    }
}

function fadeItOut() {
    $('#loading').modal('hide');
}


function tempAlert(msg,duration)
{
    var el = document.createElement("div");
    el.setAttribute("style","position:fixed;top:4%;left:15%;right:15%;z-index:1000;opacity:0.8;border-color: #d6e9c6;");
    content = "<div class=\"alert alert-warning\" id=\"success-alert\" position=\"fixed\">"+msg+"</div>";
    el.innerHTML = content;
    setTimeout(function(){
        $("#success-alert").fadeOut("slow",function(){el.parentNode.removeChild(el);});
    },duration);
    document.body.appendChild(el);
}

function dataSetChange(){
    var sel = document.getElementById('fileSelect');
    $( document ).ready(function() {
        $('#loading').modal('show');
    });
    socket.emit("showFile", {filename: sel.options[sel.selectedIndex].value} );
}

function workLoadChange(){
    var workLoad = document.getElementById('workLoadSelect');
    $( document ).ready(function() {
        $('#loading').modal('show');
    });
    socket.emit("showWorkLoad", {workLoad: workLoad.options[workLoad.selectedIndex].value} );
}

function evaluateRUM(R, U, M){

    d3.select("#RUMEvaluate").remove();

    var evaluateRy = 200 - (R/150 * 180);
    var evaluateUx = 200 - U/150 * length * Math.sin(60/180 * Math.PI);
    var evaluateUy = 200 + U/150 * length * Math.cos(60/180 * Math.PI);
    var evaluateMx = 200 + M/150 * length * Math.sin(60/180 * Math.PI);
    var evaluateMy = 200 + M/150 * length * Math.cos(60/180 * Math.PI);


    var systemEvaluate = "200," + evaluateRy + " " + evaluateUx + "," + evaluateUy + " " + evaluateMx + "," + evaluateMy + " " + "200," + evaluateRy;

    holder.append('polygon')
        .attr("id", "RUMEvaluate")
        .attr('points',systemEvaluate)
        .attr("fill", "none")
        .attr("stroke","red");
}

function costBar(data){

    d3.select("#costSVG").remove();
    var margin = {top: 10, right: 10, bottom: 30, left: 50},
        width = parseInt(d3.select('#performance').style('width'), 10) - margin.left - margin.right,
        height = 550 - margin.top - margin.bottom;

    var svg = d3.select("#costView").append("svg").attr("id",'costSVG')
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    var x0 = d3.scale.ordinal()
        .rangeRoundBands([0, width], .1);

    var x1 = d3.scale.ordinal();

    var y = d3.scale.linear()
        .range([height, 0]);

    var color = d3.scale.ordinal()
        .range(["#98abc5", "#8a89a6", "#7b6888", "#6b486b", "#a05d56", "#d0743c", "#ff8c00"]);

    var xAxis = d3.svg.axis()
        .scale(x0)
        .orient("bottom");

    var yAxis = d3.svg.axis()
        .scale(y)
        .orient("left")
        .tickFormat(d3.format(".2s"));

    var Names = d3.keys(data[0]).filter(function(key) { return key !== "Name"; });

    data.forEach(function(d) {
        d.costs = Names.map(function(name) { return {name: name, value: +d[name]}; });
    });

    x0.domain(data.map(function(d) { return d.Name; }));
    x1.domain(Names).rangeRoundBands([0, x0.rangeBand()]);
    y.domain([0, d3.max(data, function(d) { return d3.max(d.costs, function(d) { return d.value; }); })]);

    svg.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + height + ")")
        .call(xAxis);

    svg.append("g")
        .attr("class", "y axis")
        .call(yAxis)
        .append("text")
        .attr("transform", "rotate(-90)")
        .attr("y", 6)
        .attr("dy", ".71em")
        .style("text-anchor", "end")
        .text("Cost");

    var attr = svg.selectAll(".name")
        .data(data)
        .enter().append("g")
        .attr("class", "name")
        .attr("transform", function(d) { return "translate(" + x0(d.Name) + ",0)"; });

    attr.selectAll("rect")
        .data(function(d) { return d.costs; })
        .enter().append("rect")
        .attr("width", x1.rangeBand())
        .attr("x", function(d) { return x1(d.name); })
        .attr("y", function(d) { return y(d.value); })
        .attr("height", function(d) { return height - y(d.value); })
        .style("fill", function(d) { return color(d.name); });

    var legend = svg.selectAll(".legend")
        .data(Names.slice().reverse())
        .enter().append("g")
        .attr("class", "legend")
        .attr("transform", function(d, i) { return "translate(0," + i * 20 + ")"; });

    legend.append("rect")
        .attr("x", width - 18)
        .attr("width", 18)
        .attr("height", 18)
        .style("fill", color);

    legend.append("text")
        .attr("x", width - 24)
        .attr("y", 9)
        .attr("dy", ".35em")
        .style("text-anchor", "end")
        .text(function(d) { return d; });





}

function showStackedbar(data, id){

    console.log(data);

    console.log("id is" + id);

    var newID = "wl"+id;

    d3.select("#" + newID).remove();


    var margin = {top: 5, right: 20, bottom: 100, left: 50},
        width = parseInt(d3.select("#"+id).style('width'), 10) - margin.left - margin.right,
        height = 500 - margin.top - margin.bottom;
    marginOverview = { top: 430, right: margin.right, bottom: 20,  left: margin.left },
        heightOverview = 500 - marginOverview.top - marginOverview.bottom;

    // some colours to use for the bars
    var color = d3.scale.category10();

    // mathematical scales for the x and y axes
    var x = d3.scale.linear()
        .range([0, width]);
    var y = d3.scale.linear()
        .range([height, 0]);
    var xOverview = d3.scale.linear()
        .range([0, width]);
    var yOverview = d3.scale.linear()
        .range([heightOverview, 0]);

    // rendering for the x and y axes
    var xAxis = d3.svg.axis()
        .scale(x)
        .orient("bottom");
    var yAxis = d3.svg.axis()
        .scale(y)
        .orient("left");
    var xAxisOverview = d3.svg.axis()
        .scale(xOverview)
        .orient("bottom");


    var svg = d3.select("#"+id).append("svg").attr("id",newID)
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom);

    svg.append("defs").append("clipPath")
        .attr("id", "myclip")
        .append ("rect")
        .attr({x: 0, width: width, y: margin.top, height: height});

    var main = svg.append("g")
        .attr("id","mainView")
        .attr("class", "main")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");
    var overview = svg.append("g")
        .attr("class", "overview")
        .attr("transform", "translate(" + marginOverview.left + "," + marginOverview.top + ")");

    // brush tool to let us zoom and pan using the overview chart
    var brush = d3.svg.brush()
        .x(xOverview)
        .on("brush", brushed);

    var name = ["rq","pq","up","d","i"];

    data.forEach(function(d,i) {
        d.number = i+1; // turn the date string into a date object

        // adding calculated data to each count in preparation for stacking
        var y0 = 0; // keeps track of where the "previous" value "ended"
        d.costs = ["rq","pq","up","d","i"].map(function(name) {
            return { name: name,
                y0: y0,
                // add this count on to the previous "end" to create a range, and update the "previous end" for the next iteration
                y1: y0 += +d[name]
            };
        });
        // quick way to get the total from the previous calculations
        d.total = d.costs[d.costs.length - 1].y1;
        // return value;
    });

    // data ranges for the x and y axes
    x.domain([1, d3.max(data, function(d) {return d.number; })]);
    y.domain([0, d3.max(data, function(d) {return d.total; })]);

    xOverview.domain(x.domain());
    yOverview.domain(y.domain());

    // data range for the bar colours
    // (essentially maps attribute names to color values)
    color.domain(d3.keys(data[0]));



    // Legend
    var legendRectSize = width/50;
    var legendSpacing = width/45 - width/50;
    var legend = svg.append("g")
        .selectAll(".legend")
        .data(op)
        .enter()
        .append('g')
        .attr('transform', function(d, i) {
            var height = legendRectSize;
            var x = width*6/7;
            var y = i * width/55;
            return 'translate(' + x + ',' + y + ')';
        });

    legend.append('rect')
        .attr('width', legendRectSize)
        .attr('height', legendRectSize)
        .style('fill', function(d,i) { return color(name[i]); });

    legend.append('text')
        .attr('x', legendRectSize + legendSpacing)
        .attr('y', legendRectSize - 2 * legendSpacing)
        .attr("font-size", width/55)
        .text(function(d,i) { return op[i]; });




    // draw the axes now that they are fully set up
    main.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + height + ")")
        .call(xAxis);
    main.append("g")
        .attr("class", "y axis")
        .call(yAxis);
    overview.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + heightOverview + ")")
        .call(xAxisOverview);

    // draw the bars
    main.append("g")
        .attr("clip-path", "url(#myclip)")
        .attr("class", "bars")
        .attr("id","bar group")
        // a group for each stack of bars, positioned in the correct x position
        .selectAll(".bar.stack")
        .data(data)
        .enter().append("g")
        .attr("class", "bar stack")
        .attr("transform", function(d) { return "translate(" + x(d.number) + ",0)"; })
        // a bar for each value in the stack, positioned in the correct y positions
        .selectAll("rect")
        .data(function(d) { return d.costs; })
        .enter().append("rect")
        .attr("class", "bar")
        .attr("width", 6)
        .attr("y", function(d) { return y(d.y1); })
        .attr("height", function(d) { return y(d.y0) - y(d.y1); })
        .style("fill", function(d) { return color(d.name); });

    overview.append("g")
        .attr("class", "bars")
        .selectAll(".bar")
        .data(data)
        .enter().append("rect")
        .attr("class", "bar")
        .attr("x", function(d) { return xOverview(d.number) - 3; })
        .attr("width", 6)
        .attr("y", function(d) { return yOverview(d.total); })
        .attr("height", function(d) { return heightOverview - yOverview(d.total); });

    // add the brush target area on the overview chart
    overview.append("g")
        .attr("class", "x brush")
        .call(brush)
        .selectAll("rect")
        // -6 is magic number to offset positions for styling/interaction to feel right
        .attr("y", -6)
        // need to manually set the height because the brush has
        // no y scale, i.e. we should see the extent being marked
        // over the full height of the overview chart
        .attr("height", heightOverview + 7);  // +7 is magic number for styling

    var b = overview
        .select('.brush');
    b.selectAll('.resize')
        .remove();
    b.select('.background')
        .remove();
    brush.extent([1,150]);
    brush(b);
    brush.event(b);

    // zooming/panning behaviour for overview chart
    function brushed() {
        // update the main chart's x axis data range
        x.domain(brush.empty() ? xOverview.domain() : brush.extent());
        main.select(".x.axis").call(xAxis);
        // redraw the bars on the main chart
        main.selectAll(".bar.stack")
            .attr("transform", function(d) { return "translate(" +  x(d.number) + ",0)"; });
        // redraw the x axis of the main chart
    }



}


function overviewShow(data){


    d3.select("#overviewSVG").remove();
    var margin = {top: 20, right: 20, bottom: 100, left: 70},
        width = parseInt(d3.select('#define').style('width'), 10) - margin.left - margin.right,
        height = 550 - margin.top - margin.bottom;

    var svg = d3.select("#overview").append("svg").attr("id",'overviewSVG')
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    var x = d3.scale.linear().range([0, width]);
    var y = d3.scale.linear().range([height, 0]);

    var xAxis = d3.svg.axis()
        .scale(x)
        .orient("bottom");

    var yAxis = d3.svg.axis()
        .scale(y)
        .orient("left")
        .ticks(10);

    x.domain([0, data.length]);
    y.domain([0, d3.max(data)]);

    svg.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + height + ")")
        .call(xAxis)
        .selectAll("text")
        .style("text-anchor", "end")
        .attr("dx", "-.8em")
        .attr("dy", "-.120em")
        .attr("transform", "rotate(-30)" );

    svg.append("g")
        .attr("class", "y axis")
        .call(yAxis)
        .append("text")
        .attr("transform", "rotate(-90)")
        .attr("y", 6)
        .attr("dy", ".71em")
        .style("text-anchor", "end");


    // now add titles to the axes
    svg.append("text")
        .attr("text-anchor", "middle")  // this makes it easy to centre the text as the transform is applied to the anchor
        .attr("transform", "translate(-60,"+(height/2)+")rotate(-90)")  // text is drawn off the screen top left, move down and out and rotate
        .text("Frequency");

    svg.append("text")
        .attr("text-anchor", "middle")  // this makes it easy to centre the text as the transform is applied to the anchor
        .attr("transform", "translate("+ (width/2) +","+(height+50)+")")  // centre below axis
        .text("(*5000000) Domain");


    svg.selectAll("bar")
        .data(data)
        .enter().append("rect")
        .style("fill", "steelblue")
        .attr("x", function(d,i) { return x(i); })
        .attr("width", function(d,i) { return (x(i+1)-x(i)); })
        .attr("y", function(d) { return y(d); })
        .attr("height", function(d) { return height - y(d); });

    //setTimeout(showAlert(1), 3000);
    $('#loading').modal('hide');


}

function showWorkLoad(data){

    console.log("Hello");

    console.log("data!!!!!!!!!!!" + data.length);

    d3.select("#workLoadSVG").remove();

    var margin = {top: 10, right: 30, bottom: 100, left: 100},
        width = parseInt(d3.select('#define').style('width'), 10) - margin.left - margin.right,
        height = 550 - margin.top - margin.bottom;

    var x = d3.scale.linear()
        .range([0, width]);

    var y = d3.scale.linear()
        .range([height, 0]);

    var color = d3.scale.category10();

    var xAxis = d3.svg.axis()
        .scale(x)
        .orient("bottom");

    var yAxis = d3.svg.axis()
        .scale(y)
        .orient("left");

    var line = d3.svg.line()
        .interpolate("basis")
        .x(function(d) { return x(d.number); })
        .y(function(d) { return y(d.operationTime); });

    var svg = d3.select("#workLoad").append("svg").attr("id", 'workLoadSVG')
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    var tooltip =svg.append("rect")
        .attr("class","tooltip")
        .style("opacity",0.0);

    color.domain(["rq","pq","up","d","i"]);

    var operations = color.domain().map(function(name) {
        return {
            name: name,
            values: data.map(function(d,i) {
                return {number: i, operationTime: +d[name]};
            })
        };
    });

    //Todo: Change into min and max - given by server/dataset?
    x.domain([0,100]);

    y.domain([
        d3.min(operations, function(c) { return d3.min(c.values, function(v) { return v.operationTime; }); }),
        d3.max(operations, function(c) { return d3.max(c.values, function(v) { return v.operationTime; }); })
    ]);

    svg.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + height + ")")
        .call(xAxis)
        .append("text")
        .attr("class", "label")
        .attr("font-family", "Georgia")
        .attr("font-size", "15px")
        .attr("x", width)
        .attr("y", -6)
        .style("text-anchor", "end")
        .text("(*10000000) Domain");

    svg.append("g")
        .attr("class", "y axis")
        .call(yAxis)
        .append("text")
        .attr("x", 100)
        //.attr("transform", "rotate(-180)")
        //.attr("y", 6)
        //.attr("dy", ".71em")
        .style("text-anchor", "end")
        .text("Access Frequency");

    var operation = svg.selectAll(".operation")
        .data(operations)
        .enter().append("g")
        .attr("class", "operation");

    // Legend for workload distribution
    var legendRectSize = width/40;
    var legendSpacing = width/35 - width/40;
    var legend = svg.append("g")
        .selectAll(".legend")
        .data(operations)
        .enter()
        .append('g')
        .attr('transform', function(d, i) {
            var height = legendRectSize;
            var x = width*5/6;
            var y = i * width/35;
            return 'translate(' + x + ',' + y + ')';
        });

    legend.append('rect')
        .attr('width', legendRectSize)
        .attr('height', legendRectSize)
        .style('fill', function(d) { return color(d.name); });

    legend.append('text')
        .attr('x', legendRectSize + legendSpacing)
        .attr('y', legendRectSize - 2 * legendSpacing)
        .attr("font-size", width/40)
        .text(function(d,i) { return op[i]; });

    var path = operation.append("path");

    var tooltip = svg.append("text")
        .attr("class","tooltip")
        .style("opacity",0.8);

    path.attr("class", "line")
        .attr("id", function (d,i) {return "line" + i})
        .attr("d", function(d) { return line(d.values); })
        .attr("stroke-width",2)
        .style("stroke", function(d) { return color(d.name); });

    path.on("mouseover", function(d,i){

        var seline = "#line"+i;
        d3.select(seline).attr("stroke-width",6);

        var coordinates = [0, 0];
        coordinates = d3.mouse(this);
        var x = coordinates[0]+10;
        var y = coordinates[1]-10;

        console.log(x + "------" + y);

        tooltip.text(""+op[i])
            .attr("transform", "translate("+x+","+y+")")
            .style("opacity",1.0);

    }).on("mouseout", function(d,i){
        var seline = "#line"+i;
        d3.select(seline).attr("stroke-width",2);
        tooltip.style("opacity",0.0);

    });

}

function showPartitionInfo(data){


    d3.select("#partitionSVG").remove();

    var elem = document.getElementById("tooltip");
    if(elem) elem.parentNode.removeChild(elem);


    var partwid = parseInt(d3.select('#partitionResult').style('width'), 10);

    var margin =  { top: 20, right: 20, bottom: 100, left: 60 },
        width = partwid - margin.left - margin.right,
        height = 700 - margin.top - margin.bottom,
        marginOverview = { top: 650, right: margin.right, bottom: 20,  left: margin.left },
        heightOverview = 700 - marginOverview.top - marginOverview.bottom;

    var height1 = 2 * height / 3;
    var height2 = height/3;

    var color = d3.scale.category10();

    var name = ["rq","pq","up","d","i"];

    // mathematical scales for the x and y axes
    var x = d3.scale.linear()
        .range([0, width]);
    var y1 = d3.scale.linear()
        .range([height1, 0]);
    var y2 = d3.scale.linear()
        .range([height1, height2 + height1]);
    var xOverview = d3.scale.linear()
        .range([0, width]);
    var yOverview = d3.scale.linear()
        .range([heightOverview, 0]);

    // rendering for the x and y axes
    var xAxis = d3.svg.axis()
        .scale(x)
        .orient("bottom");
    var yAxis1 = d3.svg.axis()
        .scale(y1)
        .orient("left");
    var yAxis2 = d3.svg.axis()
        .scale(y2)
        .orient("left");
    var xAxisOverview = d3.svg.axis()
        .scale(xOverview)
        .orient("bottom");

    var svg = d3.select("#partitionResult").append("svg")
        .attr("id","partitionSVG")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom);

    svg.append("defs").append("clipPath")
        .attr("id", "myclip")
        .append("rect")
        .attr({x: 0, width: width, y: margin.top, height: height});

    var main = svg.append("g")
        .attr("id","mainView")
        .attr("class", "main")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");
    var overview = svg.append("g")
        .attr("class", "overview")
        .attr("transform", "translate(" + marginOverview.left + "," + marginOverview.top + ")");

    // brush tool to let us zoom and pan using the overview chart

    var showPartitionNum = 150;
    var brush = d3.svg.brush()
        .x(xOverview)
        //.extent([0, 1/50])
        .on("brush", brushed);


    console.log("extent is " + brush.extent());

    // Define the div for the partitiontooltip
    var div = d3.select("body").append("div")
        .attr("class", "partitiontooltip")
        .style("opacity", 0);

    data.forEach(function(d,i) {
        d.number = i+1; // turn the date string into a date object

        // adding calculated data to each count in preparation for stacking
        var y0 = 0; // keeps track of where the "previous" value "ended"
        d.costs = ["rq","pq","up","d","i"].map(function(name) {
            return { name: name,
                y0: y0,
                // add this count on to the previous "end" to create a range, and update the "previous end" for the next iteration
                y1: y0 += +d[name]
            };
        });
        // quick way to get the total from the previous calculations
        d.total = d.costs[d.costs.length - 1].y1;
        // return value;
    });

    // data ranges for the x and y axes
    x.domain([1, d3.max(data, function(d) {return d.number; })]);
    y1.domain([0, d3.max(data, function(d) {return d.total; })]);
    y2.domain([0, d3.max(data, function(d) {return d.size; })]);

    xOverview.domain(x.domain());
    yOverview.domain([0, d3.max(data, function(d) {return d.size; })]);

    // data range for the bar colours
    // (essentially maps attribute names to color values)
    color.domain(d3.keys(data[0]));



    // Legend
    var legendRectSize = width/50;
    var legendSpacing = width/45 - width/50;
    var legend = svg.append("g")
        .selectAll(".legend")
        .data(op)
        .enter()
        .append('g')
        .attr('transform', function(d, i) {
            var height = legendRectSize;
            var x = width*6/7;
            var y = i * width/55;
            return 'translate(' + x + ',' + y + ')';
        });

    legend.append('rect')
        .attr('width', legendRectSize)
        .attr('height', legendRectSize)
        .style('fill', function(d,i) { return color(name[i]); });

    legend.append('text')
        .attr('x', legendRectSize + legendSpacing)
        .attr('y', legendRectSize - 2 * legendSpacing)
        .attr("font-size", width/55)
        .text(function(d,i) { return op[i]; });


    // draw the axes now that they are fully set up
    main.append("g")
        .attr("class", "y axis")
        .call(yAxis1);
    main.append("g")
        .attr("class", "y axis")
        .call(yAxis2);
    overview.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + heightOverview + ")")
        .call(xAxisOverview);

    // draw the bars
    var group = main.append("g")
        .attr("clip-path", "url(#myclip)")
        .attr("class", "bars")
        .attr("id","bar group")
        // a group for each stack of bars, positioned in the correct x position
        .selectAll(".bar.stack")
        .data(data)
        .enter().append("g")
        .attr("class", "bar stack")
        .attr("transform", function(d) { return "translate(" + x(d.number) + ",0)"; })
        .on("mouseover", function(d) {
            console.log('mouseover' + d.rq);
            div.attr("id","tooltip");
            div.transition()
                .duration(200)
                .style("opacity", .9);
            div.html("<h4><h4 style='color:SeaGreen;'>Partition" + d.number + " Size is " + d.size + "</h4><h5 style='color:DarkBlue;'>Costs for each operation</h5>")
                .style("left", (d3.event.pageX - 50) + "px")
                .style("top", (d3.event.pageY - 200) + "px");
            //var tool = div.select("#tooltipsvg");

            var costArr = [Number(d.rq), Number(d.pq), Number(d.up), Number(d.d), Number(d.i)];
            var opName = ["range query", "point query", "update", "delete", "insert"];

            console.log("max Arr" + d3.max(costArr));

            var xArr = d3.scale.linear()
                .domain([0, d3.max(costArr)])
                .range([0, 200]);


            // Todo: ??? can not reuse this part?? sec time not work?
            d3.select(".partitiontooltip")
                .selectAll("#tooltip")
                .data(costArr)
                .enter().append("div")
                .style("width", function(d) { console.log("xArr(d) ---px" + xArr(d)); return (xArr(d)+"px"); })
                .style("background-color","orange")
                .style("text-align","right")
                .style("margin","1px")
                .style("font","12px sans-serif")
                .style("color","LightYellow")
                .text(function(d) { console.log("d is " + d); return (d); });

        })
        .on("mouseout", function(d) {
            div.transition()
                .duration(250)
                .style("opacity", 0);
        });

    // a bar for each value in the stack, positioned in the correct y positions
    group.selectAll("rect")
        .data(function(d) { return d.costs; })
        .enter().append("rect")
        .attr("class", "bar")
        .attr("width", 6)
        .attr("y", function(d) { return y1(d.y1); })
        .attr("height", function(d) { return y1(d.y0) - y1(d.y1); })
        .style("fill", function(d) { return color(d.name); });

    group.append("rect")
        .attr("id","hello")
        .attr("class", "bar")
        .attr("width", 6)
        .attr("y", function(d) { return (height1); })
        .attr("height", function(d) { return (y2(Number(d.size)) - height2); })
        .style("fill", "LightSalmon");

    main.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + height1 + ")")
        .call(xAxis);



    overview.append("g")
        .attr("class", "bars")
        .selectAll(".bar")
        .data(data)
        .enter().append("rect")
        .attr("class", "bar")
        .attr("x", function(d) { return xOverview(d.number) - 3; })
        .attr("width", 6)
        .attr("y", function(d) { return yOverview(d.size); })
        .attr("height", function(d) { return heightOverview - yOverview(d.size); })
        .attr("fill", "#F8BF0C");


    // add the brush target area on the overview chart
    overview.append("g")
        .attr("class", "x brush")
        .call(brush)
        .selectAll("rect")
        // -6 is magic number to offset positions for styling/interaction to feel right
        .attr("y", -6)
        // need to manually set the height because the brush has
        // no y scale, i.e. we should see the extent being marked
        // over the full height of the overview chart
        .attr("height", heightOverview + 7);  // +7 is magic number for styling


    var b = overview
        .select('.brush');
    b.selectAll('.resize')
        .remove();
    b.select('.background')
        .remove();
    brush.extent([1,200]);
    brush(b);
    brush.event(b);


    // zooming/panning behaviour for overview chart
    function brushed() {
        // update the main chart's x axis data range
        console.log("brush extent() is " + brush.extent());
        x.domain(brush.empty() ? xOverview.domain() : brush.extent());
        main.select(".x.axis").call(xAxis);
        // redraw the bars on the main chart
        main.selectAll(".bar.stack")
            .attr("transform", function(d) { return "translate(" +  x(d.number) + ",0)"; });
        // redraw the x axis of the main chart
    }

}






