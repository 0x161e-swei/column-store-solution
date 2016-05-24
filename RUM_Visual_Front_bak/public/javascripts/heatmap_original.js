var margin_heatmap = {top: 20, right: 90, bottom: 50, left: 50},
    width_heatmap = 960 - margin_heatmap.left - margin_heatmap.right,
    height_heatmap = 520 - margin_heatmap.top - margin_heatmap.bottom;



var blockSize = 8;      //need calculate    data.length/blockNum
//var blockNum = 10;      // blockNum = PartitionNum

var colorZone = d3.scale.linear().range(["white", "green"]);


d3.json("/data/dataTest.json", function (error, data) {

    var blockNum = Math.ceil(data.length/blockSize);
    //var blockSize = Math.ceil(data.length/blockNum);
    var tile_width = width_heatmap/blockNum;
    console.log(width_heatmap);
    console.log(tile_width);
    var tile_height = height_heatmap/blockSize;
    console.log(tile_height);

    var x = d3.scale.linear()
        .domain([0,blockNum])
        .range([0, width_heatmap]);

    var xAxis = d3.svg.axis()
        .scale(x)
        .orient("bottom")
        .tickSize(-height_heatmap);

    var zoom = d3.behavior.zoom()
        .x(x)
        .scaleExtent([1, 1])
        .on("zoom", zoomed);

    var svg_heatmap = d3.select("body").selectAll("#heatMap").append("svg")
        .attr("width", width_heatmap + margin_heatmap.left + margin_heatmap.right)
        .attr("height", height_heatmap + margin_heatmap.top + margin_heatmap.bottom)
        .append("g")
        .attr("transform", "translate(" + margin_heatmap.left + "," + margin_heatmap.top + ")")
        .call(zoom);


    console.log(data);

    var maxData = d3.max(data, function(d){
        return d;
    });
    console.log(maxData);

    var minData = d3.min(data, function(d){
        return d;
    });

    console.log(minData);

    colorZone.domain([minData,maxData]);    // calculate color range


    data = data.sort(sortByDateAscending);

    for(i=0;i<data.length;i += 8){

            console.log(data[i]+" "+data[i+1]+" "+data[i+2]+" "+data[i+3]+" "+data[i+4]+" "+data[i+5]+" "+data[i+6]+" "+data[i+7]);


    }


    svg_heatmap.selectAll(".tile")
        .data(data)
        .enter().append("rect")
        .attr("class", "tile")
        .attr("width", tile_width)
        .attr("height",tile_height)
        .attr("x", function(d,i){return calX(i,tile_width,blockSize)})
        .attr("y", function(d,i){return calY(i,tile_height,blockSize)})
        .attr("fill", function (d,i) {
            //console.log(numAndColor(d,maxData,minData));
            //return "rgb(0, 0, " +Math.ceil(numAndColor(d,maxData,minData))+ ")";
            return colorZone(d);
        })
        .attr("stroke-width", "0.3")
        .attr("stroke", "rgb(0,0,0)");



    svg_heatmap.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate("+ margin_heatmap.left + "," + (height_heatmap + margin_heatmap.top) + ")")
        .call(xAxis);


    function zoomed() {
        svg_heatmap.select(".x.axis").call(xAxis);
        // TODO: move data around
        console.log("x: " + d3.event.translate[0] + " y: " + d3.event.translate[1] + " scale: " + d3.event.scale);
        //svg.select(".y.axis").call(yAxis);
    }


});

function sortByDateAscending(a, b) {
    return a - b;
}

function numAndColor(x,min, max){
    return (255-255*(x-min)/(max-min));
}

function calX(i, width, blockSize){
    console.log(i);
    return (Math.floor(i/blockSize)*width+50);
}
function calY(i, height, blockSize){
    if(0 == Math.floor(i/blockSize)%2){
        return (470 - (i%blockSize+1)*height);
    }
    else{
        return (470 - (8-i%blockSize)*height);

    }
}



