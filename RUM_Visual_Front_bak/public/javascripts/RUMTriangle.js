/**
 * Created by logicRabbit on 2/23/16.
 */
var holderWidth = 400;
var holderHeight = 330;

var holder = d3.select("#changeT")
    .append("svg")
    .attr("id", "RUMTriangle")
    .attr("width", holderWidth)
    .attr("height", holderHeight);

var length = 180;

var cal = 90 * Math.tan(60/180 * Math.PI);

var array = "200,20 " + (200 - cal) +",290 " + (200 + cal) + ",290 200,20";

holder.append('polygon')
    .attr("id", "RUMHolder")
    .attr('points', array)
    .attr("fill", "none")
    .attr("stroke", "black");

holder.append("text")
    .attr("x",200)
    .attr("y",10)
    .text("Read");

holder.append("text")
    .attr("x",(180 - cal))
    .attr("y",310)
    .text("Update");

holder.append("text")
    .attr("x",(180 + cal))
    .attr("y",310)
    .text("Memory");

var points = [ [200, 40], [(200 - cal), 290], [(200 + cal), 290], [200, 40]];

holder.append("line")          // attach a line
    .attr("id","Rline")
    .style("stroke", "black")  // colour the line
    .style("stroke-dasharray", ("3, 3"))  // <== This line here!!
    .attr("x1", 200)     // x position of the first end of the line
    .attr("y1", 20)      // y position of the first end of the line
    .attr("x2", 200)     // x position of the second end of the line
    .attr("y2", 200);    // y position of the second end of the line

holder.append("line")          // attach a line
    .attr("id","Uline")
    .style("stroke", "black")  // colour the line
    .style("stroke-dasharray", ("3, 3"))  // <== This line here!!
    .attr("x1", (200 - cal))     // x position of the first end of the line
    .attr("y1", 290)      // y position of the first end of the line
    .attr("x2", 200)     // x position of the second end of the line
    .attr("y2", 200);    // y position of the second end of the line

holder.append("line")          // attach a line
    .attr("id","Mline")
    .style("stroke", "black")  // colour the line
    .style("stroke-dasharray", ("3, 3"))  // <== This line here!!
    .attr("x1", (200 + cal))     // x position of the first end of the line
    .attr("y1", 290)      // y position of the first end of the line
    .attr("x2", 200)     // x position of the second end of the line
    .attr("y2", 200);    // y position of the second end of the line


var line = d3.svg.line()
    .x(function(d) { return d[0]; })
    .y(function(d) { return d[1]; })
    .interpolate('linear');

// when the input range changes update the triangle
d3.select("#nR").on("input", function() {
    updateR(+this.value);
});

d3.select("#nU").on("input", function() {
    updateU(+this.value);
});

d3.select("#nM").on("input", function() {
    updateM(+this.value);
});

// Initial starting radius of the circle
updateR(0);

updateU(0);

updateM(0);

// update the elements
function updateR(nR) {

    // adjust the text on the range slider
    d3.select("#nR-value").text(nR);
    d3.select("#nR").property("value", nR);

    var newRX = points[0][0];
    var newRY = 200 - (nR/150 * 180);

    points[0] = [newRX, newRY];
    points[3] = points[0];

    render();

}


function updateU(nU) {

    // adjust the text on the range slider
    d3.select("#nU-value").text(nU);
    d3.select("#nU").property("value", nU);

    var newUX = 200 - nU/150 * length * Math.sin(60/180 * Math.PI);
    var newUY = 200 + nU/150 * length * Math.cos(60/180 * Math.PI);

    points[1] = [newUX, newUY];

    render();
}

function updateM(nM) {

    // adjust the text on the range slider
    d3.select("#nM-value").text(nM);
    d3.select("#nM").property("value", nM);

    var newMX = 200 + nM/150 * length * Math.sin(60/180 * Math.PI);
    var newMY = 200 + nM/150 * length * Math.cos(60/180 * Math.PI);

    points[2] = [newMX, newMY];

    render();

}


function render(){

    path = holder.selectAll('path').data([points]);
    path.attr('d', function(d){console.log("helllll!!!!!!!!!!!" + d); return line(d) + 'Z'})
        .style('stroke-width', 1)
        .style('stroke', 'steelblue')
        .style('fill', 'rgba(182,239,248,0.4)');
    path.enter().append('svg:path').attr('d', function(d){return line(d) + 'Z'})
        .style('stroke-width', 1)
        .style('stroke', 'steelblue')
        .style('fill', 'rgba(182,239,248,0.4)');
    path.exit().remove()

}