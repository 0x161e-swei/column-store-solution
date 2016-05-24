var margin = {top: 20, right: 30, bottom: 30, left: 40},
    width = 960 - margin.left - margin.right,
    height = 500 - margin.top - margin.bottom;

console.log("partition0*********------");





console.log("partition0------");

d3.json("/DataGeneration/partition0.json", function(error, faithful) {
    if (error) throw error;


    console.log("partition0------" + faithful.length);
    console.log("min---" + d3.min(faithful));
    console.log("max---" + d3.max(faithful));

    var x = d3.scale.linear()
        .domain([d3.min(faithful), d3.max(faithful)])
        .range([0, width]);

    var xAxis = d3.svg.axis()
        .scale(x)
        .orient("bottom");

    var y = d3.scale.linear()
        .domain([0, .1])
        .range([height, 0]);


    var yAxis = d3.svg.axis()
        .scale(y)
        .orient("left")
    /*.tickFormat(d3.format("%"))*/;

    var line = d3.svg.line()
        .x(function(d) { return x(d[0]); })
        .y(function(d) { return y(d[1]); });

    var histogram = d3.layout.histogram()
        .frequency(false)
        .bins(x.ticks(40));

    var svg_kde = d3.select("body").append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    svg_kde.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + height + ")")
        .call(xAxis)
        .append("text")
        .attr("class", "label")
        .attr("x", width)
        .attr("y", -6)
        .style("text-anchor", "end")
        .text("Value");

    svg_kde.append("g")
        .attr("class", "y axis")
        .call(yAxis);


    var data = histogram(faithful),
        kde = kernelDensityEstimator(epanechnikovKernel(7), x.ticks(500));



/*    svg_kde.selectAll(".bar")
        .data(data)
        .enter().insert("rect", ".axis")
        .attr("class", "bar")
        .attr("x", function(d) { return x(d.x) + 1; })
        .attr("y", function(d) { return y(d.y); })
        .attr("width", x(data[0].dx + data[0].x) - x(data[0].x) - 1)
        .attr("height", function(d) { return height - y(d.y); });*/

    svg_kde.append("path")
        .datum(kde(faithful))
        .attr("class", "line")
        .attr("d", line);
});

function kernelDensityEstimator(kernel, x) {
    return function(sample) {
        return x.map(function(x) {
            return [x, d3.mean(sample, function(v) { return kernel(x - v); })];
        });
    };
}

function epanechnikovKernel(scale) {
    return function(u) {
        return Math.abs(u /= scale) <= 1 ? .75 * (1 - u * u) / scale : 0;
    };
}