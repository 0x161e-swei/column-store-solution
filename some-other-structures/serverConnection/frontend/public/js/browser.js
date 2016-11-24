var socket = io('http://localhost:8080');

// Add EventListener to the button to send command 

// var partitionCount = 2;
// var q = queue();

// for (var m = 0; m < partitionCount; m++){
//     q.defer(d3.json, "../data/" + m + ".json");
// }

// q.awaitAll(DATAHANDLE);

// function DATAHANDLE (error, results){
// 		console.log(results);
// }


// console.log(tasks.length);
// console.log(tasks[0]);

// tasks.forEach(function(t){ q.defer(t); });
var margin = {top: 20, right: 30, bottom: 30, left: 40},
			width = 960 - margin.left - margin.right,
			height = 500 - margin.top - margin.bottom;

var svg = d3.select("body").append("svg")
		.attr("width", width + margin.left + margin.right)
		.attr("height", height + margin.top + margin.bottom)
		.append("g")
		.attr("transform", "translate(" + margin.left + "," + margin.top + ")");

document.getElementById('sendCommand').addEventListener("click", function () {
	console.log("button clicked");
	socket.emit("sendConnect")
});


document.getElementById('fileSelectButton').addEventListener("click", function () {
	console.log("file choose button clicked");
	var sel = document.getElementById('fileSelect');

	console.log("dataset chosen as " + sel.options[sel.selectedIndex].value);
	socket.emit("datasetOpen", {filename: sel.options[sel.selectedIndex].value});

});

socket.on('data', function (data) {
	var outputText = document.getElementById('serverResponse');
	outputText.innerHTML = outputText.innerHTML + data;
	console.log("data from middleware received");
});

socket.on('fileContent', function (dataContent) {
	var faithful = [];
	faithful = JSON.parse(dataContent.content);
	var x = d3.scale.linear()
				.domain([d3.min(faithful), d3.max(faithful)])
				.range([0, width]);

		var histogram = d3.layout.histogram()
				.frequency(true);
			   // .bins(x.ticks());

		var data = histogram(faithful);

		var m = -1;
		var maxLength = d3.max(data.map(function(num){
			m++;
			console.log("dataLength..." + data[m].length);
			return data[m].length;
		}));

		console.log("maxLength===" + maxLength);
	   // console.log("data0++++" + data[0].length + "ssss" + data[11].length + data.length);


		var xAxis = d3.svg.axis()
				.scale(x)
				.orient("bottom");



		//console.log("ffff" + faithful);

//TODO:  zheli xie de shenme gui !!!!!!!!!!!!!!



		var initial = 0;

		var value =  data.map(function(number){
			console.log("sssss2232323232"  + data[initial].y);
			number = data[initial].y;
			initial++;
			return number;
		});

		//console.log("hi" + data[0].y);


		var maxValue = d3.max(value);
		console.log("maxValue" + maxValue);

		var y = d3.scale.linear()
				.domain([0, maxLength])
				.range([height, 0]);

		var yAxis = d3.svg.axis()
				.scale(y)
				.orient("left");
				//.tickFormat(d3.format("%"));



		svg.append("g")
				.attr("class", "x axis")
				.attr("transform", "translate(0," + height + ")")
				.call(xAxis)
				.append("text")
				.attr("class", "label")
				.attr("x", width)
				.attr("y", -6)
				.style("text-anchor", "end");
			   // .text("Time between Eruptions (min.)");

		svg.append("g")
				.attr("class", "y axis")
				.call(yAxis);


		svg.selectAll(".bar")
				.data(data)
				.enter().insert("rect", ".axis")
				.attr("class", "bar")
				.attr("x", function(d) { return x(d.x) + 1; })
				.attr("y", function(d) { return y(d.y); })
				.attr("width", x(data[0].dx + data[0].x) - x(data[0].x) - 1)
				.attr("height", function(d) { return height - y(d.y); });

});