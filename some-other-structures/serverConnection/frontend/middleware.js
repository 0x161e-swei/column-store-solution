var app = require('http').createServer();
// var fs = require('fs');
var io = require('socket.io')(app);
// var winston = require('winston');
var net = require('net');
var queue = require('queue-async');
var fs = require('fs');

var finalhandler = require('finalhandler')
var http = require('http')
var serveIndex = require('serve-index')
var serveStatic = require('serve-static')


HTTP_PORT = 8020;
SOCKET_PORT = 8080;

DB_TELNET_PORT = 8088;
DB_TELNET_HOST = "localhost";

var current_connection = 0;
var database_client = 0;

var q = queue();

function printOutput(server, output) {
	if (current_connection) {
		if (output[0] == '1'){
			console.log("filename is " + __dirname + "/../data/" + output.substring(1));

			q.defer(fs.readFile, __dirname + "/../data/" + output.substring(1), 'utf8');
			q.await(function (error, content) {
				// console.log(content);
				current_connection.emit("fileContent", {content: content});
				fs.close
			});
		}
		else {
			current_connection.emit("data", output);
		}
	}
}

function send_command(command) {
	if (!database_client) {
		database_client = net.connect(parseInt(DB_TELNET_PORT), DB_TELNET_HOST, function (){
			database_client.setEncoding('utf8');
			console.log('Connected to Database server.');
			database_client.on('data', function (chunk) {
				var output = "";
				for (var i = 0; i < chunk.length; i++) {
					if (chunk[i] == '\n') {
						// TODO: parse the message (data) accordingly
						// process_output("db", output);	// "db" indicates the source of data
						console.log(output);

						// Display the response on screen
						printOutput("db", output);
					}
					else {
						output += chunk[i];
					}
				};
			});
			database_client.on('end', function (){console.log('database server hanged up!'); client = 0;});
			database_client.write(command + "\n");
		});
		
	}
	else {
		database_client.write(command + "\n");
	}
}

// Socket.IO
app.listen(SOCKET_PORT);
io.on('connection', function (socket) {
	current_connection = socket;
	console.log("socketIO connected");

	// button send connect clicked
	socket.on("sendConnect", function () {
		console.log("sendConnect received");
		send_command("connect");
	});

	// button choose file open clicked with para
	socket.on("datasetOpen", function (data) {
		console.log("dataser specified as " + data.filename);
		console.log("open_file " + data.filename);
		send_command("open_file " + data.filename);
	}
	);
});

// HTTP PUBLIC FOLDER 
var serve = serveStatic('public');
var server = http.createServer(function onRequest(req, res){
	var done = finalhandler(req, res);
	serve(req, res, function onNext(err) {
		if (err) return done(err);
	})
})
server.listen(HTTP_PORT);
