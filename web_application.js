var config = require('config');
var redis = require('redis');
var express = require('express');
var app = express();
var server = require('http').createServer(app);
var socket = require('socket.io')(server);
var request = require('request');


var redis_host = config.get('redis.host');
var redis_port = config.get('redis.port');
var redis_channel = config.get('redis.channel');
var web_port = config.get('web.port');

function process_redis_data(channel,message) {
	if(channel==redis_channel){
		console.log("the message is %s",message);
		socket.sockets.emit('data',message);
	}
}

//load static web page
app.use('/',express.static(__dirname+'/web_pages'));
app.use('/welcome',express.static(__dirname+'/web_pages/welcomePage.html'));
app.use('/show_chart',express.static(__dirname+'/web_pages/showChart.html'));

//map RESTful request
app.post('/addStockData',function (req,res) {
	res.send("successed");
	console.log(req.query.stock_symbol);
	var stock_name = req.query.stock_symbol;
	request.post("http://localhost:8081/add_stock_symbol").form({stock_symbol:stock_name});
});

var redis_client = redis.createClient(redis_port,redis_host);
redis_client.subscribe(redis_channel);
redis_client.on('message',process_redis_data);

server.listen(web_port);

function shutdown_hook() {
	redis_client.quit();
	console.log("close the project");
	process.exit();
}
process.on('exit', shutdown_hook);
