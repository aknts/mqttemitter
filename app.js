process.title = 'dasfest_measurements_emitter';
// Initialization 
// Config
const config = JSON.parse(Buffer.from(require('./config.js'), 'base64').toString());

// Settings
var broker = config.globalsettings.broker;
var logtopic = config.globalsettings.logtopic;
var listentopic = config.mynodeid;
var nextnode = config.nextnode;
var dbfile = config.appsettings.dbfile;
var rate_transmit = config.appsettings.rate_transmit;
var rate_sampling = config.appsettings.rate_sampling;

// Modules
const sqlite3 = require('sqlite3').verbose();
const mqttmod = require('mqttmod');
const dbclass = require('./sqlite');
const l = require('mqttlogger')(broker, logtopic, mqttmod);

// Variables
l.info("Connecting to database "+dbfile);
var db = dbclass.connectDB(sqlite3,dbfile);
var minTimestamp;
var clients = [];

// Functions
function getPreliminaryData () {
	db.get('select min(TimestampSecs) as minTimestamp from Scans', function(err, row){
		if (err) {
			l.error(err.message);
		}
		minTimestamp = row.minTimestamp;
		l.info('minTimestamp: '+minTimestamp);
		startReceiving();
	});
}

function startReceiving () {
	l.info('Now receiving messages on '+listentopic+' mqtt topic.');
	mqttmod.receive(broker,listentopic,filterRequests);	
}

function filterRequests(payload){
	try {
		data = JSON.parse(payload);
    } catch (e) {
        l.error('Received not valid JSON.\r\n'+payload);
		return false;
    }
	if (data.request == 'send') {
		var requestingNode = data.node;
		l.info('Received send request from '+requestingNode);
		if (requestingNode != nextnode) {
			l.error('Requesting node ('+requestingNode+') is not the next node ('+nextnode+') configured in node red.');
		} else {
			checkNode = clients.find(function (clients) { 
				return clients.node === requestingNode;
			});
			if (!checkNode) {
				clients.push({"node":requestingNode})
				l.info('Check node:' +checkNode);
				l.info('Accepted send request from '+requestingNode);
				getData(requestingNode);				
			} else {
				l.info('Node '+requestingNode+' has already requested data.');
			}
		}
	} else {
		l.error('Not a valid command.');
	}
}

function getData (requestingNode) {
	var from = minTimestamp;
	db.serialize(() => {
		db.run("begin transaction");
		setInterval(function(){
		var to = (+from + +rate_sampling);
		l.info('Find data between '+from+' and '+to+'.');
		var preparedQuery = db.prepare("select RPi as did,TimestampSecs as timestamp,MACAddress as uid,Signal as RSSI from Scans where TimestampSecs >= ? and TimestampSecs < ?");
		preparedQuery.all(from,to,(err, array) => {
			if (err) {
				l.error(err.message);
			}
			if (array.length > 0) {
				l.info('Found '+array.length+' results.');
				sendData(array);
			} else {
				l.info('No results between '+from+' and '+to+'.');
			}
			from = to;
		});
		preparedQuery.finalize();
		},rate_transmit);
	});
}

function sendData (results) {
	l.info('Sending data, array of '+JSON.stringify(results.length)+' results.');
	mqttmod.send(broker,nextnode,JSON.stringify(results));
}

getPreliminaryData();