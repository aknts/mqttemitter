process.title = 'emitter';
// Initialization 
// Config
const config = JSON.parse(Buffer.from(require('./config.js'), 'base64').toString());

// Settings
var broker = config.globalsettings.broker;
var mynodeid = config.mynodeid
var logtopic = mynodeid+'/log';
var controltopic = mynodeid+'/control';
var nextnode = config.nextnode;
var nextnodedatatopic = nextnode+'/data';
var pipelinetopic = config.nameid+'/broadcast';
var rate_transmit = config.appsettings.rate_transmit;
var rate_sampling = config.appsettings.rate_sampling;
var logmode = config.appsettings.logmode;
var mariadb = config.appsettings.mariadb;

// Modules
const mqttmod = require('mqttmod');
const l = require('mqttlogger')(broker, logtopic, mqttmod, logmode);
var mysql = require('mysql');
var pool  = mysql.createPool({
  connectionLimit : 10,
  host            : mariadb.host,
  port			  : mariadb.port,
  user            : mariadb.user,
  password        : mariadb.password
});

// Variables
var readyresponse = '{"node":"'+mynodeid+'","name":"emitter","request":"ready"}';
var cleanheapresponse = '{"node":"'+mynodeid+'","name":"emitter","request":"cleanheap"}';
var terminatingresponse = '{"node":"'+mynodeid+'","name":"emitter","request":"terminating"}';
//l.info("Connecting to database "+dbfile);

var minTimestamp;
var from;
var to;
var init = 0;
var halt = 1;
var appmodules = ['emitter','filter','loadbalancer','trilaterator','aggregator'];
var livemodules = [];

function getPreliminaryData () {
	pool.query('select min(TimestampSecs) as minTimestamp from '+mariadb.db+'.Scans',  (err,rows) => {
		if (err) {
			l.error(err.message);
		}
		minTimestamp = rows[0].minTimestamp;
		//l.info('minTimestamp: '+minTimestamp);
		from = minTimestamp;
		startReceiving();
	});
}

function startReceiving () {
	// Start recieving control MQTT messages
	////l.info('Started receiving control messages on '+controltopic);
	mqttmod.receive(broker,controltopic,filterRequests);
	
	// Start recieving control MQTT messages
	//l.info('Started receiving control messages on '+pipelinetopic);
	mqttmod.receive(broker,pipelinetopic,filterRequests);

	mqttmod.send(broker,pipelinetopic,readyresponse);
}

function filterRequests(payload){
	try {
		data = JSON.parse(payload);
    } catch (e) {
        l.error('Received not valid JSON.\r\n'+payload);
		return false;
    }
	var requestingNode = data.node;
	var requestingNodeName = data.name;	
	if (requestingNode != mynodeid) {
		switch(data.request) {
			case 'ready':
				if (livemodules.length < appmodules.length) {
					var alpha = -1;
					var beta = 0
					for(var i = 0; i < appmodules.length; i++){
						alpha = appmodules.indexOf(requestingNodeName);
						if (alpha > -1) {
							for(var ii = 0; ii < livemodules.length; ii++){
								if (livemodules[ii].name == requestingNodeName) {
									beta = 1;
								}
							}
						}
					}
					if (alpha > -1 && beta == 0) {
						if (requestingNodeName == 'trilaterator') {
							livemodules.push({"node":requestingNode,"pid":data.pid,"name":requestingNodeName});
							mqttmod.send(broker,requestingNode+'/'+data.pid+'/control',readyresponse);
						} else {
							livemodules.push({"node":requestingNode,"name":requestingNodeName});
							mqttmod.send(broker,requestingNode+'/control',readyresponse);
						}
						//l.info('Node '+requestingNode+' reported that is ready');
						//l.info('Informing the new nodes that local node is ready');
						console.log(livemodules);
					} 
					if (alpha > -1 && beta == 1) {
						//l.info('A '+requestingNodeName+' node already exists');
					}
					if (alpha == -1) {
						//l.info(requestingNodeName+' node is not valid');
					}
				}
				if (livemodules.length == appmodules.length) {
					if (init == 0 && halt == 1) {
						halt = 0;
						//l.info('All modules ready');
					}
					if (init == 1 && halt == 1){
						halt = 2;
						//l.info('All modules ready');
					}
					if (requestingNodeName == 'trilaterator' && init == 1 && halt == 0) {
						for(var i = 0; i < livemodules.length; i++){
								if (livemodules[i].name == requestingNodeName && livemodules[i].node == requestingNode && livemodules[i].pid != data.pid) {
									//l.info('Sending readyresponse to a new trilaterator');
									mqttmod.send(broker,requestingNode+'/'+data.pid+'/control',readyresponse);
								}	
						}
					}
				}
			break;
			case 'execute':
				if (init == 0 && halt == 0) {
					getDataNew(sendData);
					init = 1;
					//l.info('Starting application');
				} else if (init == 1 && halt == 2) {
					halt = 0;
					//l.info('Restarting application');
				} else {
					//l.info('Not all modules are loaded');
				}
			break;
			case 'terminating':
				for(var i = 0;i < livemodules.length;i++){ 
					if (livemodules[i].name == requestingNodeName && livemodules[i].node == requestingNode) { 
						switch(requestingNodeName) {
							case 'trilaterator':
								if ( data.pid == livemodules[i].pid) {
									livemodules.splice(i,1);
								}
							break;
							default:
								livemodules.splice(i,1);
						}
						console.log('livemodules');
						console.log(livemodules);
					}
				}
				if (livemodules.length < appmodules.length) {
					//l.info('Node '+requestingNode+' reported that is terminating, halt application.');
					halt = 1;
				}
			break;
			default:
				//l.info('Didn\'t receive a valid request');
		}
	}
}

function getDataNew (callback) {	
	var queryinprogress = 0;
	let i = 0;
	var retrieveData = setTimeout(function run(){
		heapCheck();
		if (halt == 0) {
			if (queryinprogress == 0) {
				queryinprogress = 1;
				to = (+from + +rate_sampling);
				pool.query('SELECT `RPi` AS "did",`TimestampSecs` AS `timestamp`,`MACAddress` AS "uid",`Signal` AS "RSSI" from '+mariadb.db+'.Scans WHERE TimestampSecs >= '+from+' AND TimestampSecs < '+to,  (err,rows) => {
					if (err) {
						l.error(err.message);
					}
					if (rows.length > 0) {
						//var alength = array.length;
						//l.info('Found '+alength+' results.');
						callback(rows);
						rows = null;
					} else {
						//l.info('No results between '+from+' and '+to+'.');
					}
					from = to;
					queryinprogress = 0;
				});
			} else {
				//l.info('Last query hasn\'t finished, looping through');
			}
		}
		setTimeout(run,rate_transmit);
		i++; 		
	},rate_transmit);
}

function sendData (results) {
	var rlength = results.length;
	l.info('Sending data, array of '+rlength+' results at: '+Date.now());
	mqttmod.send(broker,nextnodedatatopic,JSON.stringify(results));
	results = null;
	rlength = null;
}

function heapCheck () {
	var usage = '';
	const used = process.memoryUsage();
	for (let key in used) {
		usage = usage.concat(`${key} ${Math.round(used[key] / 1024 / 1024 * 100) / 100} MB, `);
		if (key == 'external') {
			usage=usage.slice(0, -2);
			l.info('Heap usage: '+usage);	
		}
	}
}

livemodules.push({"node":mynodeid,"name":"emitter"});
getPreliminaryData();

process.on('SIGTERM', function onSigterm () {
	//l.info('Got SIGTERM');
	mqttmod.send(broker,pipelinetopic,terminatingresponse);
});