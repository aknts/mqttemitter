// [Settings]
//retrieve the configuration settings
var config = require('./config.js');

// [Libraries]
const sqlite3 = require('sqlite3').verbose();
var mqttmod = require('./mqttmod.js')

// [Global variables]
var query;
var db;
if (config.loop == 1) {
	var cacheArray = new Array();
}

// [Queries]
switch (config.qsw) {
	case 1:
		query = "SELECT * FROM Scans WHERE rowid IN (SELECT rowid FROM Scans ORDER BY RANDOM() LIMIT "+config.sample+")";
	break;
	case 2:
		query = "SELECT * FROM Scans ORDER BY RANDOM() LIMIT "+config.sample+"";
	break;
}

// [Functions]
function connectDB (callback, nextstep) {
	db = new sqlite3.Database(config.sqliteDB, (err) => {
		if (err) {
			console.error(err.message);
		}
		switch (config.log) {
			case 1:
			console.log('Connected to the database.');
			break;
		}
	});
	callback(nextstep);
};

function closeDB () {
		db.close((err) => {
		if (err) {
			return console.error(err.message);
		}
		switch (config.log) {
		case 1:
			console.log('Close the database connection.');
		break;
		}
	});
}

function getData (callback) {
	db.serialize(() => {
		if (callback != null) {
			db.all(query, (err, array) => {
				if (err) {
					console.error(err.message);
				}
				if (array.length == config.sample) {
					callback(array);
				}
			});
		};
		if (config.loop == 1) {
			db.all(query, (err, array) => {
				if (err) {
					console.error(err.message);
				}
				if (array.length == config.sample) {
					cacheArray = array;
				}
			});
		};
		closeDB();
	});
};

function printArray (data) {
		
		var i = 0;
		var ii = 0;
		
		if (config.msgr >= 100) {
			ii = config.msgr;
		} else {
			ii = 100;
		}
		data.forEach(function (item, index) {
			setTimeout(function () {
				sendMessage(index,item);
			}, i);
			i=i+ii;
		});
};

function sendMessage (counter, item) {
	mqttmod.send(config.mqttBroker,config.mqttTopic,JSON.stringify(item));
	if (counter == config.sample-1) {
		if ((config.loop == 1) && (cacheArray.length == config.sample)) {
			console.log('Incoming more data');
			console.log('cacheArray is '+cacheArray.length+' rows!');
			printArray(cacheArray);
			connectDB(getData); 
		} else {
			console.log('No config.loop detected. Exiting, nothing to stream!');
		}
	}
}

// [Execution]
connectDB(getData, printArray);