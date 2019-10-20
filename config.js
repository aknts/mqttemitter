var config = {
	// Select query, 1 for faster results, 2 for more random
	qsw: 2,
	// Set message rate, please keep it in hundreds, lowest value is 100 msec, harcoded to avoid flooding
	msgr: 500,
	// How many records the sample must have
	sample: 120,
	// Looping through the database to feed the flow
	loop: 1,
	// Enable console logging
	log: 1,
	// The mqtt broker url, post must be included
	mqttBroker: 'mqtt://localhost:1883',
	// The mqtt topic that the data flows through
	mqttTopic: 'TestSub',
	// The path to the sqlite database
	sqliteDB: '../db/data.db'
};

module.exports = config;