var  mqttmod = {
	send: function(host, subtopic, topicmessage){
		var mqtt = require('mqtt')
		var client  = mqtt.connect(host)
		client.on('connect', function () {
			client.subscribe(subtopic, function (err) {
				if (!err) {
					client.publish(subtopic, topicmessage)
					client.end()
				}
			})
		})
	},
	receive: function(host, subtopic, timeout) {
		var mqtt = require('mqtt');
		var client  = mqtt.connect(host);
		var data;
		client.on('connect', function () {
			client.subscribe(subtopic, function (err) {
				if (!err) {
					client.on('message', function (topic, message) {
					// message is Buffer
					data = message.toString();
					console.log(data);
					//client.end()
					})
				}
			})
		})
	return data;
	}
}
module.exports = mqttmod;