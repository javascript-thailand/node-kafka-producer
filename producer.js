var Kafka = require('node-rdkafka');

function onLog(log) {
  console.log(log);
}

function onDR(err, report) {
  console.log('delivery-report: ' + JSON.stringify(report));
  counter++;
}

function onError(err) {
  console.error('Error from producer');
  console.error(err);
}

function onReady(arg) {
  console.log('producer ready.' + JSON.stringify(arg));

  for (var i = 0; i < maxMessages; i++) {
    var value = Buffer.from('value-' +i);
    var key = "key-"+i;
    // if partition is set to -1, librdkafka will use the default partitioner
    var partition = -1;
    producer.produce(topicName, partition, value);
  }

  //need to keep polling for a while to ensure the delivery reports are received
  var pollLoop = setInterval(function() {
      producer.poll();
      if (counter === maxMessages) {
        clearInterval(pollLoop);
        producer.disconnect();
      }
    }, 1000);
}

function onDisconnect(arg) {
  console.log('producer disconnected. ' + JSON.stringify(arg));
}

var producer = new Kafka.Producer({
  //'debug' : 'all',
  'metadata.broker.list': 'localhost:9092',
  'dr_cb': true  //delivery report callback
});

var topicName = 'test-topic';

//logging debug messages, if debug is enabled
producer.on('event.log', onLog);

//logging all errors
producer.on('event.error', onError);

//counter to stop this sample after maxMessages are sent
var counter = 0;
var maxMessages = 10;

producer.on('delivery-report', onDR);

//Wait for the ready event before producing
producer.on('ready', onReady);

producer.on('disconnected', onDisconnect);

//starting the producer
producer.connect();