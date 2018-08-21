const Kafka = require('node-rdkafka');

let numSent = 0;
let numDelivered = 0;

function onLog(log) {
  console.log(log);
}

function onDR(err, report) {
  console.log('delivery-report: ' + JSON.stringify(report));
}

function onError(err) {
  console.error('Error from producer');
  console.error(err);
}

function onReady(arg) {
  console.log('producer ready.' + JSON.stringify(arg));
  setTimeout(sendMessage, 1000);
}

function sendMessage() {
  numSent++;
  let message = Buffer.from('Message # ' +numSent);
  // if partition is set to -1, librdkafka will use the default partitioner
  let partition = -1;
  producer.produce(topicName, partition, message);
  console.log('prodcuced: ' + message);
  setTimeout(sendMessage, 1000);
}

function onDisconnect(arg) {
  console.log('producer disconnected. ' + JSON.stringify(arg));
}

let producer = new Kafka.Producer({
  'metadata.broker.list': 'localhost:9092',
  'dr_cb': true  //delivery report callback
});

let topicName = 'test-topic';

//logging debug messages, if debug is enabled
producer.on('event.log', onLog);

//logging all errors
producer.on('event.error', onError);

// producer.on('delivery-report', onDR);

producer.on('delivery-report', function(err, report) {
  console.log('delivery-report: ' + JSON.stringify(report));
  counter++;
});

//Wait for the ready event before producing
producer.on('ready', onReady);

producer.on('disconnected', onDisconnect);

//starting the producer
producer.connect();