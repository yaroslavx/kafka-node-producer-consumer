import Kafka from 'node-rdkafka';
import eventType from '../eventType.js';

const stream = Kafka.Producer.createWriteStream(
  {
    'metadata.broker.list': 'localhost:9092',
  },
  {},
  {
    topic: 'test',
  }
);

stream.on('error', (err) => {
  console.error('Something went wrong, error in our kafka stream');
  console.error(err);
});

function queueRandomMessage() {
  const category = getRandomPlatform();
  const action = getRandomAction(category);
  const event = { category, action };
  const success = stream.write(eventType.toBuffer(event));
  if (success) {
    console.log(`message queued (${JSON.stringify(event)})`);
  } else {
    console.log('Too many messages in the queue already..');
  }
}

function getRandomPlatform() {
  const categories = ['Web', 'Mobile'];
  return categories[Math.floor(Math.random() * categories.length)];
}

function getRandomAction(platform) {
  if (platform === 'Web') {
    const actions = ['mousedown', 'mouseup'];
    return actions[Math.floor(Math.random() * actions.length)];
  } else if (platform === 'Mobile') {
    const actions = ['touchstart', 'touchmove'];
    return actions[Math.floor(Math.random() * actions.length)];
  } else {
    return 'no user events';
  }
}

setInterval(() => {
  queueRandomMessage();
}, 3333);
