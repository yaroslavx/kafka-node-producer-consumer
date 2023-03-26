# Sample of producer-consumer communication through kafka

## Prerequisites

- `node`
- `docker`

## Running locally

- `npm install` - installs npm dependencies.
- `./scripts/start-kafka.sh` - starts kafka inside docker container.
- `./scripts/create-topic.sh` - creates kafka topic.
- `npm run start:producer` - starts producer.
- `npm run start:consumer` - starts consumer.
