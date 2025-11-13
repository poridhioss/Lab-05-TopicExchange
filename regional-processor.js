const amqp = require('amqplib');
const config = require('./config');
const topicConfig = require('./topic-config');

class RegionalProcessor {
  constructor(region) {
    this.region = region;
    this.connection = null;
    this.channel = null;
    this.queueName = `regional.${region}`;
    this.pattern = `${region}.#`;
  }

  async connect() {
    try {
      console.log(`Connecting Regional Processor [${this.region}]...`);
      this.connection = await amqp.connect(config.rabbitmq.url);
      this.channel = await this.connection.createChannel();

      // Ensure exchange exists
      await this.channel.assertExchange(
        config.rabbitmq.exchange.name,
        config.rabbitmq.exchange.type,
        config.rabbitmq.exchange.options
      );

      // Create queue
      await this.channel.assertQueue(this.queueName, {
        durable: true
      });

      // Bind queue to exchange with pattern
      await this.channel.bindQueue(
        this.queueName,
        config.rabbitmq.exchange.name,
        this.pattern
      );

      console.log(`Regional Processor ready [${this.region}]`);
      console.log(`Queue: ${this.queueName}`);
      console.log(`Pattern: ${this.pattern}`);
      console.log(`Listening for messages...\n`);
    } catch (error) {
      console.error('Connection error:', error);
      throw error;
    }
  }

  // Process incoming messages
  async consume() {
    this.channel.consume(
      this.queueName,
      (msg) => {
        if (msg !== null) {
          const sensorData = JSON.parse(msg.content.toString());
          const routingKey = msg.fields.routingKey;

          this.processSensorData(routingKey, sensorData);

          // Acknowledge message
          this.channel.ack(msg);
        }
      },
      {
        noAck: false  // Manual acknowledgment
      }
    );
  }

  // Process sensor data based on region
  processSensorData(routingKey, data) {
    console.log(`\nRegional Processor [${this.region.toUpperCase()}]`);
    console.log(`Routing Key: ${routingKey}`);
    console.log(`Data:`, {
      location: data.location,
      metric: data.metric,
      value: data.value,
      timestamp: data.timestamp
    });

    // Regional-specific processing logic
    if (this.region === 'usa') {
      this.processUSAData(data);
    } else if (this.region === 'europe') {
      this.processEuropeData(data);
    } else if (this.region === 'asia') {
      this.processAsiaData(data);
    }
  }

  processUSAData(data) {
    console.log(`USA Processing: Storing to US datacenter...`);
    // In real scenario: Store to US-specific database
  }

  processEuropeData(data) {
    console.log(`Europe Processing: GDPR compliance check...`);
    // In real scenario: Ensure GDPR compliance
  }

  processAsiaData(data) {
    console.log(`Asia Processing: Regional analytics...`);
    // In real scenario: Asia-specific analytics
  }

  async close() {
    await this.channel.close();
    await this.connection.close();
    console.log(`\nRegional Processor [${this.region}] stopped`);
  }
}

// Get region from command line argument
const region = process.argv[2] || 'usa';

async function main() {
  if (!['usa', 'europe', 'asia'].includes(region)) {
    console.error('Invalid region. Use: usa, europe, or asia');
    process.exit(1);
  }

  const processor = new RegionalProcessor(region);
  
  try {
    await processor.connect();
    await processor.consume();

    process.on('SIGINT', async () => {
      await processor.close();
      process.exit(0);
    });
  } catch (error) {
    console.error('Error:', error);
    process.exit(1);
  }
}

main();
