const amqp = require('amqplib');
const config = require('./config');
const topicConfig = require('./topic-config');

class IoTSimulator {
  constructor() {
    this.connection = null;
    this.channel = null;
  }

  // Initialize connection and channel
  async connect() {
    try {
      console.log('Connecting to RabbitMQ...');
      this.connection = await amqp.connect(config.rabbitmq.url);
      this.channel = await this.connection.createChannel();

      // Create the topic exchange
      await this.channel.assertExchange(
        config.rabbitmq.exchange.name,
        config.rabbitmq.exchange.type,
        config.rabbitmq.exchange.options
      );

      console.log('Connected to RabbitMQ');
      console.log(`Exchange "${config.rabbitmq.exchange.name}" ready`);
    } catch (error) {
      console.error('Connection error:', error);
      throw error;
    }
  }

  // Generate random sensor reading
  generateSensorData(region, location, metric) {
    const data = {
      region,
      location,
      metric,
      value: this.getRandomValue(metric),
      timestamp: new Date().toISOString(),
      sensorId: `${region}-${location}-${metric}-001`
    };

    return data;
  }

  // Generate realistic values based on metric type
  getRandomValue(metric) {
    switch(metric) {
      case 'temperature':
        return (Math.random() * 40 + 10).toFixed(2); // 10-50°C
      case 'humidity':
        return (Math.random() * 100).toFixed(2);     // 0-100%
      case 'pressure':
        return (Math.random() * 200 + 900).toFixed(2); // 900-1100 hPa
      default:
        return (Math.random() * 100).toFixed(2);
    }
  }

  // Publish sensor reading
  async publishReading(region, location, metric) {
    // Create routing key: region.location.metric
    const routingKey = `${region}.${location}.${metric}`;
    
    // Generate sensor data
    const sensorData = this.generateSensorData(region, location, metric);
    
    // Convert to buffer
    const message = Buffer.from(JSON.stringify(sensorData));

    // Publish to exchange with routing key
    this.channel.publish(
      config.rabbitmq.exchange.name,
      routingKey,
      message,
      {
        persistent: true,  // Survive broker restart
        contentType: 'application/json',
        timestamp: Date.now()
      }
    );

    console.log(`Published [${routingKey}]:`, sensorData);
  }

  // Simulate random sensor readings
  async startSimulation() {
    console.log('\nStarting IoT simulation...\n');

    setInterval(() => {
      // Pick random region
      const regions = Object.keys(topicConfig.locations);
      const region = regions[Math.floor(Math.random() * regions.length)]; // usa, europe, asia

      // Pick random location within region
      const locations = topicConfig.locations[region];
      const location = locations[Math.floor(Math.random() * locations.length)];

      // Pick random metric
      const metric = topicConfig.metrics[
        Math.floor(Math.random() * topicConfig.metrics.length)
      ];

      // Publish reading
      this.publishReading(region, location, metric);
    }, 2000); // Every 2 seconds
  }

  // Cleanup on exit
  async close() {
    await this.channel.close();
    await this.connection.close();
    console.log('\nSimulator stopped');
  }
}

// Run the simulator
async function main() {
  const simulator = new IoTSimulator();
  
  try {
    await simulator.connect();
    await simulator.startSimulation();

    // Handle graceful shutdown
    process.on('SIGINT', async () => {
      await simulator.close();
      process.exit(0);
    });
  } catch (error) {
    console.error('Error:', error);
    process.exit(1);
  }
}

main();
