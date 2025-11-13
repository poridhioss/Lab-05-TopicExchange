const amqp = require('amqplib');
const config = require('./config');

class MetricAnalyzer {
  constructor(metric) {
    this.metric = metric;
    this.connection = null;
    this.channel = null;
    this.queueName = `metric.${metric}`;
    this.pattern = `*.*.${metric}`;
    this.readings = [];
  }

  async connect() {
    try {
      console.log(`Connecting Metric Analyzer [${this.metric}]...`);
      // 1. Connect to RabbitMQ
      this.connection = await amqp.connect(config.rabbitmq.url);
      // 2. Create channel  
      this.channel = await this.connection.createChannel();
    
      // 3. Ensure exchange exists
      await this.channel.assertExchange(
        config.rabbitmq.exchange.name,
        config.rabbitmq.exchange.type,
        config.rabbitmq.exchange.options
      );

      // 4. Create queue
      await this.channel.assertQueue(this.queueName, {
        durable: true
      });

      // Bind queue to exchange with pattern
      await this.channel.bindQueue(
        this.queueName,
        config.rabbitmq.exchange.name,
        this.pattern
      );

      console.log(`Metric Analyzer ready [${this.metric}]`);
      console.log(`Queue: ${this.queueName}`);
      console.log(`Pattern: ${this.pattern}`);
      console.log(`Listening for ${this.metric} readings...\n`);
    } catch (error) {
      console.error('Connection error:', error);
      throw error;
    }
  }

  async consume() {
    this.channel.consume(
      this.queueName,
      (msg) => {
        if (msg !== null) {
          const sensorData = JSON.parse(msg.content.toString());
          const routingKey = msg.fields.routingKey;

          this.analyzeMetric(routingKey, sensorData);
          this.channel.ack(msg);
        }
      }
    );
  }

  analyzeMetric(routingKey, data) {
    console.log(`\Metric Analyzer [${this.metric.toUpperCase()}]`);
    console.log(`Routing Key: ${routingKey}`);
    console.log(`Reading:`, {
      region: data.region,
      location: data.location,
      value: `${data.value} ${this.getUnit()}`,
      timestamp: data.timestamp
    });

    // Store reading for analysis
    this.readings.push({
      value: parseFloat(data.value),
      timestamp: data.timestamp
    });

    // Keep only last 10 readings
    if (this.readings.length > 10) {
      this.readings.shift();
    }

    // Calculate statistics
    this.displayStatistics();
  }

  displayStatistics() {
    if (this.readings.length === 0) return;

    const values = this.readings.map(r => r.value);
    const avg = (values.reduce((a, b) => a + b, 0) / values.length).toFixed(2);
    const min = Math.min(...values).toFixed(2);
    const max = Math.max(...values).toFixed(2);

    console.log(`\nStatistics (last ${this.readings.length} readings):`);
    console.log(`   Average: ${avg} ${this.getUnit()}`);
    console.log(`   Min: ${min} ${this.getUnit()}`);
    console.log(`   Max: ${max} ${this.getUnit()}`);
  }

  getUnit() {
    switch(this.metric) {
      case 'temperature': return '°C';
      case 'humidity': return '%';
      case 'pressure': return 'hPa';
      default: return '';
    }
  }

  async close() {
    await this.channel.close();
    await this.connection.close();
    console.log(`\nMetric Analyzer [${this.metric}] stopped`);
  }
}

const metric = process.argv[2] || 'temperature';

async function main() {
  if (!['temperature', 'humidity', 'pressure'].includes(metric)) {
    console.error('Invalid metric. Use: temperature, humidity, or pressure');
    process.exit(1);
  }

  const analyzer = new MetricAnalyzer(metric);
  
  try {
    await analyzer.connect();
    await analyzer.consume();

    process.on('SIGINT', async () => {
      await analyzer.close();
      process.exit(0);
    });
  } catch (error) {
    console.error('Error:', error);
    process.exit(1);
  }
}

main();
