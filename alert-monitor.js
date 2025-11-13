const amqp = require('amqplib');
const config = require('./config');

class AlertMonitor {
  constructor() {
    this.connection = null;
    this.channel = null;
    this.queueName = 'alert.temperature';
    this.pattern = '*.*.temperature';
    
    // Alert thresholds
    this.thresholds = {
      high: 40,  // Alert if temp > 40°C
      low: 15    // Alert if temp < 15°C
    };
  }

  async connect() {
    try {
      console.log('Connecting Alert Monitor...');
      this.connection = await amqp.connect(config.rabbitmq.url);
      this.channel = await this.connection.createChannel();

      await this.channel.assertExchange(
        config.rabbitmq.exchange.name,
        config.rabbitmq.exchange.type,
        config.rabbitmq.exchange.options
      );

      await this.channel.assertQueue(this.queueName, {
        durable: true
      });

      await this.channel.bindQueue(
        this.queueName,
        config.rabbitmq.exchange.name,
        this.pattern
      );

      console.log('Alert Monitor ready');
      console.log(`Queue: ${this.queueName}`);
      console.log(`Pattern: ${this.pattern}`);
      console.log(`Thresholds: Low < ${this.thresholds.low}°C, High > ${this.thresholds.high}°C`);
      console.log('Monitoring temperature alerts...\n');
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

          this.checkAlert(routingKey, sensorData);
          this.channel.ack(msg);
        }
      }
    );
  }

  checkAlert(routingKey, data) {
    const temperature = parseFloat(data.value);

    if (temperature > this.thresholds.high) {
      this.sendAlert('HIGH', routingKey, data, temperature);
    } else if (temperature < this.thresholds.low) {
      this.sendAlert('LOW', routingKey, data, temperature);
    } else {
      console.log(`[${routingKey}] Temperature normal: ${temperature}°C`);
    }
  }

  sendAlert(severity, routingKey, data, temperature) {
    console.log('\n' + '='.repeat(60));
    console.log(`ALERT: ${severity} TEMPERATURE DETECTED`);
    console.log('='.repeat(60));
    console.log(`Location: ${data.region} > ${data.location}`);
    console.log(`Temperature: ${temperature}°C`);
    console.log(`Threshold: ${severity === 'HIGH' ? `> ${this.thresholds.high}°C` : `< ${this.thresholds.low}°C`}`);
    console.log(`Sensor ID: ${data.sensorId}`);
    console.log(`Time: ${data.timestamp}`);
    console.log('='.repeat(60));

    // In real scenario: Send email, SMS, webhook, etc.
    this.notifyOps(severity, data, temperature);
  }

  notifyOps(severity, data, temperature) {
    console.log(`Sending notification to ops team...`);
    console.log(`Triggering SMS alert...`);
    console.log(`Updating dashboard...`);
    
    // Simulate notification
    // In production: integrate with email service, SMS gateway, etc.
  }

  async close() {
    await this.channel.close();
    await this.connection.close();
    console.log('\n👋 Alert Monitor stopped');
  }
}

async function main() {
  const monitor = new AlertMonitor();
  
  try {
    await monitor.connect();
    await monitor.consume();

    process.on('SIGINT', async () => {
      await monitor.close();
      process.exit(0);
    });
  } catch (error) {
    console.error('Error:', error);
    process.exit(1);
  }
}

main();
