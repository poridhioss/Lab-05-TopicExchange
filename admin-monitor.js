const amqp = require('amqplib');
const config = require('./config');

class AdminMonitor {
  constructor() {
    this.connection = null;
    this.channel = null;
    this.queueName = 'admin.all';
    this.pattern = '#';  // Catch ALL messages
    this.messageCount = 0;
    this.regionStats = {};
    this.metricStats = {};
  }

  async connect() {
    try {
      console.log('🔌 Connecting Admin Monitor...');
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

      console.log('Admin Monitor ready');
      console.log(`Queue: ${this.queueName}`);
      console.log(`Pattern: ${this.pattern} (ALL MESSAGES)`);
      console.log('Monitoring all IoT traffic...\n');

      // Display stats every 10 seconds
      this.startStatsReporting();
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

          this.logMessage(routingKey, sensorData);
          this.updateStats(sensorData);
          
          this.channel.ack(msg);
        }
      }
    );
  }

  logMessage(routingKey, data) {
    this.messageCount++;
    console.log(`\n[${this.messageCount}] 📡 Admin Monitor`);
    console.log(`   Routing Key: ${routingKey}`);
    console.log(`   Region: ${data.region}`);
    console.log(`   Location: ${data.location}`);
    console.log(`   Metric: ${data.metric}`);
    console.log(`   Value: ${data.value}`);
  }

  updateStats(data) {
    // Count by region
    this.regionStats[data.region] = (this.regionStats[data.region] || 0) + 1;
    
    // Count by metric
    this.metricStats[data.metric] = (this.metricStats[data.metric] || 0) + 1;
  }

  startStatsReporting() {
    setInterval(() => {
      this.displayDashboard();
    }, 10000); // Every 10 seconds
  }

  displayDashboard() {
    console.log('\n' + '='.repeat(60));
    console.log('ADMIN DASHBOARD - IoT System Statistics');
    console.log('='.repeat(60));
    console.log(`Total Messages Processed: ${this.messageCount}`);
    console.log('\nMessages by Region:');
    Object.entries(this.regionStats).forEach(([region, count]) => {
      console.log(`   ${region}: ${count} messages`);
    });
    console.log('\nMessages by Metric:');
    Object.entries(this.metricStats).forEach(([metric, count]) => {
      console.log(`   ${metric}: ${count} messages`);
    });
    console.log('='.repeat(60) + '\n');
  }

  async close() {
    await this.channel.close();
    await this.connection.close();
    console.log('\nAdmin Monitor stopped');
  }
}

async function main() {
  const monitor = new AdminMonitor();
  
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
