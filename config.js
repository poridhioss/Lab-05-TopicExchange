// RabbitMQ connection configuration
module.exports = {
  rabbitmq: {
    url: 'amqp://localhost',
    exchange: {
      name: 'iot.events',
      type: 'topic',
      options: {
        durable: true  // Exchange survives broker restart
      }
    }
  }
};
