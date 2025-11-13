// Routing patterns and queue configurations
module.exports = {
  // Define all routing patterns
  patterns: {
    // Regional patterns
    usaAll: 'usa.#',              // All USA sensors
    europeAll: 'europe.#',        // All Europe sensors
    asiaAll: 'asia.#',            // All Asia sensors
    
    // Metric-specific patterns
    allTemperature: '*.*.temperature',  // Temperature from anywhere
    allHumidity: '*.*.humidity',        // Humidity from anywhere
    allPressure: '*.*.pressure',        // Pressure from anywhere
    
    // Combined patterns
    usaTemperature: 'usa.*.temperature',
    
    // Admin pattern
    everything: '#'               // Monitor everything
  },

  // Queue configurations
  queues: {
    regional_usa: {
      name: 'regional.usa',
      pattern: 'usa.#',
      durable: true
    },
    regional_europe: {
      name: 'regional.europe',
      pattern: 'europe.#',
      durable: true
    },
    metric_temperature: {
      name: 'metric.temperature',
      pattern: '*.*.temperature',
      durable: true
    },
    metric_humidity: {
      name: 'metric.humidity',
      pattern: '*.*.humidity',
      durable: true
    },
    alert_temperature: {
      name: 'alert.temperature',
      pattern: '*.*.temperature',
      durable: true
    },
    admin_monitor: {
      name: 'admin.all',
      pattern: '#',
      durable: true
    }
  },

  // Sensor locations
  locations: {
    usa: ['california', 'texas', 'newyork', 'florida'],
    europe: ['germany', 'france', 'spain', 'italy'],
    asia: ['japan', 'china', 'india', 'singapore']
  },

  // Metric types
  metrics: ['temperature', 'humidity', 'pressure']
};
