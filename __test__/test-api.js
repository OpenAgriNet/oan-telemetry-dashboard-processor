const request = require('supertest');
const { app } = require('../index');

// Mock the processTelemetryLogs function
jest.mock('../index', () => {
  const originalModule = jest.requireActual('../index');
  return {
    ...originalModule,
    processTelemetryLogs: jest.fn().mockResolvedValue({
      processed: 5,
      status: 'success'
    })
  };
});

// Mock event processors module
jest.mock('../eventProcessors', () => ({
  eventProcessors: {
    'OE_ITEM_RESPONSE': jest.fn(),
    'Feedback': jest.fn()
  },
  loadEventProcessors: {
    loadFromDatabase: jest.fn(),
    registerEventProcessor: jest.fn().mockResolvedValue({ success: true })
  }
}));

describe('API Endpoints', () => {
  describe('GET /health', () => {
    test('should return health status', async () => {
      const response = await request(app).get('/health');
      expect(response.statusCode).toBe(200);
      expect(response.body).toHaveProperty('status', 'UP');
    });
  });

  describe('POST /api/process-logs', () => {
    test('should process logs and return success', async () => {
      const response = await request(app).post('/api/process-logs');
      expect(response.statusCode).toBe(200);
      expect(response.body).toHaveProperty('message', 'Telemetry log processing triggered successfully');
      expect(response.body).toHaveProperty('processed', 5);
      expect(response.body).toHaveProperty('status', 'success');
    });
  });

  describe('GET /api/event-processors', () => {
    test('should return list of registered event processors', async () => {
      const response = await request(app).get('/api/event-processors');
      expect(response.statusCode).toBe(200);
      expect(response.body).toHaveProperty('processors');
      expect(response.body.processors).toBeInstanceOf(Array);
      expect(response.body.processors).toHaveLength(2); // OE_ITEM_RESPONSE and Feedback
    });
  });

  describe('POST /api/event-processors', () => {
    test('should register a new event processor', async () => {
      const newProcessor = {
        eventType: 'TEST_EVENT',
        tableName: 'test_events',
        fieldMappings: {
          uid: 'uid',
          eventData: 'edata.eks.data'
        }
      };

      const response = await request(app)
        .post('/api/event-processors')
        .send(newProcessor)
        .set('Content-Type', 'application/json');

      expect(response.statusCode).toBe(201);
      expect(response.body).toHaveProperty('message', 'Event processor registered successfully');
      expect(response.body).toHaveProperty('eventType', 'TEST_EVENT');
    });

    test('should return 400 for missing required fields', async () => {
      const invalidProcessor = {
        eventType: 'TEST_EVENT'
        // Missing tableName and fieldMappings
      };

      const response = await request(app)
        .post('/api/event-processors')
        .send(invalidProcessor)
        .set('Content-Type', 'application/json');

      expect(response.statusCode).toBe(400);
      expect(response.body).toHaveProperty('error');
    });
  });
});
