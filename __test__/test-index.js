const { parseTelemetryMessage } = require('../index');

jest.mock('../logger', () => ({
  info: jest.fn(),
  error: jest.fn(),
  debug: jest.fn()
}));

jest.mock('../eventProcessors', () => ({
  eventProcessors: {
    'OE_ITEM_RESPONSE': jest.fn(),
    'Feedback': jest.fn()
  },
  loadEventProcessors: {
    loadFromDatabase: jest.fn(),
    registerEventProcessor: jest.fn()
  }
}));

describe('Telemetry Log Processor', () => {
  describe('parseTelemetryMessage function', () => {
    test('should parse valid JSON message with events', () => {
      const message = JSON.stringify({
        id: 'ekstep.telemetry',
        ver: '2.2',
        events: [
          { eid: 'OE_START', uid: 'test@example.com' },
          { eid: 'OE_ITEM_RESPONSE', uid: 'test@example.com' }
        ]
      });
      
      const result = parseTelemetryMessage(message);
      expect(result).toHaveLength(2);
      expect(result[0].eid).toBe('OE_START');
      expect(result[1].eid).toBe('OE_ITEM_RESPONSE');
    });

    test('should return empty array for message without events', () => {
      const message = JSON.stringify({
        id: 'ekstep.telemetry',
        ver: '2.2'
      });
      
      const result = parseTelemetryMessage(message);
      expect(result).toEqual([]);
    });

    test('should return empty array for invalid JSON message', () => {
      const message = 'Not a valid JSON';
      const result = parseTelemetryMessage(message);
      expect(result).toEqual([]);
    });

    test('should return empty array for null or undefined message', () => {
      expect(parseTelemetryMessage(null)).toEqual([]);
      expect(parseTelemetryMessage(undefined)).toEqual([]);
    });
  });

  // More comprehensive tests would include database interaction tests
  // using a test database or mocks
});
