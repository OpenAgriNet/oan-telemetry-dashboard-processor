const { parseTelemetryMessage } = require('../telemetryBatch');

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

    test('rejects messages without an events array', () => {
      const message = JSON.stringify({
        id: 'ekstep.telemetry',
        ver: '2.2'
      });
      
      expect(() => parseTelemetryMessage(message)).toThrow('events array');
    });

    test('rejects invalid JSON instead of treating it as an empty message', () => {
      const message = 'Not a valid JSON';
      expect(() => parseTelemetryMessage(message)).toThrow('valid JSON');
    });

    test('rejects missing messages', () => {
      expect(() => parseTelemetryMessage(null)).toThrow('valid JSON');
      expect(() => parseTelemetryMessage(undefined)).toThrow('valid JSON');
    });
  });

  // More comprehensive tests would include database interaction tests
  // using a test database or mocks
});
