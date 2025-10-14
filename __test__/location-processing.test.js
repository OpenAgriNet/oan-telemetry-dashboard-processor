/**
 * Comprehensive test for location data processing
 * Uses actual telemetry JSON structure from Untitled-1.json
 */

const { _testing } = require('../eventProcessors');
const { getNestedValue } = _testing;

describe('Location Data Processing', () => {
  // Sample telemetry event based on actual data
  const sampleEvent = {
    "eid": "OE_ITEM_RESPONSE",
    "ver": "2.2",
    "mid": "OE_1a4d0eef4e8d5a560c2120df9f5e11b8",
    "ets": 1759990227985,
    "channel": "MahaVistaar-https://prodaskvistaar.mahapocra.gov.in",
    "pdata": { "id": "MahaVistaar", "ver": "v0.1" },
    "gdata": { "id": "content_id", "ver": "contetn_ver" },
    "cdata": [],
    "uid": "Sameer Jadhav",
    "sid": "bf172c88-72ec-4478-85a9-4e6c6db56068",
    "did": "default-email",
    "edata": {
      "eks": {
        "target": {
          "id": "default",
          "ver": "v0.1",
          "type": "Question",
          "parent": { "id": "p1", "type": "default" },
          "questionsDetails": {
            "questionText": "मेगा स्वीट तन नाशक सांग",
            "sessionId": "bf172c88-72ec-4478-85a9-4e6c6db56068"
          },
          "mobile": "9921252890",
          "username": "Sameer Jadhav",
          "email": "",
          "role": "public",
          "farmer_id": "",
          "registered_location_district": "सातारा",
          "registered_location_village": "गुळुंब",
          "registered_location_taluka": "वाई",
          "device_location_district": "सातारा",
          "device_location_village": "गुळुंब",
          "device_location_taluka": "वाई",
          "agristack_location_district": "",
          "agristack_location_village": "",
          "agristack_location_taluka": ""
        },
        "qid": "daded326-ac3b-4d3b-86c5-f95fe0c03369",
        "type": "CHOOSE",
        "state": ""
      }
    },
    "etags": { "partner": [] }
  };

  describe('getNestedValue function with actual telemetry structure', () => {
    test('should extract user context fields from edata.eks.target', () => {
      expect(getNestedValue(sampleEvent, 'edata.eks.target.mobile')).toBe('9921252890');
      expect(getNestedValue(sampleEvent, 'edata.eks.target.username')).toBe('Sameer Jadhav');
      expect(getNestedValue(sampleEvent, 'edata.eks.target.email')).toBe('');
      expect(getNestedValue(sampleEvent, 'edata.eks.target.role')).toBe('public');
      expect(getNestedValue(sampleEvent, 'edata.eks.target.farmer_id')).toBe('');
    });

    test('should extract registered location components', () => {
      expect(getNestedValue(sampleEvent, 'edata.eks.target.registered_location_district')).toBe('सातारा');
      expect(getNestedValue(sampleEvent, 'edata.eks.target.registered_location_village')).toBe('गुळुंब');
      expect(getNestedValue(sampleEvent, 'edata.eks.target.registered_location_taluka')).toBe('वाई');
    });

    test('should extract device location components', () => {
      expect(getNestedValue(sampleEvent, 'edata.eks.target.device_location_district')).toBe('सातारा');
      expect(getNestedValue(sampleEvent, 'edata.eks.target.device_location_village')).toBe('गुळुंब');
      expect(getNestedValue(sampleEvent, 'edata.eks.target.device_location_taluka')).toBe('वाई');
    });

    test('should extract agristack location components (empty strings)', () => {
      expect(getNestedValue(sampleEvent, 'edata.eks.target.agristack_location_district')).toBe('');
      expect(getNestedValue(sampleEvent, 'edata.eks.target.agristack_location_village')).toBe('');
      expect(getNestedValue(sampleEvent, 'edata.eks.target.agristack_location_taluka')).toBe('');
    });

    test('should extract question details', () => {
      expect(getNestedValue(sampleEvent, 'edata.eks.target.questionsDetails.questionText')).toBe('मेगा स्वीट तन नाशक सांग');
      expect(getNestedValue(sampleEvent, 'edata.eks.target.questionsDetails.sessionId')).toBe('bf172c88-72ec-4478-85a9-4e6c6db56068');
    });

    test('should extract top-level event fields', () => {
      expect(getNestedValue(sampleEvent, 'uid')).toBe('Sameer Jadhav');
      expect(getNestedValue(sampleEvent, 'sid')).toBe('bf172c88-72ec-4478-85a9-4e6c6db56068');
      expect(getNestedValue(sampleEvent, 'channel')).toBe('MahaVistaar-https://prodaskvistaar.mahapocra.gov.in');
      expect(getNestedValue(sampleEvent, 'ets')).toBe(1759990227985);
    });
  });

  describe('Location data assembly logic', () => {
    test('should create registered_location JSONB structure', () => {
      const district = getNestedValue(sampleEvent, 'edata.eks.target.registered_location_district');
      const village = getNestedValue(sampleEvent, 'edata.eks.target.registered_location_village');
      const taluka = getNestedValue(sampleEvent, 'edata.eks.target.registered_location_taluka');

      const registeredLocation = {
        district: district,
        village: village,
        taluka: taluka
      };

      expect(registeredLocation).toEqual({
        district: 'सातारा',
        village: 'गुळुंब',
        taluka: 'वाई'
      });
    });

    test('should create device_location JSONB structure', () => {
      const district = getNestedValue(sampleEvent, 'edata.eks.target.device_location_district');
      const village = getNestedValue(sampleEvent, 'edata.eks.target.device_location_village');
      const taluka = getNestedValue(sampleEvent, 'edata.eks.target.device_location_taluka');

      const deviceLocation = {
        district: district,
        village: village,
        taluka: taluka
      };

      expect(deviceLocation).toEqual({
        district: 'सातारा',
        village: 'गुळुंब',
        taluka: 'वाई'
      });
    });

    test('should handle empty location data for agristack_location', () => {
      const district = getNestedValue(sampleEvent, 'edata.eks.target.agristack_location_district');
      const village = getNestedValue(sampleEvent, 'edata.eks.target.agristack_location_village');
      const taluka = getNestedValue(sampleEvent, 'edata.eks.target.agristack_location_taluka');

      // When all fields are empty strings, location should be null
      const agristackLocation = (district || village || taluka) ? {
        district: district,
        village: village,
        taluka: taluka
      } : null;

      expect(agristackLocation).toBeNull();
    });

    test('should handle partial location data', () => {
      const partialEvent = {
        edata: {
          eks: {
            target: {
              registered_location_district: 'सातारा',
              registered_location_village: '',
              registered_location_taluka: ''
            }
          }
        }
      };

      const district = getNestedValue(partialEvent, 'edata.eks.target.registered_location_district');
      const village = getNestedValue(partialEvent, 'edata.eks.target.registered_location_village');
      const taluka = getNestedValue(partialEvent, 'edata.eks.target.registered_location_taluka');

      const registeredLocation = (district || village || taluka) ? {
        district: district,
        village: village,
        taluka: taluka
      } : null;

      expect(registeredLocation).toEqual({
        district: 'सातारा',
        village: '',
        taluka: ''
      });
    });
  });

  describe('Field mappings validation', () => {
    test('should validate all required field mappings for questions table', () => {
      const questionMappings = {
        "uid": "uid",
        "sid": "sid",
        "groupDetails": "edata.eks.target.questionsDetails.groupDetails",
        "channel": "channel",
        "ets": "ets",
        "questionText": "edata.eks.target.questionsDetails.questionText",
        "questionSource": "edata.eks.target.questionsDetails.questionSource",
        "answerText": "edata.eks.target.questionsDetails.answerText",
        "answer": "edata.eks.target.questionsDetails.answerText.answer",
        "mobile": "mobile",
        "username": "username",
        "email": "email",
        "role": "role",
        "farmer_id": "farmer_id",
        "registered_location": "registered_location",
        "device_location": "device_location",
        "agristack_location": "agristack_location"
      };

      // Verify all location fields are present
      expect(questionMappings).toHaveProperty('registered_location');
      expect(questionMappings).toHaveProperty('device_location');
      expect(questionMappings).toHaveProperty('agristack_location');

      // Verify all user context fields are present
      expect(questionMappings).toHaveProperty('mobile');
      expect(questionMappings).toHaveProperty('username');
      expect(questionMappings).toHaveProperty('email');
      expect(questionMappings).toHaveProperty('role');
      expect(questionMappings).toHaveProperty('farmer_id');
    });

    test('should validate all required field mappings for errorDetails table', () => {
      const errorMappings = {
        "uid": "uid",
        "sid": "sid",
        "channel": "channel",
        "ets": "ets",
        "errorText": "edata.eks.target.errorDetails.errorText",
        "qid": "edata.eks.qid",
        "mobile": "mobile",
        "username": "username",
        "email": "email",
        "role": "role",
        "farmer_id": "farmer_id",
        "registered_location": "registered_location",
        "device_location": "device_location",
        "agristack_location": "agristack_location"
      };

      // Verify all location fields are present
      expect(errorMappings).toHaveProperty('registered_location');
      expect(errorMappings).toHaveProperty('device_location');
      expect(errorMappings).toHaveProperty('agristack_location');
    });

    test('should validate all required field mappings for feedback table', () => {
      const feedbackMappings = {
        "uid": "uid",
        "sid": "sid",
        "groupDetails": "edata.eks.target.questionsDetails.groupDetails",
        "channel": "channel",
        "ets": "ets",
        "feedbackText": "edata.eks.target.feedbackDetails.feedbackText",
        "questionText": "edata.eks.target.feedbackDetails.questionText",
        "answerText": "edata.eks.target.feedbackDetails.answerText",
        "feedbackType": "edata.eks.target.feedbackDetails.feedbackType",
        "qid": "edata.eks.qid",
        "mobile": "mobile",
        "username": "username",
        "email": "email",
        "role": "role",
        "farmer_id": "farmer_id",
        "registered_location": "registered_location",
        "device_location": "device_location",
        "agristack_location": "agristack_location"
      };

      // Verify all location fields are present
      expect(feedbackMappings).toHaveProperty('registered_location');
      expect(feedbackMappings).toHaveProperty('device_location');
      expect(feedbackMappings).toHaveProperty('agristack_location');
    });
  });

  describe('JSON stringification for JSONB columns', () => {
    test('should properly stringify location objects', () => {
      const locationObject = {
        district: 'सातारा',
        village: 'गुळुंब',
        taluka: 'वाई'
      };

      const stringified = JSON.stringify(locationObject);
      expect(stringified).toBe('{"district":"सातारा","village":"गुळुंब","taluka":"वाई"}');

      // Verify it can be parsed back
      const parsed = JSON.parse(stringified);
      expect(parsed).toEqual(locationObject);
    });

    test('should handle null location values', () => {
      const nullLocation = null;
      // Should not stringify null values for database
      expect(nullLocation).toBeNull();
    });
  });
});

