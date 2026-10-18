function invalid(message, status = 400) {
  return Object.assign(new Error(message), { status, code: 'INVALID_PROCESSOR_CONFIGURATION' });
}

function identifier(value) {
  if (typeof value !== 'string' || !/^[a-z_][a-z0-9_]{0,62}$/i.test(value)) {
    throw invalid('Table and field names must contain only letters, numbers and underscores');
  }
  return value.toLowerCase();
}

function quoteIdentifier(value) { return `"${identifier(value)}"`; }

function validateRegistration(eventType, tableName, fieldMappings, fieldVerification) {
  if (typeof eventType !== 'string' || !eventType.trim() || eventType.length > 100) {
    throw invalid('An event type is required');
  }
  const normalizedTable = identifier(tableName);
  const allowed = (process.env.PROCESSOR_ALLOWED_EVENT_TABLES || 'questions,feedback,errordetails')
    .split(',').map(value => value.trim().toLowerCase()).filter(Boolean);
  if (!allowed.includes(normalizedTable)) throw invalid('This reporting table is not allowed', 403);
  if (!fieldMappings || typeof fieldMappings !== 'object' || Array.isArray(fieldMappings)
      || Object.keys(fieldMappings).length === 0 || Object.keys(fieldMappings).length > 100) {
    throw invalid('A nonempty set of field mappings is required');
  }
  if (new Set(Object.keys(fieldMappings).map(name => name.toLowerCase())).size !== Object.keys(fieldMappings).length) {
    throw invalid('Field names must be distinct');
  }
  for (const [name, value] of Object.entries(fieldMappings)) {
    const normalized = identifier(name);
    if (normalized === 'id' || normalized === 'created_at') throw invalid('Generated record fields cannot be mapped');
    if (typeof value !== 'string' || !value.trim() || value.length > 512) throw invalid('Each field mapping needs a data path');
  }
  if (typeof fieldVerification !== 'string' || !fieldVerification.trim() || fieldVerification.length > 512) {
    throw invalid('A field verification path is required');
  }
  return normalizedTable;
}

module.exports = { quoteIdentifier, validateRegistration };
