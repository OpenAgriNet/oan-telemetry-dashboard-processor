const { createHash, timingSafeEqual } = require('node:crypto');

function requireManagementAuth(req, res, next) {
  const expected = process.env.PROCESSOR_MANAGEMENT_API_KEY;
  if (!expected) return res.status(503).json({ error: 'Management access is not configured' });
  const match = /^Bearer (.+)$/i.exec(req.get('authorization') || '');
  if (!match) return res.status(401).json({ error: 'Management authentication required' });
  const digest = (value) => createHash('sha256').update(value).digest();
  if (!timingSafeEqual(digest(match[1]), digest(expected))) {
    return res.status(401).json({ error: 'Management authentication required' });
  }
  return next();
}

module.exports = { requireManagementAuth };
