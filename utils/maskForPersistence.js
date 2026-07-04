/** @deprecated Import from `middleware/pii` — use `pii.maskColumn`, `pii.maskMessage`, etc. */
const { pii } = require("../middleware/pii");

module.exports = {
  isMaskingEnabled: () => pii.isEnabled(),
  maskColumnValue: pii.maskColumn.bind(pii),
  maskMessageContent: pii.maskMessage.bind(pii),
};