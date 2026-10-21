class FeedbackValidationError extends Error {
  constructor() {
    super('A comment is required for a dislike');
    this.code = 'FEEDBACK_COMMENT_REQUIRED';
  }
}

function validateFeedback(rating, comment) {
  if (rating === 'dislike' && (typeof comment !== 'string' || !comment.trim())) {
    throw new FeedbackValidationError();
  }
}

module.exports = { FeedbackValidationError, validateFeedback };
