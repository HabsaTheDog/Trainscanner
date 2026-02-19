const VALID_SWITCH_STATES = new Set(['idle', 'switching', 'importing', 'restarting', 'ready', 'failed']);

function isInFlightState(state) {
  return state === 'switching' || state === 'importing' || state === 'restarting';
}

module.exports = {
  VALID_SWITCH_STATES,
  isInFlightState
};
