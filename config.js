const logPort = 3001;
const parseInterval = 10000; // 10 seconds in milliseconds
const poolNodeTimingParseInterval = 60 * 1000 * 10; // 10 minutes = 60 seconds * 1000 ms * 10
const maxLogEntries = 20000;


module.exports = {
  logPort,
  parseInterval,
  poolNodeTimingParseInterval,
  maxLogEntries,
};