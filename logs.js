const fs = require('fs');
const path = require('path');
const https = require('https');

// Import utilities
const { getMapContents } = require('./utils/dataTransformers');
const {
    parseLogFile,
    parsePoolNodeLog,
    parsePoolCompareResultsLog,
    parsePoolNodeTimingLog,
    parsePoolNodeTimeoutCache
} = require('./utils/logParsers');
const {
    updateRequestHistory,
    getDashboardMetrics,
    updateRequestorMetrics,
    calculateNodeTimeoutMetrics
} = require('./utils/metricsCalculators');

// Data storage maps
const fallbackRequestsMap = new Map();
const cacheRequestsMap = new Map();
const poolRequestsMap = new Map();
const poolNodesMap = new Map();
const poolNodesTimingMap = new Map();
const poolNodesTimeoutCache = new Map();
const poolCompareResultsMap = new Map();

// Configuration
const {logPort, maxLogEntries, parseInterval, poolNodeTimingParseInterval, maxRequestHistoryHours } = require('./config');

// Track last processed line index for each file
const lastProcessedIndexes = {
    fallback: -1,
    cache: -1,
    pool: -1,
    poolNodes: -1,
    poolNodesTimeout: -1,
    poolCompareResults: -1
};

// Track byte offsets for efficient incremental reading
const lastByteOffsets = {
    fallback: 0,
    cache: 0,
    pool: 0,
    poolNodes: 0,
    poolCompareResults: 0,
    poolNodesTimeout: 0
};

// Store request history data
const requestHistory = new Map();

// Cache object for requestor metrics
let cachedRequestorMetrics = null;
const lastProcessedRequestorEpoch = { value: 0 };

// Log file paths
const fallbackLogPath = path.join(__dirname, '../shared/fallbackRequests.log');
const cacheLogPath = path.join(__dirname, '../shared/cacheRequests.log');
const poolLogPath = path.join(__dirname, '../shared/poolRequests.log');
const poolNodesLogPath = path.join(__dirname, '../shared/poolNodes.log');
const poolCompareResultsLogPath = path.join(__dirname, '../shared/poolCompareResults.log');

// Cache object for dashboard metrics
let cachedDashboardMetrics = null;

// Cache object for node timeout metrics
let cachedNodeTimeoutMetricsLastWeek = null;
let cachedNodeTimeoutMetricsLastDay = null;

// Create HTTPS server to serve map contents
const server = https.createServer(
  {
    key: fs.readFileSync("/home/ubuntu/shared/server.key"),
    cert: fs.readFileSync("/home/ubuntu/shared/server.cert"),
  },
  (req, res) => {
    res.setHeader('Content-Type', 'application/json');
    
    if (req.url === '/fallbackRequests') {
        res.end(JSON.stringify(getMapContents(fallbackRequestsMap, poolCompareResultsMap), null, 2));
    } else if (req.url === '/cacheRequests') {
        res.end(JSON.stringify(getMapContents(cacheRequestsMap, poolCompareResultsMap), null, 2));
    } else if (req.url === '/poolRequests') {
        res.end(JSON.stringify(getMapContents(poolRequestsMap, poolCompareResultsMap), null, 2));
    } else if (req.url === '/poolCompareResults') {
        res.end(JSON.stringify(getMapContents(poolCompareResultsMap, poolCompareResultsMap), null, 2));
    } else if (req.url === '/poolNodes') {
        res.end(JSON.stringify(getMapContents(poolNodesMap, poolCompareResultsMap), null, 2));
    } else if (req.url === '/dashboard') {
        res.end(JSON.stringify(cachedDashboardMetrics, null, 2));
    } else if (req.url === '/requestorTable') {
        res.end(JSON.stringify(cachedRequestorMetrics, null, 2));
    } else if (req.url === '/nodeTimeoutPercentLastWeek') {
        res.end(JSON.stringify(cachedNodeTimeoutMetricsLastWeek, null, 2));
    } else if (req.url === '/nodeTimeoutPercentLastDay') {
        res.end(JSON.stringify(cachedNodeTimeoutMetricsLastDay, null, 2));
    } else {
        res.statusCode = 404;
        res.end(JSON.stringify({ error: 'Not found' }));
    }
});

server.listen(logPort, () => {
    console.log("----------------------------------------------------------------------------------------------------------------");
    console.log("----------------------------------------------------------------------------------------------------------------");
    console.log(`HTTPS logs server running at https://localhost:${logPort}`);
});

// Function to update cached metrics
function updateCachedMetrics() {
    cachedDashboardMetrics = getDashboardMetrics(
        fallbackRequestsMap,
        cacheRequestsMap,
        poolRequestsMap,
        poolNodesMap,
        poolNodesTimingMap,
        requestHistory
    );
    cachedRequestorMetrics = updateRequestorMetrics(
        cachedRequestorMetrics,
        lastProcessedRequestorEpoch,
        fallbackRequestsMap,
        cacheRequestsMap,
        poolRequestsMap
    );
}

// Initial processing in an async IIFE
(async () => {
    await parseLogFile(fallbackLogPath, fallbackRequestsMap, 'fallback', lastProcessedIndexes, lastByteOffsets, maxLogEntries);
    await parseLogFile(cacheLogPath, cacheRequestsMap, 'cache', lastProcessedIndexes, lastByteOffsets, maxLogEntries);
    await parseLogFile(poolLogPath, poolRequestsMap, 'pool', lastProcessedIndexes, lastByteOffsets, maxLogEntries);
    await parsePoolNodeLog(poolNodesLogPath, poolNodesMap, lastProcessedIndexes, lastByteOffsets, maxLogEntries);
    await parsePoolNodeTimingLog(poolNodesLogPath, poolNodesTimingMap);
    await parsePoolNodeTimeoutCache(poolNodesLogPath, poolNodesTimeoutCache, lastByteOffsets);
    await parsePoolCompareResultsLog(poolCompareResultsLogPath, poolCompareResultsMap, lastProcessedIndexes, lastByteOffsets);

    // Calculate initial node timeout metrics after poolNodesTimeoutCache is populated
    cachedNodeTimeoutMetricsLastWeek = calculateNodeTimeoutMetrics(poolNodesTimeoutCache, 'week');
    cachedNodeTimeoutMetricsLastDay = calculateNodeTimeoutMetrics(poolNodesTimeoutCache, 'day');

    // Reset lastProcessedRequestorEpoch to ensure we process all entries on first run
    lastProcessedRequestorEpoch.value = 0;

    updateRequestHistory(requestHistory, fallbackRequestsMap, cacheRequestsMap, poolRequestsMap, maxRequestHistoryHours);
    updateCachedMetrics();

    // Track the last hour we processed to detect hour changes
    let lastProcessedHour = new Date().getHours();

    // Keep regular dashboard metrics updates at current interval
    setInterval(async () => {
        const currentHour = new Date().getHours();
        // Check if we've crossed an hour boundary
        if (currentHour !== lastProcessedHour) {
            console.log(`Hour changed from ${lastProcessedHour} to ${currentHour}, updating request history and node timeout metrics`);
            updateRequestHistory(requestHistory, fallbackRequestsMap, cacheRequestsMap, poolRequestsMap, maxRequestHistoryHours);
            await parsePoolNodeTimeoutCache(poolNodesLogPath, poolNodesTimeoutCache, lastByteOffsets); // Update timeout cache every hour
            cachedNodeTimeoutMetricsLastWeek = calculateNodeTimeoutMetrics(poolNodesTimeoutCache, 'week'); // Update node timeout metrics for last week every hour
            cachedNodeTimeoutMetricsLastDay = calculateNodeTimeoutMetrics(poolNodesTimeoutCache, 'day'); // Update node timeout metrics for last day every hour
            lastProcessedHour = currentHour;
        }

        await parseLogFile(fallbackLogPath, fallbackRequestsMap, 'fallback', lastProcessedIndexes, lastByteOffsets, maxLogEntries);
        await parseLogFile(cacheLogPath, cacheRequestsMap, 'cache', lastProcessedIndexes, lastByteOffsets, maxLogEntries);
        await parseLogFile(poolLogPath, poolRequestsMap, 'pool', lastProcessedIndexes, lastByteOffsets, maxLogEntries);
        await parsePoolNodeLog(poolNodesLogPath, poolNodesMap, lastProcessedIndexes, lastByteOffsets, maxLogEntries);
        await parsePoolCompareResultsLog(poolCompareResultsLogPath, poolCompareResultsMap, lastProcessedIndexes, lastByteOffsets);
        updateCachedMetrics();
    }, parseInterval);

    setInterval(async () => {
        await parsePoolNodeTimingLog(poolNodesLogPath, poolNodesTimingMap);
    }, poolNodeTimingParseInterval);
})();
