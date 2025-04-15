const fs = require('fs');
const path = require('path');
const https = require('https');

const fallbackRequestsMap = new Map();
const cacheRequestsMap = new Map();
const poolRequestsMap = new Map();
const poolNodesMap = new Map();
const poolNodesTimingMap = new Map();
const poolCompareResultsMap = new Map();

const {logPort, maxLogEntries, parseInterval, poolNodeTimingParseInterval } = require('./config');

// Track last processed line index for each file
const lastProcessedIndexes = {
    fallback: -1,
    cache: -1,
    pool: -1,
    poolNodes: -1,
    poolCompareResults: -1
};

// Store request history data
const requestHistory = new Map();

// Cache object for requestor metrics
let cachedRequestorMetrics = null;
let lastProcessedRequestorEpoch = 0;

const fallbackLogPath = path.join(__dirname, '../shared/fallbackRequests.log');
const cacheLogPath = path.join(__dirname, '../shared/cacheRequests.log');
const poolLogPath = path.join(__dirname, '../shared/poolRequests.log');
const poolNodesLogPath = path.join(__dirname, '../shared/poolNodes.log');
const poolCompareResultsLogPath = path.join(__dirname, '../shared/poolCompareResults.log');

// Cache object for dashboard metrics
let cachedDashboardMetrics = null;

// Cache object for node timing metrics
let cachedNodeTimingMetrics = null;

// Helper function to calculate percentiles
function calculatePercentiles(values, percentiles) {
    if (values.length === 0) return {};
    
    // Sort values in ascending order
    values.sort((a, b) => a - b);
    
    const results = {};
    percentiles.forEach(p => {
        const index = Math.ceil((p/100) * values.length) - 1;
        results[`p${p}`] = values[index];
    });
    
    return results;
}

function parseLogFile(logPath, targetMap, logType) {
    try {
        const fileContent = fs.readFileSync(logPath, 'utf8');
        const lines = fileContent.trim().split('\n');
        let newEntriesCount = 0;
        
        // Calculate start and end indexes for processing
        const totalLines = lines.length;
        const oldestAllowedIndex = Math.max(0, totalLines - maxLogEntries);
        const lastProcessedIndex = lastProcessedIndexes[logType];
        
        // If we need to start fresh due to rotation or first run
        if (lastProcessedIndex >= totalLines || lastProcessedIndex < oldestAllowedIndex - 1) {
            targetMap.clear();
            lastProcessedIndexes[logType] = oldestAllowedIndex - 1;
        }
        
        // Process only new lines, starting from the last processed index
        const startIndex = Math.max(oldestAllowedIndex, lastProcessedIndex + 1);
        const newLines = lines.slice(startIndex);
        
        newLines.forEach((line, index) => {
            const [timestamp, epoch, requester, method, params, elapsed, status] = line.split('|');
            const absoluteIndex = startIndex + index;
            
            // Use both epoch and absolute line index to create a unique key
            const key = `${epoch}-${absoluteIndex}`;
            
            targetMap.set(key, {
                timestamp,
                epoch,
                requester: requester || '',
                method,
                params,
                elapsed: parseFloat(elapsed),
                status,
                lineIndex: absoluteIndex
            });
            newEntriesCount++;
            lastProcessedIndexes[logType] = absoluteIndex;
        });
        
        // Remove entries that are too old
        if (targetMap.size > maxLogEntries) {
            const entriesToRemove = Array.from(targetMap.entries())
                .sort((a, b) => a[1].lineIndex - b[1].lineIndex)
                .slice(0, targetMap.size - maxLogEntries);
            
            entriesToRemove.forEach(([key]) => targetMap.delete(key));
        }
        
        if (newEntriesCount > 0) {
            const mapName = targetMap === fallbackRequestsMap ? 'fallbackRequestsMap' : targetMap === cacheRequestsMap ? 'cacheRequestsMap' : 'poolRequestsMap';
            console.log(`Added ${newEntriesCount} new entries to ${mapName}. Total entries: ${targetMap.size}`);
        }
    } catch (error) {
        const mapName = targetMap === fallbackRequestsMap ? 'fallbackRequestsMap' : targetMap === cacheRequestsMap ? 'cacheRequestsMap' : 'poolRequestsMap';
        console.error(`Error parsing ${mapName} log file:`, error);
    }
}

function parsePoolNodeLog(logPath, targetMap) {
    try {
        const fileContent = fs.readFileSync(logPath, 'utf8');
        const lines = fileContent.trim().split('\n');
        let newEntriesCount = 0;
        
        // Calculate start and end indexes for processing
        const totalLines = lines.length;
        const oldestAllowedIndex = Math.max(0, totalLines - maxLogEntries);
        const lastProcessedIndex = lastProcessedIndexes.poolNodes;
        
        // If we need to start fresh due to rotation or first run
        if (lastProcessedIndex >= totalLines || lastProcessedIndex < oldestAllowedIndex - 1) {
            targetMap.clear();
            lastProcessedIndexes.poolNodes = oldestAllowedIndex - 1;
        }
        
        // Process only new lines, starting from the last processed index
        const startIndex = Math.max(oldestAllowedIndex, lastProcessedIndex + 1);
        const newLines = lines.slice(startIndex);
        
        newLines.forEach((line, index) => {
            const [timestamp, epoch, nodeId, owner, method, params, duration, status] = line.split('|');
            const absoluteIndex = startIndex + index;
            
            // Create a composite key using epoch, nodeId and absolute line index
            const key = `${epoch}-${nodeId}-${absoluteIndex}`;
            
            targetMap.set(key, {
                timestamp,
                epoch,
                nodeId,
                owner,
                method,
                params,
                duration: parseFloat(duration),
                status,
                lineIndex: absoluteIndex
            });
            newEntriesCount++;
            lastProcessedIndexes.poolNodes = absoluteIndex;
        });
        
        // Remove entries that are too old
        if (targetMap.size > maxLogEntries) {
            const entriesToRemove = Array.from(targetMap.entries())
                .sort((a, b) => a[1].lineIndex - b[1].lineIndex)
                .slice(0, targetMap.size - maxLogEntries);
            
            entriesToRemove.forEach(([key]) => targetMap.delete(key));
        }
        
        if (newEntriesCount > 0) {
            console.log(`Added ${newEntriesCount} new entries to poolNodesMap. Total entries: ${targetMap.size}`);
        }
    } catch (error) {
        console.error('Error parsing poolNodesMap log file:', error);
    }
}

function parsePoolCompareResultsLog(logPath, targetMap) {
    try {
        const fileContent = fs.readFileSync(logPath, 'utf8');
        const lines = fileContent.trim().split('\n');
        let newEntriesCount = 0;
        
        // Calculate start and end indexes for processing
        const totalLines = lines.length;
        const oldestAllowedIndex = Math.max(0, totalLines - maxLogEntries);
        const lastProcessedIndex = lastProcessedIndexes.poolCompareResults;
        
        // First run or log rotation case
        if (lastProcessedIndex >= totalLines || lastProcessedIndex < oldestAllowedIndex - 1) {
            lastProcessedIndexes.poolCompareResults = oldestAllowedIndex - 1;
        }
        
        // Process only new lines, starting from the last processed index
        const startIndex = Math.max(oldestAllowedIndex, lastProcessedIndex + 1);
        const newLines = lines.slice(startIndex);
        
        // Arrays to hold all entries (existing + new)
        const matchedEntries = [];
        const mismatchedEntries = [];
        
        // First, categorize existing entries
        targetMap.forEach((value, key) => {
            if (value.resultsMatch) {
                matchedEntries.push({ key, value });
            } else {
                mismatchedEntries.push({ key, value });
            }
        });
        
        // Helper function to parse and categorize an entry
        const parseEntry = (line, absoluteIndex) => {
            const [
                timestamp, epoch, resultsMatch, mismatchedNode, mismatchedOwner, 
                mismatchedResults, nodeId1, nodeResult1, nodeId2, nodeResult2, 
                nodeId3, nodeResult3, method, params
            ] = line.split('|');
            
            // Create a composite key using epoch and absolute line index
            const key = `${epoch}-${absoluteIndex}`;
            
            // Parse mismatchedResults from string to array, handling empty case
            const parsedMismatchedResults = mismatchedResults === '[]' ? 
                [] : 
                JSON.parse(mismatchedResults);
            
            const entry = {
                key,
                value: {
                    timestamp,
                    epoch,
                    resultsMatch: resultsMatch === 'true',
                    mismatchedNode: mismatchedNode === 'nan' ? null : mismatchedNode,
                    mismatchedOwner: mismatchedOwner === 'nan' ? null : mismatchedOwner,
                    mismatchedResults: parsedMismatchedResults,
                    nodeId1,
                    nodeResult1: JSON.parse(nodeResult1),
                    nodeId2,
                    nodeResult2: JSON.parse(nodeResult2),
                    nodeId3,
                    nodeResult3: JSON.parse(nodeResult3),
                    method,
                    params,
                    lineIndex: absoluteIndex
                }
            };
            
            if (resultsMatch === 'true') {
                matchedEntries.push(entry);
            } else {
                mismatchedEntries.push(entry);
            }
            newEntriesCount++;
        };
        
        // Process new lines
        newLines.forEach((line, index) => {
            const absoluteIndex = startIndex + index;
            parseEntry(line, absoluteIndex);
            lastProcessedIndexes.poolCompareResults = absoluteIndex;
        });
        
        // Sort both arrays by timestamp (newest first)
        const sortByTimestamp = (a, b) => {
            const timeA = new Date(a.value.timestamp).getTime();
            const timeB = new Date(b.value.timestamp).getTime();
            if (timeA !== timeB) {
                return timeB - timeA;
            }
            return b.value.lineIndex - a.value.lineIndex;
        };
        
        matchedEntries.sort(sortByTimestamp);
        mismatchedEntries.sort(sortByTimestamp);
        
        // Clear the map
        targetMap.clear();
        
        // Combine and sort all entries we want to keep
        const allEntries = [
            ...mismatchedEntries, // Keep all mismatched entries
            ...matchedEntries.slice(0, maxLogEntries) // Keep only up to maxLogEntries matched entries
        ];
        
        // Sort all entries together by timestamp
        allEntries.sort(sortByTimestamp);
        
        // Add all entries to the map in sorted order
        allEntries.forEach(({key, value}) => {
            targetMap.set(key, value);
        });
        
        if (newEntriesCount > 0) {
            const matchedCount = Math.min(matchedEntries.length, maxLogEntries);
            console.log(`Added ${newEntriesCount} new entries to poolCompareResultsMap. ` +
                       `Total entries: ${targetMap.size} (${mismatchedEntries.length} mismatched + ${matchedCount} matched results)`);
        }
    } catch (error) {
        console.error('Error parsing poolCompareResultsMap log file:', error);
    }
}

function getMapContents(targetMap) {
  let entries = Array.from(targetMap.entries()).map(([key, entry]) => {
      // For poolNodesMap entries, the key is a composite of epoch-nodeId
      // For other maps, the key is just the epoch
      const isPoolNodesMap = 'nodeId' in entry;
      return {
          epoch: entry.epoch,
          ...entry
      };
  });

  // Special handling for poolCompareResultsMap - reverse the order
  if (targetMap === poolCompareResultsMap) {
      entries.reverse();
  }

  return entries;
}

function getStartOfHour(timestamp) {
    const date = new Date(parseInt(timestamp));
    date.setMinutes(0, 0, 0);
    return date.getTime();
}

function updateRequestHistory() {
    // Find the latest hour we've processed
    let latestProcessedHour = 0;
    if (requestHistory.size > 0) {
        latestProcessedHour = Math.max(...requestHistory.keys());
    }
    
    // Process all maps
    [
        { map: fallbackRequestsMap, prefix: 'fallback' },
        { map: cacheRequestsMap, prefix: 'cache' },
        { map: poolRequestsMap, prefix: 'pool' }
    ].forEach(({ map, prefix }) => {
        map.forEach(entry => {
            const entryHour = getStartOfHour(entry.epoch);
            
            // Skip if this hour has already been processed
            if (entryHour <= latestProcessedHour) {
                return;
            }
            
            if (!requestHistory.has(entryHour)) {
                requestHistory.set(entryHour, {
                    hourMs: entryHour,
                    nCacheRequestsSuccess: 0,
                    nCacheRequestsError: 0,
                    nCacheRequestsWarning: 0,
                    nPoolRequestsSuccess: 0,
                    nPoolRequestsError: 0,
                    nPoolRequestsWarning: 0,
                    nFallbackRequestsSuccess: 0,
                    nFallbackRequestsError: 0,
                    nFallbackRequestsWarning: 0
                });
            }
            
            const hourData = requestHistory.get(entryHour);
            
            if (entry.status === 'success') {
                hourData[`n${prefix.charAt(0).toUpperCase() + prefix.slice(1)}RequestsSuccess`]++;
            } else {
                try {
                    const statusObj = JSON.parse(entry.status);
                    if (statusObj.error && statusObj.error.code && statusObj.error.code.toString().startsWith('-69')) {
                        hourData[`n${prefix.charAt(0).toUpperCase() + prefix.slice(1)}RequestsWarning`]++;
                    } else {
                        hourData[`n${prefix.charAt(0).toUpperCase() + prefix.slice(1)}RequestsError`]++;
                    }
                } catch (e) {
                    // If status is not JSON or doesn't have expected structure, count as error
                    hourData[`n${prefix.charAt(0).toUpperCase() + prefix.slice(1)}RequestsError`]++;
                }
            }
        });
    });
}

function getDashboardMetrics() {
    const oneHourAgo = Date.now() - (60 * 60 * 1000); // 1 hour in milliseconds
    
    let nFallbackRequestsLastHour = 0;
    let nErrorFallbackRequestsLastHour = 0;
    let nWarningFallbackRequestsLastHour = 0;
    let nCacheRequestsLastHour = 0;
    let nErrorCacheRequestsLastHour = 0;
    let nWarningCacheRequestsLastHour = 0;
    let nPoolRequestsLastHour = 0;
    let nErrorPoolRequestsLastHour = 0;
    let nWarningPoolRequestsLastHour = 0;
    let totalFallbackTime = 0;
    let totalCacheTime = 0;
    let totalPoolTime = 0;
    
    // Object to store method-based, origin-based, and node-based times for ALL requests
    const methodTimes = {};
    const originTimes = {};
    const nodeTimes = {};
    
    // Helper function to process entries for method times - now processes ALL entries
    const processEntryForMethodTimes = (entry) => {
        if (!methodTimes[entry.method]) {
            methodTimes[entry.method] = [];
        }
        methodTimes[entry.method].push(entry.elapsed);

        // Process origin times
        const origin = entry.requester || 'N/A';
        if (!originTimes[origin]) {
            originTimes[origin] = [];
        }
        originTimes[origin].push(entry.elapsed);
    };
    
    // Process fallback requests
    fallbackRequestsMap.forEach(entry => {
        // Process ALL entries for histograms
        processEntryForMethodTimes(entry);
        
        // Only count last hour stats for hourly metrics
        if (parseInt(entry.epoch) >= oneHourAgo) {
            nFallbackRequestsLastHour++;
            totalFallbackTime += entry.elapsed;
            // Check if status is not success and error code doesn't start with -69
            if (entry.status !== 'success') {
                try {
                    const statusObj = JSON.parse(entry.status);
                    if (statusObj.error && statusObj.error.code && statusObj.error.code.toString().startsWith('-69')) {
                        nWarningFallbackRequestsLastHour++;
                    } else {
                        nErrorFallbackRequestsLastHour++;
                    }
                } catch (e) {
                    // If status is not JSON or doesn't have expected structure, count as error
                    nErrorFallbackRequestsLastHour++;
                }
            }
        }
    });
    
    // Process cache requests
    cacheRequestsMap.forEach(entry => {
        // Process ALL entries for histograms
        processEntryForMethodTimes(entry);
        
        // Only count last hour stats for hourly metrics
        if (parseInt(entry.epoch) >= oneHourAgo) {
            nCacheRequestsLastHour++;
            totalCacheTime += entry.elapsed;
            if (entry.status !== 'success') {
                try {
                    const statusObj = JSON.parse(entry.status);
                    if (statusObj.error && statusObj.error.code && statusObj.error.code.toString().startsWith('-69')) {
                        nWarningCacheRequestsLastHour++;
                    } else {
                        nErrorCacheRequestsLastHour++;
                    }
                } catch (e) {
                    // If status is not JSON or doesn't have expected structure, count as error
                    nErrorCacheRequestsLastHour++;
                }
            }
        }
    });

    // Process pool requests
    poolRequestsMap.forEach(entry => {
        // Process ALL entries for histograms
        processEntryForMethodTimes(entry);
        
        // Only count last hour stats for hourly metrics
        if (parseInt(entry.epoch) >= oneHourAgo) {
            nPoolRequestsLastHour++;
            totalPoolTime += entry.elapsed;
            if (entry.status !== 'success') {
                try {
                    const statusObj = JSON.parse(entry.status);
                    if (statusObj.error && statusObj.error.code && statusObj.error.code.toString().startsWith('-69')) {
                        nWarningPoolRequestsLastHour++;
                    } else {
                        nErrorPoolRequestsLastHour++;
                    }
                } catch (e) {
                    // If status is not JSON or doesn't have expected structure, count as error
                    nErrorPoolRequestsLastHour++;
                }
            }
        }
    });

    // Process node durations from poolNodesMap
    poolNodesMap.forEach(entry => {
        const nodeId = entry.nodeId;
        if (!nodeTimes[nodeId]) {
            nodeTimes[nodeId] = [];
        }
        nodeTimes[nodeId].push(parseFloat(entry.duration));
    });
    
    // Calculate percentiles for each method and origin using ALL data
    const methodDurationHist = {};
    Object.entries(methodTimes).forEach(([method, times]) => {
        methodDurationHist[method] = calculatePercentiles(times, [1, 25, 50, 75, 99]);
    });

    const originDurationHist = {};
    Object.entries(originTimes).forEach(([origin, times]) => {
        originDurationHist[origin] = calculatePercentiles(times, [1, 25, 50, 75, 99]);
    });

    // Calculate percentiles for each node using ALL timing data
    const nodeDurationHist = {};
    poolNodesTimingMap.forEach((times, nodeId) => {
        nodeDurationHist[nodeId] = calculatePercentiles(times, [1, 25, 50, 75, 99]);
    });
    
    // Collect request times for each type of request from the last hour
    const fallbackRequestTimesLastHour = [];
    const cacheRequestTimesLastHour = [];
    const poolRequestTimesLastHour = [];
    
    // Collect fallback request times from the last hour
    fallbackRequestsMap.forEach(entry => {
        if (parseInt(entry.epoch) >= oneHourAgo) {
            fallbackRequestTimesLastHour.push(entry.elapsed);
        }
    });
    
    // Collect cache request times from the last hour
    cacheRequestsMap.forEach(entry => {
        if (parseInt(entry.epoch) >= oneHourAgo) {
            cacheRequestTimesLastHour.push(entry.elapsed);
        }
    });
    
    // Collect pool request times from the last hour
    poolRequestsMap.forEach(entry => {
        if (parseInt(entry.epoch) >= oneHourAgo) {
            poolRequestTimesLastHour.push(entry.elapsed);
        }
    });
    
    // Calculate true medians using the calculatePercentiles function
    const medFallbackRequestTimeLastHour = fallbackRequestTimesLastHour.length > 0 ? 
        calculatePercentiles(fallbackRequestTimesLastHour, [50]).p50 : 0;
    const medCacheRequestTimeLastHour = cacheRequestTimesLastHour.length > 0 ? 
        calculatePercentiles(cacheRequestTimesLastHour, [50]).p50 : 0;
    const medPoolRequestTimeLastHour = poolRequestTimesLastHour.length > 0 ? 
        calculatePercentiles(poolRequestTimesLastHour, [50]).p50 : 0;
    
    return {
        timestamp: Date.now(),
        nTotalRequestsLastHour: nFallbackRequestsLastHour + nCacheRequestsLastHour + nPoolRequestsLastHour,
        nFallbackRequestsLastHour,
        nCacheRequestsLastHour,
        nPoolRequestsLastHour,
        nErrorFallbackRequestsLastHour,
        nErrorCacheRequestsLastHour,
        nErrorPoolRequestsLastHour,
        nWarningFallbackRequestsLastHour,
        nWarningCacheRequestsLastHour,
        nWarningPoolRequestsLastHour,
        medFallbackRequestTimeLastHour,
        medCacheRequestTimeLastHour,
        medPoolRequestTimeLastHour,
        methodDurationHist,
        originDurationHist,
        nodeDurationHist,
        requestHistory: Array.from(requestHistory.values()).sort((a, b) => a.hourMs - b.hourMs)
    };
}

function updateRequestorMetrics() {
    const oneWeekAgo = Date.now() - (7 * 24 * 60 * 60 * 1000); // 1 week in milliseconds
    
    // Initialize metrics map with existing data if available
    const requestorMetrics = new Map(
        cachedRequestorMetrics ? 
        Object.entries(cachedRequestorMetrics).map(([key, value]) => [key, {...value}]) : 
        []
    );

    // Helper function to process a single map entry
    const processEntry = (entry, type) => {
        // Skip if we've already processed this epoch
        if (parseInt(entry.epoch) <= lastProcessedRequestorEpoch) {
            return;
        }

        const requester = entry.requester || 'unknown';
        if (!requestorMetrics.has(requester)) {
            requestorMetrics.set(requester, {
                nAllRequestsAllTime: 0,
                nCacheRequestsAllTime: 0,
                nPoolRequestsAllTime: 0,
                nFallbackRequestsAllTime: 0,
                nAllRequestsLastWeek: 0,
                nCacheRequestsLastWeek: 0,
                nPoolRequestsLastWeek: 0,
                nFallbackRequestsLastWeek: 0
            });
        }

        const metrics = requestorMetrics.get(requester);
        const isLastWeek = parseInt(entry.epoch) >= oneWeekAgo;

        // Update all-time metrics
        metrics.nAllRequestsAllTime++;
        metrics[`n${type}RequestsAllTime`]++;

        // Update last week metrics if applicable
        if (isLastWeek) {
            metrics.nAllRequestsLastWeek++;
            metrics[`n${type}RequestsLastWeek`]++;
        }
    };

    // Process each map
    fallbackRequestsMap.forEach(entry => processEntry(entry, 'Fallback'));
    cacheRequestsMap.forEach(entry => processEntry(entry, 'Cache'));
    poolRequestsMap.forEach(entry => processEntry(entry, 'Pool'));

    // Update the last processed epoch to the latest one we've seen
    const allEpochs = [
        ...Array.from(fallbackRequestsMap.values()).map(e => parseInt(e.epoch)),
        ...Array.from(cacheRequestsMap.values()).map(e => parseInt(e.epoch)),
        ...Array.from(poolRequestsMap.values()).map(e => parseInt(e.epoch))
    ];
    if (allEpochs.length > 0) {
        lastProcessedRequestorEpoch = Math.max(...allEpochs);
    }

    // Convert Map to object for JSON serialization
    cachedRequestorMetrics = Object.fromEntries(requestorMetrics);
}

function calculateNodeTimingMetrics() {
    const oneWeekAgo = Date.now() - (7 * 24 * 60 * 60 * 1000); // 1 week in milliseconds
    const nodeTimings = new Map();

    // Process each entry in poolNodesMap
    poolNodesMap.forEach(entry => {
        const epoch = parseInt(entry.epoch);
        if (epoch >= oneWeekAgo) {
            const nodeId = entry.nodeId;
            if (!nodeTimings.has(nodeId)) {
                nodeTimings.set(nodeId, []);
            }
            nodeTimings.get(nodeId).push(parseFloat(entry.duration));
        }
    });

    // Calculate 75th percentile for each node
    const result = {};
    nodeTimings.forEach((durations, nodeId) => {
        if (durations.length > 0) {
            const percentiles = calculatePercentiles(durations, [75]);
            result[nodeId] = percentiles.p75;
        }
    });

    cachedNodeTimingMetrics = result;
}

function parsePoolNodeTimingLog(logPath) {
    try {
        const fileContent = fs.readFileSync(logPath, 'utf8');
        const lines = fileContent.trim().split('\n');
        let newEntriesCount = 0;
        
        lines.forEach((line) => {
            const [, , nodeId, , , , duration] = line.split('|');
            
            if (!poolNodesTimingMap.has(nodeId)) {
                poolNodesTimingMap.set(nodeId, []);
            }
            
            poolNodesTimingMap.get(nodeId).push(parseFloat(duration));
            newEntriesCount++;
        });
        
        if (newEntriesCount > 0) {
            console.log(`Added ${newEntriesCount} timing entries to poolNodesTimingMap. Total nodes: ${poolNodesTimingMap.size}`);
        }
    } catch (error) {
        console.error('Error parsing poolNodesTimingMap log file:', error);
    }
}

// Create HTTPS server to serve map contents
const server = https.createServer(
  {
    key: fs.readFileSync("/home/ubuntu/shared/server.key"),
    cert: fs.readFileSync("/home/ubuntu/shared/server.cert"),
  },
  (req, res) => {
    res.setHeader('Content-Type', 'application/json');
    
    if (req.url === '/fallbackRequests') {
        res.end(JSON.stringify(getMapContents(fallbackRequestsMap), null, 2));
    } else if (req.url === '/cacheRequests') {
        res.end(JSON.stringify(getMapContents(cacheRequestsMap), null, 2));
    } else if (req.url === '/poolRequests') {
        res.end(JSON.stringify(getMapContents(poolRequestsMap), null, 2));
    } else if (req.url === '/poolCompareResults') {
        res.end(JSON.stringify(getMapContents(poolCompareResultsMap), null, 2));
    } else if (req.url === '/poolNodes') {
        res.end(JSON.stringify(getMapContents(poolNodesMap), null, 2));
    } else if (req.url === '/dashboard') {
        res.end(JSON.stringify(cachedDashboardMetrics, null, 2));
    } else if (req.url === '/requestorTable') {
        res.end(JSON.stringify(cachedRequestorMetrics, null, 2));
    } else if (req.url === '/nodeTimingLastWeek') {
        res.end(JSON.stringify(cachedNodeTimingMetrics, null, 2));
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
    cachedDashboardMetrics = getDashboardMetrics();
    updateRequestorMetrics();
}

// Initial processing
parseLogFile(fallbackLogPath, fallbackRequestsMap, 'fallback');
parseLogFile(cacheLogPath, cacheRequestsMap, 'cache');
parseLogFile(poolLogPath, poolRequestsMap, 'pool');
parsePoolNodeLog(poolNodesLogPath, poolNodesMap);
parsePoolNodeTimingLog(poolNodesLogPath);
parsePoolCompareResultsLog(poolCompareResultsLogPath, poolCompareResultsMap);

// Calculate initial node timing metrics after poolNodesMap is populated
calculateNodeTimingMetrics();

// Reset lastProcessedRequestorEpoch to ensure we process all entries on first run
lastProcessedRequestorEpoch = 0;

updateRequestHistory(); // Initial history calculation
updateCachedMetrics();

// Track the last hour we processed to detect hour changes
let lastProcessedHour = new Date().getHours();

// Keep regular dashboard metrics updates at current interval
setInterval(() => {
    const currentHour = new Date().getHours();
    
    // Check if we've crossed an hour boundary
    if (currentHour !== lastProcessedHour) {
        console.log(`Hour changed from ${lastProcessedHour} to ${currentHour}, updating request history...`);
        updateRequestHistory();
        calculateNodeTimingMetrics(); // Update node timing metrics every hour
        lastProcessedHour = currentHour;
    }
    
    parseLogFile(fallbackLogPath, fallbackRequestsMap, 'fallback');
    parseLogFile(cacheLogPath, cacheRequestsMap, 'cache');
    parseLogFile(poolLogPath, poolRequestsMap, 'pool');
    parsePoolNodeLog(poolNodesLogPath, poolNodesMap);
    parsePoolCompareResultsLog(poolCompareResultsLogPath, poolCompareResultsMap);
    updateCachedMetrics();
}, parseInterval);

setInterval(() => {
    parsePoolNodeTimingLog(poolNodesLogPath);
}, poolNodeTimingParseInterval);