const fs = require('fs');
const path = require('path');
const http = require('http');

const { parseInterval } = require('./config');
const { logPort } = require('./config');

const fallbackRequestsMap = new Map();
const cacheRequestsMap = new Map();
const poolRequestsMap = new Map();
const poolNodeMap = new Map();

const fallbackLogPath = path.join(__dirname, '../shared/fallbackRequests.log');
const cacheLogPath = path.join(__dirname, '../shared/cacheRequests.log');
const poolLogPath = path.join(__dirname, '../shared/poolRequests.log');
const poolNodeLogPath = path.join(__dirname, '../shared/poolNode.log');

// Cache object for dashboard metrics
let cachedDashboardMetrics = null;

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

function parseLogFile(logPath, targetMap) {
    try {
        const fileContent = fs.readFileSync(logPath, 'utf8');
        const lines = fileContent.trim().split('\n');
        let newEntriesCount = 0;
        
        lines.forEach(line => {
            const [timestamp, epoch, requester, method, params, elapsed, status] = line.split('|');
            
            // Only add entries that aren't already in the map
            if (!targetMap.has(epoch)) {
                targetMap.set(epoch, {
                    timestamp,
                    epoch,
                    requester: requester || '',
                    method,
                    params,
                    elapsed: parseFloat(elapsed),
                    status
                });
                newEntriesCount++;
            }
        });
        
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
        
        lines.forEach(line => {
            const [timestamp, epoch, nodeId, owner, method, params, duration, status] = line.split('|');
            
            // Only add entries that aren't already in the map
            if (!targetMap.has(epoch)) {
                targetMap.set(epoch, {
                    timestamp,
                    epoch,
                    nodeId,
                    owner,
                    method,
                    params,
                    duration: parseFloat(duration),
                    status
                });
                newEntriesCount++;
            }
        });
        
        if (newEntriesCount > 0) {
            console.log(`Added ${newEntriesCount} new entries to poolNodeMap. Total entries: ${targetMap.size}`);
        }
    } catch (error) {
        console.error('Error parsing poolNodeMap log file:', error);
    }
}

function getMapContents(targetMap) {
  return Array.from(targetMap.entries()).map(([epoch, entry]) => ({
      epoch,
      ...entry
  }));
}

function getDashboardMetrics() {
    const oneHourAgo = Date.now() - (60 * 60 * 1000); // 1 hour in milliseconds
    
    let nFallbackRequestsLastHour = 0;
    let nErrorFallbackRequestsLastHour = 0;
    let nCacheRequestsLastHour = 0;
    let nErrorCacheRequestsLastHour = 0;
    let nPoolRequestsLastHour = 0;
    let nErrorPoolRequestsLastHour = 0;
    let totalFallbackTime = 0;
    let totalCacheTime = 0;
    let totalPoolTime = 0;
    
    // Object to store method-based and origin-based times for ALL requests
    const methodTimes = {};
    const originTimes = {};
    
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
            if (entry.status !== 'success') {
                nErrorFallbackRequestsLastHour++;
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
                nErrorCacheRequestsLastHour++;
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
                nErrorPoolRequestsLastHour++;
            }
        }
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
    
    const aveFallbackRequestTimeLastHour = nFallbackRequestsLastHour > 0 ? totalFallbackTime / nFallbackRequestsLastHour : 0;
    const aveCacheRequestTimeLastHour = nCacheRequestsLastHour > 0 ? totalCacheTime / nCacheRequestsLastHour : 0;
    const avePoolRequestTimeLastHour = nPoolRequestsLastHour > 0 ? totalPoolTime / nPoolRequestsLastHour : 0;
    
    return {
        timestamp: Date.now(),
        nTotalRequestsLastHour: nFallbackRequestsLastHour + nCacheRequestsLastHour + nPoolRequestsLastHour,
        nFallbackRequestsLastHour,
        nCacheRequestsLastHour,
        nPoolRequestsLastHour,
        nErrorFallbackRequestsLastHour,
        nErrorCacheRequestsLastHour,
        nErrorPoolRequestsLastHour,
        aveFallbackRequestTimeLastHour,
        aveCacheRequestTimeLastHour,
        avePoolRequestTimeLastHour,
        methodDurationHist,
        originDurationHist
    };
}

// Create HTTP server to serve map contents
const server = http.createServer((req, res) => {
    res.setHeader('Content-Type', 'application/json');
    
    if (req.url === '/fallbackRequests') {
        res.end(JSON.stringify(getMapContents(fallbackRequestsMap), null, 2));
    } else if (req.url === '/cacheRequests') {
        res.end(JSON.stringify(getMapContents(cacheRequestsMap), null, 2));
    } else if (req.url === '/poolRequests') {
        res.end(JSON.stringify(getMapContents(poolRequestsMap), null, 2));
    } else if (req.url === '/poolNode') {
        res.end(JSON.stringify(getMapContents(poolNodeMap), null, 2));
    } else if (req.url === '/dashboard') {
        res.end(JSON.stringify(cachedDashboardMetrics, null, 2));
    } else {
        res.statusCode = 404;
        res.end(JSON.stringify({ error: 'Not found' }));
    }
});

server.listen(logPort, () => {
    console.log("----------------------------------------------------------------------------------------------------------------");
    console.log("----------------------------------------------------------------------------------------------------------------");
    console.log(`Logs server running at http://localhost:${logPort}`);
});

// Function to update cached metrics
function updateCachedMetrics() {
    cachedDashboardMetrics = getDashboardMetrics();
}

parseLogFile(fallbackLogPath, fallbackRequestsMap);
parseLogFile(cacheLogPath, cacheRequestsMap);
parseLogFile(poolLogPath, poolRequestsMap);
parsePoolNodeLog(poolNodeLogPath, poolNodeMap);
updateCachedMetrics();

setInterval(() => {
    parseLogFile(fallbackLogPath, fallbackRequestsMap);
    parseLogFile(cacheLogPath, cacheRequestsMap);
    parseLogFile(poolLogPath, poolRequestsMap);
    parsePoolNodeLog(poolNodeLogPath, poolNodeMap);
    updateCachedMetrics();
}, parseInterval);
