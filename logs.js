const fs = require('fs');
const path = require('path');
const http = require('http');

const { parseInterval } = require('./config');
const { logPort } = require('./config');

const fallbackRequestsMap = new Map();
const cacheRequestsMap = new Map();

// Read and parse the fallback requests log file
const fallbackLogPath = path.join(__dirname, '../shared/fallbackRequests.log');
const cacheLogPath = path.join(__dirname, '../shared/cacheRequests.log');

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
            const mapName = targetMap === fallbackRequestsMap ? 'fallbackRequestsMap' : 'cacheRequestsMap';
            console.log(`Added ${newEntriesCount} new entries to ${mapName}. Total entries: ${targetMap.size}`);
        }
    } catch (error) {
        const mapName = targetMap === fallbackRequestsMap ? 'fallbackRequestsMap' : 'cacheRequestsMap';
        console.error(`Error parsing ${mapName} log file:`, error);
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
    
    // Count fallback requests
    fallbackRequestsMap.forEach(entry => {
        if (parseInt(entry.epoch) >= oneHourAgo) {
            nFallbackRequestsLastHour++;
            if (entry.status !== 'success') {
                nErrorFallbackRequestsLastHour++;
            }
        }
    });
    
    // Count cache requests
    cacheRequestsMap.forEach(entry => {
        if (parseInt(entry.epoch) >= oneHourAgo) {
            nCacheRequestsLastHour++;
            if (entry.status !== 'success') {
                nErrorCacheRequestsLastHour++;
            }
        }
    });
    
    return {
        nTotalRequestsLastHour: nFallbackRequestsLastHour + nCacheRequestsLastHour,
        nFallbackRequestsLastHour,
        nCacheRequestsLastHour,
        nErrorFallbackRequestsLastHour,
        nErrorCacheRequestsLastHour
    };
}

// Create HTTP server to serve map contents
const server = http.createServer((req, res) => {
    res.setHeader('Content-Type', 'application/json');
    
    if (req.url === '/fallbackRequests') {
        res.end(JSON.stringify(getMapContents(fallbackRequestsMap), null, 2));
    } else if (req.url === '/cacheRequests') {
        res.end(JSON.stringify(getMapContents(cacheRequestsMap), null, 2));
    } else if (req.url === '/dashboard') {
        res.end(JSON.stringify(getDashboardMetrics(), null, 2));
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

// Start the log parsing for both fallback and cache logs
parseLogFile(fallbackLogPath, fallbackRequestsMap);
parseLogFile(cacheLogPath, cacheRequestsMap);
setInterval(() => {
    parseLogFile(fallbackLogPath, fallbackRequestsMap);
    parseLogFile(cacheLogPath, cacheRequestsMap);
}, parseInterval);