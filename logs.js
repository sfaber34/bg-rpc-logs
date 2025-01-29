const fs = require('fs');
const path = require('path');
const http = require('http');

const { parseInterval } = require('./config');
const { logPort } = require('./config');

const fallbackRequestsLog = new Map();

// Read and parse the fallback requests log file
const logFilePath = path.join(__dirname, '../shared/fallbackRequests.log');

function getMapContents() {
    return {
        size: fallbackRequestsLog.size,
        entries: Array.from(fallbackRequestsLog.entries()).map(([epoch, entry]) => ({
            epoch,
            ...entry
        }))
    };
}

function parseLogFile() {
    try {
        const fileContent = fs.readFileSync(logFilePath, 'utf8');
        const lines = fileContent.trim().split('\n');
        let newEntriesCount = 0;
        
        lines.forEach(line => {
            const [timestamp, epoch, requester, method, params, elapsed, status] = line.split('|');
            
            // Only add entries that aren't already in the map
            if (!fallbackRequestsLog.has(epoch)) {
                fallbackRequestsLog.set(epoch, {
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
            console.log(`Added ${newEntriesCount} new entries. Total entries: ${fallbackRequestsLog.size}`);
        }
    } catch (error) {
        console.error('Error parsing fallback requests log:', error);
    }
}

// Create HTTP server to serve map contents
const server = http.createServer((req, res) => {
    if (req.url === '/map') {
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify(getMapContents(), null, 2));
    } else {
        res.statusCode = 404;
        res.end('Not found');
    }
});

server.listen(logPort, () => {
    console.log(`Server running at http://localhost:${logPort}`);
});

// Start the log parsing
parseLogFile();
setInterval(parseLogFile, parseInterval);