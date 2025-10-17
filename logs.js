const fs = require('fs');
const path = require('path');
const https = require('https');
const readline = require('readline');

const fallbackRequestsMap = new Map();
const cacheRequestsMap = new Map();
const poolRequestsMap = new Map();
const poolNodesMap = new Map();
const poolNodesTimingMap = new Map();
const poolNodesTimeoutCache = new Map();
const poolCompareResultsMap = new Map();

const {logPort, maxLogEntries, parseInterval, poolNodeTimingParseInterval, maxRequestHistoryHours } = require('./config');
const { ignoredErrorCodes } = require('../shared/ignoredErrorCodes');

// Helper function to count lines in a file efficiently
async function countLines(filePath) {
    return new Promise((resolve, reject) => {
        let lineCount = 0;
        const rl = readline.createInterface({
            input: fs.createReadStream(filePath),
            crlfDelay: Infinity
        });
        rl.on('line', () => lineCount++);
        rl.on('close', () => resolve(lineCount));
        rl.on('error', reject);
    });
}

// Helper function to get byte offset for a specific line number
async function getByteOffsetForLine(filePath, targetLine) {
    return new Promise((resolve, reject) => {
        let currentLine = 0;
        let byteOffset = 0;
        const rl = readline.createInterface({
            input: fs.createReadStream(filePath),
            crlfDelay: Infinity
        });
        rl.on('line', (line) => {
            if (currentLine >= targetLine) {
                rl.close();
                return;
            }
            byteOffset += Buffer.byteLength(line + '\n', 'utf8');
            currentLine++;
        });
        rl.on('close', () => resolve(byteOffset));
        rl.on('error', reject);
    });
}

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
let lastProcessedRequestorEpoch = 0;

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

// Efficient, incremental log parsing for all log types
async function parseLogFile(logPath, targetMap, logType) {
    try {
        let newEntriesCount = 0;
        const lastProcessedIndex = lastProcessedIndexes[logType];
        
        // On first run, count total lines and only read the last maxLogEntries
        let startLine = 0;
        let startByte = 0;
        
        if (lastProcessedIndex === -1) {
            // First run - need to find where to start
            const totalLines = await countLines(logPath);
            if (totalLines > maxLogEntries) {
                startLine = totalLines - maxLogEntries;
                console.log(`${logType}: Skipping first ${startLine} lines, reading last ${maxLogEntries} entries from ${totalLines} total lines`);
                // Calculate byte offset to start from
                startByte = await getByteOffsetForLine(logPath, startLine);
            }
        } else {
            // Subsequent runs - start from where we left off
            startLine = lastProcessedIndex + 1;
            startByte = lastByteOffsets[logType];
        }
        
        // Check if file exists and get its size
        const stats = await fs.promises.stat(logPath);
        if (stats.size === startByte) {
            // No new data
            return;
        }
        
        // If file was truncated or is smaller than our offset, reset
        if (stats.size < startByte) {
            console.log(`${logType}: Log file was rotated or truncated, re-reading from start`);
            targetMap.clear();
            lastProcessedIndexes[logType] = -1;
            lastByteOffsets[logType] = 0;
            startLine = 0;
            startByte = 0;
        }
        
        // Initialize currentLine to startLine since we're reading from that position
        let currentLine = startLine;
        let currentByte = startByte;
        let lineBuffer = '';
        
        await new Promise((resolve, reject) => {
            const stream = fs.createReadStream(logPath, {
                start: startByte,
                encoding: 'utf8'
            });
            
            stream.on('data', (chunk) => {
                lineBuffer += chunk;
                const lines = lineBuffer.split('\n');
                // Keep the last incomplete line in buffer
                lineBuffer = lines.pop() || '';
                
                for (const line of lines) {
                    if (line.trim()) {
                        const [timestamp, epoch, requester, method, params, elapsed, status] = line.split('|');
                        const key = `${epoch}-${currentLine}`;
                        targetMap.set(key, {
                            timestamp,
                            epoch,
                            requester: requester || '',
                            method,
                            params,
                            elapsed: parseFloat(elapsed),
                            status,
                            lineIndex: currentLine
                        });
                        newEntriesCount++;
                        lastProcessedIndexes[logType] = currentLine;
                    }
                    currentLine++;
                    currentByte += Buffer.byteLength(line + '\n', 'utf8');
                }
            });
            
            stream.on('end', () => {
                // Process last line if exists
                if (lineBuffer.trim()) {
                    const line = lineBuffer;
                    const [timestamp, epoch, requester, method, params, elapsed, status] = line.split('|');
                    const key = `${epoch}-${currentLine}`;
                    targetMap.set(key, {
                        timestamp,
                        epoch,
                        requester: requester || '',
                        method,
                        params,
                        elapsed: parseFloat(elapsed),
                        status,
                        lineIndex: currentLine
                    });
                    newEntriesCount++;
                    lastProcessedIndexes[logType] = currentLine;
                    currentByte += Buffer.byteLength(line + '\n', 'utf8');
                }
                lastByteOffsets[logType] = currentByte;
                resolve();
            });
            
            stream.on('error', reject);
        });
        
        // Prune oldest entries if needed
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

async function parsePoolNodeLog(logPath, targetMap) {
    try {
        let newEntriesCount = 0;
        const lastProcessedIndex = lastProcessedIndexes.poolNodes;
        
        // On first run, count total lines and only read the last maxLogEntries
        let startLine = 0;
        let startByte = 0;
        
        if (lastProcessedIndex === -1) {
            // First run - need to find where to start
            const totalLines = await countLines(logPath);
            if (totalLines > maxLogEntries) {
                startLine = totalLines - maxLogEntries;
                console.log(`poolNodes: Skipping first ${startLine} lines, reading last ${maxLogEntries} entries from ${totalLines} total lines`);
                // Calculate byte offset to start from
                startByte = await getByteOffsetForLine(logPath, startLine);
            }
        } else {
            // Subsequent runs - start from where we left off
            startLine = lastProcessedIndex + 1;
            startByte = lastByteOffsets.poolNodes;
        }
        
        // Check if file exists and get its size
        const stats = await fs.promises.stat(logPath);
        if (stats.size === startByte) {
            // No new data
            return;
        }
        
        // If file was truncated or is smaller than our offset, reset
        if (stats.size < startByte) {
            console.log(`poolNodes: Log file was rotated or truncated, re-reading from start`);
            targetMap.clear();
            lastProcessedIndexes.poolNodes = -1;
            lastByteOffsets.poolNodes = 0;
            startLine = 0;
            startByte = 0;
        }
        
        // Initialize currentLine to startLine since we're reading from that position
        let currentLine = startLine;
        let currentByte = startByte;
        let lineBuffer = '';
        
        await new Promise((resolve, reject) => {
            const stream = fs.createReadStream(logPath, {
                start: startByte,
                encoding: 'utf8'
            });
            
            stream.on('data', (chunk) => {
                lineBuffer += chunk;
                const lines = lineBuffer.split('\n');
                // Keep the last incomplete line in buffer
                lineBuffer = lines.pop() || '';
                
                for (const line of lines) {
                    if (line.trim()) {
                        const [timestamp, epoch, nodeId, owner, method, params, duration, status] = line.split('|');
                        const key = `${epoch}-${nodeId}-${currentLine}`;
                        targetMap.set(key, {
                            timestamp,
                            epoch,
                            nodeId,
                            owner,
                            method,
                            params,
                            duration: parseFloat(duration),
                            status,
                            lineIndex: currentLine
                        });
                        newEntriesCount++;
                        lastProcessedIndexes.poolNodes = currentLine;
                    }
                    currentLine++;
                    currentByte += Buffer.byteLength(line + '\n', 'utf8');
                }
            });
            
            stream.on('end', () => {
                // Process last line if exists
                if (lineBuffer.trim()) {
                    const line = lineBuffer;
                    const [timestamp, epoch, nodeId, owner, method, params, duration, status] = line.split('|');
                    const key = `${epoch}-${nodeId}-${currentLine}`;
                    targetMap.set(key, {
                        timestamp,
                        epoch,
                        nodeId,
                        owner,
                        method,
                        params,
                        duration: parseFloat(duration),
                        status,
                        lineIndex: currentLine
                    });
                    newEntriesCount++;
                    lastProcessedIndexes.poolNodes = currentLine;
                    currentByte += Buffer.byteLength(line + '\n', 'utf8');
                }
                lastByteOffsets.poolNodes = currentByte;
                resolve();
            });
            
            stream.on('error', reject);
        });
        
        // Prune oldest entries if needed
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

async function parsePoolCompareResultsLog(logPath, targetMap) {
    try {
        let newEntriesCount = 0;
        const lastProcessedIndex = lastProcessedIndexes.poolCompareResults;
        
        // Keep existing mismatched entries
        const mismatchedEntries = [];
        targetMap.forEach((value, key) => {
            if (!value.resultsMatch) {
                mismatchedEntries.push({ key, value });
            }
        });
        
        // On first run, calculate starting position
        let startLine = 0;
        let startByte = 0;
        
        if (lastProcessedIndex === -1) {
            const totalLines = await countLines(logPath);
            console.log(`poolCompareResults: Scanning ${totalLines} total lines for mismatches on initial load`);
        } else {
            // Subsequent runs - start from where we left off
            startLine = lastProcessedIndex + 1;
            startByte = lastByteOffsets.poolCompareResults;
        }
        
        // Check if file exists and get its size
        const stats = await fs.promises.stat(logPath);
        if (stats.size === startByte) {
            // No new data
            return;
        }
        
        // If file was truncated or is smaller than our offset, reset
        if (stats.size < startByte) {
            console.log(`poolCompareResults: Log file was rotated or truncated, re-reading from start`);
            targetMap.clear();
            lastProcessedIndexes.poolCompareResults = -1;
            lastByteOffsets.poolCompareResults = 0;
            startLine = 0;
            startByte = 0;
        }
        
        // Initialize currentLine to startLine since we're reading from that position
        let currentLine = startLine;
        let currentByte = startByte;
        let lineBuffer = '';
        
        await new Promise((resolve, reject) => {
            const stream = fs.createReadStream(logPath, {
                start: startByte,
                encoding: 'utf8'
            });
            
            stream.on('data', (chunk) => {
                lineBuffer += chunk;
                const lines = lineBuffer.split('\n');
                // Keep the last incomplete line in buffer
                lineBuffer = lines.pop() || '';
                
                for (const line of lines) {
                    if (line.trim()) {
                        const [
                            timestamp, epoch, resultsMatch, mismatchedNode, mismatchedOwner, 
                            mismatchedResults, nodeId1, nodeResult1, nodeId2, nodeResult2, 
                            nodeId3, nodeResult3, method, params
                        ] = line.split('|');
                        
                        // Always update the last processed index
                        lastProcessedIndexes.poolCompareResults = currentLine;
                        
                        // Only store mismatches
                        if (resultsMatch === 'false') {
                            const key = `${epoch}-${currentLine}`;
                            const parsedMismatchedResults = mismatchedResults === '[]' ? [] : JSON.parse(mismatchedResults);
                            const entry = {
                                key,
                                value: {
                                    timestamp,
                                    epoch,
                                    resultsMatch: false,
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
                                    lineIndex: currentLine
                                }
                            };
                            mismatchedEntries.push(entry);
                            newEntriesCount++;
                        }
                    }
                    currentLine++;
                    currentByte += Buffer.byteLength(line + '\n', 'utf8');
                }
            });
            
            stream.on('end', () => {
                // Process last line if exists
                if (lineBuffer.trim()) {
                    const line = lineBuffer;
                    const [
                        timestamp, epoch, resultsMatch, mismatchedNode, mismatchedOwner, 
                        mismatchedResults, nodeId1, nodeResult1, nodeId2, nodeResult2, 
                        nodeId3, nodeResult3, method, params
                    ] = line.split('|');
                    
                    lastProcessedIndexes.poolCompareResults = currentLine;
                    
                    if (resultsMatch === 'false') {
                        const key = `${epoch}-${currentLine}`;
                        const parsedMismatchedResults = mismatchedResults === '[]' ? [] : JSON.parse(mismatchedResults);
                        const entry = {
                            key,
                            value: {
                                timestamp,
                                epoch,
                                resultsMatch: false,
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
                                lineIndex: currentLine
                            }
                        };
                        mismatchedEntries.push(entry);
                        newEntriesCount++;
                    }
                    currentByte += Buffer.byteLength(line + '\n', 'utf8');
                }
                lastByteOffsets.poolCompareResults = currentByte;
                resolve();
            });
            
            stream.on('error', reject);
        });
        // Sort mismatched entries by timestamp (newest first)
        mismatchedEntries.sort((a, b) => {
            const timeA = new Date(a.value.timestamp).getTime();
            const timeB = new Date(b.value.timestamp).getTime();
            if (timeA !== timeB) {
                return timeB - timeA;
            }
            return b.value.lineIndex - a.value.lineIndex;
        });
        // Clear and re-add only mismatched entries
        targetMap.clear();
        mismatchedEntries.forEach(({key, value}) => {
            targetMap.set(key, value);
        });
        if (newEntriesCount > 0) {
            console.log(`Added ${newEntriesCount} new mismatched entries to poolCompareResultsMap. Total mismatched entries: ${targetMap.size}`);
        }
    } catch (error) {
        console.error('Error parsing poolCompareResultsMap log file:', error);
    }
}

async function parsePoolNodeTimingLog(logPath) {
    try {
        let currentLine = 0;
        let newEntriesCount = 0;
        // We want to keep all durations for each node, but only add new ones
        await new Promise((resolve, reject) => {
            const rl = readline.createInterface({
                input: fs.createReadStream(logPath),
                crlfDelay: Infinity
            });
            rl.on('line', (line) => {
                if (currentLine > (poolNodesTimingMap.lastProcessedIndex || -1)) {
                    const [, , nodeId, , , , duration] = line.split('|');
                    if (!poolNodesTimingMap.has(nodeId)) {
                        poolNodesTimingMap.set(nodeId, []);
                    }
                    poolNodesTimingMap.get(nodeId).push(parseFloat(duration));
                    newEntriesCount++;
                    poolNodesTimingMap.lastProcessedIndex = currentLine;
                }
                currentLine++;
            });
            rl.on('close', resolve);
            rl.on('error', reject);
        });
        if (newEntriesCount > 0) {
            console.log(`Added ${newEntriesCount} timing entries to poolNodesTimingMap. Total nodes: ${poolNodesTimingMap.size}`);
        }
    } catch (error) {
        console.error('Error parsing poolNodesTimingMap log file:', error);
    }
}

async function parsePoolNodeTimeoutCache(logPath) {
    try {
        let newEntriesCount = 0;
        const sevenDaysAgo = Date.now() - (7 * 24 * 60 * 60 * 1000);
        const startByte = lastByteOffsets.poolNodesTimeout;
        
        // Check if file exists and get its size
        const stats = await fs.promises.stat(logPath);
        if (stats.size === startByte) {
            // No new data, just prune old entries
            const entriesToRemove = [];
            poolNodesTimeoutCache.forEach((entry, key) => {
                const entryEpoch = parseInt(entry.epoch);
                if (entryEpoch < sevenDaysAgo) {
                    entriesToRemove.push(key);
                }
            });
            entriesToRemove.forEach(key => poolNodesTimeoutCache.delete(key));
            if (entriesToRemove.length > 0) {
                console.log(`Pruned ${entriesToRemove.length} old entries from poolNodesTimeoutCache. Total entries: ${poolNodesTimeoutCache.size}`);
            }
            return;
        }
        
        // If file was truncated or is smaller than our offset, reset
        if (stats.size < startByte) {
            console.log('Log file was rotated or truncated, resetting poolNodesTimeoutCache');
            poolNodesTimeoutCache.clear();
            lastByteOffsets.poolNodesTimeout = 0;
        }
        
        let currentByte = lastByteOffsets.poolNodesTimeout;
        let lineBuffer = '';
        
        await new Promise((resolve, reject) => {
            const stream = fs.createReadStream(logPath, { 
                start: lastByteOffsets.poolNodesTimeout,
                encoding: 'utf8'
            });
            
            stream.on('data', (chunk) => {
                lineBuffer += chunk;
                const lines = lineBuffer.split('\n');
                // Keep the last incomplete line in buffer
                lineBuffer = lines.pop() || '';
                
                for (const line of lines) {
                    if (line.trim()) {
                        const [, epoch, nodeId, owner, , , , status] = line.split('|');
                        const key = `${epoch}-${nodeId}-${currentByte}`;
                        poolNodesTimeoutCache.set(key, {
                            epoch,
                            nodeId,
                            owner,
                            status
                        });
                        newEntriesCount++;
                    }
                    currentByte += Buffer.byteLength(line + '\n', 'utf8');
                }
            });
            
            stream.on('end', () => {
                // Process last line if exists
                if (lineBuffer.trim()) {
                    const line = lineBuffer;
                    const [, epoch, nodeId, owner, , , , status] = line.split('|');
                    const key = `${epoch}-${nodeId}-${currentByte}`;
                    poolNodesTimeoutCache.set(key, {
                        epoch,
                        nodeId,
                        owner,
                        status
                    });
                    newEntriesCount++;
                    currentByte += Buffer.byteLength(line + '\n', 'utf8');
                }
                lastByteOffsets.poolNodesTimeout = currentByte;
                resolve();
            });
            
            stream.on('error', reject);
        });
        
        // Prune entries older than 7 days
        const entriesToRemove = [];
        poolNodesTimeoutCache.forEach((entry, key) => {
            const entryEpoch = parseInt(entry.epoch);
            if (entryEpoch < sevenDaysAgo) {
                entriesToRemove.push(key);
            }
        });
        entriesToRemove.forEach(key => poolNodesTimeoutCache.delete(key));
        
        if (newEntriesCount > 0) {
            console.log(`Added ${newEntriesCount} timeout entries to poolNodesTimeoutCache (read from byte ${startByte} to ${currentByte}). Total entries: ${poolNodesTimeoutCache.size} (pruned ${entriesToRemove.length} old entries)`);
        } else if (entriesToRemove.length > 0) {
            console.log(`Pruned ${entriesToRemove.length} old entries from poolNodesTimeoutCache. Total entries: ${poolNodesTimeoutCache.size}`);
        }
    } catch (error) {
        console.error('Error parsing poolNodesTimeoutCache log file:', error);
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
            
            // Only count non-buidlguidl-client cache requests for nCacheRequests* fields
            if (prefix === 'cache' && entry.requester === 'buidlguidl-client') {
                return;
            }

            if (entry.status === 'success') {
                hourData[`n${prefix.charAt(0).toUpperCase() + prefix.slice(1)}RequestsSuccess`]++;
            } else {
                try {
                    const statusObj = JSON.parse(entry.status);
                    const code = statusObj.error && statusObj.error.code;
                    if (code !== undefined && ignoredErrorCodes.includes(Number(code))) {
                        // Skip ignored error codes
                        return;
                    }
                    if (code && code.toString().startsWith('-69')) {
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

    // Prune old hours from requestHistory to prevent memory leak
    if (requestHistory.size > maxRequestHistoryHours) {
        const sortedKeys = Array.from(requestHistory.keys()).sort((a, b) => a - b);
        for (let i = 0; i < requestHistory.size - maxRequestHistoryHours; i++) {
            requestHistory.delete(sortedKeys[i]);
        }
    }
}

function getDashboardMetrics() {
    const oneHourAgo = Date.now() - (60 * 60 * 1000); // 1 hour in milliseconds
    
    let nFallbackRequestsLastHour = 0;
    let nErrorFallbackRequestsLastHour = 0;
    let nWarningFallbackRequestsLastHour = 0;
    let nCacheRequestsLastHour = 0;
    let nErrorCacheRequestsLastHour = 0;
    let nWarningCacheRequestsLastHour = 0;
    let nCacheRequestsClientLastHour = 0;
    let nErrorCacheRequestsClientLastHour = 0;
    let nWarningCacheRequestsClientLastHour = 0;
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
    
    // Arrays to store request times for different types
    const fallbackRequestTimesLastHour = [];
    const cacheRequestTimesLastHour = [];
    const cacheRequestClientTimesLastHour = [];
    const poolRequestTimesLastHour = [];
    
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
                    const code = statusObj.error && statusObj.error.code;
                    if (code !== undefined && ignoredErrorCodes.includes(Number(code))) {
                        // Skip ignored error codes
                        return;
                    }
                    if (code && code.toString().startsWith('-69')) {
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
            const isClientRequest = entry.requester === 'buidlguidl-client';
            
            if (isClientRequest) {
                nCacheRequestsClientLastHour++;
                cacheRequestClientTimesLastHour.push(entry.elapsed);
                if (entry.status !== 'success') {
                    try {
                        const statusObj = JSON.parse(entry.status);
                        const code = statusObj.error && statusObj.error.code;
                        if (code !== undefined && ignoredErrorCodes.includes(Number(code))) {
                            // Skip ignored error codes
                            return;
                        }
                        if (code && code.toString().startsWith('-69')) {
                            nWarningCacheRequestsClientLastHour++;
                        } else {
                            nErrorCacheRequestsClientLastHour++;
                        }
                    } catch (e) {
                        // If status is not JSON or doesn't have expected structure, count as error
                        nErrorCacheRequestsClientLastHour++;
                    }
                }
            } else {
                nCacheRequestsLastHour++;
                cacheRequestTimesLastHour.push(entry.elapsed);
                if (entry.status !== 'success') {
                    try {
                        const statusObj = JSON.parse(entry.status);
                        const code = statusObj.error && statusObj.error.code;
                        if (code !== undefined && ignoredErrorCodes.includes(Number(code))) {
                            // Skip ignored error codes
                            return;
                        }
                        if (code && code.toString().startsWith('-69')) {
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
            totalCacheTime += entry.elapsed;
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
                    const code = statusObj.error && statusObj.error.code;
                    if (code !== undefined && ignoredErrorCodes.includes(Number(code))) {
                        // Skip ignored error codes
                        return;
                    }
                    if (code && code.toString().startsWith('-69')) {
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
    const medCacheRequestClientTimeLastHour = cacheRequestClientTimesLastHour.length > 0 ? 
        calculatePercentiles(cacheRequestClientTimesLastHour, [50]).p50 : 0;
    const medPoolRequestTimeLastHour = poolRequestTimesLastHour.length > 0 ? 
        calculatePercentiles(poolRequestTimesLastHour, [50]).p50 : 0;
    
    return {
        timestamp: Date.now(),
        nTotalRequestsLastHour: nFallbackRequestsLastHour + nCacheRequestsLastHour + nPoolRequestsLastHour,
        nFallbackRequestsLastHour,
        nCacheRequestsLastHour,
        nCacheRequestsClientLastHour,
        nPoolRequestsLastHour,
        nErrorFallbackRequestsLastHour,
        nErrorCacheRequestsLastHour,
        nErrorCacheRequestsClientLastHour,
        nErrorPoolRequestsLastHour,
        nWarningFallbackRequestsLastHour,
        nWarningCacheRequestsLastHour,
        nWarningCacheRequestsClientLastHour,
        nWarningPoolRequestsLastHour,
        medFallbackRequestTimeLastHour,
        medCacheRequestTimeLastHour,
        medCacheRequestClientTimeLastHour,
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

function calculateNodeTimeoutMetrics(timeframe = 'week') {
    const daysToLookBack = timeframe === 'day' ? 1 : 7;
    const timeAgo = Date.now() - (daysToLookBack * 24 * 60 * 60 * 1000);
    const nodeStats = new Map();

    // Helper function to extract nodeId prefix before MAC address
    function extractNodePrefix(fullNodeId) {
        // MAC address pattern: XX:XX:XX:XX:XX:XX (where X is hex digit)
        const macAddressPattern = /-[0-9a-fA-F]{2}:[0-9a-fA-F]{2}:[0-9a-fA-F]{2}:[0-9a-fA-F]{2}:[0-9a-fA-F]{2}:[0-9a-fA-F]{2}/;
        const match = fullNodeId.match(macAddressPattern);
        if (match) {
            // Return everything before the MAC address (excluding the dash before it)
            return fullNodeId.substring(0, match.index);
        }
        return fullNodeId; // Return full ID if no MAC address found
    }

    // Process each entry in poolNodesTimeoutCache
    poolNodesTimeoutCache.forEach(entry => {
        const epoch = parseInt(entry.epoch);
        if (epoch >= timeAgo) {
            const fullNodeId = entry.nodeId;
            const owner = entry.owner;
            const status = entry.status;

            // Use full nodeId as key for aggregating stats
            if (!nodeStats.has(fullNodeId)) {
                nodeStats.set(fullNodeId, {
                    fullNodeId,
                    owner,
                    totalRequests: 0,
                    timeoutRequests: 0
                });
            }

            const stats = nodeStats.get(fullNodeId);
            stats.totalRequests++;
            if (status === 'timeout_error') {
                stats.timeoutRequests++;
            }
        }
    });

    // Convert to array with full nodeId, pretty nodeId, and percentTimeout
    // Filter out nodes with zero requests in the timeframe
    const result = Array.from(nodeStats.values())
        .filter(stats => stats.totalRequests > 0)
        .map(stats => ({
            nodeId: stats.fullNodeId,
            nodeIdPretty: extractNodePrefix(stats.fullNodeId),
            owner: stats.owner,
            percentTimeout: stats.timeoutRequests / stats.totalRequests
        }));

    // Sort by owner then by nodeIdPretty
    result.sort((a, b) => {
        const ownerCompare = (a.owner || '').localeCompare(b.owner || '');
        if (ownerCompare !== 0) {
            return ownerCompare;
        }
        return (a.nodeIdPretty || '').localeCompare(b.nodeIdPretty || '');
    });

    // Update the appropriate cache based on timeframe
    if (timeframe === 'day') {
        cachedNodeTimeoutMetricsLastDay = result;
    } else {
        cachedNodeTimeoutMetricsLastWeek = result;
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
    cachedDashboardMetrics = getDashboardMetrics();
    updateRequestorMetrics();
}

// Initial processing in an async IIFE
(async () => {
    await parseLogFile(fallbackLogPath, fallbackRequestsMap, 'fallback');
    await parseLogFile(cacheLogPath, cacheRequestsMap, 'cache');
    await parseLogFile(poolLogPath, poolRequestsMap, 'pool');
    await parsePoolNodeLog(poolNodesLogPath, poolNodesMap);
    await parsePoolNodeTimingLog(poolNodesLogPath);
    await parsePoolNodeTimeoutCache(poolNodesLogPath);
    await parsePoolCompareResultsLog(poolCompareResultsLogPath, poolCompareResultsMap);

    // Calculate initial node timeout metrics after poolNodesTimeoutCache is populated
    calculateNodeTimeoutMetrics('week');
    calculateNodeTimeoutMetrics('day');

    // Reset lastProcessedRequestorEpoch to ensure we process all entries on first run
    lastProcessedRequestorEpoch = 0;

    updateRequestHistory(); // Initial history calculation
    updateCachedMetrics();

    // Track the last hour we processed to detect hour changes
    let lastProcessedHour = new Date().getHours();

    // Keep regular dashboard metrics updates at current interval
    setInterval(async () => {
        const currentHour = new Date().getHours();
        // Check if we've crossed an hour boundary
        if (currentHour !== lastProcessedHour) {
            console.log(`Hour changed from ${lastProcessedHour} to ${currentHour}, updating request history and node timeout metrics`);
            updateRequestHistory();
            await parsePoolNodeTimeoutCache(poolNodesLogPath); // Update timeout cache every hour
            calculateNodeTimeoutMetrics('week'); // Update node timeout metrics for last week every hour
            calculateNodeTimeoutMetrics('day'); // Update node timeout metrics for last day every hour
            lastProcessedHour = currentHour;
        }

        await parseLogFile(fallbackLogPath, fallbackRequestsMap, 'fallback');
        await parseLogFile(cacheLogPath, cacheRequestsMap, 'cache');
        await parseLogFile(poolLogPath, poolRequestsMap, 'pool');
        await parsePoolNodeLog(poolNodesLogPath, poolNodesMap);
        await parsePoolCompareResultsLog(poolCompareResultsLogPath, poolCompareResultsMap);
        updateCachedMetrics();
    }, parseInterval);

    setInterval(async () => {
        await parsePoolNodeTimingLog(poolNodesLogPath);
    }, poolNodeTimingParseInterval);
})();