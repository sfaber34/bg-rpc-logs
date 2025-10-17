const fs = require('fs');
const readline = require('readline');
const { countLines, getByteOffsetForLine } = require('./fileUtils');

/**
 * Parse a standard log file incrementally
 * Efficiently reads only new entries from log files
 */
async function parseLogFile(logPath, targetMap, logType, lastProcessedIndexes, lastByteOffsets, maxLogEntries) {
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
            const mapName = logType + 'RequestsMap';
            console.log(`Added ${newEntriesCount} new entries to ${mapName}. Total entries: ${targetMap.size}`);
        }
    } catch (error) {
        const mapName = logType + 'RequestsMap';
        console.error(`Error parsing ${mapName} log file:`, error);
    }
}

/**
 * Parse pool node log file incrementally
 */
async function parsePoolNodeLog(logPath, targetMap, lastProcessedIndexes, lastByteOffsets, maxLogEntries) {
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

/**
 * Parse pool compare results log file - only stores mismatches
 */
async function parsePoolCompareResultsLog(logPath, targetMap, lastProcessedIndexes, lastByteOffsets) {
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

/**
 * Parse pool node timing log file
 * Accumulates timing data for each node
 */
async function parsePoolNodeTimingLog(logPath, poolNodesTimingMap) {
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

/**
 * Parse pool node timeout cache
 * Keeps track of node timeouts for the last 7 days
 */
async function parsePoolNodeTimeoutCache(logPath, poolNodesTimeoutCache, lastByteOffsets) {
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

module.exports = {
    parseLogFile,
    parsePoolNodeLog,
    parsePoolCompareResultsLog,
    parsePoolNodeTimingLog,
    parsePoolNodeTimeoutCache
};

