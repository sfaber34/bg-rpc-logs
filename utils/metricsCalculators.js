const { calculatePercentiles } = require('./mathUtils');
const { getStartOfHour } = require('./timeUtils');
const { extractNodePrefix } = require('./dataTransformers');
const { ignoredErrorCodes } = require('../../shared/ignoredErrorCodes');

/**
 * Update request history with hourly aggregated data
 */
function updateRequestHistory(
    requestHistory,
    fallbackRequestsMap,
    cacheRequestsMap,
    poolRequestsMap,
    maxRequestHistoryHours
) {
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

/**
 * Get comprehensive dashboard metrics
 */
function getDashboardMetrics(
    fallbackRequestsMap,
    cacheRequestsMap,
    poolRequestsMap,
    poolNodesMap,
    poolNodesTimingMap,
    requestHistory
) {
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

/**
 * Update requestor metrics
 */
function updateRequestorMetrics(
    cachedRequestorMetrics,
    lastProcessedRequestorEpoch,
    fallbackRequestsMap,
    cacheRequestsMap,
    poolRequestsMap
) {
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
        if (parseInt(entry.epoch) <= lastProcessedRequestorEpoch.value) {
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
        lastProcessedRequestorEpoch.value = Math.max(...allEpochs);
    }

    // Convert Map to object for JSON serialization
    return Object.fromEntries(requestorMetrics);
}

/**
 * Calculate node timeout metrics
 */
function calculateNodeTimeoutMetrics(poolNodesTimeoutCache, timeframe = 'week') {
    const daysToLookBack = timeframe === 'day' ? 1 : 7;
    const timeAgo = Date.now() - (daysToLookBack * 24 * 60 * 60 * 1000);
    const nodeStats = new Map();

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

    return result;
}

module.exports = {
    updateRequestHistory,
    getDashboardMetrics,
    updateRequestorMetrics,
    calculateNodeTimeoutMetrics
};

