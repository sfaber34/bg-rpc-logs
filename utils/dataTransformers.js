/**
 * Extract map contents and format for output
 * @param {Map} targetMap - Map to extract entries from
 * @param {Map} poolCompareResultsMap - Reference to poolCompareResultsMap for special handling
 * @returns {Array} - Array of formatted entries
 */
function getMapContents(targetMap, poolCompareResultsMap) {
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

/**
 * Extract node ID prefix before MAC address
 * @param {string} fullNodeId - Full node ID including MAC address
 * @returns {string} - Node ID prefix without MAC address
 */
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

module.exports = {
    getMapContents,
    extractNodePrefix
};

