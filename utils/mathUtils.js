/**
 * Calculate percentiles for an array of values
 * @param {number[]} values - Array of numeric values
 * @param {number[]} percentiles - Array of percentile values to calculate (e.g., [25, 50, 75])
 * @returns {Object} - Object with percentile keys (e.g., {p25: 10, p50: 20, p75: 30})
 */
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

module.exports = {
    calculatePercentiles
};

