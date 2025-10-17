/**
 * Get the start of hour timestamp for a given timestamp
 * @param {string|number} timestamp - Timestamp in milliseconds
 * @returns {number} - Timestamp at the start of the hour
 */
function getStartOfHour(timestamp) {
    const date = new Date(parseInt(timestamp));
    date.setMinutes(0, 0, 0);
    return date.getTime();
}

module.exports = {
    getStartOfHour
};

