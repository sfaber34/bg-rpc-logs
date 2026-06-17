const fs = require('fs');
const readline = require('readline');

/**
 * Count lines in a file efficiently
 * @param {string} filePath - Path to the file
 * @returns {Promise<number>} - Number of lines in the file
 */
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

/**
 * Get byte offset for a specific line number in a file
 * @param {string} filePath - Path to the file
 * @param {number} targetLine - Target line number
 * @returns {Promise<number>} - Byte offset for the target line
 */
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

module.exports = {
    countLines,
    getByteOffsetForLine
};

