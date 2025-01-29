const http = require('http');

const { logPort } = require('./config');

function displayLogMap() {
    const options = {
        hostname: 'localhost',
        port: logPort,
        path: '/map',
        method: 'GET'
    };

    const req = http.request(options, res => {
        let data = '';

        res.on('data', chunk => {
            data += chunk;
        });

        res.on('end', () => {
            const mapData = JSON.parse(data);
            
            console.log(`\nFallback Requests Log Map contains ${mapData.size} entries:`);
            console.log('\n=== Map Contents ===');
            
            mapData.entries.forEach(entry => {
                console.log(`\nEntry for epoch ${entry.epoch}:`);
                console.log(JSON.stringify(entry, null, 2));
            });
            
            console.log('\n=== End of Map Contents ===\n');
        });
    });

    req.on('error', error => {
        console.error('Error fetching map data:', error.message);
        console.error('Make sure logs.js is running first!');
    });

    req.end();
}

displayLogMap();