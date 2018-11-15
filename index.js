const _ = require("underscore");
const fs = require('fs');
const getJSON = require("get-json");
const baseUrl = "http://api.geonet.org.nz";
const csvWriter = require('csv-write-stream')
const crypto = require('crypto');

var minQuakeStrength = 3;

function downloadQuakeData() {
    for (var quakeStrength=6;quakeStrength>minQuakeStrength;quakeStrength--) {
        var next = "/quake?MMI=" + quakeStrength;
        getJSON(baseUrl + next)
            .then(function(response) {
                _.each(response.features, processQuake)
            }).catch(function(error) {
                console.log("ERROR:" + error);
            });
    }

    // FIXME should this only after everything finished.. not sure how 
    //writer.end()

}

function processQuake(quake) {
    var publicID = quake.properties.publicID;
    var detail = "/intensity?type=reported&publicID=" + publicID;
    getJSON(baseUrl + detail)
        .then(function(response) {
            _.each(response.features, function(feltReport) {
                processFelt(quake,feltReport);
            });
        }).catch(function(error) {
            console.log("ERROR:" + error);
        });
}

function processFelt(quake, feltReport) {

    // quake:
    // { type: 'Feature',
    //   geometry:
    //    { type: 'Point', coordinates: [ 176.2995148, -39.94897461 ] },
    //   properties:
    //    { publicID: '2018p130600',
    //      time: '2018-02-18T07:43:48.127Z',
    //      depth: 20.59459877,
    //      magnitude: 5.156706293,
    //      locality: '20 km west of Waipukurau',
    //      mmi: 6,
    //      quality: 'best' } }
    // feltReport:
    // { type: 'Feature',
    //   geometry:
    //    { type: 'Point',
    //      coordinates: [ 175.116577148438, -39.8995971679688 ] },
    //   properties: { mmi: 4, count: 1 } }
    
    var feltCoordsStr = "" + feltReport.geometry.coordinates[0] + ":" + feltReport.geometry.coordinates[1];
    var coordHash = crypto.createHash('md5').update(feltCoordsStr).digest('hex');
    var geoAgentId = coordHash.substring(0,8);

    row = {
        quake_lat: quake.geometry.coordinates[1],
        quake_lon: quake.geometry.coordinates[0],
        quake_public_id: quake.properties.publicID,
        quake_time: quake.properties.time,
        quake_depth: quake.properties.depth,
        quake_magnitude: quake.properties.magnitude,
        //quake_locality: quake.properties.locality,
        quake_mmi: quake.properties.mmi,
        //quake_quality: quake.properties.quality,
        felt_lat: feltReport.geometry.coordinates[1],
        felt_lon: feltReport.geometry.coordinates[0],
        felt_mmi: feltReport.properties.mmi,
        felt_count: feltReport.properties.count,
        felt_agent_id: geoAgentId
    }
    csvFile.write(row);
}




function setupWriter() {
    csvFile = csvWriter()
    csvFile.pipe(fs.createWriteStream('felt_reports.csv'))  
} 

var rows = [];
var csvFile;
setupWriter();
downloadQuakeData();

//writeCsv(rows);
