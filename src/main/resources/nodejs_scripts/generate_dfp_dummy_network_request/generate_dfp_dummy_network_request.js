const co = require('co');
const util = require('util');
const generator = require('./generate_dfp_dummies');
const Storage = require('@google-cloud/storage');
const csvWriter = require('csv-write-stream');
const fs = require('fs');
const moment = require('moment');

if (require.main === module) {
    co(async function() {
        const projectId = 'dfp-data-analysis-186405';
        const storage = new Storage({
            projectId:projectId
        });
        const bucketName = 'gdfp-dummy-data-store';
        const filename = `NetworkRequests_1_${moment().format('YYYYMMDD_HH')}.csv`;
        const writer = csvWriter();
        writer.pipe(fs.createWriteStream(filename));

        const count = 10000 + Math.floor(Math.random() * 90000);
        util.log(`Will be generated ${count} logs`);

        for (let i = 0; i < count; i++) {
            const genObj = generator.generateDummyByRwc();
            // util.log(`generated: ${JSON.stringify(genObj, null, 4)}`);
            //write json objs to csv file
            writer.write(genObj);
        }
        writer.end();

        //upload to google cloud
        await storage.bucket(bucketName).upload(filename);
        util.log(`Finished upload ${filename} to ${bucketName}`);

        //remove file
        fs.unlinkSync(filename);
        util.log(`Finished unlink local file ${filename}`);
    }).catch((err) => {
        util.log(`! Exception caught: ${err.stack}`);
    });
}
