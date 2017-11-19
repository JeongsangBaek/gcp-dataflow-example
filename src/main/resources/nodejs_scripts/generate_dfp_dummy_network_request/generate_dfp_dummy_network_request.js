const co = require('co');
const util = require('util');
const generator = require('./generate_dfp_dummies');

if (require.main === module) {
    co(async function() {
        const genObj = generator.generateDummyByRwc();
        util.log(`generated: ${JSON.stringify(genObj, null, 4)}`);

    }).catch((err) => {
        util.log(`! Exception caught: ${err}`);
    });
}