const moment = require('moment');
const rwc = require('random-weighted-choice');
const Hashids = require('hashids');
const hashids = new Hashids();

function generateDummyByRwc() {
    const momentObj = rwc([
        {weight:7, id:(moment().format('YYYY-MM-DD HH:mm:ss'))},
        {weight:1, id:(moment().add(-1, 'days').format('YYYY-MM-DD HH:mm:ss'))},
        {weight:1, id:(moment().add(-2, 'days').format('YYYY-MM-DD HH:mm:ss'))},
        {weight:1, id:(moment().add(-4, 'days').format('YYYY-MM-DD HH:mm:ss'))}
    ]);
    const userId = Math.floor(Math.random() * 5000);
    const adUnitId = Math.floor(Math.random() * 10000000);
    const ct = rwc([
        {weight:7, id:'pos=banner;test=false'},
        {weight:2, id:'pos=wallpaper;test=true'},
        {weight:1, id:'top=true'},
    ]);
    const country = 'United States';
    const region = rwc([
        {weight:8, id:'California'},
        {weight:1, id:'Texas'},
        {weight:1, id:'Oregon'},
    ]);
    const browser = rwc([
        {weight:6, id:'Google Chrome Any.Any'},
        {weight:3, id:'Safari (iPhone/iPod) Any.Any'},
        {weight:1, id:'Firefox Any.Any'}
    ]);

    let os;
    switch(browser) {
        case 'Google Chrome Any.Any':
        case 'Firefox Any.Any':
            os = 'Android';
            break;
        case 'Safari (iPhone/iPod) Any.Any':
            os = 'iOS';
            break;
    }

    let city;
    switch(region) {
        case 'California':
            city = 'San Francisro';
            break;
        case 'Texas':
            city = 'San Antonio';
            break;
        case 'Oregon':
            city = 'Seattle';
            break;
    }
    let metro = city + ' metro';

    const domain = 'espn.com';

    const postal_code = 78247;
    const bandwidth = 'cable';
    const gfp_content_id = Math.floor(Math.random() * 10000000);
    const browser_id = Math.floor(Math.random() * 10000000);
    const os_id = Math.floor(Math.random() * 10000000);
    const country_id = Math.floor(Math.random() * 10000000);
    const region_id = Math.floor(Math.random() * 10000000);
    const city_id= Math.floor(Math.random() * 10000000);
    const metro_id = Math.floor(Math.random() * 10000000);
    const postal_code_id = Math.floor(Math.random() * 10000000);
    const bandwidth_id = Math.floor(Math.random() * 10000000);
    const audiencd_segment_ids = '20192|84|1234';
    const request_ad_unit_sizes = '320x50|300x600';

    let mobile_device;
    switch(os) {
        case 'Android':
            mobile_device = 'sm-g900t_from_Samsung';
            break;
        case 'iOS':
            mobile_device = 'iphone92_from_Apple';
            break;
    }
    let os_version;
    switch(os) {
        case 'Android':
            os_version = 'Android_6_0';
            break;
        case 'iOS':
            os_version = 'iOS_11_0';
            break;
    }
    const mobile_capability = 'Mobile Apps';
    const mobile_carrier = 'Wifi';
    const bandwidth_group_id = 4;
    const publisher_provided_id = 'aDC4aX3Y-wMyryV6Vl1frA';
    const video_position = Math.floor(Math.random() * 10);
    const pod_position = Math.floor(Math.random() * 10);
    const device_category = 'Smartphone';
    const is_interstitial = 'False';
    const referer_url =  rwc([
        {weight:3, id:'https://www.espn.com'},
        {weight:3, id:'https://www.nyt.com'},
        {weight:4, id:'https://www.google.com'}
    ]);
    const mobile_app_id = 'com.google.android.youtube';
    const request_language = 'en';
    const anonymous = 'false';

    const dummyJsonObj = {
        time:momentObj,
        time_u_sec_2:moment(momentObj).unix(),
        key_part:hashids.encode(moment(momentObj).unix()),
        user_id:`user_${userId}`,
        ad_unit_id:adUnitId,
        custom_targeting:ct,
        country:country,
        region:region,
        browser:browser,
        os:os,
        domain:domain,
        metro:metro,
        city:city,
        postal_code:postal_code,
        bandwidth:bandwidth,
        gfp_content_id:gfp_content_id,
        browser_id:browser_id,
        os_id:os_id,
        country_id:country_id,
        region_id:region_id,
        city_id:city_id,
        metro_id:metro_id,
        postal_code_id:postal_code_id,
        bandwidth_id:bandwidth_id,
        audience_segment_ids:audiencd_segment_ids,
        requested_ad_unit_size:request_ad_unit_sizes,
        mobile_device:mobile_device,
        os_version:os_version,
        mobile_capability:mobile_capability,
        mobile_carrier:mobile_carrier,
        bandwidth_group_id:bandwidth_group_id,
        publisher_provided_id:publisher_provided_id,
        video_position:video_position,
        pod_position:pod_position,
        device_category:device_category,
        is_interstitial:is_interstitial,
        referer_url:referer_url,
        mobile_app_id:mobile_app_id,
        request_language:request_language,
        anonymous:anonymous
    };

    return dummyJsonObj;
}

module.exports.generateDummyByRwc = generateDummyByRwc;