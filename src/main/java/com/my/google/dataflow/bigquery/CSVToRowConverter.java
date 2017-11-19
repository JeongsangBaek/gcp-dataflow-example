package com.my.google.dataflow.bigquery;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.DoFnWithContext.ProcessElement;

public class CSVToRowConverter extends DoFn<String, TableRow> {
	private static final long serialVersionUID = 1;
	
	@ProcessElement
	public void processElement(ProcessContext c) {
		String[] split = c.element().split(",");
		//skip the header
		if (split[0].equals("Time")) return;
		
		ZonedDateTime utc = ZonedDateTime.now(ZoneOffset.UTC);
	
		TableRow output = new TableRow();
		output.set("time", split[0]);
		output.set("time_u_sec_2", split[1]);
		output.set("key_part", split[2]);
		output.set("user_id", split[3]);
		output.set("ad_unit_id", split[4]);
		output.set("custom_targeting", split[5]);
		output.set("country", split[6]);
		output.set("region", split[7]);
		output.set("browser", split[8]);
		output.set("os", split[9]);
		output.set("domain", split[10]);
		output.set("metro", split[11]);
		output.set("city", split[12]);
		output.set("postal_code", split[13]);
		output.set("bandwidth", split[14]);
		output.set("gfp_content_id", split[15]);
		output.set("browser_id", split[16]);
		output.set("os_id", split[17]);
		output.set("country_id", split[18]);
		output.set("region_id", split[19]);
		output.set("city_id", split[20]);
		output.set("metro_id", split[21]);
		output.set("postal_code_id", split[22]);
		output.set("bandwidth_id", split[23]);
		output.set("audience_segment_ids", split[24]);
		output.set("requestd_ad_unit_sizes", split[25]);
		output.set("mobile_device", split[26]);
		output.set("os_version", split[27]);
		output.set("mobile_capability", split[28]);
		output.set("mobile_career", split[29]);
		output.set("bandwidth_group_id", split[30]);
		output.set("publisher_provided_id", split[31]);
		output.set("video_position", split[32]);
		output.set("pod_position", split[33]);
		output.set("device_category", split[34]);
		output.set("is_interstitial", split[35]);
		output.set("referer_url", split[36]);
		output.set("mobile_app_id", split[37]);
		output.set("request_language", split[38]);
		output.set("anonymous", split[39]);
		//add arrival_time to compare how many datas are late arrival
		output.set("arrival_time", utc.format(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss")));

		c.output(output);
	}

}
