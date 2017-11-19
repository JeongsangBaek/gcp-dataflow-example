/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.my.google.dataflow.bigquery;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.my.google.dataflow.bigquery.CSVToRowConverter;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A starter example for writing Google Cloud Dataflow programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectPipelineRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<YOUR_PROJECT_ID>
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=BlockingDataflowPipelineRunner
 */
public class StarterPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);

  public static void main(String[] args) {
		PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
	    Pipeline p = Pipeline.create(options);

	    LOG.debug("Start data processing");
	    //Read data from GCS
	    PCollection<String> lines = p.apply(TextIO.Read.from("gs://gdfp-data-store/NetworkRequests_1_20171118_05.csv"));
	    
	    //Make TableRow
	    PCollection<TableRow> rows = lines.apply(ParDo.of(new CSVToRowConverter()));
	    
	    List<TableFieldSchema> fields = new ArrayList<>();
	    fields.add(new TableFieldSchema().setName("time").setType("STRING"));
	    fields.add(new TableFieldSchema().setName("time_u_sec_2").setType("INTEGER"));
	    fields.add(new TableFieldSchema().setName("key_part").setType("STRING"));
	    fields.add(new TableFieldSchema().setName("user_id").setType("INTEGER"));
	    fields.add(new TableFieldSchema().setName("ad_unit_id").setType("INTEGER"));
	    fields.add(new TableFieldSchema().setName("custom_targeting").setType("STRING"));
	    fields.add(new TableFieldSchema().setName("country").setType("STRING"));
	    fields.add(new TableFieldSchema().setName("region").setType("STRING"));
	    fields.add(new TableFieldSchema().setName("browser").setType("STRING"));
	    fields.add(new TableFieldSchema().setName("os").setType("STRING"));
	    fields.add(new TableFieldSchema().setName("domain").setType("STRING"));
	    fields.add(new TableFieldSchema().setName("metro").setType("STRING"));
	    fields.add(new TableFieldSchema().setName("city").setType("STRING"));
	    fields.add(new TableFieldSchema().setName("postal_code").setType("INTEGER"));
	    fields.add(new TableFieldSchema().setName("bandwidth").setType("STRING"));
	    fields.add(new TableFieldSchema().setName("gfp_content_id").setType("INTEGER"));
	    fields.add(new TableFieldSchema().setName("browser_id").setType("INTEGER"));
	    fields.add(new TableFieldSchema().setName("os_id").setType("INTEGER"));
	    fields.add(new TableFieldSchema().setName("country_id").setType("INTEGER"));
	    fields.add(new TableFieldSchema().setName("region_id").setType("INTEGER"));
	    fields.add(new TableFieldSchema().setName("city_id").setType("INTEGER"));
	    fields.add(new TableFieldSchema().setName("metro_id").setType("INTEGER"));
	    fields.add(new TableFieldSchema().setName("postal_code_id").setType("INTEGER"));
	    fields.add(new TableFieldSchema().setName("bandwidth_id").setType("INTEGER"));
	    fields.add(new TableFieldSchema().setName("audience_segment_ids").setType("STRING"));
	    fields.add(new TableFieldSchema().setName("requestd_ad_unit_sizes").setType("STRING"));
	    fields.add(new TableFieldSchema().setName("mobile_device").setType("STRING"));
	    fields.add(new TableFieldSchema().setName("os_version").setType("STRING"));
	    fields.add(new TableFieldSchema().setName("mobile_capability").setType("STRING"));
	    fields.add(new TableFieldSchema().setName("mobile_career").setType("STRING"));
	    fields.add(new TableFieldSchema().setName("bandwidth_group_id").setType("INTEGER"));
	    fields.add(new TableFieldSchema().setName("publisher_provided_id").setType("STRING"));
	    fields.add(new TableFieldSchema().setName("video_position").setType("INTEGER"));
	    fields.add(new TableFieldSchema().setName("pod_position").setType("INTEGER"));
	    fields.add(new TableFieldSchema().setName("device_category").setType("STRING"));
	    fields.add(new TableFieldSchema().setName("is_interstitial").setType("BOOLEAN"));
	    fields.add(new TableFieldSchema().setName("referer_url").setType("STRING"));
	    fields.add(new TableFieldSchema().setName("mobile_app_id").setType("STRING"));
	    fields.add(new TableFieldSchema().setName("request_language").setType("STRING"));
	    fields.add(new TableFieldSchema().setName("anonymous").setType("BOOLEAN"));
	    fields.add(new TableFieldSchema().setName("arrival_time").setType("TIMESTAMP"));	//added by converter

	    
	    TableSchema schema = new TableSchema().setFields(fields);
	    rows.apply(BigQueryIO.Write
	    		.named("Write")
	    		.to("dfp-data-analysis-186405:dfp_data.network_request_1")
	    		.withSchema(schema)
	    		.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
	    		.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
	    		);

	    p.run();
  }
}
