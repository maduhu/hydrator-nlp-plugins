/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch.file;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.TimePartitionDetail;
import co.cask.cdap.api.dataset.lib.TimePartitionOutput;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.etl.batch.ETLWorkflow;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Plugin;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.WorkflowManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Test;
import parquet.avro.AvroParquetWriter;
import parquet.hadoop.ParquetWriter;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class ETLTPFSTestRun extends ETLBatchTestBase {

  @Test
  public void testPartitionOffsetAndCleanup() throws Exception {
    Schema recordSchema = Schema.recordOf("record",
                                          Schema.Field.of("i", Schema.of(Schema.Type.INT)),
                                          Schema.Field.of("l", Schema.of(Schema.Type.LONG))
    );

    Plugin sourceConfig = new Plugin("TPFSParquet",
                                     ImmutableMap.of(
                                       "schema", recordSchema.toString(),
                                       "name", "cleanupInput",
                                       "delay", "0d",
                                       "duration", "1h"));
    Plugin sinkConfig = new Plugin("TPFSParquet",
                                   ImmutableMap.of(
                                     "schema", recordSchema.toString(),
                                     "name", "cleanupOutput",
                                     "partitionOffset", "1h",
                                     "cleanPartitionsOlderThan", "30d"));

    ETLStage source = new ETLStage("source", sourceConfig);
    ETLStage sink = new ETLStage("sink", sinkConfig);

    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, ImmutableList.<ETLStage>of());

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "parquetTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // write input to read
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(recordSchema.toString());
    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("i", Integer.MAX_VALUE)
      .set("l", Long.MAX_VALUE)
      .build();

    DataSetManager<TimePartitionedFileSet> inputManager = getDataset("cleanupInput");
    DataSetManager<TimePartitionedFileSet> outputManager = getDataset("cleanupOutput");

    long runtime = 1451606400000L;
    // add a partition from a 30 days and a minute ago to the output. This should get cleaned up
    long oldOutputTime = runtime - TimeUnit.DAYS.toMillis(30) - TimeUnit.MINUTES.toMillis(1);
    outputManager.get().getPartitionOutput(oldOutputTime).addPartition();
    // also add a partition from a 30 days to the output. This should not get cleaned up.
    long borderlineOutputTime = runtime - TimeUnit.DAYS.toMillis(30);
    outputManager.get().getPartitionOutput(borderlineOutputTime).addPartition();
    outputManager.flush();

    // add data to a partition from 30 minutes before the runtime
    long inputTime = runtime - TimeUnit.MINUTES.toMillis(30);
    TimePartitionOutput timePartitionOutput = inputManager.get().getPartitionOutput(inputTime);
    Location location = timePartitionOutput.getLocation();
    AvroParquetWriter<GenericRecord> parquetWriter = new AvroParquetWriter<>(new Path(location.toURI()), avroSchema);
    parquetWriter.write(record);
    parquetWriter.close();
    timePartitionOutput.addPartition();
    inputManager.flush();

    // run the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(ETLWorkflow.NAME);
    workflowManager.start(ImmutableMap.of("logical.start.time", String.valueOf(runtime)));
    workflowManager.waitForFinish(4, TimeUnit.MINUTES);

    outputManager.flush();
    // check old partition was cleaned up
    Assert.assertNull(outputManager.get().getPartitionByTime(oldOutputTime));
    // check borderline partition was not cleaned up
    Assert.assertNotNull(outputManager.get().getPartitionByTime(borderlineOutputTime));

    // check data is written to output from 1 hour before
    long outputTime = runtime - TimeUnit.HOURS.toMillis(1);
    TimePartitionDetail outputPartition = outputManager.get().getPartitionByTime(outputTime);
    Assert.assertNotNull(outputPartition);

    List<GenericRecord> outputRecords = readOutput(outputPartition.getLocation(), recordSchema);
    Assert.assertEquals(1, outputRecords.size());
    Assert.assertEquals(Integer.MAX_VALUE, outputRecords.get(0).get("i"));
    Assert.assertEquals(Long.MAX_VALUE, outputRecords.get(0).get("l"));
  }

  @Test
  public void testParquet() throws Exception {
    Schema recordSchema = Schema.recordOf("record",
                                          Schema.Field.of("i", Schema.of(Schema.Type.INT)),
                                          Schema.Field.of("l", Schema.of(Schema.Type.LONG))
    );

    Plugin sourceConfig = new Plugin("TPFSParquet",
                                     ImmutableMap.of(
                                       "schema", recordSchema.toString(),
                                       "name", "inputParquet",
                                       "delay", "0d",
                                       "duration", "1h"));
    Plugin sinkConfig = new Plugin("TPFSParquet",
                                   ImmutableMap.of(
                                     "schema", recordSchema.toString(),
                                     "name", "outputParquet"));

    ETLStage source = new ETLStage("source", sourceConfig);
    ETLStage sink = new ETLStage("sink", sinkConfig);

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .setSource(source)
      .addSink(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "parquetTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // write input to read
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(recordSchema.toString());
    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("i", Integer.MAX_VALUE)
      .set("l", Long.MAX_VALUE)
      .build();
    DataSetManager<TimePartitionedFileSet> inputManager = getDataset("inputParquet");

    long timeInMillis = System.currentTimeMillis();
    inputManager.get().addPartition(timeInMillis, "directory");
    inputManager.flush();
    Location location = inputManager.get().getPartitionByTime(timeInMillis).getLocation();
    location = location.append("file.parquet");
    ParquetWriter<GenericRecord> parquetWriter =
      new AvroParquetWriter<>(new Path(location.toURI()), avroSchema);
    parquetWriter.write(record);
    parquetWriter.close();
    inputManager.flush();

    // run the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(ETLWorkflow.NAME);
    // add a minute to the end time to make sure the newly added partition is included in the run.
    workflowManager.start(ImmutableMap.of("runtime", String.valueOf(timeInMillis + 60 * 1000)));
    workflowManager.waitForFinish(4, TimeUnit.MINUTES);

    DataSetManager<TimePartitionedFileSet> outputManager = getDataset("outputParquet");
    TimePartitionedFileSet newFileSet = outputManager.get();

    List<GenericRecord> newRecords = readOutput(newFileSet, recordSchema);
    Assert.assertEquals(1, newRecords.size());
    Assert.assertEquals(Integer.MAX_VALUE, newRecords.get(0).get("i"));
    Assert.assertEquals(Long.MAX_VALUE, newRecords.get(0).get("l"));
  }

}
