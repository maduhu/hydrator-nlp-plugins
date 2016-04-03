/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.Transform;
import co.cask.hydrator.common.MockPipelineConfigurer;
import co.cask.hydrator.common.test.MockEmitter;
import org.junit.Assert;
import org.junit.Test;


/**
 * Tests the {@link WordTokenizer}.
 */
public class WordTokenizerTest {

  private static final Schema INPUT1 = Schema.recordOf("input1",
                                                       Schema.Field.of("body", Schema.of(Schema.Type.STRING)));

  private static final Schema OUTPUT1 = Schema.recordOf("output1",
                                                        Schema.Field.of("word", Schema.of(Schema.Type.STRING)));

  @Test
  public void testWordTokenizer() throws Exception {
    WordTokenizer.Config config = new WordTokenizer.Config("body", OUTPUT1.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new WordTokenizer(config);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();

    // Test missing field.
    emitter.clear();
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("body", "This is a simple test").build(), emitter);
    Assert.assertEquals("This", emitter.getEmitted().get(0).get("word"));
    Assert.assertEquals("is", emitter.getEmitted().get(1).get("word"));
    Assert.assertEquals("a", emitter.getEmitted().get(2).get("word"));
    Assert.assertEquals("simple", emitter.getEmitted().get(3).get("word"));
    Assert.assertEquals("test", emitter.getEmitted().get(4).get("word"));

    // Test adding quote to field value.
    emitter.clear();
  }


}