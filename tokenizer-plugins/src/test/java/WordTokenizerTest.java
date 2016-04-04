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
import co.cask.hydrator.common.test.MockEmitter;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.Sentence;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Properties;


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

  @Test
  public void testABC() throws Exception {
    Properties props = new Properties();
    props.put("annotators", "tokenize, ssplit, pos, lemma, ner");

    // StanfordCoreNLP loads a lot of models, so you probably
    // only want to do this once per execution
    StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
    Annotation document = new Annotation("An autopsy on a Palestinian assailant showed Sunday that he was killed by a" +
                                           " bullet to the head, backing a manslaughter case against an Israeli " +
                                           "soldier caught on video shooting him, a doctor said.\n" +
                                           "\n" +
                                           "The soldier shot Abdul Fatah al-Sharif in the head on March 24 as he lay " +
                                           "on the ground while apparently seriously wounded from earlier gunshot " +
                                           "wounds.\n" +
                                           "\n" +
                                           "Video of the incident in Hebron in the occupied West Bank spread widely " +
                                           "online and the soldier was arrested, with rights groups labelling it a " +
                                           "summary execution.");
    pipeline.annotate(document);
    List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);
    for(CoreMap sentence: sentences) {
      for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
        String ner = token.get(CoreAnnotations.NamedEntityTagAnnotation.class);
        //if (!ner.equalsIgnoreCase("o")) {
          String line = String.format("%s -> %s", token.word(), ner);
          System.out.println(line);
        //}
      }
    }
  }


  }
