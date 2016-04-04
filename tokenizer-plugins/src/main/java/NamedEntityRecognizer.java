import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.Word;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.process.PTBTokenizer;
import edu.stanford.nlp.process.TokenizerFactory;
import edu.stanford.nlp.util.CoreMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Properties;

/**
 * Tokenizes word using Stanford NLP PTBTokenizer.
 */
@Plugin(type = "transform")
@Name("NER")
@Description("Named Entity Recognizer labels sequences of words in a text which are the names of things, " +
  "such as person and company names, or gene and protein names")
public class NamedEntityRecognizer extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(NamedEntityRecognizer.class);
  private final Config config;

  // Output Schema associated with transform output.
  private Schema outSchema;

  // List of fields specified in the schema.
  private List<Schema.Field> fields;

  //
  private StanfordCoreNLP pipeline;

  // This is used only for tests, otherwise this is being injected by the ingestion framework.
  public NamedEntityRecognizer(Config config) {
    this.config = config;
  }
  
  @Override
  public void configurePipeline(PipelineConfigurer configurer) throws IllegalArgumentException {
    super.configurePipeline(configurer);
    
    // Check if config field specified to be tokenized is in input schema.
    if (configurer.getStageConfigurer().getInputSchema() != null) {
      Schema.Field inputSchemaField = configurer.getStageConfigurer().getInputSchema().getField(config.field);
      if (inputSchemaField == null) {
        throw new IllegalArgumentException(
          "Field " + config.field + " is not present in the input schema");
      } else {
        if (!inputSchemaField.getSchema().getType().equals(Schema.Type.STRING)) {
          throw new IllegalArgumentException(
            "Type for field  " + config.field + " must be String");
        }
      }
    }

    // Check if schema specified is a valid schema or no.
    try {
      Schema outputSchema = Schema.parseJson(this.config.schema);
      if (outputSchema.getFields().size() != 2) {
        throw new IllegalArgumentException("Output schema should have two fields of type string");
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Format of schema specified is invalid. Please check the format.");
    }
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    try {
      outSchema = Schema.parseJson(config.schema);
      fields = outSchema.getFields();
    } catch (IOException e) {
      throw new IllegalArgumentException("Format of schema specified is invalid. Please check the format.");
    }
    
    // Initialize StanfordCoreNLP.
    Properties props = new Properties();
    props.put("annotators", "tokenize, ssplit, pos, lemma, ner");
    pipeline = new StanfordCoreNLP(props);
  }
  
  @Override
  public void transform(StructuredRecord record, Emitter<StructuredRecord> emitter) throws Exception {
    Annotation document = new Annotation(record.<String>get(config.field));
    pipeline.annotate(document);
    List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);
    for(CoreMap sentence: sentences) {
      for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
        String ner = token.get(CoreAnnotations.NamedEntityTagAnnotation.class);
        if (!ner.equalsIgnoreCase("o")) {
          StructuredRecord.Builder builder = StructuredRecord.builder(outSchema);
          builder.set(fields.get(0).getName(), token.get(CoreAnnotations.TextAnnotation.class));
          builder.set(fields.get(1).getName(), ner);
          emitter.emit(builder.build());
        }
      }
    }
  }

  /**
   * Configuration for the plugin.
   */
  public static class Config extends PluginConfig {
    @Name("field")
    @Description("Specify the field that should be used tokenized")
    private final String field;

    @Name("schema")
    @Description("Specifies the schema that has to be output.")
    private final String schema;

    public Config(String field, String schema) {
      this.field = field;
      this.schema = schema;
    }
  }

}
