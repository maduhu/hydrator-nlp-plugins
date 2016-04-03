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
import edu.stanford.nlp.ling.Word;
import edu.stanford.nlp.process.PTBTokenizer;
import edu.stanford.nlp.process.TokenizerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

/**
 * Tokenizes word using Stanford NLP PTBTokenizer.
 */
@Plugin(type = "transform")
@Name("tokenizer")
@Description("Tokenizes words in a text using Stanford NLP PTBTokenizer.")
public class WordTokenizer extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(WordTokenizer.class);
  private final Config config;

  // Output Schema associated with transform output.
  private Schema outSchema;

  // List of fields specified in the schema.
  private List<Schema.Field> fields;

  //
  TokenizerFactory<Word> factory;

  // This is used only for tests, otherwise this is being injected by the ingestion framework.
  public WordTokenizer(Config config) {
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
      if (outputSchema.getFields().size() != 1) {
        throw new IllegalArgumentException("Output schema should have only field of type string");
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
    factory = PTBTokenizer.factory();
  }
  
  @Override
  public void transform(StructuredRecord record, Emitter<StructuredRecord> emitter) throws Exception {
    List<Word> words = factory.getTokenizer(new StringReader(record.<String>get(config.field))).tokenize();
    StructuredRecord.Builder builder = StructuredRecord.builder(outSchema);
    for(Word word : words) {
      builder.set(config.field, word);
    }
    emitter.emit(builder.build());
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
