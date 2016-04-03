import edu.stanford.nlp.ling.Word;
import edu.stanford.nlp.process.PTBTokenizer;
import edu.stanford.nlp.process.TokenizerFactory;
import org.junit.Test;

import java.io.StringReader;
import java.util.List;

/**
 * Created by nmotgi on 4/3/16.
 */
public class TestXYZ {
  
  @Test
  public void testFoo() throws Exception {
    String tweet = "Partial invoice (€100,000, so roughly 40%) for the consignment C27655 we " +
                                           "shipped on 15th August to London from the Make Believe Town depot. " +
                                           "INV2345 is for the balance.. Customer contact (Sigourney) says they " +
                                           "will pay this on the usual credit terms (30 days).";
    TokenizerFactory<Word> factory = PTBTokenizer.factory();
    List<Word> words = factory.getTokenizer(new StringReader(tweet)).tokenize();
    for(Word word : words) {
      System.out.println(word.value() + " : " + word.word());
    }
  }
}
