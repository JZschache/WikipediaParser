package wikipedia.parser;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.xml.sax.SAXException;

/**
 * https://stackoverflow.com/questions/26310595/how-to-parse-big-50-gb-xml-files-in-java
 * @author zschache
 *
 */
public class XMLManager {

	static private final String FILE_NAME = "https://dumps.wikimedia.org/enwiki/20180901/enwiki-20180901-pages-meta-history5.xml-p564715p565313.bz2";
	
    public static void load(PageProcessor processor) {
    	            
        try {
			SAXParserFactory factory = SAXParserFactory.newInstance();
			SAXParser parser = factory.newSAXParser();
			InputStream stream = new URL(FILE_NAME).openStream();
			BZip2CompressorInputStream bzip2 = new BZip2CompressorInputStream(stream);
			PageHandler pageHandler = new PageHandler(processor);
          
			parser.parse(bzip2, pageHandler);
		} catch (SAXException | IOException | ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
            
}