package wikipedia.parser;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.xml.sax.SAXException;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.actor.ActorRef;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.pf.ReceiveBuilder;
import wikipedia.model.WikipediaPage;



/**
 * using: https://stackoverflow.com/questions/26310595/how-to-parse-big-50-gb-xml-files-in-java
 * @author zschache
 *
 */
public class XMLManager extends AbstractActor {
	
	static public Props props(ActorRef neo4jActor, ActorRef mongoActor) {
		return Props.create(XMLManager.class, () -> new XMLManager(neo4jActor, mongoActor));
	}
	
	static public class Load {
		public final String fileName;

	    public Load(String fileName) {
	        this.fileName = fileName;
	    }
	}
	static public class AddPage {
	    public final WikipediaPage page;

	    public AddPage(WikipediaPage page) {
	        this.page = page;
	    }
	}
	
//	private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
	private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);
	private final SAXParserFactory factory = SAXParserFactory.newInstance();
	
	public XMLManager(ActorRef neo4jActor, ActorRef mongoActor) {
		receive(ReceiveBuilder
				.match(Load.class, load -> {
					log.info("Start loading file from: {}", load.fileName);
					try {
			        	SAXParser parser = factory.newSAXParser();
						InputStream stream = new URL(load.fileName).openStream();
						BZip2CompressorInputStream bzip2 = new BZip2CompressorInputStream(stream);
						PageHandler pageHandler = new PageHandler(new PageProcessor() {
				            @Override
				            public void process(WikipediaPage page) {
				            	log.info("Done parsing page: {}", page);
				            	neo4jActor.tell(new AddPage(page), self());
				            	mongoActor.tell(new AddPage(page), self());
				           }
				        });
						parser.parse(bzip2, pageHandler);
					} catch (SAXException | IOException | ParserConfigurationException e) {
						e.printStackTrace();
					}
				})
				.matchAny(o -> log.info("received unknown message"))
				.build());

	}
	
	
//    @Override
//	public Receive createReceive() {
//		return receiveBuilder()
//			.match(Load.class, load -> {
//				log.info("Start loading file from: {}", load.fileName);
//				try {
//		        	SAXParser parser = factory.newSAXParser();
//					InputStream stream = new URL(load.fileName).openStream();
//					BZip2CompressorInputStream bzip2 = new BZip2CompressorInputStream(stream);
//					PageHandler pageHandler = new PageHandler(new PageProcessor() {
//			            @Override
//			            public void process(WikipediaPage page) {
//			            	log.info("Done parsing page: {}", page);
//			            	neo4jActor.tell(new AddPage(page), getSelf());
//			            	mongoActor.tell(new AddPage(page), getSelf());
//			           }
//			        });
//					parser.parse(bzip2, pageHandler);
//				} catch (SAXException | IOException | ParserConfigurationException e) {
//					e.printStackTrace();
//				}
//			})
//			.matchAny(o -> log.info("received unknown message"))
//			.build();
//	  }
        
}