package wikipedia.parser;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.Queue;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.xml.sax.SAXException;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.agent.Agent;
import akka.dispatch.ExecutionContexts;
import akka.dispatch.Mapper;
import akka.actor.ActorRef;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.pf.ReceiveBuilder;
import scala.concurrent.ExecutionContext;
import wikipedia.Main;
import wikipedia.model.WikipediaPage;
import wikipedia.model.WikipediaRevision;
import wikipedia.neo4j.Neo4jManager;



/**
 * using: https://stackoverflow.com/questions/26310595/how-to-parse-big-50-gb-xml-files-in-java
 * @author zschache
 *
 */
public class XMLManager extends AbstractActor {
	
	static public Props props(ActorRef neo4jActor, ActorRef mongoActor) {
		return Props.create(XMLManager.class, () -> new XMLManager(neo4jActor, mongoActor));
	}
	
	static public class LoadURL {
		public final String urlString;

	    public LoadURL(String urlString) {
	        this.urlString = urlString;
	    }
	}
	
	static public class LoadFile {
		public final String fileName;

	    public LoadFile(String fileName) {
	        this.fileName = fileName;
	    }
	}
		
	
	static public class AddRevision {
	    public final WikipediaRevision revision;

	    public AddRevision(WikipediaRevision revision) {
	        this.revision = revision;
	    }
	}

	static public class AddPage {
	    public final WikipediaPage page;

	    public AddPage(WikipediaPage page) {
	        this.page = page;
	    }
	}
	
	static public class AddRevisions {
		public final WikipediaPage page;
	    public final Collection<WikipediaRevision> revisions;

	    public AddRevisions(WikipediaPage page, Collection<WikipediaRevision> revisions) {
	        this.page = page;
	        this.revisions = revisions;
	    }
	}
	
	static public class NewFile {
	    public final String filename;

	    public NewFile(String filename) {
	        this.filename = filename;
	    }
	}
	static public class EndDocument {
	    
	}
	
		
	
//	private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
	private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);
	private final SAXParserFactory factory = SAXParserFactory.newInstance();
	
	ExecutionContext ec = ExecutionContexts.global();
    Agent<Integer> xmlCounter = Agent.create(0, ec);
	
//    private Queue<String> urlStrings = new ArrayDeque<String>();
    
	public XMLManager(ActorRef neo4jActor, ActorRef mongoActor) {
		receive(ReceiveBuilder
				.match(LoadFile.class, load -> {
					String urlString = load.fileName;
					if (urlString != null) {
						mongoActor.tell(new NewFile(urlString), self());
						String[] splits = urlString.split("p");
						String lastPageString = splits[splits.length-1];
						int lastPage = Integer.parseInt(lastPageString.substring(0, lastPageString.length() - 4));
						int firstPage = Integer.parseInt(splits[splits.length-2]);
						log.info("Start loading pages {} until {} from: {}", firstPage, lastPage, urlString);
						InputStream stream;
						try {
				        	SAXParser parser = factory.newSAXParser();
//							stream = new URL(urlString).openStream();
							stream = new FileInputStream(new File(urlString));
							BZip2CompressorInputStream bzip2 = new BZip2CompressorInputStream(stream);
							PageHandler pageHandler = new PageHandler(new PageProcessor() {
								PageActor pageActor;
								@Override
					            public void startPage(WikipediaPage page) {
									pageActor = new PageActor(neo4jActor, mongoActor);
								}
					            @Override
					            public void process(WikipediaRevision revision) {
					            	pageActor.addRevision(revision);
					            }
					            @Override
					            public void endPage(WikipediaPage page) {
					            	pageActor.addPage(page);
					            	xmlCounter.send(new Mapper<Integer, Integer>() {
					            		public Integer apply(Integer i) {
					            			if ((i+1) % Main.outputFreq == 0)
							            		log.info("Done parsing {} pages. Current page id: {}.", (i+1), page.getId());
					            			return i + 1;
					            		}				            		
					            	});
					            }
					            @Override
					            public void endDocument () {
					            	//TODO shutdown
					            }
					        });
							parser.parse(bzip2, pageHandler);
						} catch (SAXException | IOException | ParserConfigurationException e) {
							e.printStackTrace();
						} finally {
							
						}
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