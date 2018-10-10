package wikipedia.parser;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Optional;
import java.util.Queue;

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
import wikipedia.Main;
import wikipedia.model.WikipediaPage;
import wikipedia.model.WikipediaRevision;


/**
 * using: https://stackoverflow.com/questions/26310595/how-to-parse-big-50-gb-xml-files-in-java
 * @author zschache
 *
 */
public class XMLActor extends AbstractActor {
	
	static public Props props(ActorRef neo4jActor, ActorRef mongoActor, int lastPageId) {
		return Props.create(XMLActor.class, () -> new XMLActor(neo4jActor, mongoActor, lastPageId));
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
	
	static public class ParseNextFile {
		
	    public ParseNextFile() {
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
	
	private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);
	private final SAXParserFactory factory = SAXParserFactory.newInstance();
	
	int xmlCounter = 0;
	int lastPageId;
	
	boolean waitingForFirstFile = true;
    private Queue<String> fileStrings = new ArrayDeque<String>();
    
    ActorRef mongoActor;
    ActorRef neo4jActor;
    
	public XMLActor(ActorRef neo4jActor, ActorRef mongoActor, int lastPageId) {
		this.mongoActor = mongoActor;
		this.neo4jActor = neo4jActor;
		this.lastPageId = lastPageId;
	}
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LoadFile.class, load -> {
					fileStrings.add(load.fileName);
					if (waitingForFirstFile) {
						waitingForFirstFile = false;
						self().tell(new ParseNextFile(), self());
					}
						
				})
				.match(ParseNextFile.class, load -> {
					String fileString = fileStrings.poll();
					if (fileString != null) {
						String[] splits = fileString.split("p");
						String lastPageString = splits[splits.length-1];
						int lastPage = Integer.parseInt(lastPageString.substring(0, lastPageString.length() - 4));
						int firstPage = Integer.parseInt(splits[splits.length-2]);
						if (lastPage > lastPageId) {
							log.info("Start loading pages {} until {} from: {}", firstPage, lastPage, fileString);
							mongoActor.tell(new NewFile(fileString), self());
							InputStream stream;
							try {
					        	SAXParser parser = factory.newSAXParser();
	//							stream = new URL(urlString).openStream();
								stream = new FileInputStream(new File(fileString));
								BZip2CompressorInputStream bzip2 = new BZip2CompressorInputStream(stream);
								PageHandler pageHandler = new PageHandler(new PageProcessor() {
									PageManager pageManger;
									@Override
						            public void startPage(WikipediaPage page) {
										pageManger = new PageManager(neo4jActor, mongoActor, getContext());
									}
						            @Override
						            public void process(WikipediaRevision revision) {
						            	pageManger.addRevision(revision);
						            }
						            @Override
						            public void endPage(WikipediaPage page) {
						            	if (Integer.parseInt(page.getId()) > lastPageId)
						            		pageManger.addPage(page);
						            	xmlCounter++;
										if (xmlCounter % Main.outputFreq == 0) {
											log.info("Done parsing {} pages. Current page id: {}.", xmlCounter, page.getId());
						            	}
						            }
						            @Override
						            public void endDocument () {
						            	try {
											bzip2.close();
										} catch (IOException e) {
											// TODO Auto-generated catch block
											e.printStackTrace();
										}
						            	self().tell(new ParseNextFile(), self());
						            }
						        });
								parser.parse(bzip2, pageHandler);
							} catch (SAXException | IOException | ParserConfigurationException e) {
								e.printStackTrace();
							} finally {
								
							}
						} else { // lastPage <= lastPageId
							self().tell(new ParseNextFile(), self());
						}
					} else { // queue of fileStrings is empty
						mongoActor.tell(akka.actor.PoisonPill.getInstance(), ActorRef.noSender());
						neo4jActor.tell(akka.actor.PoisonPill.getInstance(), ActorRef.noSender());
						getContext().stop(getSelf());
					}
				})
				.matchAny(o -> log.info("received unknown message"))
				.build();
	}
	
	@Override
	public void preStart() {
		log.debug("Starting");
	}
	@Override
	public void preRestart(Throwable reason, Optional<Object> message) {
		log.error(reason, "Restarting due to [{}] when processing [{}]",
				reason.getMessage(), message.isPresent() ? message.get() : "");
	}
	
	public void postStop() {
		log.debug("Stopping");
	}
        
}