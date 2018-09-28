package wikipedia.parser;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Node;
import org.xml.sax.SAXException;

import akka.actor.AbstractActor;
import akka.actor.AbstractActorContext;
import akka.actor.ActorContext;
import akka.actor.Props;
import akka.agent.Agent;
import akka.dispatch.ExecutionContexts;
import akka.dispatch.Mapper;
import akka.actor.ActorRef;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.pf.ReceiveBuilder;
import name.fraser.neil.plaintext.StandardBreakScorer;
import name.fraser.neil.plaintext.diff_match_patch;
import name.fraser.neil.plaintext.diff_match_patch.Operation;
import scala.concurrent.ExecutionContext;
import wikipedia.Main;
import wikipedia.model.WikipediaPage;
import wikipedia.model.WikipediaRevision;
import wikipedia.neo4j.Neo4jManager;
import wikipedia.parser.XMLManager.AddPage;
import wikipedia.parser.XMLManager.AddRevision;
import wikipedia.parser.XMLManager.AddRevisions;



/**
 * 
 * @author zschache
 *
 */
public class PageActor {
	
	private Map<String, WikipediaRevision> revisions = new LinkedHashMap<String, WikipediaRevision>();
	private List<WikipediaRevision> lonelyChildren = new ArrayList<WikipediaRevision>();
	
	private ActorRef neo4jActor;
	private ActorRef mongoActor;
		
	public PageActor(ActorRef neo4jActor, ActorRef mongoActor) {
		this.neo4jActor = neo4jActor;
		this.mongoActor = mongoActor;
	}
	public void addRevision(WikipediaRevision revision) {
		revisions.put(revision.getId(), revision);
		setDiff(revision);
	}
	
	public void addPage(WikipediaPage page) {
		// another run to find the parent of lonely children (revisions that were parsed before their parent)
		for (WikipediaRevision revision : lonelyChildren) {
			setDiff(revision);
		}
		// removing text to reduce memory/storage
		for (WikipediaRevision revision : revisions.values()) {
			if (revision.getParentId() != null)
				revision.setText(null);
		}
		lonelyChildren = new ArrayList<WikipediaRevision>();
		if (!revisions.isEmpty()) {
			neo4jActor.tell(new AddRevisions(page, revisions.values()), ActorRef.noSender());			
			mongoActor.tell(new AddRevisions(page, revisions.values()), ActorRef.noSender());
		}
	}
		
	private void setDiff(WikipediaRevision revision) {
		String parentId = revision.getParentId();
		if (parentId != null) {
			WikipediaRevision parent = revisions.get(parentId);
			if (parent != null) {
				diff_match_patch dmp = new diff_match_patch(new StandardBreakScorer() );
			    LinkedList<diff_match_patch.Diff> diff = dmp.diff_main(parent.getText(), revision.getText());
			    dmp.diff_cleanupSemantic(diff);
			    for (diff_match_patch.Diff d : diff) {
			    	if (d.operation.equals(Operation.INSERT))
			    		revision.setTextAdded(d.text);
			    	if (d.operation.equals(Operation.DELETE))
			    		revision.setTextRemoved(d.text);
			    }
			} else {
				lonelyChildren.add(revision);
			}
		}
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