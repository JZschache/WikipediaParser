package wikipedia;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.agent.Agent;
import akka.dispatch.ExecutionContexts;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import scala.concurrent.ExecutionContext;
import wikipedia.mongo.MongoActor;
import wikipedia.neo4j.Neo4jActor;
import wikipedia.parser.PageManager;
import wikipedia.parser.XMLActor;
import wikipedia.parser.XMLActor.LoadURL;

public class Main  {
	
	public final static int outputFreq = 1000;
	
    public static void main(String[] args) {
    	
//    	int lastPageId = 15904925;
    	int lastPageId = 0;
    	
    	final ActorSystem system = ActorSystem.create("wikipediaparser");
    	
    	final LoggingAdapter log = Logging.getLogger(system.eventStream(), "Main");

    	final ActorRef mongoActor = system.actorOf(MongoActor.props(), "MongoActor");
    	final ActorRef neo4jActor = system.actorOf(Neo4jActor.props("/local/hd/wikipedia/neo4j"), "Neo4jActor");
        final ActorRef xmlActor = system.actorOf(XMLActor.props(neo4jActor, mongoActor, lastPageId), "XMLActor");
        final ActorRef downloadActor = system.actorOf(DownloadActor.props(xmlActor), "DownloadActor");
        
        String indexUrl = "https://dumps.wikimedia.org";
        String indexPath = "/enwiki/20180901/index.html";
        
        log.info("Reading index from: {}", indexUrl + indexPath);
		try {
			InputStream stream = new URL(indexUrl + indexPath).openStream();
			BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
			String line;
//			boolean nothingFound = true;
	        while ((line = reader.readLine()) != null) {
	        	if (line.contains("pages-meta-history")) {
	        		String[] splits = line.split("\"");
	        		for (String s : splits) {
	        			if (s.contains("pages-meta-history18") && s.endsWith(".bz2")) {
	        				downloadActor.tell(new LoadURL(indexUrl + s), ActorRef.noSender());
//	        				nothingFound = false;
	        			}
	        		}
	        		
	        	}
	        }
	        downloadActor.tell(akka.actor.PoisonPill.getInstance(), ActorRef.noSender());
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
}

