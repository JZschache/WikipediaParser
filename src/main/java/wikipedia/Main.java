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
import wikipedia.mongo.MongoManager;
import wikipedia.neo4j.Neo4jManager;
import wikipedia.parser.PageActor;
import wikipedia.parser.XMLManager;
import wikipedia.parser.XMLManager.LoadURL;

public class Main  {
	
	public final static int outputFreq = 1000;
	
    public static void main(String[] args) {
    	
    	//System.out.println(Runtime.getRuntime().maxMemory());
    	//System.out.println(System.getProperty("https.protocols"));
    	System.setProperty("https.protocols", "TLSv1,TLSv1.1,TLSv1.2");
    	
    	final ActorSystem system = ActorSystem.create("wikipediaparser");
    	
    	final LoggingAdapter log = Logging.getLogger(system, "Main");

    	final ActorRef mongoActor = system.actorOf(MongoManager.props(), "MongoActor");
    	final ActorRef neo4jActor = system.actorOf(Neo4jManager.props(), "Neo4jActor");
        final ActorRef xmlActor = system.actorOf(XMLManager.props(neo4jActor, mongoActor), "XMLActor");
        final ActorRef downloadActor = system.actorOf(DownloadManager.props(xmlActor), "DownloadActor");
        
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
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
}

