package wikipedia;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import wikipedia.mongo.JsonActor;
import wikipedia.neo4j.Neo4jActor;
import wikipedia.parser.XMLActor;
import wikipedia.DownloadActor.LoadURL;

public class Main  {
	
	public final static int outputFreq = 1000;
	
    public static void main(String[] args) {
    	

        Config regularConfig = ConfigFactory.load();
                
    	String path = regularConfig.getString("wikipedia.path");
    	String indexUrl = regularConfig.getString("wikipedia.indexUrl");
        String indexPath = regularConfig.getString("wikipedia.indexPath");
        String filePrefix = regularConfig.getString("wikipedia.filePrefix");
        int lastPageId = regularConfig.getInt("wikipedia.lastPageId");
        boolean writeToHdfs = regularConfig.getBoolean("wikipedia.writeToHdfs");
        
    	final ActorSystem system = ActorSystem.create("wikipediaparser");
    	final LoggingAdapter log = Logging.getLogger(system.eventStream(), "Main");

    	final ActorRef mongoActor = system.actorOf(JsonActor.props(path), "JsonActor");
    	final ActorRef neo4jActor = system.actorOf(Neo4jActor.props(path), "Neo4jActor");
        final ActorRef xmlActor = system.actorOf(XMLActor.props(neo4jActor, mongoActor, lastPageId), "XMLActor");
        final ActorRef downloadActor = system.actorOf(DownloadActor.props(xmlActor, path, writeToHdfs), "DownloadActor");
        
        log.info("Reading index from: {}", indexUrl + indexPath);
		try {
			InputStream stream = new URL(indexUrl + indexPath).openStream();
			BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
			String line;
	        while ((line = reader.readLine()) != null) {
	        	if (line.contains(filePrefix)) {
	        		String[] splits = line.split("\"");
	        		for (String s : splits) {
	        			if (s.contains(filePrefix) && s.endsWith(".bz2")) {
	        				downloadActor.tell(new LoadURL(indexUrl + s), ActorRef.noSender());
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

