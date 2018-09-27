package wikipedia;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import wikipedia.mongo.MongoManager;
import wikipedia.neo4j.Neo4jManager;
import wikipedia.parser.XMLManager;
import wikipedia.parser.XMLManager.Load;

public class Main  {
	
    public static void main(String[] args) {
    	
    	final ActorSystem system = ActorSystem.create("wikipediaparser");
    	
    	final ActorRef neo4jActor = system.actorOf(Neo4jManager.props(), "Neo4jActor");
    	final ActorRef mongoActor = system.actorOf(MongoManager.props(), "MongoActor");
        final ActorRef xmlActor = system.actorOf(XMLManager.props(neo4jActor, mongoActor), "XMLActor");
        
        String urlString = "https://dumps.wikimedia.org/enwiki/20180901/enwiki-20180901-pages-meta-history5.xml-p564715p565313.bz2";
        xmlActor.tell(new Load(urlString), ActorRef.noSender());
        
    }

}

