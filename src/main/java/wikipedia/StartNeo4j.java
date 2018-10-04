package wikipedia;

import akka.actor.ActorSystem;

import wikipedia.neo4j.Neo4jActor;

public class StartNeo4j  {
	
	public final static int outputFreq = 1000;
	
    public static void main(String[] args) {
    	
    	final ActorSystem system = ActorSystem.create("wikipediaparser");
    	system.actorOf(Neo4jActor.props("/local/hd/wikipedia/neo4j"), "Neo4jActor");

    }
        
}

