package wikipedia.neo4j;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.kernel.configuration.BoltConnector;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import wikipedia.Main;
import wikipedia.model.WikipediaPage;
import wikipedia.model.WikipediaRevision;
import wikipedia.model.WikipediaUser;
import wikipedia.parser.XMLActor.AddRevisions;



public class Neo4jActor extends AbstractActor {
	
	static public Props props(String path) {
		return Props.create(Neo4jActor.class, () -> new Neo4jActor(path));
	}
	
	private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);
	
	public GraphDatabaseService graphDb;
	
	public Label pageLabel = Label.label( "Page" );
	public Label revisionLabel = Label.label( "Revision" );
	public Label userLabel = Label.label( "User" );
	
	int neo4jCounter = 0;
	
	public Neo4jActor(String path) {

		BoltConnector bolt = new BoltConnector();

		graphDb = new GraphDatabaseFactory()
		        .newEmbeddedDatabaseBuilder( new File(path) )
		        .setConfig( bolt.type, "BOLT" )
		        .setConfig( bolt.enabled, "true" )
		        .setConfig( bolt.listen_address, "localhost:7688" )
		        .newGraphDatabase();
		
		// create indices if not exist
		try ( Transaction tx = graphDb.beginTx() )
		{
		    Schema schema = graphDb.schema();
		    boolean needsUserLabelIndex = true;
		    boolean needsRevisionLabelIndex = true;
		    boolean needsPageLabelIndex = true;
		    for (IndexDefinition id : schema.getIndexes()) {
		    	for (String propertyKey : id.getPropertyKeys()) {
		    		if (id.getLabel().equals(userLabel) && propertyKey.equals("wikipedia_id")) {
		    			needsUserLabelIndex = false;
		    		} 
		    		if (id.getLabel().equals(revisionLabel) && propertyKey.equals("_id")) {
		    			needsRevisionLabelIndex = false;
		    		}
		    		if (id.getLabel().equals(pageLabel) && propertyKey.equals("_id")) {
		    			needsPageLabelIndex = false;
		    		}
		    	}
		    }
		    if (needsUserLabelIndex) {
		    	schema.indexFor( userLabel )
				    	.on( "wikipedia_id" )
				    	.create();
		    }
		    if (needsRevisionLabelIndex) {
		    	schema.indexFor( revisionLabel )
		    			.on( "_id" )
		    			.create();
		    }
		    if (needsPageLabelIndex) {
		    	schema.indexFor( pageLabel )
		    			.on( "_id" )
		    			.create();
		    }
		    tx.success();
		}
		
		registerShutdownHook( graphDb );

	}
	
	@Override
	  public Receive createReceive() {
	    return receiveBuilder()
	    		.match(AddRevisions.class, ar -> {
					addWikipediaPage(ar.page, ar.revisions);
					neo4jCounter++;
					if (neo4jCounter % Main.outputFreq == 0) {
						log.info("Done storing {} pages. Current page id: {}.", neo4jCounter, ar.page.getId());
	            	}				            		
				})
				.matchAny(o -> log.info("received unknown message"))
				.build();
	  }
	
	private void addWikipediaPage(WikipediaPage page, Collection<WikipediaRevision> revisions) {
		try ( Transaction tx = graphDb.beginTx() )
		{
			
			Node pageNode = graphDb.findNode(pageLabel, "_id", page.getId());
			if (pageNode == null) {
				pageNode = graphDb.createNode(pageLabel);
				pageNode.setProperty("_id", page.getId());
				pageNode.setProperty("title", page.getTitle());
			
				Node parentNode = null;
				
				Map<Node, String> lonelyChildren = new HashMap<Node,String>();
				
				for (WikipediaRevision r: revisions) {
					Node revNode = graphDb.createNode(revisionLabel);
					revNode.setProperty("_id", r.getId());
					revNode.setProperty("timestamp", r.getTimestamp());
					revNode.createRelationshipTo( pageNode, RelTypes.REVISION_OF );
					
					WikipediaUser user = r.getContributor();
					if (user != null) {
						Node userNode = graphDb.findNode(userLabel, "wikipedia_id", user.getId());
						if (userNode == null) {
							userNode = graphDb.createNode(userLabel);
							try {
								userNode.setProperty("wikipedia_id", user.getId());
							} catch (Exception e) {
								System.out.println(r.getId());
								e.printStackTrace();
							}
							userNode.setProperty("name", user.getName());
						}
						userNode.createRelationshipTo( revNode, RelTypes.WROTE );
					} else if (r.getContributorIp() != null) {
						revNode.setProperty("userIp", r.getContributorIp());
					}
					String parentId = r.getParentId();
					if (parentId != null) {
						if (parentNode == null || !parentNode.getProperty("_id").equals(parentId)) {
							parentNode = graphDb.findNode(revisionLabel, "_id", parentId);
						}
						if (parentNode == null) {
							lonelyChildren.put(revNode, parentId);
						} else 
							parentNode.createRelationshipTo(revNode, RelTypes.PARENT_OF);
					}
					// normally the xml-file shows the revisions ordered by timestamp
					parentNode = revNode;
				}
				// another run to find the parent of lonely children (revisions that were parsed before their parent)
				int counter = 0;
				while (!lonelyChildren.isEmpty() && counter < 10) {
					counter++;
					lonelyChildren = findParent(lonelyChildren);
				}
			}
				
		    tx.success();
		}
	}
	
	private Map<Node, String> findParent(Map<Node, String> lonelyChildren){
		Map<Node, String> remainingLonelyChildren = new HashMap<Node,String>();
		for (Node n: lonelyChildren.keySet()) {
			Node parentNode = graphDb.findNode(revisionLabel, "_id", lonelyChildren.get(n));
			if (parentNode == null) {
				log.warning("Parent revision with id {} was not found.", lonelyChildren.get(n));
				remainingLonelyChildren.put(n, lonelyChildren.get(n));
			} else {
				parentNode.createRelationshipTo(n, RelTypes.PARENT_OF);
			}
		}
		return remainingLonelyChildren;
	}
	
	private static void registerShutdownHook( final GraphDatabaseService graphDb )	{
	    // Registers a shutdown hook for the Neo4j instance so that it
	    // shuts down nicely when the VM exits (even if you "Ctrl-C" the
	    // running application).
	    Runtime.getRuntime().addShutdownHook( new Thread()
	    {
	        @Override
	        public void run()
	        {
	            graphDb.shutdown();
	        }
	    } );
 	}
	
}