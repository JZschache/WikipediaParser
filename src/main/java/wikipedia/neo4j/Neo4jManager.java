package wikipedia.neo4j;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.pf.ReceiveBuilder;
import wikipedia.model.WikipediaPage;
import wikipedia.model.WikipediaRevision;
import wikipedia.model.WikipediaUser;
import wikipedia.parser.XMLManager.AddPage;



public class Neo4jManager extends AbstractActor {
	
	static public Props props() {
		return Props.create(Neo4jManager.class, () -> new Neo4jManager());
	}
	
//	private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
	private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

	GraphDatabaseService graphDb;
	
	private Label pageLabel = Label.label( "Page" );
	private Label revisionLabel = Label.label( "Revision" );
	private Label userLabel = Label.label( "User" );
	
	private Map<String, Node> idMapping = new HashMap<String, Node>();
	
	public Neo4jManager() {
		
		GraphDatabaseSettings.BoltConnector bolt = GraphDatabaseSettings.boltConnector( "0" );

		graphDb = new GraphDatabaseFactory()
		        .newEmbeddedDatabaseBuilder( new File("/local/hd/neo4jwikipedia") )
		        .setConfig( bolt.type, "BOLT" )
		        .setConfig( bolt.enabled, "true" )
		        .setConfig( bolt.address, "localhost:7688" )
		        .newGraphDatabase();
		
		try ( Transaction tx = graphDb.beginTx() )
		{
		    Schema schema = graphDb.schema();
		    schema.indexFor( userLabel )
		            .on( "wikipedia_id" )
		            .create();
		    schema.indexFor( revisionLabel )
		    		.on( "_id" )
		    		.create();
		    tx.success();
		}
		
		//graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( new File("/local/hd/neo4jwikipedia"));
		registerShutdownHook( graphDb );
		
		receive(ReceiveBuilder
				.match(AddPage.class, ap -> {
					log.info("Done storing page: {}", ap.page.getId());
					addWikipediaPage(ap.page);
					log.info("Done storing page: {}", ap.page.getId());
				})
				.matchAny(o -> log.info("received unknown message"))
				.build());
	}
	

	private void addWikipediaPage(WikipediaPage page) {
		try ( Transaction tx = graphDb.beginTx() )
		{
			Node pageNode = graphDb.createNode(pageLabel);
			pageNode.setProperty("_id", page.getId());
			pageNode.setProperty("title", page.getTitle());
			
			Node parentNode = null;
			
			for (WikipediaRevision r: page.getRevisions()) {
				Node revNode = graphDb.createNode(revisionLabel);
				revNode.setProperty("_id", r.getId());
				revNode.setProperty("timestamp", r.getTimestamp());
				revNode.createRelationshipTo( pageNode, RelTypes.REVISION_OF );
				
				WikipediaUser user = r.getContributor();
				if (user != null) {
					Node userNode = graphDb.findNode(userLabel, "wikipedia_id", user.getId());
//					Node userNode = idMapping.get(user.getId());
					if (userNode == null) {
						userNode = graphDb.createNode(userLabel);
						try {
							userNode.setProperty("wikipedia_id", user.getId());
						} catch (Exception e) {
							System.out.println(r.getId());
							e.printStackTrace();
						}
						userNode.setProperty("name", user.getName());
//						idMapping.put(user.getId(), userNode);
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
					parentNode.createRelationshipTo(revNode, RelTypes.PARENT_OF);
				}
				parentNode = revNode;
			}
		    tx.success();
		}
	}
	
//	@Override
//	public Receive createReceive() {
//		return receiveBuilder()
//			.match(AddPage.class, ap -> {
//				addWikipediaPage(ap.page);
//				log.info("Done storing page: {}", ap.page.getId());
//			})
//			.matchAny(o -> log.info("received unknown message"))
//			.build();
//	  }
	
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