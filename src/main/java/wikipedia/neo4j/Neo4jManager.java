package wikipedia.neo4j;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import wikipedia.model.WikipediaPage;
import wikipedia.model.WikipediaRevision;
import wikipedia.model.WikipediaUser;



public class Neo4jManager {

	GraphDatabaseService graphDb;
	
	public Neo4jManager() {
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( new File("/local/hd/neo4jwikipedia"));
		registerShutdownHook( graphDb );
	}
	
	private Label pageLabel = Label.label( "Page" );
	private Label revisionLabel = Label.label( "Revision" );
	private Label userLabel = Label.label( "User" );
	
	public void addWikipediaPage(WikipediaPage page) {
		try ( Transaction tx = graphDb.beginTx() )
		{
			Node pageNode = graphDb.createNode(pageLabel);
			pageNode.setProperty("_id", page.getId());
			pageNode.setProperty("title", page.getTitle());
			
			for (WikipediaRevision r: page.getRevisions()) {
				Node revNode = graphDb.createNode(revisionLabel);
				revNode.setProperty("_id", r.getId());
				revNode.setProperty("timestamp", r.getTimestamp());
				revNode.createRelationshipTo( pageNode, RelTypes.REVISION_OF );
				
				WikipediaUser user = r.getContributor();
				if (user != null) {
					Node userNode = graphDb.findNode(userLabel, "_id", user.getId());
					if (userNode == null) {
						userNode = graphDb.createNode(userLabel);
						userNode.setProperty("_id", user.getId());
						userNode.setProperty("name", user.getName());
					}
					userNode.createRelationshipTo( revNode, RelTypes.WROTE );
				} else {
					revNode.setProperty("userIp", r.getContributorIp());
				}
				String parentId = r.getParentId();
				if (parentId != null) {
					Node parentNode = graphDb.findNode(revisionLabel, "_id", parentId);
					revNode.createRelationshipTo(parentNode, RelTypes.CHILD_OF);
					parentNode.createRelationshipTo(revNode, RelTypes.PARENT_OF);
				}
			}
		    tx.success();
		}
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