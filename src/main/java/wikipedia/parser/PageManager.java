package wikipedia.parser;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import name.fraser.neil.plaintext.StandardBreakScorer;
import name.fraser.neil.plaintext.diff_match_patch;
import name.fraser.neil.plaintext.diff_match_patch.Operation;
import wikipedia.model.WikipediaPage;
import wikipedia.model.WikipediaRevision;
import wikipedia.parser.XMLActor.AddRevisions;



/**
 * 
 * @author zschache
 *
 */
public class PageManager {
	
	private Map<String, WikipediaRevision> revisions = new LinkedHashMap<String, WikipediaRevision>();
	private List<WikipediaRevision> lonelyChildren = new ArrayList<WikipediaRevision>();
	
	private LoggingAdapter log;
	
	private ActorRef neo4jActor;
	private ActorRef mongoActor;
		
	public PageManager(ActorRef neo4jActor, ActorRef mongoActor, ActorContext context) {
		this.neo4jActor = neo4jActor;
		this.mongoActor = mongoActor;
		lonelyChildren = new ArrayList<WikipediaRevision>();
		
		log = Logging.getLogger(context.system(), this);
		
	}
	public void addRevision(WikipediaRevision revision) {
		revisions.put(revision.getId(), revision);
		setDiff(revision, lonelyChildren);
	}
	
	public void addPage(WikipediaPage page) {
		// another run to find the parent of lonely children (revisions that were parsed before their parent)
		int counter = 0;
		while(!lonelyChildren.isEmpty() && counter < 10) {
			counter ++;
			lonelyChildren = findParent(lonelyChildren);
		}
				
		// removing text to reduce memory/storage
		for (WikipediaRevision revision : revisions.values()) {
			if (revision.getParentId() != null)
				revision.setText(null);
		}
		
		if (!revisions.isEmpty()) {
			neo4jActor.tell(new AddRevisions(page, revisions.values()), ActorRef.noSender());			
			mongoActor.tell(new AddRevisions(page, revisions.values()), ActorRef.noSender());
		}
	}
		
	private void setDiff(WikipediaRevision revision, List<WikipediaRevision> lonelyChildren) {
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
				log.warning("Parent revision with id {} was not found.", revision.getParentId());
				lonelyChildren.add(revision);
			}
		}
	}
	
	private List<WikipediaRevision> findParent(List<WikipediaRevision> lonelyChildren){
		List<WikipediaRevision> remainingLonelyChildren = new ArrayList<WikipediaRevision>();
		for (WikipediaRevision revision : lonelyChildren) {
			setDiff(revision, remainingLonelyChildren);
		}
		return remainingLonelyChildren;
	}
       
}