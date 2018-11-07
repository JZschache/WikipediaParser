package wikipedia.parser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import name.fraser.neil.plaintext.StandardBreakScorer;
import name.fraser.neil.plaintext.diff_match_patch;
import name.fraser.neil.plaintext.diff_match_patch.Operation;
import wikipedia.model.WikipediaPage;
import wikipedia.model.WikipediaRevision;
import wikipedia.parser.XMLActor.AddRevisions;



/**
 * 
 * The PageManager collects multiple revisions by the same author until another author submits a revision (aggregation by author).
 * Only the last revision is kept and the difference to the previous revision (by another author) is calculated.
 * 
 * Usually, the revision are ordered chronologically in the Wikipedia-dump. But sometimes, the revisions are out of order.
 * In case of the latter, the Wikipedia-Page is skipped and another parsing of the same xml-file is started after completing the current one.
 * During the second parsing, revisions of the skipped pages are collected and ordered by date before aggregation by author and calculating the differences.
 * This approach should be avoided in general because it requires a huge amount of memory (saving all revisions of a page in full text).
 * 
 * The parentId of the entries in the xml-file is not reliable and overwritten.
 * 
 * @author zschache
 *
 */
public class PageManager {
	
//	private DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);
	
	private LinkedList<WikipediaRevision> revisions = new LinkedList<WikipediaRevision>();
	private List<WikipediaRevision> orderedRevisions = new ArrayList<WikipediaRevision>();
		
	private ActorRef neo4jActor;
	private ActorRef mongoActor;
	
	private String currentUser;	
	private WikipediaRevision currentUserRevision;
	
	private List<String> skippedPages;
	
	private boolean skipPage = false;
	private boolean orderedByDate = false;
	
	public PageManager(ActorRef neo4jActor, ActorRef mongoActor, ActorContext context, List<String> skippedPages, boolean orderedByDate) {
		this.neo4jActor = neo4jActor;
		this.mongoActor = mongoActor;
		
		this.skippedPages = skippedPages;
		this.orderedByDate = orderedByDate;
		
	}
	public void addRevision(WikipediaRevision revision) {
		if (orderedByDate) {
			orderedRevisions.add(revision);
		} else if (!skipPage) {
			if (currentUser == null || 
					(revision.getContributorIp() == null && revision.getContributor() == null) ||
					(revision.getContributorIp() == null && !currentUser.equals(revision.getContributor().getId())) ||
				    (revision.getContributor() == null && !currentUser.equals(revision.getContributorIp()))) {
				
				addCurrentUserRevision();
				
				// change currentUser
				if (revision.getContributor() != null)
					currentUser = revision.getContributor().getId();
				else 
					currentUser = revision.getContributorIp();
				
			}
			currentUserRevision = revision;
		}
	}
	
	public void addPage(WikipediaPage page) {
		if (orderedByDate) {
			Collections.sort(orderedRevisions);
			orderedByDate = false;
			for (WikipediaRevision rev : orderedRevisions) {
				addRevision(rev);
			}			
		}
		
		if (!skipPage) {
			addCurrentUserRevision();
			currentUserRevision.setText(null);
		
			if (!skipPage) {
				Collections.sort(revisions);
			
				if (!revisions.isEmpty()) {
					neo4jActor.tell(new AddRevisions(page, revisions), ActorRef.noSender());			
					mongoActor.tell(new AddRevisions(page, revisions), ActorRef.noSender());
				}
			}
		}
	}
	
	private void addCurrentUserRevision() {
		
		if (currentUserRevision != null) {
			
			if (!revisions.isEmpty()) {
				WikipediaRevision previousRevision = revisions.getLast();
				
				if (currentUserRevision.getTimestampDate().isBefore(previousRevision.getTimestampDate())) {
					skipPage = true;
					skippedPages.add(currentUserRevision.getPage().getId());
				}
				else {				
					// overwrite parentId
					currentUserRevision.setParentId(previousRevision.getId());
					// set diff
					diff_match_patch dmp = new diff_match_patch(new StandardBreakScorer() );
					LinkedList<diff_match_patch.Diff> diff = dmp.diff_main(previousRevision.getText(), currentUserRevision.getText());
					dmp.diff_cleanupSemantic(diff);
					for (diff_match_patch.Diff d : diff) {
					  	if (d.operation.equals(Operation.INSERT))
					  		currentUserRevision.addTextAdded(d.text);
					   	if (d.operation.equals(Operation.DELETE))
					   		currentUserRevision.addTextRemoved(d.text);
					}
					//clear text to save memory/storage
					if (previousRevision.getParentId() != null)
						previousRevision.setText(null);
				}
				
			} else { // currentUserRevision is first revision of page
				currentUserRevision.setParentId(null);
			}
			revisions.add(currentUserRevision);
		}
	}
       
}