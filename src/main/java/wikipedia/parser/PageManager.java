package wikipedia.parser;

import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import name.fraser.neil.plaintext.StandardBreakScorer;
import name.fraser.neil.plaintext.diff_match_patch;
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
	
	private final LoggingAdapter log;
	
	private LinkedList<WikipediaRevision> revisions = new LinkedList<WikipediaRevision>();
	private LinkedList<WikipediaRevision> forkedRevisions = new LinkedList<WikipediaRevision>();
	private LocalDateTime forkedDateTime;
			
	private ActorRef neo4jActor;
	private ActorRef mongoActor;
	
	private String currentUser;	
	private WikipediaRevision currentUserRevision;
	
	private diff_match_patch dmp = new diff_match_patch(new StandardBreakScorer() );
	
	public PageManager(ActorRef neo4jActor, ActorRef mongoActor, ActorContext context) {
		this.neo4jActor = neo4jActor;
		this.mongoActor = mongoActor;
		log = Logging.getLogger(context.system(), this);
	}
	public void addRevision(WikipediaRevision revision) {
					
		// check for new user
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
		if (currentUserRevision == null || currentUserRevision.getTimestampDate().isBefore(revision.getTimestampDate()))
			currentUserRevision = revision; 
		
	}
	
	public void addPage(WikipediaPage page) {
		
		addCurrentUserRevision();
		
		if (forkedDateTime != null && forkedDateTime.isBefore(currentUserRevision.getTimestampDate())) {
			appendForkedRevisions();
		}
		
		// check integrity
		String text = revisions.getFirst().getText();
		for (WikipediaRevision r : revisions) {
			if (r.getPatch() != null) {
				text = applyPatch(text, r.getPatch());
			}
		}
		if (!revisions.getLast().getText().equals(text)) {
			log.warning("patches do not add up to the final version of page " + revisions.getLast().getPage().getId() + " : " + text);
		}
				
		if (!revisions.isEmpty()) {
			revisions.getLast().setText(null);
			neo4jActor.tell(new AddRevisions(page, revisions), ActorRef.noSender());			
			mongoActor.tell(new AddRevisions(page, revisions), ActorRef.noSender());
		}
	}
	
	private void addCurrentUserRevision() {
		
		if (currentUserRevision != null) { // addRevision was called at least once
			
			if (forkedDateTime != null && forkedDateTime.isBefore(currentUserRevision.getTimestampDate())) {
				appendForkedRevisions();
			}
			
			if (!revisions.isEmpty()) {
				
				WikipediaRevision previousRevision = revisions.getLast();
				
				// check whether current revision was written before the last revision
				if (currentUserRevision.getTimestampDate().isBefore(previousRevision.getTimestampDate())) {
					
					// if there is a pending fork, resolve it first
					if (forkedDateTime != null && forkedDateTime.isBefore(currentUserRevision.getTimestampDate())) {
						appendForkedRevisions();
					}
					
					// find last revision that is before current revision
					Iterator<WikipediaRevision> revRev = revisions.descendingIterator();
					WikipediaRevision lastRevision = revRev.next();
					int index = revisions.size() - 2;
					while(revRev.hasNext()) {
						lastRevision = revRev.next();
						if (currentUserRevision.getTimestampDate().isAfter(lastRevision.getTimestampDate())) {
							break;
						} else {
							index--;
						}
					}
					// split revisions into two parts
					if (index > -1) {
						List<WikipediaRevision> sub = revisions.subList(index, revisions.size());
						forkedRevisions = new LinkedList<WikipediaRevision>(sub);
						sub.clear();
						
						// calculate text of last revision
						
						String text = revisions.getFirst().getText();
						for (WikipediaRevision r : revisions) {
							if (r.getPatch() != null) {
								text = applyPatch(text, r.getPatch());
							}
						}
						revisions.getLast().setText(text);
						// calculate text of first forked revision
						WikipediaRevision ffr = forkedRevisions.getFirst();
						ffr.setText(applyPatch(text, ffr.getPatch()));
						ffr.setPatch(null);
					} else { // currentRevision is before all previous revisions
						forkedRevisions = new LinkedList<WikipediaRevision>(revisions);
						revisions = new LinkedList<WikipediaRevision>();
					}
					forkedDateTime = forkedRevisions.getFirst().getTimestampDate();
				}
				
				if (!revisions.isEmpty()) {
					previousRevision = revisions.getLast();
					// overwrite parentId
					currentUserRevision.setParentId(previousRevision.getId());
					// set diff/create patches
					LinkedList<diff_match_patch.Patch> patches = dmp.patch_make(previousRevision.getText(), currentUserRevision.getText());
					currentUserRevision.setPatch(dmp.patch_toText(patches));
					
					//clear text to save memory/storage
					if (previousRevision.getParentId() != null)
						previousRevision.setText(null);
				} else { // currentUserRevision is first revision of page
					currentUserRevision.setParentId(null);
				}
				revisions.add(currentUserRevision);
			}
		}
	}
	
	private String applyPatch(String text, String patch) {
		Object[] result = dmp.patch_apply(new LinkedList<diff_match_patch.Patch>(dmp.patch_fromText(patch)), text);
		return (String)result[0];
	}
	
	private void appendForkedRevisions() {
		WikipediaRevision previousRevision = revisions.getLast();
		WikipediaRevision forkedRevision = forkedRevisions.getFirst();
		// overwrite parentId
		forkedRevision.setParentId(previousRevision.getId());
		// set diff/create patches
		LinkedList<diff_match_patch.Patch> patches = dmp.patch_make(previousRevision.getText(), forkedRevision.getText());
		forkedRevision.setPatch(dmp.patch_toText(patches));
		
		//clear text to save memory/storage
		if (previousRevision.getParentId() != null)
			previousRevision.setText(null);
		
		revisions.addAll(forkedRevisions);
		forkedDateTime = null;
	}
	
       
}