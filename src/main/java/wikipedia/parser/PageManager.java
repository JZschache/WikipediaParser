package wikipedia.parser;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.Locale;

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
	
	private DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);
	
	private LinkedList<WikipediaRevision> revisions = new LinkedList<WikipediaRevision>();
//	private List<WikipediaRevision> lonelyChildren = new ArrayList<WikipediaRevision>();
	
	private LoggingAdapter log;
	
	private ActorRef neo4jActor;
	private ActorRef mongoActor;
	
	private String currentUser;	
	private WikipediaRevision currentUserRevision;
	
	public PageManager(ActorRef neo4jActor, ActorRef mongoActor, ActorContext context) {
		this.neo4jActor = neo4jActor;
		this.mongoActor = mongoActor;
		
		log = Logging.getLogger(context.system(), this);
		
	}
	public void addRevision(WikipediaRevision revision) {
		
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
	
	public void addPage(WikipediaPage page) {
		
		addCurrentUserRevision();
		currentUserRevision.setText(null);
				
		if (!revisions.isEmpty()) {
			neo4jActor.tell(new AddRevisions(page, revisions), ActorRef.noSender());			
			mongoActor.tell(new AddRevisions(page, revisions), ActorRef.noSender());
		}
	}
	
	private void addCurrentUserRevision() {
		
		if (currentUserRevision != null) {
			
			if (!revisions.isEmpty()) {
				WikipediaRevision previousRevision = revisions.getLast();
				// overwrite parentId
				currentUserRevision.setParentId(previousRevision.getId());
				// set diff
				diff_match_patch dmp = new diff_match_patch(new StandardBreakScorer() );
				LinkedList<diff_match_patch.Diff> diff = dmp.diff_main(previousRevision.getText(), currentUserRevision.getText());
				dmp.diff_cleanupSemantic(diff);
				for (diff_match_patch.Diff d : diff) {
				  	if (d.operation.equals(Operation.INSERT))
				  		currentUserRevision.setTextAdded(d.text);
				   	if (d.operation.equals(Operation.DELETE))
				   		currentUserRevision.setTextRemoved(d.text);
				}
				//clear text to save memory/storage
				if (previousRevision.getParentId() != null)
					previousRevision.setText(null);
				LocalDateTime currentTime = LocalDateTime.parse(currentUserRevision.getTimestamp(), DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"));
				LocalDateTime previousTime = LocalDateTime.parse(previousRevision.getTimestamp(), DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"));
				if (currentTime.isBefore(previousTime))
					log.warning("Current revision {} was before previous revision {}.", currentUserRevision.getId(), previousRevision.getId());
			} else { // currentUserRevision is first revision of page
				currentUserRevision.setParentId(null);
			}
			revisions.add(currentUserRevision);
		}
	}
       
}