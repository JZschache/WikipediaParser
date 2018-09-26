package wikipedia.model;

import java.util.ArrayList;
import java.util.List;

/**
 * https://github.com/martinffx/wikipedia-data-parser
 * 
 */
public class WikipediaPage {


	private String id;
	private String title;
    private boolean isRedirecting = false;
    private List<WikipediaRevision> revisions = new ArrayList<WikipediaRevision>();

    public WikipediaPage() {
        
    }
    
    public String toString() {
        return "Id: " + getId() + ", Title: " + getTitle() + ", Revisions: " + revisions.size();
    }
    
    public void setRedirecting(boolean redirecting) {
    	this.isRedirecting = redirecting;
    }
    
    public boolean isRedirecting() {
    	return isRedirecting;
    }
    
    public void setId(String id) {
		this.id = id;
	}

	public void setTitle(String title) {
		this.title = title;
	}
    
    public String getId() {
		return id;
	}

	public String getTitle() {
		return title;
	}
	
	public void addRevision(WikipediaRevision revision) {
		revisions.add(revision);
	}
	
	public List<WikipediaRevision> getRevisions(){
		return revisions;
	}

	public void setRevisions(List<WikipediaRevision> revisions) {
		this.revisions = revisions;
	}
}