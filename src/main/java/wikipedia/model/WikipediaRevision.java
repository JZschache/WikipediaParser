package wikipedia.model;

public class WikipediaRevision {
	
	private String id;
	private String parentId;
	private String timestamp;
	private WikipediaUser contributor;
	private String contributorIp;
	private String text;
	private String textAdded;
	private String textRemoved;
	private WikipediaPage page;
	
	public WikipediaRevision(WikipediaPage page) {
		this.page = page;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getParentId() {
		return parentId;
	}
	public void setParentId(String parentId) {
		this.parentId = parentId;
	}
	public String getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	public WikipediaUser getContributor() {
		return contributor;
	}
	public void setContributor(WikipediaUser contributor) {
		this.contributor = contributor;
	}
	public String getText() {
		return text;
	}
	public void setText(String text) {
		this.text = text;
	}
	public String getContributorIp() {
		return contributorIp;
	}
	public void setContributorIp(String contributorIp) {
		this.contributorIp = contributorIp;
	}
	public WikipediaPage getPage() {
		return page;
	}
	public void setPage(WikipediaPage page) {
		this.page = page;
	}
	public String getTextAdded() {
		return textAdded;
	}
	public void setTextAdded(String textAdded) {
		this.textAdded = textAdded;
	}
	public String getTextRemoved() {
		return textRemoved;
	}
	public void setTextRemoved(String textRemoved) {
		this.textRemoved = textRemoved;
	}

}
