package wikipedia.model;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class WikipediaRevision implements Comparable<WikipediaRevision>{
	
	private String id;
	private String parentId;
	private String timestamp;
	private LocalDateTime timestampDate;
	private WikipediaUser contributor;
	private String contributorIp;
	private String text;
	private List<String> textAdded;
	private List<String> textRemoved;
	private WikipediaPage page;
	
	public WikipediaRevision(WikipediaPage page) {
		this.page = page;
		textAdded = new ArrayList<String>();
		textRemoved = new ArrayList<String>();
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
	@JsonIgnore
	public LocalDateTime getTimestampDate() {
		return timestampDate;
	}
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
		this.timestampDate = LocalDateTime.parse(timestamp, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"));
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
	public List<String> getTextAdded() {
		return textAdded;
	}
	public void addTextAdded(String textAdded) {
		this.textAdded.add(textAdded);
	}
	public List<String> getTextRemoved() {
		return textRemoved;
	}
	public void addTextRemoved(String textRemoved) {
		this.textRemoved.add(textRemoved);
	}
	
	@Override
	public int compareTo(WikipediaRevision o) {
		if (timestampDate == null || o.getTimestampDate() == null)
		      return 0;
		else 
			return timestampDate.compareTo(o.getTimestampDate());
	}

}
