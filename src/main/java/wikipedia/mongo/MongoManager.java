package wikipedia.mongo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import wikipedia.model.WikipediaRevision;

public class MongoManager {

	
	final static MongoClient mongoClient = MongoClients.create("mongodb://127.0.0.1:27017");
	MongoDatabase database = mongoClient.getDatabase("wikipedia");
	MongoCollection<Document> revisionsColl = database.getCollection("enwiki_20180901");
	ObjectMapper mapper = new ObjectMapper();
	
	public MongoManager() {
			 
	}
	
	public void addWikipediaRevision(WikipediaRevision revision) {
		try {
			String jsonInString = mapper.writeValueAsString(revision);
			Document d = Document.parse(jsonInString);
			d.remove("id");
			d.append("_id", revision.getId());
			revisionsColl.insertOne(d);
		} catch (com.mongodb.MongoWriteException e) {
			System.out.println(e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void addWikipediaRevisions(List<WikipediaRevision> revisions) {
		List<Document> documents = new ArrayList<Document>();
		for (WikipediaRevision revision: revisions) {
			try {
				String jsonInString = mapper.writeValueAsString(revision);
				Document d = Document.parse(jsonInString);
				d.remove("id");
				d.append("_id", revision.getId());
				documents.add(d);
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}  
		}
		try {
			revisionsColl.insertMany(documents);
		} catch (com.mongodb.MongoBulkWriteException e) {
			System.out.println(e.getMessage());
		}
	}	
		
}
