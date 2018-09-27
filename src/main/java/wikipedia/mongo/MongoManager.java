package wikipedia.mongo;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.agent.Agent;
import akka.dispatch.ExecutionContexts;
import akka.dispatch.Mapper;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.pf.ReceiveBuilder;
import scala.concurrent.ExecutionContext;
import wikipedia.Main;
import wikipedia.model.WikipediaPage;
import wikipedia.model.WikipediaRevision;
import wikipedia.parser.XMLManager.AddPage;

public class MongoManager extends AbstractActor {
	
	static public Props props() {
		return Props.create(MongoManager.class, () -> new MongoManager());
	}
	
//	private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
	private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

	
	final static MongoClient mongoClient = MongoClients.create("mongodb://127.0.0.1:27017");
	MongoDatabase database = mongoClient.getDatabase("wikipedia");
	MongoCollection<Document> revisionsColl = database.getCollection("enwiki_20180901");
	ObjectMapper mapper = new ObjectMapper();
	
	ExecutionContext ec = ExecutionContexts.global();
    Agent<Integer> mongoCounter = Agent.create(0, ec);
	
	
	public MongoManager() {
		receive(ReceiveBuilder
			.match(AddPage.class, ap -> {
				addWikipediaPage(ap.page);
				mongoCounter.send(new Mapper<Integer, Integer>() {
            		public Integer apply(Integer i) {
            			int idx = Integer.parseInt(ap.page.getId());
            			if (i % Main.outputFreq == 0)
		            		log.info("Done storing page {}.", idx);
            			return i + 1;
            		}				            		
            	});
			})
			.matchAny(o -> log.info("received unknown message"))
			.build());
	}
	
	private int batchsize = 100;
	
	private void addWikipediaPage(WikipediaPage page) {
		
		List<WikipediaRevision> revisions = page.getRevisions();
		for (int i = 0; i<revisions.size(); i += batchsize) {
			List<Document> documents = new ArrayList<Document>(batchsize);
			for (WikipediaRevision revision: revisions.subList(i, Math.min(revisions.size(), i + batchsize))) {
				try {
					String jsonInString = mapper.writeValueAsString(revision);
					Document d = Document.parse(jsonInString);
					d.remove("id");
					d.append("_id", revision.getId());
					d.append("page", new Document("id", page.getId()).append("title", page.getTitle()));
					documents.add(d);
				} catch (JsonProcessingException e) {
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
		
}
