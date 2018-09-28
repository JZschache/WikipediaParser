package wikipedia.mongo;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.bson.Document;
import org.neo4j.graphdb.GraphDatabaseService;

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
import wikipedia.parser.XMLManager.AddRevisions;
import wikipedia.parser.XMLManager.NewFile;

public class MongoManager extends AbstractActor {
	
	static public Props props() {
		return Props.create(MongoManager.class, () -> new MongoManager());
	}
	
//	private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
	private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

	
//	final static MongoClient mongoClient = MongoClients.create("mongodb://127.0.0.1:27017");
//	MongoDatabase database = mongoClient.getDatabase("wikipedia");
//	MongoCollection<Document> revisionsColl = database.getCollection("enwiki_20180901");
	ObjectMapper mapper = new ObjectMapper();
		
	ExecutionContext ec = ExecutionContexts.global();
    Agent<Integer> mongoCounter = Agent.create(0, ec);
	
    PrintWriter out;
	
	public MongoManager() {
		
		receive(ReceiveBuilder
			.match(AddRevisions.class, ap -> {
				addWikipediaRevisions(ap.revisions);
				mongoCounter.send(new Mapper<Integer, Integer>() {
            		public Integer apply(Integer i) {
            			if ((i+1) % Main.outputFreq == 0)
		            		log.info("Done storing {} pages.", (i+1));
            			return i + 1;
            		}				            		
            	});
			})
			.match(NewFile.class, nf -> {
				if (out != null)
					out.close();
				try {
					String[] splits = nf.filename.split("/")[5].split("-");
					String last = splits[4];
					String filename = splits[0] + "-" + splits[1] + "-" + last.substring(0, last.length() - 4);  
					out = new PrintWriter(new BufferedWriter(new FileWriter("/local/hd/wikipedia/json/" + filename + ".json", true)));
					registerShutdownHook(out);
				} catch (IOException e) {
					e.printStackTrace();
				} 
			})
			.matchAny(o -> log.info("received unknown message"))
			.build());
	}
	
//	private int batchsize = 100;
	
	private void addWikipediaRevisions(Collection<WikipediaRevision> revisions) {
		
//		List<WikipediaRevision> revisions = page.getRevisions();
//		for (int i = 0; i<revisions.size(); i += batchsize) {
//			List<Document> documents = new ArrayList<Document>(batchsize);
//			for (WikipediaRevision revision: revisions.subList(i, Math.min(revisions.size(), i + batchsize))) {
		for (WikipediaRevision revision: revisions) {
			try {
				String jsonInString = mapper.writeValueAsString(revision);
				out.println(jsonInString);
//				Document d = Document.parse(jsonInString);
//				d.remove("id");
//				d.append("_id", revision.getId());
//				d.append("page", new Document("id", page.getId()).append("title", page.getTitle()));
//				out.println(d.toJson());
//				documents.add(d);
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
//			}
//			out.flush();
//			try {
//				revisionsColl.insertMany(documents);
//			} catch (com.mongodb.MongoBulkWriteException e) {
//				System.out.println(e.getMessage());
//			}
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
	
	private void registerShutdownHook( PrintWriter out )	{

	    Runtime.getRuntime().addShutdownHook( new Thread()
	    {
	        @Override
	        public void run(){
	        	if (out != null)
	        		out.close();
	        }
	    } );
 	}
		
}
