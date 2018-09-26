package wikipedia;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;


import wikipedia.model.WikipediaPage;
import wikipedia.model.WikipediaRevision;
import wikipedia.mongo.MongoManager;
import wikipedia.neo4j.Neo4jManager;
import wikipedia.parser.PageProcessor;
import wikipedia.parser.XMLManager;

public class Main  {
	
    public static void main(String[] args) {

    	MongoManager mongoMan = new MongoManager();
    	Neo4jManager neo4jMan = new Neo4jManager();
    	
        XMLManager.load(new PageProcessor() {
            @Override
            public void process(WikipediaPage page) {
                // Obviously you want to do something other than just printing, 
                // but I don't know what that is...
                System.out.println(page);
                neo4jMan.addWikipediaPage(page);
//                List<WikipediaRevision> revisions = page.getRevisions();
//                for (WikipediaRevision r : revisions) {
//                	r.getPage().setRevisions(null);
//                }
//                mongoMan.addWikipediaRevisions(revisions);
           }
        }) ;
    }

}

