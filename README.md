# Wikipedia Parser

A Java-Application that parses the whole history of changes on Wikipedia and builds a JSON-DB. The JSON files are much smaller than the original dump because they contain only the changes that have been made to a page on Wikipedia and not the complete text of each version of a page. For instance, while the uncompressed data of the first part of history1 (pages 10-2065) is 73 GB, the respective JSON file measures only 4.3 GB (6%). This size is approximately the same as the original data in bz2-compression (4.1 GB in case of pages 10-2065). 

Additionally a Neo4j-DB is filled with relational data (appr. 0.02% of uncompressed data). 

This application uses the [akka actor library](https://akka.io/). 

After cloning the repository, an executable jar is made by

```
mvn clean package
```

Useful settings of JVM-parameters are given in [Wikiparser.sh](Wikiparser.sh).

## Configuration

In [application.conf](src/main/resources/application.conf), the following parameters can be adjusted:
- wikipedia.path: local directory for downloads and DBs
- indexUrl: the Url of the Wikipedia Downloads (https://dumps.wikimedia.org)
- indexPath: path to the desired version (e.g. the one from January 1st, 2019)
- filePrefix: part of the file name that specifies the kind of dump (e.g. all pages with complete page edit history)
- lastPageId: used to skip the first pages including page with this Id

The logging can be configured in [logback.XML](src/main/resources/logback.xml). See also [online documentation](https://logback.qos.ch/manual/configuration.html).

## Data

At wikipedia.path, four directories are created.

1. log: logging files
2. original
    - downloads of Wikipedia dump: enwiki-20190101-pages-meta-history1.xml-p10p2065.bz2, ...
    - requires sufficient storage (915 GB)
3. json
    - the JSON-files: enwiki-20190101-history1.json, ...
    - requires sufficient storage (appr. ?? TB) 
4. neo4j
    - the Neo4j-DB
    - requires sufficient storage (appr. ?? TB)
    
### Format of JSON data

WikipediaParser parses through the XML files of the Wikipedia dump and extracts the changes between two versions of an article. A Wikipedia page consists of multiple revisions by different authors. The WikipediaParser collects all revisions by the same author until another author submits a revision. Only the last revision is kept and the difference to the previous revision (by another author) is calculated.

An example of the article "Anarchism" in JSON format:

```
{
	"id":"233194",
	"parentId":null,
	"timestamp":"2001-10-11T20:18:47Z",
	"contributor":{
		"id":"31",
		"name":"The Cunctator"
	},
	"contributorIp":null,
	"text":"''Anarchism'' is the political theory [...]",
	"patch":null,
	"page":{
		"id":"12",
		"title":"Anarchism",
		"redirecting":false
	}
}
{
	"id":"233195",
	"parentId":"233194",
	"timestamp":"2001-11-28T13:32:25Z",
	"contributor":{
		"id":"157",
		"name":"Ffaker"
	},
	"contributorIp":null,
	"text":null,
	"patch":"@@ -2832,24 +2832,26 @@\n d order.%0A%0A%0A%0A\n+%0A%0A\n Anarcho-capi\n@@ -7666,24 +7666,102 @@\n rchism [...]",
	"page":{
		"id":"12",
		"title":"Anarchism",
		"redirecting":false
	}
}
```

The first version of a page has no parentId, and the attribute "text" contains the full article. Later entries that refer to the same page link to the previous version by the parentId. Changes are given as patch: see [diff-match-patch](https://github.com/google/diff-match-patch). For a faster comparison of different versions, the diff algorithms operates not on the level of characters but only on the level of lines (as found by `\n`): for the difference, see [Line-or-Word-Diffs](https://github.com/google/diff-match-patch/wiki/Line-or-Word-Diffs)

The revisions are ordered chronologically in the JSON file. The ordering is done without storing all revisions in memory since this would require memory space of at least 2 GB for the texts only.

The original parentId as given in the XML file is not reliable and, therefore, overwritten.

### Format of Neo4j data

The Neo4j database contains the relational information. The attributes "text" and "patch" are not stored. Example: 

![](neo4jExample.png)
