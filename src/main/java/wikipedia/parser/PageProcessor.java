package wikipedia.parser;

import wikipedia.model.WikipediaPage;

public interface PageProcessor {
    void process(WikipediaPage page);
}