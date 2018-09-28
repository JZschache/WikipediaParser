package wikipedia;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.pf.ReceiveBuilder;
import wikipedia.parser.XMLManager.LoadFile;
import wikipedia.parser.XMLManager.LoadURL;

public class DownloadManager extends AbstractActor {
	
	static public Props props(ActorRef xmlManager) {
		return Props.create(DownloadManager.class, () -> new DownloadManager(xmlManager));
	}
	private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);
	public final String filepath = "/local/hd/wikipedia/original/";
	
	public DownloadManager(ActorRef xmlManager) {
		receive(ReceiveBuilder
				.match(LoadURL.class, load -> {
					
					String fileName = filepath + load.urlString.split("/")[5];
					File f = new File(fileName);
					if(!f.exists()) { 
						f.getParentFile().mkdirs(); 
						f.createNewFile();
					    InputStream inStream = new URL(load.urlString).openStream();
					    OutputStream outStream = new FileOutputStream(f);
					
						byte[] buffer = new byte[8 * 1024];
					    int bytesRead;
					    while ((bytesRead = inStream.read(buffer)) != -1) {
					        outStream.write(buffer, 0, bytesRead);
					    }
					    inStream.close();
					    outStream.close();
					}
					xmlManager.tell(new LoadFile(fileName), self());
				})
				.matchAny(o -> log.info("received unknown message"))
				.build());

	}

}
