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
import wikipedia.parser.XMLActor.LoadFile;
import wikipedia.parser.XMLActor.LoadURL;

public class DownloadActor extends AbstractActor {
	
	static public Props props(ActorRef xmlManager) {
		return Props.create(DownloadActor.class, () -> new DownloadActor(xmlManager));
	}
	private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);
	public final String filepath = "/local/hd/wikipedia/original/";
	
	private ActorRef xmlManager;
	
	public DownloadActor(ActorRef xmlManager) {
		this.xmlManager = xmlManager;
	}
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
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
				.build();
	}

}
