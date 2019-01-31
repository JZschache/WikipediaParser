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

public class DownloadActor extends AbstractActor {
	
	static public Props props(ActorRef xmlManager, String path) {
		return Props.create(DownloadActor.class, () -> new DownloadActor(xmlManager, path));
	}
	
	// START: messages
	
	static public class LoadURL {
		public final String urlString;

	    public LoadURL(String urlString) {
	        this.urlString = urlString;
	    }
	}
	
	static public class LoadFile {
		public final String fileName;

	    public LoadFile(String fileName) {
	        this.fileName = fileName;
	    }
	}
	// END: messages

	
	private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);
	private String path;
	
	private ActorRef xmlManager;
	
	public DownloadActor(ActorRef xmlManager, String path) {
		this.xmlManager = xmlManager;
		this.path = path + "/original/";
	}
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LoadURL.class, load -> {
					String fileName = path + load.urlString.split("/")[5];
					OutputStream outStream;
				    File f = new File(fileName);
			    	if(!f.exists()) {  
			    		f.getParentFile().mkdirs();
			    		f.createNewFile();
			    	
					    outStream = new FileOutputStream(f);
					    InputStream inStream = new URL(load.urlString).openStream();
					    byte[] buffer = new byte[8 * 1024];
						int bytesRead;
						while ((bytesRead = inStream.read(buffer)) != -1) {
							outStream.write(buffer, 0, bytesRead);
						}
						outStream.close();
						inStream.close();
			    	}
					    
					xmlManager.tell(new LoadFile(fileName), self());
				})
				.matchAny(o -> log.info("received unknown message"))
				.build();
	}

}
