package lg.inspector;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractExecutionThreadService;

import mdt.client.HttpMDTManager;
import mdt.model.sm.ref.ElementReferences;
import mdt.model.sm.ref.MDTElementReference;

import picocli.CommandLine.Option;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
class UpperImageUploader extends AbstractExecutionThreadService {
	private static final Logger s_logger = LoggerFactory.getLogger(UpperImageUploader.class);
	
	@Option(names={"--dir"}, paramLabel="directory",  description="Target image directory to watch for new images")
	private Path m_imageDir;
	
	@Option(names={"--fileRef"}, paramLabel="ref",  description="File reference to upload images to")
	public void setFileRef(String fileRefExpr) {
		m_fileRef = ElementReferences.parseExpr(fileRefExpr);
	}
	private MDTElementReference m_fileRef;
	
	UpperImageUploader() { }
	UpperImageUploader(HttpMDTManager mdt, MDTElementReference fileRef, File targetDir) {
		Preconditions.checkArgument(fileRef != null, "AASFile Reference is null");
		Preconditions.checkArgument(targetDir != null, "targetDir is null");
		Preconditions.checkArgument(Files.isDirectory(targetDir.toPath()), "targetDir is not a directory");
		
		m_fileRef = fileRef;
		m_fileRef.activate(mdt.getInstanceManager());
		
		m_imageDir = targetDir.toPath();
	}

	@Override
	protected void run() throws Exception {
		if ( !Files.isDirectory(m_imageDir) ) {
			throw new IllegalArgumentException("Target directory is not a directory: " + m_imageDir.toAbsolutePath());
		}
		
		try {
			WatchService watchService = FileSystems.getDefault().newWatchService();
			m_imageDir.register(watchService, ENTRY_CREATE);

			if ( s_logger.isInfoEnabled() ) {
				s_logger.info("Watching directory: " + m_imageDir.toAbsolutePath());
			}

			while ( true ) {
				WatchKey key;
				try {
					key = watchService.take();	// Blocks until events are available
				}
				catch ( InterruptedException e ) {
					Thread.currentThread().interrupt();
					return;
				}

				for ( WatchEvent<?> event : key.pollEvents() ) {
					if ( event.kind() == ENTRY_CREATE ) {
						@SuppressWarnings("unchecked")
						Path filename = ((WatchEvent<Path>) event).context();
						Path fullPath = m_imageDir.resolve(filename);

						if ( s_logger.isInfoEnabled() ) {
							s_logger.info("Uploading image file: " + fullPath.toAbsolutePath());
						}
						m_fileRef.uploadFile(fullPath.toFile());
					}
				}

				if ( !key.reset() ) {
					s_logger.warn("Directory is no longer accessible: " + m_imageDir.toAbsolutePath());
					break;
				}
			}
		}
		catch ( IOException e ) {
			s_logger.error("Failed to create watch service: " + e.getMessage());
		}
	}
}
