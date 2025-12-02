package lg.inspector;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;

import java.io.File;
import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import utils.async.AbstractLoopThreadService;
import utils.io.FileUtils;

import mdt.client.HttpMDTManager;
import mdt.model.sm.ref.ElementReferences;
import mdt.model.sm.ref.MDTElementReference;

import picocli.CommandLine.Option;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
class UpperImageUploader extends AbstractLoopThreadService {
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
		
		m_fileRef = fileRef;
		m_fileRef.activate(mdt.getInstanceManager());
		
		if ( !targetDir.exists() ) {
			try {
				FileUtils.createDirectory(targetDir);
			}
			catch ( IOException e ) {
				throw new IllegalArgumentException("Failed to create target directory: " + targetDir, e);
			}
		}
		m_imageDir = targetDir.toPath();
	}

	@Override
	protected void triggerShutdown() {
		System.out.printf("Shutting-down %s...%n", getClass().getSimpleName());
		markStopRequested();
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
				if ( isStopRequested() ) {
					watchService.close();
					break;
				}
				
				WatchKey key;
				try {
					key = watchService.poll(1, TimeUnit.SECONDS);	// poll any events are available
				}
				catch ( InterruptedException e ) {
					watchService.close();
					Thread.currentThread().interrupt();
					break;
				}
				catch ( ClosedWatchServiceException e ) {
					s_logger.info("Watch service closed: " + e.getMessage());
					break;
				}

				if ( key != null ) {
					for ( WatchEvent<?> event : key.pollEvents() ) {
						if ( event.kind() == ENTRY_CREATE ) {
							@SuppressWarnings("unchecked")
							Path filename = ((WatchEvent<Path>) event).context();
							Path fullPath = m_imageDir.resolve(filename);
	
							s_logger.info("Uploading image file: " + fullPath.toAbsolutePath());
							m_fileRef.updateAttachment(fullPath.toFile());
						}
					}
	
					if ( !key.reset() ) {
						s_logger.warn("Directory is no longer accessible: " + m_imageDir.toAbsolutePath());
						break;
					}
				}
			}
			
			s_logger.info("Stopped watching directory: " + m_imageDir.toAbsolutePath());
		}
		catch ( IOException e ) {
			s_logger.error("Failed to create watch service: " + e.getMessage());
		}
	}
}
