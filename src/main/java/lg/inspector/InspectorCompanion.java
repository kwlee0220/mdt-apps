package lg.inspector;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;

import mdt.client.HttpMDTManager;
import mdt.model.sm.ref.ElementReferences;
import mdt.model.sm.ref.MDTElementReference;

import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class InspectorCompanion implements Runnable {
	private static final Logger s_logger = LoggerFactory.getLogger(UpperImageUploader.class);
    private static final String MQTT_BROKER_CONFIG_FILE = "mqtt_broker_config.json";
    private static final String TOPIC = "mdt/inspector/parameters/UpperImage";
	
	@Option(names={"--imageDir"}, paramLabel="directory", required=true,
			description="Target image directory to watch for new images")
	private Path m_imageDir;
	
	@Option(names={"--fileRef"}, paramLabel="ref", required=true, description="File reference to upload images to")
	public void setFileRef(String fileRefExpr) {
		m_fileRef = ElementReferences.parseExpr(fileRefExpr);
	}
	private MDTElementReference m_fileRef;
	
	@Option(names={"--mqttConf"}, paramLabel="path", defaultValue=MQTT_BROKER_CONFIG_FILE,
			description="MQTT broker config file path (default: ${DEFAULT-VALUE})")
	private File m_mqttConfigPath;
	
	@Option(names = {"--topic", "-t" }, paramLabel = "topic", defaultValue = TOPIC,
			description = "MQTT topic to subscribe to for image upload (default: ${DEFAULT-VALUE})")
	private String m_topic;
	
	@Option(names = {"--workflow", "-w" }, paramLabel="template-id", required=true,
			description = "Workflow model ID to use for image processing")
	private String m_workflowTemplateId;
	
	@Override
	public void run() {
		try {
			runChecked();
		}
		catch ( Exception e ) {
			System.err.println("Error running InspectorCompanion: " + e.getMessage());
			s_logger.error("Error running InspectorCompanion: " + e.getMessage(), e);
			System.exit(1);
		}
	}
	
	private void runChecked() throws IOException {
		HttpMDTManager mdt = HttpMDTManager.connectWithDefault();
		
		SurfaceInspectionInvoker invoker = new SurfaceInspectionInvoker(mdt, m_mqttConfigPath, m_topic,
																		m_workflowTemplateId);
		UpperImageUploader uploader = new UpperImageUploader(mdt, m_fileRef, m_imageDir.toFile());
		
		ServiceManager manager = new ServiceManager(Arrays.asList(invoker, uploader));
		manager.addListener(new ServiceManager.Listener() {
			@Override
			public void failure(Service service) {
				s_logger.error("Service failed: " + service.failureCause());
			}

			@Override
			public void healthy() {
				s_logger.info("All services are healthy.");
			}
		}, Runnable::run);
		
		manager.startAsync().awaitHealthy();
		System.out.println("All services started successfully.");
		
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			try {
				manager.stopAsync().awaitStopped(Duration.ofSeconds(10));
				System.out.println("All services stopped successfully.");
			}
			catch ( Exception e ) {
				s_logger.error("Error stopping services: " + e.getMessage(), e);
			}
		}));
		
		manager.awaitStopped();
	}
	
	public static final void main(String... args) throws Exception {
		InspectorCompanion companion = new InspectorCompanion();
		CommandLine commandLine = new CommandLine(companion)
										.setCaseInsensitiveEnumValuesAllowed(true)
										.setUsageHelpWidth(110);
		try {
			commandLine.parseArgs(args);
			if ( commandLine.isUsageHelpRequested() ) {
				commandLine.usage(System.out, Ansi.OFF);
			}
			
			companion.run();
		}
		catch ( Throwable e ) {
			System.err.println(e);
			commandLine.usage(System.out, Ansi.OFF);
		}
	}
}
