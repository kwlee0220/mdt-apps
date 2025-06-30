package lg.inspector;

import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AbstractService;

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
public class InspectorCompanion extends AbstractService{
	private static final Logger s_logger = LoggerFactory.getLogger(UpperImageUploader.class);
    private static final String BROKER_URL = "tcp://localhost:1883";
    private static final String TOPIC = "mdt/inspector/parameters/UpperImage";
    private static final String CLIENT_ID = "UpperImageSubscriber";
    private static final String WORKFLOW_TEMPLATE_ID = "thickness-simulation-short";
	
	@Option(names={"--dir"}, paramLabel="directory", required=true,
			description="Target image directory to watch for new images")
	private Path m_imageDir;
	
	@Option(names={"--fileRef"}, paramLabel="ref", required=true,
			description="File reference to upload images to")
	public void setFileRef(String fileRefExpr) {
		m_fileRef = ElementReferences.parseExpr(fileRefExpr);
	}
	private MDTElementReference m_fileRef;
	
	@Option(names={"--brokerUrl"}, paramLabel="url", defaultValue=BROKER_URL,
			description="MQTT broker URL (default: ${DEFAULT-VALUE})")
	private String m_brokerUrl;
	
	@Option(names = {"--topic", "-t" }, paramLabel = "topic", defaultValue = TOPIC,
			description = "MQTT topic to subscribe to for image upload (default: ${DEFAULT-VALUE})")
	private String m_topic;
	
	@Option(names = {"--clientId" }, paramLabel = "id", defaultValue = CLIENT_ID,
			description = "MQTT client ID (default: ${DEFAULT-VALUE})")
	private String m_clientId;
	
	@Option(names = {"--workflow", "-w" }, paramLabel="template-id", defaultValue=WORKFLOW_TEMPLATE_ID,
			description = "Workflow template ID to use for image processing (default: ${DEFAULT-VALUE})")
	private String m_workflowTemplateId;
	
	private SurfaceInspectionStarter m_starter;
	private UpperImageUploader m_uploader;

	@Override
	protected void doStart() {
		HttpMDTManager mdt = HttpMDTManager.connectWithDefault();
		
		m_starter = new SurfaceInspectionStarter(mdt, m_brokerUrl, m_topic, m_clientId, m_workflowTemplateId);
		m_starter.startAsync();

		m_uploader = new UpperImageUploader(mdt, m_fileRef, m_imageDir.toFile());
		m_uploader.startAsync();
	}

	@Override
	protected void doStop() {
		m_uploader.stopAsync();
		m_starter.stopAsync();
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
			
			companion.startAsync().awaitTerminated();
		}
		catch ( Throwable e ) {
			System.err.println(e);
			commandLine.usage(System.out, Ansi.OFF);
		}
	}
}
