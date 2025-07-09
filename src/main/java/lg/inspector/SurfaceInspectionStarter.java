
package lg.inspector;

import java.time.Duration;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.Throwables;
import utils.async.AbstractLoopThreadService;

import mdt.client.HttpMDTManager;
import mdt.workflow.Workflow;
import mdt.workflow.WorkflowListener;
import mdt.workflow.WorkflowManager;
import mdt.workflow.model.WorkflowStatusMonitor;

import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
class SurfaceInspectionStarter extends AbstractLoopThreadService {
    private static final Logger s_logger = LoggerFactory.getLogger(SurfaceInspectionStarter.class);
    
    private static final String BROKER_URL = "tcp://localhost:1883";
    private static final String TOPIC = "mdt/inspector/parameters/UpperImage";
    private static final String CLIENT_ID = "UpperImageSubscriber";
    private static final String WORKFLOW_TEMPLATE_ID = "thickness-simulation-short";
    private static final Duration DEFAULT_STATUS_POLL_INTERVAL = Duration.ofSeconds(3);
	
	@Option(names={"--brokerUrl"}, paramLabel="url", defaultValue=BROKER_URL,
			description="MQTT broker URL (default: ${DEFAULT-VALUE})")
	private String m_brokerUrl;
	
	@Option(names = {"--topic", "-t" }, paramLabel = "topic", defaultValue = TOPIC,
			description = "MQTT topic to subscribe to for image upload (default: ${DEFAULT-VALUE})")
	private String m_topic;
	
	@Option(names = {"--clientId" }, paramLabel = "id", defaultValue = CLIENT_ID,
			description = "MQTT client ID (default: ${DEFAULT-VALUE})")
	private String m_clientId;
	
	@Option(names = {"--workflow", "-w" }, paramLabel = "template-id", defaultValue=WORKFLOW_TEMPLATE_ID,
			description = "Workflow template ID to use for image processing (default: ${DEFAULT-VALUE})")
	private String m_workflowTemplateId;

	private MqttClient m_mqttClient;
    private final WorkflowManager m_wfMgr;
    private WorkflowStatusMonitor m_statusMonitor;
    
	public SurfaceInspectionStarter(HttpMDTManager mdt) {
		m_wfMgr = mdt.getWorkflowManager();
	}
	
	public SurfaceInspectionStarter(HttpMDTManager mdt, String brokerUrl, String topic, String clientId,
									String wfTemplateId) {
		m_brokerUrl = brokerUrl;
		m_topic = topic;
		m_clientId = clientId;
		m_wfMgr = mdt.getWorkflowManager();
		m_workflowTemplateId = wfTemplateId;
	}

	@Override
	protected void triggerShutdown() {
		System.out.printf("Stopping %s...%n", getClass().getSimpleName());
		markStopRequested();
	}

	@Override
	protected void run() throws MqttException {
        // Set connection options
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);
        options.setAutomaticReconnect(true);
        
		m_mqttClient = new MqttClient(m_brokerUrl, m_clientId, new MemoryPersistence());
        m_mqttClient.setCallback(m_mqttCallback);
        
        try {
	        // Connect to broker
	        m_mqttClient.connect(options);
	
	        System.out.println("Press Ctrl+C to exit");

	        // Keep the application running
	        awaitUntilStopRequested();
	    }
        catch ( MqttException e ) {
	        s_logger.error("MQTT error: {}", e.getMessage(), e);
	        System.err.println("MQTT error: " + e.getMessage());
	    }
		catch ( InterruptedException e ) {
			Thread.currentThread().interrupt();
	        s_logger.info("[%s] Interrupted", getClass().getSimpleName());
		}
        finally {
        	m_mqttClient.close(true);
//        	m_mqttClient.disconnectForcibly();
        }
	}
	
	private final MqttCallback m_mqttCallback = new MqttCallbackExtended() {
		@Override
		public void connectionLost(Throwable cause) {
			cause = Throwables.unwrapThrowable(cause);
			s_logger.debug("MQTT broker disconnected: cause={}", ""+ cause);
		}

		@Override
		public void messageArrived(String topic, MqttMessage message) throws Exception {
    		String payload = new String(message.getPayload());
            s_logger.debug("Message arrived on topic[{}]: {}", topic, payload);

			s_logger.info("Starting a workflow: {}", m_workflowTemplateId);
    		try {
				Workflow workflow = m_wfMgr.startWorkflow(m_workflowTemplateId);
				m_statusMonitor = new WorkflowStatusMonitor(m_wfMgr, workflow.getName(), m_wfListener,
															DEFAULT_STATUS_POLL_INTERVAL, false);
				m_statusMonitor.setLogger(s_logger);
				m_statusMonitor.start();
			}
			catch ( Exception e ) {
				System.out.printf("Failed to start workflow '%s': %s%n", m_workflowTemplateId, e.getMessage());
			}
		}

		@Override
		public void connectComplete(boolean reconnect, String serverURI) {
			if ( reconnect ) {
				s_logger.info("[%s] reconnected to MQTT broker: serverURI={} -> try resubscribe to topic[%s]",
								getClass().getSimpleName(), serverURI, m_topic);
			}
	
	        try {
				// Subscribe to topic
				m_mqttClient.subscribe(m_topic);
				s_logger.info("Subscribed to topic[{}] at MQTT broker {}", m_topic, m_brokerUrl);
			}
			catch ( MqttException e ) {
				s_logger.error("Failed to resubscribe to topic[{}]: {}", m_topic, e.getMessage());
			}
		}

		@Override
		public void deliveryComplete(IMqttDeliveryToken token) {}
	};
	
	private WorkflowListener m_wfListener = new WorkflowListener() {
		@Override
		public void onWorkflowCompleted(String wfName) {
			System.out.printf("Workflow completed: name=%s%n", wfName);
		}
		
		@Override
		public void onWorkflowFailed(String wfName) {
			System.out.printf("Workflow failed: name=%s%n", wfName);
		}

		@Override
		public void onWorkflowStarting(String wfName) {
			System.out.println("Workflow starting: name=" + wfName);
		}

		@Override
		public void onWorkflowStarted(String wfName) {
			System.out.println("Workflow running: name=" + wfName);
		}
	};

    public static void main(String[] args) throws Exception {
		HttpMDTManager mdt = HttpMDTManager.connectWithDefault();
		
    	SurfaceInspectionStarter companion = new SurfaceInspectionStarter(mdt);
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
