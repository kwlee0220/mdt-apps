
package lg.inspector;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AbstractExecutionThreadService;

import mdt.client.HttpMDTManager;
import mdt.workflow.Workflow;
import mdt.workflow.WorkflowManager;

import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
class SurfaceInspectionStarter extends AbstractExecutionThreadService {
    private static final Logger s_logger = LoggerFactory.getLogger(SurfaceInspectionStarter.class);
    private static final String BROKER_URL = "tcp://localhost:1883";
    private static final String TOPIC = "mdt/inspector/parameters/UpperImage";
    private static final String CLIENT_ID = "UpperImageSubscriber";
    private static final String WORKFLOW_TEMPLATE_ID = "thickness-simulation-short";
	
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

    private final WorkflowManager m_wfMgr;
    
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
	protected void run() throws MqttException {
        try ( MqttClient client = new MqttClient(m_brokerUrl, m_clientId, new MemoryPersistence()) ) {
	        // Set connection options
	        MqttConnectOptions options = new MqttConnectOptions();
	        options.setCleanSession(true);
	        options.setAutomaticReconnect(true);
	
	        // Set callback for message arrival and connection loss
	        client.setCallback(new MqttCallback() {
	            @Override
	            public void connectionLost(Throwable cause) {
        			s_logger.debug("MQTT broker disconnected");
	            }
	
	            @Override
	            public void messageArrived(String topic, MqttMessage msg) throws Exception {
	        		String payload = new String(msg.getPayload());
                    s_logger.debug("Message arrived on topic[{}]: {}", topic, payload);

        			s_logger.debug("Starting a workflow: {}", m_workflowTemplateId);
	        		Workflow workflow = m_wfMgr.startWorkflow(m_workflowTemplateId);
	        		System.out.println(workflow);
	            }
	
	            @Override
	            public void deliveryComplete(IMqttDeliveryToken token) {}
	        });
	
	        // Connect to broker
	        client.connect(options);
	
	        // Subscribe to topic
	        client.subscribe(m_topic);
	
	        System.out.printf("Subscribed to topic[%s] at MQTT broker %s%n", m_topic, m_brokerUrl);
	        System.out.println("Press Ctrl+C to exit");

        // Keep the application running
	        while (true) {
	            Thread.sleep(1000);
	        }
	    }
        catch ( MqttException e ) {
	        s_logger.error("MQTT error: {}", e.getMessage(), e);
	        System.err.println("MQTT error: " + e.getMessage());
	    }
		catch ( InterruptedException e ) {
			Thread.currentThread().interrupt();
			s_logger.info("Subscriber interrupted");
		}
	}

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
