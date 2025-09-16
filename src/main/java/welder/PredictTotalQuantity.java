
package welder;

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.util.concurrent.AbstractExecutionThreadService;

import utils.Initializable;
import utils.Throwables;

import mdt.aas.DataTypes;
import mdt.client.HttpMDTManager;
import mdt.model.expr.MDTExpressionParser;
import mdt.model.instance.MDTInstanceManager;
import mdt.model.sm.ref.MDTElementReference;
import mdt.model.sm.variable.Variables;
import mdt.task.TaskException;
import mdt.task.builtin.AASOperationTask;
import mdt.task.builtin.TaskUtils;
import mdt.workflow.model.TaskDescriptor;
import mdt.workflow.model.TaskDescriptors;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name = "predictTotalQuantity",
		mixinStandardHelpOptions = true,
		description = "Predicts total quantity based on the quantity produced")
class PredictTotalQuantity extends AbstractExecutionThreadService implements Initializable {
    private static final Logger s_logger = LoggerFactory.getLogger(PredictTotalQuantity.class);
    private static final String OP_POLL_INTERVAL = "1s";
    private static final String OP_TIMEOUT = "1m";
    private static final String DEFAULT_BROKER_URL = "tcp://localhost:1883";
    private static final String DEFAULT_SUBMODEL_IDSHORT = "TotalQuantityPrediction";
    private static final String CLIENT_ID = DEFAULT_SUBMODEL_IDSHORT;
    
	private final MDTInstanceManager m_manager;
	private final JsonMapper m_mapper;
	
	@Option(names={"--brokerUrl"}, paramLabel="url", defaultValue=DEFAULT_BROKER_URL,
			description="MQTT broker URL (default: ${DEFAULT-VALUE})")
	private String m_brokerUrl;
	
	@Option(names = {"--topic", "-t" }, paramLabel = "topic",
			description = "MQTT topic to subscribe to for image upload (default: \"mdt/<inst-id>/parameters/QuantityProduced\")")
	private String m_topic;
	
	@Option(names = {"--clientId" }, paramLabel = "id", defaultValue = CLIENT_ID,
			description = "MQTT client ID (default: ${DEFAULT-VALUE})")
	private String m_clientId;
	
	@Option(names = {"--instance" }, paramLabel = "id", required=true,
			description = "Target MDTInstance id")
	private String m_instanceId;
	
	@Option(names = {"--submodel" }, paramLabel = "idShort", defaultValue = DEFAULT_SUBMODEL_IDSHORT,
			description = "Target Submodel idShort (default: ${DEFAULT-VALUE})")
	private String m_submodelIdShort;
	
	@Option(names = {"--interval" }, paramLabel = "count", type=Integer.class, defaultValue="5",
			description = "Total quantity prediction interval (default: ${DEFAULT-VALUE})")
	private int m_interval;
	
	private TaskDescriptor m_taskDescriptor;
	private int m_quantityProduced = -1;
	
	private PredictTotalQuantity(MDTInstanceManager manager) {
		m_manager = manager;
		m_mapper = JsonMapper.builder()
							.findAndAddModules()
							.addModule(new JavaTimeModule())
							.build();
	}

	@Override
	public void initialize() throws Exception {
		String smRef = String.format("%s:%s", m_instanceId, m_submodelIdShort);
		String opExpr = String.format("%s:Operation", smRef);
		String paramNozzleProduction = String.format("param:%s:NozzleProduction", m_instanceId);
		String paramTotalQuantityPrediction = String.format("param:%s:TotalQuantityPrediction", m_instanceId);
		
		MDTElementReference opRef = MDTExpressionParser.parseElementReference(opExpr).evaluate();
		m_taskDescriptor = TaskDescriptors.aasOperationTaskBuilder()
						                .id("ProductivityPrediction")
										.operationRef(opRef)
										.pollInterval(OP_POLL_INTERVAL)
										.timeout(OP_TIMEOUT)
										.addOption("loglevel", "info")
										.addLabel(TaskUtils.LABEL_MDT_OPERATION, smRef)
										.addInputVariable(Variables.newInstance("NozzleProduction", "",
																				paramNozzleProduction))
										.addOutputVariable(Variables.newInstance("TotalQuantityPrediction", "",
																				paramTotalQuantityPrediction))
										.build();
	}

	@Override
	public void destroy() throws Exception { }

	@Override
	protected void run() throws MqttException {
		if ( m_topic == null ) {
			m_topic = String.format("mdt/%s/parameters/QuantityProduced", m_instanceId);
		}
		
        // Set connection options
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);
        options.setAutomaticReconnect(true);
        
        try ( MqttClient client = new MqttClient(m_brokerUrl, m_clientId, new MemoryPersistence()) ) {	
	        // Set callback for message arrival and connection loss
	        client.setCallback(new MqttCallback() {
	            @Override
	            public void connectionLost(Throwable cause) {
	        		if ( s_logger.isInfoEnabled() ) {
	        			s_logger.info("MQTT broker disconnected");
	        		}
	            }
	
	            @Override
	            public void messageArrived(String topic, MqttMessage msg) throws Exception {
	        		String payload = new String(msg.getPayload());
	        		if ( s_logger.isDebugEnabled() ) {
	                    s_logger.debug("Message arrived on topic[{}]: {}", topic, payload);
	                }
	        		
	        		String count = m_mapper.readTree(payload).asText();
	        		int value = DataTypes.INT.parseValueString(count);
					if ( value <= m_quantityProduced ) {
						return;
					}

					m_quantityProduced = value;
	        		try {
	        			predictTotalQuantity();
	        		}
	        		catch ( Throwable e ) {
	        			Throwable cause = Throwables.unwrapThrowable(e);
	        			System.out.println("Failed to invoke predictTotalQuantity: cause=" + cause);
	        		}
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
	
	private void predictTotalQuantity()
		throws CancellationException, TimeoutException, InterruptedException, TaskException {
		AASOperationTask task = new AASOperationTask(m_taskDescriptor);
		task.run(m_manager);
	}

    public static void main(String[] args) throws Exception {
		HttpMDTManager mdt = HttpMDTManager.connectWithDefault();
		
		PredictTotalQuantity app = new PredictTotalQuantity(mdt.getInstanceManager());
		CommandLine commandLine = new CommandLine(app)
									.setCaseInsensitiveEnumValuesAllowed(true)
									.setUsageHelpWidth(110);
		try {
			CommandLine.ParseResult parseResult = commandLine.parseArgs(args);
			if ( parseResult.isUsageHelpRequested() ) {
				commandLine.usage(System.out);
			}
			else {
				app.initialize();
				app.startAsync().awaitTerminated();
			}
		}
		catch ( Throwable e ) {
			System.err.println(e);
			commandLine.usage(System.out, Ansi.OFF);
		}
    }
}
