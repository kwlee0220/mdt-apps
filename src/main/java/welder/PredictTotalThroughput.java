
package welder;

import java.time.Duration;
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

import utils.Throwables;

import mdt.aas.DataTypes;
import mdt.client.HttpMDTManager;
import mdt.model.expr.MDTExprParser;
import mdt.model.instance.MDTInstanceManager;
import mdt.model.sm.ref.MDTElementReference;
import mdt.model.sm.value.ElementCollectionValue;
import mdt.model.sm.value.PropertyValue;
import mdt.model.sm.variable.Variables;
import mdt.task.TaskException;
import mdt.task.builtin.AASOperationTask;
import mdt.task.builtin.TaskUtils;
import mdt.workflow.model.Options;
import mdt.workflow.model.TaskDescriptor;
import mdt.workflow.model.TaskDescriptors;

import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
class PredictTotalThroughput extends AbstractExecutionThreadService {
    private static final Logger s_logger = LoggerFactory.getLogger(PredictTotalThroughput.class);
    private static final String BROKER_URL = "tcp://localhost:1883";
    private static final String TOPIC = "mdt/welder/parameters/QuantityProduced";
    private static final String CLIENT_ID = "PredictTotalThroughput";
    private static final String OP_REF = "welder:ProductivityPrediction:Operation";
	
	@Option(names={"--brokerUrl"}, paramLabel="url", defaultValue=BROKER_URL,
			description="MQTT broker URL (default: ${DEFAULT-VALUE})")
	private String m_brokerUrl;
	
	@Option(names = {"--topic", "-t" }, paramLabel = "topic", defaultValue = TOPIC,
			description = "MQTT topic to subscribe to for image upload (default: ${DEFAULT-VALUE})")
	private String m_topic;
	
	@Option(names = {"--clientId" }, paramLabel = "id", defaultValue = CLIENT_ID,
			description = "MQTT client ID (default: ${DEFAULT-VALUE})")
	private String m_clientId;
	
	@Option(names = {"--operation" }, paramLabel = "op-ref", defaultValue=OP_REF,
			description = "AAS Operation reference to invoke (default: ${DEFAULT-VALUE})")
	private String m_opExpr;
	
	@Option(names = {"--interval" }, paramLabel = "count", type=Integer.class, defaultValue="5",
			description = "Total throughput prediction interval")
	private int m_interval;
	
	private final MDTInstanceManager m_manager;
	private final JsonMapper m_mapper = JsonMapper.builder().findAndAddModules().build();
    
	public PredictTotalThroughput(HttpMDTManager mdt) {
		m_manager = mdt.getInstanceManager();
	}
	
	private int m_quantityProduced = -1;
	private int m_lastStep = 0;
	
	private static JsonMapper s_mapper;
	static {
		s_mapper = JsonMapper.builder()
							.findAndAddModules()
							.addModule(new JavaTimeModule())
							.build();
	}
	
	public PredictTotalThroughput(HttpMDTManager mdt, String brokerUrl, String topic, String clientId,
									String opExpr) {
		m_brokerUrl = brokerUrl;
		m_topic = topic;
		m_clientId = clientId;
		m_manager = mdt.getInstanceManager();
		m_opExpr = opExpr;
	}
	
	private void predictTotalThroughput()
		throws CancellationException, TimeoutException, InterruptedException, TaskException {
		String expr = String.format("welder:ProductivityPrediction:Operation");
		MDTElementReference ref = MDTExprParser.parseElementReference(expr).evaluate();
		TaskDescriptor descriptor
				= TaskDescriptors.aasOperationTaskBuilder()
				                .id("ProductivityPrediction")
								.operationRef(ref)
								.pollInterval("1s")
								.timeout("1m")
								.addOption(Options.newOption("loglevel", "info"))
								.addLabel(TaskUtils.LABEL_MDT_OPERATION, "welder:ProductivityPrediction")
								.addInputVariable(Variables.newInstance("Timestamp", "",
															"param:welder:NozzleProduction:EventDateTime"))
								.addInputVariable(Variables.newInstance("NozzleProduction", "",
															"param:welder:NozzleProduction:ParameterValue"))
								.addOutputVariable(Variables.newInstance("TotalThroughput", "",
															"param:welder:TotalThroughput"))
								.build();
		AASOperationTask task = new AASOperationTask(descriptor);
		task.run(m_manager);
	}

	@Override
	protected void run() throws MqttException {
        // Set connection options
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);
        options.setAutomaticReconnect(true);
        
        try ( MqttClient client = new MqttClient(m_brokerUrl, m_clientId, new MemoryPersistence()) ) {	
	        // Set callback for message arrival and connection loss
	        client.setCallback(new MqttCallback() {
	            @Override
	            public void connectionLost(Throwable cause) {
	        		if ( s_logger.isDebugEnabled() ) {
	        			s_logger.debug("MQTT broker disconnected");
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
	        			predictTotalThroughput();
	        		}
	        		catch ( Throwable e ) {
	        			Throwable cause = Throwables.unwrapThrowable(e);
	        			System.out.println("Failed to invoke predictTotalThroughput: cause=" + cause);
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
	
	private static int getIntField(ElementCollectionValue val, String field) {
		PropertyValue pv = (PropertyValue)val.getField(field);
		return (int)pv.get();
	}
	private static Duration getDurationField(ElementCollectionValue val, String field) {
		PropertyValue pv = (PropertyValue)val.getField(field);
		return (Duration)pv.get();
	}
	private static Float getFloatField(ElementCollectionValue val, String field) {
		PropertyValue pv = (PropertyValue)val.getField(field);
		return (float)pv.get();
	}

    public static void main(String[] args) throws Exception {
		HttpMDTManager mdt = HttpMDTManager.connectWithDefault();
		
    	PredictTotalThroughput companion = new PredictTotalThroughput(mdt);
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
