
package lg.inspector;

import java.io.File;
import java.io.IOException;
import java.time.Duration;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.util.concurrent.AbstractService;

import utils.func.FOption;

import mdt.client.HttpMDTManager;
import mdt.client.support.MqttBrokerConfig;
import mdt.client.support.MqttService;
import mdt.client.support.MqttService.Subscriber;
import mdt.workflow.Workflow;
import mdt.workflow.WorkflowListener;
import mdt.workflow.WorkflowManager;
import mdt.workflow.model.WorkflowStatusMonitor;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
class SurfaceInspectionInvoker extends AbstractService implements Subscriber {
    private static final Logger s_logger = LoggerFactory.getLogger(SurfaceInspectionInvoker.class);
	public static final JsonMapper MAPPER = JsonMapper.builder()
														.addModule(new JavaTimeModule())
														.findAndAddModules()
														.build();
    
    private static final String TOPIC = "mdt/inspector/parameters/UpperImage";
    private static final Duration WF_STATUS_POLL_INTERVAL = Duration.ofSeconds(3);

	private final MqttService m_mqttService;
    private final WorkflowManager m_wfMgr;
	private String m_workflowTemplateId;

    private WorkflowStatusMonitor m_statusMonitor;
	
	public SurfaceInspectionInvoker(HttpMDTManager mdt, @Nullable File confPath, @Nullable String topic,
									String wfTemplateId)
		throws IOException {
		MqttBrokerConfig brokerConf = (confPath != null)
									? MAPPER.readerFor(MqttBrokerConfig.class).readValue(confPath)
									: MqttBrokerConfig.DEFAULT;
		m_mqttService = new MqttService(brokerConf);
		m_mqttService.subscribe(FOption.getOrElse(topic, TOPIC), this);
		
		m_wfMgr = mdt.getWorkflowManager();
		m_workflowTemplateId = wfTemplateId;
	}

	@Override
	protected void doStart() {
		m_mqttService.startAsync();
	}

	@Override
	protected void doStop() {
		m_mqttService.stopAsync();
	}

	@Override
	public void onMessage(String topic, MqttMessage message) {
		String payload = new String(message.getPayload());
        s_logger.debug("Message arrived on topic[{}]: {}", topic, payload);

		s_logger.info("Starting a workflow: {}", m_workflowTemplateId);
		try {
			Workflow workflow = m_wfMgr.startWorkflow(m_workflowTemplateId);
			m_statusMonitor = new WorkflowStatusMonitor(m_wfMgr, workflow.getName(), m_wfListener,
														WF_STATUS_POLL_INTERVAL, false);
			m_statusMonitor.setLogger(s_logger);
			m_statusMonitor.start();
		}
		catch ( Exception e ) {
			System.out.printf("Failed to start workflow '%s': %s%n", m_workflowTemplateId, e.getMessage());
		}
	}
	
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
		
    	SurfaceInspectionInvoker companion = new SurfaceInspectionInvoker(mdt, null, null,
    																		"lgrefridge-process-optimization");
    	companion.startAsync();
    	companion.awaitTerminated();
    }
}
