/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.workflow.timer;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.broker.test.EmbeddedBrokerRule;
import io.zeebe.exporter.record.Assertions;
import io.zeebe.exporter.record.Record;
import io.zeebe.exporter.record.value.TimerRecordValue;
import io.zeebe.exporter.record.value.WorkflowInstanceRecordValue;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.intent.TimerIntent;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import io.zeebe.test.broker.protocol.clientapi.ClientApiRule;
import io.zeebe.test.broker.protocol.clientapi.PartitionTestClient;
import io.zeebe.test.util.Strings;
import io.zeebe.test.util.record.RecordingExporter;
import io.zeebe.test.util.record.RecordingExporterTestWatcher;
import java.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

public class TimerCatchEventTest {
  private static EmbeddedBrokerRule brokerRule = new EmbeddedBrokerRule();
  private static ClientApiRule apiRule = new ClientApiRule(brokerRule::getClientAddress);
  @ClassRule public static RuleChain ruleChain = RuleChain.outerRule(brokerRule).around(apiRule);

  @Rule
  public RecordingExporterTestWatcher recordingExporterTestWatcher =
      new RecordingExporterTestWatcher();

  private static final String SINGLE_TIMER_WORKFLOW_PROCESS_ID = "single-timer-workflow";
  private static final BpmnModelInstance SINGLE_TIMER_WORKFLOW =
      Bpmn.createExecutableProcess(SINGLE_TIMER_WORKFLOW_PROCESS_ID)
          .startEvent()
          .intermediateCatchEvent("timer", c -> c.timerWithDuration("PT0.1S"))
          .endEvent()
          .done();

  private static final String BOUNDARY_EVENT_WORKFLOW_PROCESS_ID = "boundary-event-workflow";
  private static final BpmnModelInstance BOUNDARY_EVENT_WORKFLOW =
      Bpmn.createExecutableProcess(BOUNDARY_EVENT_WORKFLOW_PROCESS_ID)
          .startEvent()
          .serviceTask("task", b -> b.zeebeTaskType("type"))
          .boundaryEvent("timer")
          .cancelActivity(true)
          .timerWithDuration("PT1S")
          .endEvent("eventEnd")
          .moveToActivity("task")
          .endEvent("taskEnd")
          .done();

  private static final String TWO_REPS_CYCLE_WORKFLOW_PROCESS_ID = "two-reps-cycle-workflow";
  private static final BpmnModelInstance TWO_REPS_CYCLE_WORKFLOW =
      Bpmn.createExecutableProcess(TWO_REPS_CYCLE_WORKFLOW_PROCESS_ID)
          .startEvent()
          .serviceTask("task", b -> b.zeebeTaskType("type"))
          .boundaryEvent("timer")
          .cancelActivity(false)
          .timerWithCycle("R2/PT1S")
          .endEvent()
          .moveToNode("task")
          .endEvent()
          .done();

  private static final String INFINITE_CYCLE_WORKFLOW_PROCESS_ID = "infinite-cycle-workflow";
  private static final BpmnModelInstance INFINITE_CYCLE_WORKFLOW =
      Bpmn.createExecutableProcess(INFINITE_CYCLE_WORKFLOW_PROCESS_ID)
          .startEvent()
          .serviceTask("task", b -> b.zeebeTaskType("type"))
          .boundaryEvent("timer")
          .cancelActivity(false)
          .timerWithCycle("R/PT1S")
          .endEvent()
          .moveToNode("task")
          .endEvent()
          .done();

  private PartitionTestClient testClient;
  private String processId;

  @Before
  public void init() {
    testClient = apiRule.partitionClient();
    processId = Strings.newRandomValidBpmnId();
  }

  @After
  public void tearDown() {
    brokerRule.getClock().reset();
  }

  @Test
  public void testLifeCycle() {
    // given
    final String processId = Strings.newRandomValidBpmnId();
    final BpmnModelInstance workflow =
        Bpmn.createExecutableProcess(processId)
            .startEvent()
            .intermediateCatchEvent("timer", c -> c.timerWithDuration("PT0S"))
            .endEvent()
            .done();

    testClient.deploy(workflow);
    testClient.createWorkflowInstance(r -> r.setBpmnProcessId(processId));

    // then
    assertThat(
            RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_COMPLETED)
                .withElementId(processId)
                .exists())
        .isTrue();

    assertThat(RecordingExporter.workflowInstanceRecords().withElementId("timer").limit(5))
        .extracting(r -> r.getMetadata().getIntent())
        .containsExactly(
            WorkflowInstanceIntent.ELEMENT_ACTIVATING,
            WorkflowInstanceIntent.ELEMENT_ACTIVATED,
            WorkflowInstanceIntent.EVENT_OCCURRED,
            WorkflowInstanceIntent.ELEMENT_COMPLETING,
            WorkflowInstanceIntent.ELEMENT_COMPLETED);

    assertThat(RecordingExporter.timerRecords().limit(4))
        .extracting(r -> r.getMetadata().getIntent())
        .containsExactly(
            TimerIntent.CREATE, TimerIntent.CREATED, TimerIntent.TRIGGER, TimerIntent.TRIGGERED);
  }

  @Test
  public void shouldCreateTimer() {
    // given
    final String processId = Strings.newRandomValidBpmnId();
    final BpmnModelInstance workflow =
        Bpmn.createExecutableProcess(processId)
            .startEvent()
            .intermediateCatchEvent("timer", c -> c.timerWithDuration("PT10S"))
            .endEvent()
            .done();

    testClient.deploy(workflow);
    final long workflowInstanceKey =
        testClient.createWorkflowInstance(r -> r.setBpmnProcessId(processId)).getInstanceKey();

    // when
    final Record<WorkflowInstanceRecordValue> activatedEvent =
        RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_ACTIVATED)
            .withElementId("timer")
            .getFirst();

    // then
    final Record<TimerRecordValue> createdEvent =
        RecordingExporter.timerRecords(TimerIntent.CREATED).getFirst();

    Assertions.assertThat(createdEvent.getValue())
        .hasElementInstanceKey(activatedEvent.getKey())
        .hasWorkflowInstanceKey(workflowInstanceKey);
    assertThat(createdEvent.getValue().getDueDate())
        .isGreaterThan(brokerRule.getClock().getCurrentTimeInMillis());
  }

  @Test
  public void shouldTriggerTimer() {
    // given
    testClient.deploy(SINGLE_TIMER_WORKFLOW);
    testClient.createWorkflowInstance(r -> r.setBpmnProcessId(SINGLE_TIMER_WORKFLOW_PROCESS_ID));

    // when
    final Record<TimerRecordValue> createdEvent =
        RecordingExporter.timerRecords(TimerIntent.CREATED).getFirst();

    brokerRule.getClock().addTime(Duration.ofSeconds(1));

    // then
    final Record<TimerRecordValue> triggeredEvent =
        RecordingExporter.timerRecords(TimerIntent.TRIGGERED).getFirst();

    assertThat(triggeredEvent.getKey()).isEqualTo(createdEvent.getKey());
    assertThat(triggeredEvent.getValue()).isEqualTo(createdEvent.getValue());
    assertThat(Duration.between(createdEvent.getTimestamp(), triggeredEvent.getTimestamp()))
        .isGreaterThanOrEqualTo(Duration.ofMillis(100));
  }

  @Test
  public void shouldCompleteTimerEvent() {
    // given
    testClient.deploy(SINGLE_TIMER_WORKFLOW);
    final String timerId = "timer";
    final long workflowInstanceKey =
        testClient
            .createWorkflowInstance(r -> r.setBpmnProcessId(SINGLE_TIMER_WORKFLOW_PROCESS_ID))
            .getInstanceKey();

    // when
    final Record<WorkflowInstanceRecordValue> activatedEvent =
        RecordingExporter.workflowInstanceRecords()
            .withWorkflowInstanceKey(workflowInstanceKey)
            .withElementId(timerId)
            .withIntent(WorkflowInstanceIntent.ELEMENT_ACTIVATED)
            .limitToWorkflowInstanceCompleted()
            .getFirst();

    brokerRule.getClock().addTime(Duration.ofSeconds(1));

    // then
    final Record<WorkflowInstanceRecordValue> completedEvent =
        RecordingExporter.workflowInstanceRecords()
            .withWorkflowInstanceKey(workflowInstanceKey)
            .withElementId(timerId)
            .withIntent(WorkflowInstanceIntent.ELEMENT_COMPLETED)
            .limitToWorkflowInstanceCompleted()
            .getFirst();

    assertThat(completedEvent.getKey()).isEqualTo(activatedEvent.getKey());
    assertThat(completedEvent.getValue()).isEqualTo(activatedEvent.getValue());
  }

  @Test
  public void shouldTriggerTimerWithZeroDuration() {
    // given
    final String processId = Strings.newRandomValidBpmnId();
    final String timerId = Strings.newRandomValidBpmnId();
    final BpmnModelInstance workflow =
        Bpmn.createExecutableProcess(processId)
            .startEvent()
            .intermediateCatchEvent(timerId, c -> c.timerWithDuration("PT0S"))
            .endEvent()
            .done();

    testClient.deploy(workflow);
    final long workflowInstanceKey =
        testClient.createWorkflowInstance(r -> r.setBpmnProcessId(processId)).getInstanceKey();

    // then
    assertThat(
            RecordingExporter.timerRecords(TimerIntent.TRIGGERED)
                .withWorkflowInstanceKey(workflowInstanceKey)
                .exists())
        .isTrue();
  }

  @Test
  public void shouldTriggerTimerWithNegativeDuration() {
    // given
    final String timerId = Strings.newRandomValidBpmnId();
    final BpmnModelInstance workflow =
        Bpmn.createExecutableProcess(processId)
            .startEvent()
            .intermediateCatchEvent(timerId, c -> c.timerWithDuration("-PT1H"))
            .endEvent()
            .done();

    testClient.deploy(workflow);
    final long workflowInstanceKey =
        testClient.createWorkflowInstance(r -> r.setBpmnProcessId(processId)).getInstanceKey();

    // then
    assertThat(
            RecordingExporter.timerRecords(TimerIntent.TRIGGERED)
                .withWorkflowInstanceKey(workflowInstanceKey)
                .exists())
        .isTrue();
  }

  @Test
  public void shouldTriggerMultipleTimers() {
    // given
    final String processId = Strings.newRandomValidBpmnId();
    final String firstTimerId = Strings.newRandomValidBpmnId();
    final String secondTimerId = Strings.newRandomValidBpmnId();
    final BpmnModelInstance workflow =
        Bpmn.createExecutableProcess(processId)
            .startEvent()
            .parallelGateway()
            .intermediateCatchEvent(firstTimerId, c -> c.timerWithDuration("PT0.1S"))
            .endEvent()
            .moveToLastGateway()
            .intermediateCatchEvent(secondTimerId, c -> c.timerWithDuration("PT0.2S"))
            .endEvent()
            .done();

    testClient.deploy(workflow);
    final long workflowInstanceKey =
        testClient.createWorkflowInstance(r -> r.setBpmnProcessId(processId)).getInstanceKey();

    // when
    brokerRule.getClock().addTime(Duration.ofSeconds(1));

    // then
    final Record<WorkflowInstanceRecordValue> timer1 =
        RecordingExporter.records()
            .limitToWorkflowInstance(workflowInstanceKey)
            .workflowInstanceRecords()
            .withWorkflowInstanceKey(workflowInstanceKey)
            .withElementId(firstTimerId)
            .withIntent(WorkflowInstanceIntent.ELEMENT_COMPLETED)
            .getFirst();
    final Record<WorkflowInstanceRecordValue> timer2 =
        RecordingExporter.records()
            .limitToWorkflowInstance(workflowInstanceKey)
            .workflowInstanceRecords()
            .withWorkflowInstanceKey(workflowInstanceKey)
            .withElementId(secondTimerId)
            .withIntent(WorkflowInstanceIntent.ELEMENT_COMPLETED)
            .getFirst();

    final Record<TimerRecordValue> triggeredTimer1 =
        RecordingExporter.timerRecords(TimerIntent.TRIGGERED)
            .withElementInstanceKey(timer1.getKey())
            .withWorkflowInstanceKey(workflowInstanceKey)
            .getFirst();
    final Record<TimerRecordValue> triggeredTimer2 =
        RecordingExporter.timerRecords(TimerIntent.TRIGGERED)
            .withElementInstanceKey(timer2.getKey())
            .withWorkflowInstanceKey(workflowInstanceKey)
            .getFirst();

    assertThat(triggeredTimer1.getValue().getDueDate())
        .isLessThan(triggeredTimer2.getValue().getDueDate());
  }

  @Test
  public void shouldCancelTimer() {
    // given
    final String processId = Strings.newRandomValidBpmnId();
    final String timerId = Strings.newRandomValidBpmnId();
    final BpmnModelInstance workflow =
        Bpmn.createExecutableProcess(processId)
            .startEvent()
            .parallelGateway()
            .intermediateCatchEvent(timerId, c -> c.timerWithDuration("PT10S"))
            .endEvent()
            .done();

    testClient.deploy(workflow);
    final long workflowInstanceKey =
        testClient.createWorkflowInstance(r -> r.setBpmnProcessId(processId)).getInstanceKey();

    // when
    final Record<TimerRecordValue> createdEvent =
        RecordingExporter.timerRecords(TimerIntent.CREATED)
            .withHandlerNodeId(timerId)
            .withWorkflowInstanceKey(workflowInstanceKey)
            .getFirst();
    testClient.cancelWorkflowInstance(workflowInstanceKey);

    // then
    final Record<TimerRecordValue> canceledEvent =
        RecordingExporter.timerRecords(TimerIntent.CANCELED)
            .withHandlerNodeId(timerId)
            .withWorkflowInstanceKey(workflowInstanceKey)
            .getFirst();
    assertThat(canceledEvent.getKey()).isEqualTo(createdEvent.getKey());
    assertThat(canceledEvent.getValue()).isEqualTo(createdEvent.getValue());
    assertThat(canceledEvent.getValue().getDueDate())
        .isGreaterThan(brokerRule.getClock().getCurrentTimeInMillis());
  }

  @Test
  public void shouldCreateTimerBasedOnBoundaryEvent() {
    // given
    testClient.deploy(BOUNDARY_EVENT_WORKFLOW);
    brokerRule.getClock().pinCurrentTime();
    final long nowMs = brokerRule.getClock().getCurrentTimeInMillis();
    testClient.createWorkflowInstance(r -> r.setBpmnProcessId(BOUNDARY_EVENT_WORKFLOW_PROCESS_ID));

    // when
    final Record<TimerRecordValue> timerCreatedRecord =
        RecordingExporter.timerRecords(TimerIntent.CREATED).getFirst();
    final Record<WorkflowInstanceRecordValue> activityRecord =
        RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_ACTIVATED)
            .withElementId("task")
            .getFirst();

    // then
    assertThat(timerCreatedRecord.getValue().getDueDate()).isEqualTo(nowMs + 1000);
    assertThat(timerCreatedRecord.getValue().getElementInstanceKey())
        .isEqualTo(activityRecord.getKey());
    assertThat(timerCreatedRecord.getValue().getHandlerFlowNodeId()).isEqualTo("timer");
  }

  @Test
  public void shouldTriggerHandlerNodeWhenAttachedToActivity() {
    // given
    testClient.deploy(BOUNDARY_EVENT_WORKFLOW);
    testClient.createWorkflowInstance(r -> r.setBpmnProcessId(BOUNDARY_EVENT_WORKFLOW_PROCESS_ID));

    // when
    RecordingExporter.timerRecords(TimerIntent.CREATED).getFirst();
    brokerRule.getClock().addTime(Duration.ofSeconds(10));

    // then
    assertThat(
            RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_COMPLETING)
                .withElementId("timer")
                .getFirst())
        .isNotNull();
  }

  @Test
  public void shouldRecreateATimerWithCycle() {
    // given
    testClient.deploy(TWO_REPS_CYCLE_WORKFLOW);
    brokerRule.getClock().pinCurrentTime();
    final long nowMs = brokerRule.getClock().getCurrentTimeInMillis();
    testClient.createWorkflowInstance(r -> r.setBpmnProcessId(TWO_REPS_CYCLE_WORKFLOW_PROCESS_ID));

    // when
    final Record<TimerRecordValue> timerCreatedRecord =
        RecordingExporter.timerRecords(TimerIntent.CREATED).getFirst();
    brokerRule.getClock().addTime(Duration.ofSeconds(5));
    final Record<TimerRecordValue> timerRescheduledRecord =
        RecordingExporter.timerRecords(TimerIntent.CREATED).limit(2).getLast();

    // then
    assertThat(timerCreatedRecord).isNotEqualTo(timerRescheduledRecord);
    assertThat(timerCreatedRecord.getValue().getDueDate()).isEqualTo(nowMs + 1000);
    assertThat(timerRescheduledRecord.getValue().getDueDate()).isEqualTo(nowMs + 6000);
  }

  @Test
  public void shouldRecreateATimerForTheSpecifiedAmountOfRepetitions() {
    // given
    testClient.deploy(TWO_REPS_CYCLE_WORKFLOW);
    brokerRule.getClock().pinCurrentTime();
    final long workflowInstanceKey =
        testClient
            .createWorkflowInstance(r -> r.setBpmnProcessId(TWO_REPS_CYCLE_WORKFLOW_PROCESS_ID))
            .getInstanceKey();

    // when
    assertThat(
            RecordingExporter.timerRecords(TimerIntent.CREATED)
                .withWorkflowInstanceKey(workflowInstanceKey)
                .getFirst())
        .isNotNull();
    brokerRule.getClock().addTime(Duration.ofSeconds(5));
    assertThat(
            RecordingExporter.timerRecords(TimerIntent.CREATED)
                .withWorkflowInstanceKey(workflowInstanceKey)
                .limit(2))
        .hasSize(2);
    brokerRule.getClock().addTime(Duration.ofSeconds(5));
    testClient.completeJobOfType("type");

    // then
    assertThat(
            RecordingExporter.workflowInstanceRecords()
                .withWorkflowInstanceKey(workflowInstanceKey)
                .withIntent(WorkflowInstanceIntent.ELEMENT_COMPLETED)
                .limitToWorkflowInstanceCompleted()
                .exists())
        .isTrue();
    assertThat(
            RecordingExporter.timerRecords(TimerIntent.CREATED)
                .withWorkflowInstanceKey(workflowInstanceKey)
                .limit(2)
                .count())
        .isEqualTo(2);
  }

  @Test
  public void shouldRecreateATimerInfinitely() {
    // given
    final int expectedRepetitions = 5;
    final String timerId = "timer";
    testClient.deploy(INFINITE_CYCLE_WORKFLOW);
    brokerRule.getClock().pinCurrentTime();
    final long workflowInstanceKey =
        testClient
            .createWorkflowInstance(r -> r.setBpmnProcessId(INFINITE_CYCLE_WORKFLOW_PROCESS_ID))
            .getInstanceKey();

    // when
    for (int i = 1; i <= expectedRepetitions; i++) {
      brokerRule.getClock().addTime(Duration.ofSeconds(1));
      assertThat(
              RecordingExporter.timerRecords()
                  .withWorkflowInstanceKey(workflowInstanceKey)
                  .withHandlerNodeId(timerId)
                  .withIntent(TimerIntent.CREATED)
                  .limit(i)
                  .count())
          .isEqualTo(i);
    }
    testClient.completeJobOfType("type");

    // then
    assertThat(
            RecordingExporter.records()
                .limitToWorkflowInstance(workflowInstanceKey)
                .workflowInstanceRecords()
                .withElementId(INFINITE_CYCLE_WORKFLOW_PROCESS_ID)
                .withWorkflowInstanceKey(workflowInstanceKey)
                .withIntent(WorkflowInstanceIntent.ELEMENT_COMPLETED)
                .exists())
        .isTrue();
    assertThat(
            RecordingExporter.records()
                .limitToWorkflowInstance(workflowInstanceKey)
                .timerRecords()
                .withWorkflowInstanceKey(workflowInstanceKey)
                .withHandlerNodeId(timerId)
                .withIntent(TimerIntent.CREATED)
                .count())
        .isEqualTo(expectedRepetitions);
  }
}
