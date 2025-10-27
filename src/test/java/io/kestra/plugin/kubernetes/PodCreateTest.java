package io.kestra.plugin.kubernetes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.io.CharStreams;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.models.executions.TaskRun;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.runners.*;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.Await;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.kubernetes.models.SideCar;
import io.kestra.plugin.kubernetes.services.PodService;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.RetryingTest;
import org.slf4j.event.Level;
import reactor.core.publisher.Flux;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
@Slf4j
class PodCreateTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    @Named(QueueFactoryInterface.WORKERTASKLOG_NAMED)
    private QueueInterface<LogEntry> workerTaskLogQueue;

    @Inject
    private StorageInterface storageInterface;

    @Inject
    private RunContextInitializer runContextInitializer;

    @org.junit.jupiter.api.RepeatedTest(20)  // Run 20 times to catch flakiness
    void run() throws Exception {
        log.info("=".repeat(80));
        log.info("Starting run() test - DETERMINISTIC VERSION with .take(14)");
        log.info("=".repeat(80));

        // Track timing metrics
        AtomicInteger logCounter = new AtomicInteger(0);
        AtomicLong firstLogTimestamp = new AtomicLong(0);
        AtomicLong lastLogTimestamp = new AtomicLong(0);

        // Set up Flux consumer with detailed logging
        Flux<LogEntry> receive = TestsUtils.receive(workerTaskLogQueue, logEntry -> {
            if (logEntry.getLeft().getLevel() == Level.INFO) {
                int count = logCounter.incrementAndGet();
                long timestamp = System.currentTimeMillis();
                if (count == 1) {
                    firstLogTimestamp.set(timestamp);
                }
                lastLogTimestamp.set(timestamp);
                log.info("[TEST-QUEUE] Received INFO log #{}: '{}' at offset +{}ms",
                    count,
                    logEntry.getLeft().getMessage(),
                    firstLogTimestamp.get() > 0 ? timestamp - firstLogTimestamp.get() : 0);
            }
        });

        // Test with production-like timing: 10s waitForLogInterval
        log.info("[TEST] Building task with waitForLogInterval=10s");
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(10)))
//            .delete(Property.ofValue(false)) // Uncomment for tests if you need to check kubectl logs your_pod
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: unittest",
                "  image: debian:stable-slim",
                "  command: ",
                "    - 'bash' ",
                "    - '-c'",
                "    - 'for i in {1..10}; do echo $i; {{ inputs.command }} 0.1; done; >&2 echo \"error\"'",
                "restartPolicy: Never"
            ))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of("command", "sleep"));

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        runContext = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(TestsUtils.mockTaskRun(execution, task)).build());

        log.info("[TEST] Executing task.run() - will wait 10s internally for log collection");
        long taskStartTime = System.currentTimeMillis();
        PodCreate.Output runOutput = task.run(runContext);
        long taskEndTime = System.currentTimeMillis();
        long taskDuration = taskEndTime - taskStartTime;

        log.info("[TEST] task.run() completed after {}ms", taskDuration);
        log.info("[TEST] Logs already in counter: {}/14", logCounter.get());

        assertThat(runOutput.getMetadata().getName(), containsString("iokestrapluginkubernetespodcreatetest-run-podcreate"));

        // Wait for all logs to be collected using Await.until (polling approach)
        log.info("[TEST] Starting wait for exactly 14 INFO logs using Await.until()");
        long collectStartTime = System.currentTimeMillis();

        try {
            Await.until(
                () -> {
                    int current = logCounter.get();
                    if (current < 14) {
                        log.info("[TEST] Await check: {}/14 logs received", current);
                    }
                    return current >= 14;
                },
                Duration.ofMillis(500),  // Check every 500ms
                Duration.ofSeconds(15)   // Timeout: waitForLogInterval (10s) + buffer (5s)
            );

            long collectDuration = System.currentTimeMillis() - collectStartTime;
            log.info("[TEST] Successfully received all 14 logs after {}ms", collectDuration);
            log.info("[TEST] Total time from task.run() completion: {}ms", System.currentTimeMillis() - taskEndTime);

        } catch (Exception e) {
            long collectDuration = System.currentTimeMillis() - collectStartTime;
            log.error("[TEST] FAILED to collect 14 logs after {}ms timeout", collectDuration);
            log.error("[TEST] Logs in counter at timeout: {}/14", logCounter.get());
            log.error("[TEST] This indicates either:");
            log.error("[TEST]   1. Logs lost in Gap #1 (Kubernetes → Queue via fetchFinalLogs)");
            log.error("[TEST]   2. Logs lost in Gap #2 (Queue → Test consumption)");
            log.error("[TEST]   3. Pod didn't generate all expected logs");

            // Wait a bit longer to see if more logs arrive
            log.error("[TEST] Waiting additional 3 seconds to see if more logs arrive...");
            Thread.sleep(3000);
            log.error("[TEST] After 3s wait, logs in counter: {}/14", logCounter.get());

            throw e;
        }

        // Now collect all logs from the Flux
        log.info("[TEST] Collecting all logs from Flux...");
        List<LogEntry> logs = receive
            .filter(logEntry -> logEntry.getLevel() == Level.INFO)
            .take(Duration.ofMillis(100))  // Just a short wait to collect what's available
            .collectList()
            .block();

        // Log all collected logs
        log.info("[TEST] All {} INFO logs collected in order:", logs.size());
        logs.forEach(logEntry -> log.info("[TEST]   - '{}'", logEntry.getMessage()));

        // Timing summary
        long totalTime = System.currentTimeMillis() - taskStartTime;
        log.info("[TEST] Timing Summary:");
        log.info("[TEST]   - task.run() execution: {}ms", taskDuration);
        log.info("[TEST]   - Log collection wait: {}ms", totalTime - taskDuration);
        log.info("[TEST]   - Total test time: {}ms", totalTime);
        log.info("[TEST]   - First log offset: +{}ms",
            firstLogTimestamp.get() > 0 ? firstLogTimestamp.get() - taskStartTime : -1);
        log.info("[TEST]   - Last log offset: +{}ms",
            lastLogTimestamp.get() > 0 ? lastLogTimestamp.get() - taskStartTime : -1);
        log.info("[TEST]   - Log reception window: {}ms",
            (firstLogTimestamp.get() > 0 && lastLogTimestamp.get() > 0)
                ? lastLogTimestamp.get() - firstLogTimestamp.get() : -1);

        // Assertions
        assertThat(logs.size(), is(14));
        assertThat(logs.stream().filter(logEntry -> logEntry.getMessage().equals("10")).count(), is(1L));
        assertThat(logs.stream().filter(logEntry -> logEntry.getMessage().contains("is deleted")).count(), is(1L));
        assertThat(logs.stream().filter(logEntry -> logEntry.getMessage().equals("error")).count(), is(1L));

        log.info("[TEST] Test completed successfully");
        log.info("=".repeat(80));
    }

    @Test
    void failed() throws Exception {
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .resume(Property.ofValue(false))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: unittest",
                "  image: debian:stable-slim",
                "  command: ",
                "    - 'bash' ",
                "    - '-c'",
                "    - 'exit 1'",
                "restartPolicy: Never"
            ))
            .build();

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        TaskRun taskRun = TestsUtils.mockTaskRun(execution, task);
        RunContext runContextFinal = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(taskRun).build());

        String labelSelector = "kestra.io/taskrun-id=" + taskRun.getId();

        try (KubernetesClient client = PodService.client(runContextFinal, null)) {
            assertThrows(IllegalStateException.class, () -> task.run(runContextFinal));

            // Verify pod was deleted after failure
            Await.until(() -> client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems().isEmpty(),
                Duration.ofMillis(200), Duration.ofSeconds(10));
        }
    }

    @Test
    void failedAfterStartup() throws Exception {
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: unittest",
                "  image: debian:stable-slim",
                "  command: ",
                "    - 'bash' ",
                "    - '-c'",
                "    - 'sleep 1 && exit 1'",
                "restartPolicy: Never"
            ))
            .build();

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        RunContext runContextFinal = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(TestsUtils.mockTaskRun(execution, task)).build());

        assertThrows(IllegalStateException.class, () -> task.run(runContextFinal));
    }

    @Test
    void failedWithOutputFiles() throws Exception {
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .outputFiles(Property.ofValue(List.of("results.json")))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(10)))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: unittest",
                "  image: debian:stable-slim",
                "  command: ",
                "    - 'bash' ",
                "    - '-c'",
                "    - 'echo \"Container failing\" && exit 1'",
                "restartPolicy: Never"
            ))
            .build();

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        RunContext runContextFinal = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(TestsUtils.mockTaskRun(execution, task)).build());

        assertThrows(IllegalStateException.class, () -> task.run(runContextFinal));
    }

    @Test
    void failedWithOutputFilesDeletesPod() throws Exception {
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .outputFiles(Property.ofValue(List.of("results.json")))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(10)))
            .delete(Property.ofValue(true))
            .resume(Property.ofValue(false))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: unittest",
                "  image: debian:stable-slim",
                "  command: ",
                "    - 'bash' ",
                "    - '-c'",
                "    - 'echo \"Container failing\" && sleep 1 && exit 1'",
                "restartPolicy: Never"
            ))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        TaskRun taskRun = TestsUtils.mockTaskRun(execution, task);
        RunContext finalRunContext = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(taskRun).build());

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<?> taskFuture = executorService.submit(() -> {
            try {
                task.run(finalRunContext);
            } catch (Exception e) {
                log.debug("Task failed as expected.", e);
            }
        });

        String labelSelector = "kestra.io/taskrun-id=" + taskRun.getId();

        try (KubernetesClient client = PodService.client(finalRunContext, null)) {
            // Wait for pod creation
            Await.until(() -> {
                var pods = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems();
                return !pods.isEmpty();
            }, Duration.ofMillis(200), Duration.ofMinutes(1));

            var createdPod = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems().get(0);
            String podName = createdPod.getMetadata().getName();
            log.info("Test detected pod creation: {}", podName);

            // Wait for pod to be deleted despite the failure
            Await.until(() -> client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems().isEmpty(),
                Duration.ofMillis(200), Duration.ofMinutes(2));

            log.info("Pod {} was successfully deleted after failure with outputFiles.", podName);
        } finally {
            taskFuture.cancel(true);
            executorService.shutdownNow();
        }
    }

    @Test
    void sidecarExitsGracefullyOnFailure() throws Exception {
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .outputFiles(Property.ofValue(List.of("results.json")))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(10)))
            .delete(Property.ofValue(true))
            .resume(Property.ofValue(false))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: unittest",
                "  image: debian:stable-slim",
                "  command: ",
                "    - 'bash' ",
                "    - '-c'",
                "    - 'echo \"Container failing\" && exit 1'",
                "restartPolicy: Never"
            ))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        TaskRun taskRun = TestsUtils.mockTaskRun(execution, task);
        RunContext finalRunContext = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(taskRun).build());

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<?> taskFuture = executorService.submit(() -> {
            try {
                task.run(finalRunContext);
            } catch (Exception e) {
                log.debug("Task failed as expected.", e);
            }
        });

        String labelSelector = "kestra.io/taskrun-id=" + taskRun.getId();

        try (KubernetesClient client = PodService.client(finalRunContext, null)) {
            // Wait for pod creation and completion
            Await.until(() -> {
                var pods = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems();
                if (pods.isEmpty()) {
                    return false;
                }
                var pod = pods.get(0);
                String phase = pod.getStatus() != null ? pod.getStatus().getPhase() : null;
                return "Failed".equals(phase) || "Succeeded".equals(phase);
            }, Duration.ofMillis(200), Duration.ofMinutes(1));

            var completedPod = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems().get(0);
            String podName = completedPod.getMetadata().getName();
            log.info("Pod {} completed with phase: {}", podName, completedPod.getStatus().getPhase());

            // Verify sidecar container exists and check its status
            var containerStatuses = completedPod.getStatus().getContainerStatuses();
            var sidecarStatus = containerStatuses.stream()
                .filter(status -> status.getName().equals("out-files"))
                .findFirst();

            assertThat("Sidecar container should exist", sidecarStatus.isPresent(), is(true));

            // Check if sidecar is still running (it shouldn't be if marker was signaled)
            if (sidecarStatus.get().getState().getRunning() != null) {
                log.warn("Sidecar is still running - marker may not have been signaled");
            }

            // Wait for pod deletion and measure time
            long deletionStartTime = System.currentTimeMillis();
            Await.until(() -> {
                var pods = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems();
                if (!pods.isEmpty()) {
                    var pod = pods.get(0);
                    var sidecarCurrentStatus = pod.getStatus().getContainerStatuses().stream()
                        .filter(status -> status.getName().equals("out-files"))
                        .findFirst();

                    if (sidecarCurrentStatus.isPresent()) {
                        var state = sidecarCurrentStatus.get().getState();
                        if (state.getTerminated() != null) {
                            String reason = state.getTerminated().getReason();
                            log.info("Sidecar terminated with reason: {}", reason);
                            // Verify sidecar terminated gracefully (Completed), not force-killed (Error/Killed)
                            assertThat("Sidecar should exit gracefully with Completed status",
                                reason, is("Completed"));
                        }
                    }
                }
                return pods.isEmpty();
            }, Duration.ofMillis(200), Duration.ofMinutes(1));
            long deletionDuration = System.currentTimeMillis() - deletionStartTime;

            // Verify pod deletion was fast (< 15 seconds indicates graceful sidecar exit)
            assertThat("Pod deletion should be fast when sidecar exits gracefully (< 15s)",
                deletionDuration, lessThan(15000L));

            log.info("Pod {} deleted in {}ms - sidecar exited gracefully", podName, deletionDuration);
        } finally {
            taskFuture.cancel(true);
            executorService.shutdownNow();
        }
    }

    @Test
    void missingInputFilesFailsFastWithValidation() throws Exception {
        // Test for issue #211: validation prevents pod creation when inputFiles are invalid
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .inputFiles(Map.of("data.txt", "{{ outputs['nonexistent-task']['outputFiles']['data.txt'] }}"))
            .waitUntilRunning(Property.ofValue(Duration.ofSeconds(10)))
            .delete(Property.ofValue(true))
            .resume(Property.ofValue(false))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: unittest",
                "  image: debian:stable-slim",
                "  command: ",
                "    - 'bash' ",
                "    - '-c'",
                "    - 'cat {{ workingDir }}/data.txt'",
                "restartPolicy: Never"
            ))
            .build();

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        TaskRun taskRun = TestsUtils.mockTaskRun(execution, task);
        RunContext finalRunContext = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(taskRun).build());

        String labelSelector = "kestra.io/taskrun-id=" + taskRun.getId();

        // Should fail fast with validation error before pod creation
        long startTime = System.currentTimeMillis();
        Exception exception = assertThrows(Exception.class, () -> task.run(finalRunContext));
        long elapsedTime = System.currentTimeMillis() - startTime;

        // Verify error message mentions the problematic file reference
        assertThat(exception.getMessage(), containsString("outputs"));
        assertThat(exception.getMessage(), containsString("nonexistent-task"));

        // Should fail in < 2 seconds, not wait 30 seconds for timeout
        assertThat(elapsedTime, lessThan(2000L));

        // Verify no pod was created
        try (KubernetesClient client = PodService.client(finalRunContext, null)) {
            var pods = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems();
            assertThat(pods, empty());
        }

        log.info("Validation prevented pod creation and failed in {}ms", elapsedTime);
    }

    @Test
    void resume() throws Exception {
        Flux<LogEntry> receive = TestsUtils.receive(workerTaskLogQueue);

        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(10)))
//            .delete(Property.ofValue(false)) // Uncomment for tests if you need to check kubectl logs your_pod
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: unittest",
                "  image: debian:stable-slim",
                "  command: ",
                "    - 'bash' ",
                "    - '-c'",
                "    - 'for i in {1..10}; do echo $i; {{ inputs.command }} 0.1; done'",
                "restartPolicy: Never"
            ))
            .resume(Property.ofValue(true))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of("command", "sleep"));
        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());

        runContext = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(TestsUtils.mockTaskRun(execution, task)).build());

        RunContext finalRunContext = runContext;

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        Flux<LogEntry> shutdownReceive = TestsUtils.receive(workerTaskLogQueue, logEntry -> {
            if (logEntry.getLeft().getMessage().equals("1")) {
                executorService.shutdownNow();
            }
        });

        executorService.execute(() -> {
            try {
                task.run(finalRunContext);
            } catch (Exception e) {
                log.warn("Exception", e);
            }
        });

        Await.until(executorService::isShutdown, Duration.ofMillis(100), Duration.ofMinutes(1));
        shutdownReceive.blockLast();

        task.run(finalRunContext);

        assertThat(receive.toStream().filter(logEntry -> logEntry.getLevel() == Level.INFO).filter(logEntry -> logEntry.getMessage().equals("10")).count(), greaterThan(0L));
    }

    @RetryingTest(value = 3)
    void inputOutputFiles() throws Exception {
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .outputFiles(Property.ofValue(Arrays.asList("xml", "csv")))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(10)))
//            .delete(Property.ofValue(false)) // Uncomment for tests if you need to check kubectl logs your_pod
            .inputFiles(Map.of(
                "files/in/in.txt", "I'm here",
                "main.sh", "sleep 1\n" +
                    "echo '::{\"outputs\": {\"extract\":\"'$(cat files/in/in.txt)'\"}}::'\n" +
                    "echo 1 >> {{ outputFiles.xml }}\n" +
                    "echo 2 >> {{ outputFiles.csv }}\n" +
                    "echo 3 >> {{ outputFiles.xml }}\n" +
                    "sleep 1"
            ))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: unittest",
                "  image: debian:stable-slim",
                "  workingDir: /kestra/working-dir",
                "  command: ",
                "    - 'bash' ",
                "    - '-c' ",
                "    - 'ls -lh && bash main.sh {{ outputFiles.xml }}'",
                "restartPolicy: Never"
            ))
            .metadata(Map.of("name", "custom-name-" + IdUtils.create().toLowerCase()))
            .resume(Property.ofValue(true))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of("command", "sleep"));

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        runContext = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(TestsUtils.mockTaskRun(execution, task)).build());

        PodCreate.Output run = task.run(runContext);

        Thread.sleep(500);

        assertThat(run.getVars().get("extract"), is("I'm here"));

        InputStream get = storageInterface.get(TenantService.MAIN_TENANT, null, run.getOutputFiles().get("xml"));

        assertThat(
            CharStreams.toString(new InputStreamReader(get)),
            is("1\n3\n")
        );

        get = storageInterface.get(TenantService.MAIN_TENANT, null, run.getOutputFiles().get("csv"));

        assertThat(
            CharStreams.toString(new InputStreamReader(get)),
            is("2\n")
        );
    }

    @Test
    void workingDirCreatedWithOnlyOutputFiles() throws Exception {
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .outputFiles(Property.ofValue(List.of("*.txt")))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(10)))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: file-writer",
                "  image: debian:stable-slim",
                "  command: [\"/bin/sh\"]",
                "  args:",
                "    - -c",
                "    - >-",
                "      echo 'hello from pod' > {{ workingDir }}/hello.txt",
                "restartPolicy: Never"
            ))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        runContext = runContextInitializer.forWorker(
            (DefaultRunContext) runContext,
            WorkerTask.builder().task(task).taskRun(TestsUtils.mockTaskRun(execution, task)).build()
        );

        PodCreate.Output run = task.run(runContext);

        assertThat(run.getOutputFiles(), hasKey("hello.txt"));

        InputStream file = storageInterface.get(TenantService.MAIN_TENANT, null, run.getOutputFiles().get("hello.txt"));
        String content = CharStreams.toString(new InputStreamReader(file));
        assertThat(content.trim(), is("hello from pod"));
    }

    @Test
    void sidecarResources() throws Exception {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory()).findAndRegisterModules();
        SideCar sidecar = mapper.readValue(
            """
                resources:
                  limits:
                    cpu: 200m
                    memory: 256Mi
                  requests:
                    cpu: 100m
                    memory: 128Mi""",
            SideCar.class);

        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(10)))
            .fileSidecar(sidecar)
            .inputFiles(Map.of(
                "in.txt", "File content"
            ))
            .outputFiles(Property.ofValue(List.of("out.txt")))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: in-out-files",
                "  image: debian:stable-slim",
                "  command: [\"/bin/sh\"]",
                "  args:",
                "    - -c",
                "    - >-",
                "      cat {{ workingDir }}/in.txt > {{ workingDir }}/out.txt",
                "restartPolicy: Never"
            ))
            .build();

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        TaskRun taskRun = TestsUtils.mockTaskRun(execution, task);
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        RunContext finalRunContext = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(taskRun).build());

        final PodCreate.Output[] run = new PodCreate.Output[1];
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            try {
                run[0] = task.run(finalRunContext);
            } catch (Exception e) {
                log.debug("Unexpected error.", e);
            }
        });

        String labelSelector = "kestra.io/taskrun-id=" + taskRun.getId();

        try (KubernetesClient client = PodService.client(finalRunContext, null)) {
            Await.until(() -> {
                var pods = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems();
                return !pods.isEmpty() && pods.getFirst().getStatus().getPhase().equals("Running");
            }, Duration.ofMillis(200), Duration.ofMinutes(1));

            var createdPod = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems().getFirst();
            assertThat(createdPod.getStatus().getPhase(), is("Running"));
            String podName = createdPod.getMetadata().getName();
            log.info("Test detected pod creation: {}", podName);

            ResourceRequirements initReqs = createdPod.getSpec().getInitContainers().getFirst().getResources();
            assertThat(initReqs.getLimits().get("cpu"), is(Quantity.parse("200m")));
            assertThat(initReqs.getLimits().get("memory"), is(Quantity.parse("256Mi")));
            assertThat(initReqs.getRequests().get("cpu"), is(Quantity.parse("100m")));
            assertThat(initReqs.getRequests().get("memory"), is(Quantity.parse("128Mi")));

            ResourceRequirements sideReqs = createdPod.getSpec().getContainers().getLast().getResources();
            assertThat(sideReqs.getLimits().get("cpu"), is(Quantity.parse("200m")));
            assertThat(sideReqs.getLimits().get("memory"), is(Quantity.parse("256Mi")));
            assertThat(sideReqs.getRequests().get("cpu"), is(Quantity.parse("100m")));
            assertThat(sideReqs.getRequests().get("memory"), is(Quantity.parse("128Mi")));

            Await.until(() -> client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems().isEmpty(),
                Duration.ofMillis(200), Duration.ofMinutes(1));

            log.info("Pod {} has successfully completed.", podName);
        }

        assertThat(run[0].getOutputFiles(), hasKey("out.txt"));
        InputStream file = storageInterface.get(TenantService.MAIN_TENANT, null, run[0].getOutputFiles().get("out.txt"));
        String content = CharStreams.toString(new InputStreamReader(file));
        assertThat(content.trim(), is("File content"));
    }

    @Test
    void outputFilesWithSpecialChars() throws Exception {
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .outputFiles(Property.ofValue(List.of("**.txt")))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(10)))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: file-writer",
                "  image: debian:stable-slim",
                "  command: [\"/bin/sh\"]",
                "  args:",
                "    - -c",
                "    - >-",
                "      echo 'I am fulfilled' > {{ workingDir }}/special\\ file.txt &&",
                "      mkdir {{ workingDir }}/sub\\ dir &&",
                "      echo 'I have content' > {{ workingDir }}/sub\\ dir/more\\ special\\ file.txt",
                "restartPolicy: Never"
            ))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        runContext = runContextInitializer.forWorker(
            (DefaultRunContext) runContext,
            WorkerTask.builder().task(task).taskRun(TestsUtils.mockTaskRun(execution, task)).build()
        );

        PodCreate.Output run = task.run(runContext);

        assertThat(run.getOutputFiles(), hasKey("special file.txt"));
        assertThat(run.getOutputFiles(), hasKey("sub dir/more special file.txt"));

        InputStream file = storageInterface.get(TenantService.MAIN_TENANT, null, run.getOutputFiles().get("special file.txt"));
        String content = CharStreams.toString(new InputStreamReader(file));
        assertThat(content.trim(), is("I am fulfilled"));

        file = storageInterface.get(TenantService.MAIN_TENANT, null, run.getOutputFiles().get("sub dir/more special file.txt"));
        content = CharStreams.toString(new InputStreamReader(file));
        assertThat(content.trim(), is("I have content"));
    }

    @Test
    void kill() throws Exception {
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: unittest",
                "  image: debian:stable-slim",
                "  command: ",
                "    - 'bash' ",
                "    - '-c'",
                "    - 'echo start kestra task && sleep 60 && echo end kestra test'",
                "restartPolicy: Never"
            ))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        TaskRun taskRun = TestsUtils.mockTaskRun(execution, task);
        RunContext finalRunContext = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(taskRun).build());

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<?> taskFuture = executorService.submit(() -> {
            try {
                task.run(finalRunContext);
            } catch (Exception e) {
                log.debug("Task run interrupted.", e);
            }
        });

        String labelSelector = "kestra.io/taskrun-id=" + taskRun.getId();

        try (KubernetesClient client = PodService.client(finalRunContext, null)) {
            Await.until(() -> {
                var pods = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems();
                return !pods.isEmpty() && pods.get(0).getStatus().getPhase().equals("Running");
            }, Duration.ofMillis(200), Duration.ofMinutes(1));

            var createdPod = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems().get(0);
            String podName = createdPod.getMetadata().getName();
            log.info("Test detected pod creation: {}", podName);
            assertThat(createdPod.getStatus().getPhase(), is("Running"));

            task.kill();

            Await.until(() -> client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems().isEmpty(),
                Duration.ofMillis(200), Duration.ofMinutes(1));

            log.info("Pod {} has been successfully deleted after kill.", podName);
        } finally {
            taskFuture.cancel(true);
            executorService.shutdownNow();
        }
    }

    @Test
    void parseOutputsWithSpecialChars() throws Exception {
        PodCreate task = PodCreate.builder()
            .id("special-char-test")
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(10)))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: special-char-container",
                "  image: debian:stable-slim",
                "  command:",
                "    - 'bash'",
                "    - '-c'",
                "    - \"echo '::{\\\"outputs\\\": {\\\"PROJECT_ID\\\": 101, \\\"PROJECT_NAME\\\": \\\"One O One\\\", \\\"LABEL\\\": \\\"4004\\\"}}::'\"",
                "restartPolicy: Never"
            ))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        runContext = runContextInitializer.forWorker(
            (DefaultRunContext) runContext,
            WorkerTask.builder().task(task).taskRun(TestsUtils.mockTaskRun(execution, task)).build()
        );

        PodCreate.Output runOutput = task.run(runContext);

        // Assert that all special char outputs are parsed and available
        assertThat(runOutput.getVars().get("PROJECT_ID").toString(), is("101"));
        assertThat(runOutput.getVars().get("PROJECT_NAME"), is("One O One"));
        assertThat(runOutput.getVars().get("LABEL").toString(), is("4004"));
    }

    @Test
    void successWithOutputFiles() throws Exception {
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .outputFiles(Property.ofValue(List.of("result.txt")))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(10)))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: unittest",
                "  image: debian:stable-slim",
                "  command: ",
                "    - 'bash' ",
                "    - '-c'",
                "    - 'echo \"Task succeeded\" > {{ workingDir }}/result.txt && exit 0'",
                "restartPolicy: Never"
            ))
            .build();

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        RunContext runContextFinal = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(TestsUtils.mockTaskRun(execution, task)).build());

        PodCreate.Output output = task.run(runContextFinal);

        assertThat(output.getOutputFiles(), hasKey("result.txt"));
        InputStream file = storageInterface.get(TenantService.MAIN_TENANT, null, output.getOutputFiles().get("result.txt"));
        String content = CharStreams.toString(new InputStreamReader(file));
        assertThat(content.trim(), is("Task succeeded"));
    }

    @Test
    void multipleContainersOneFailsWithOutputFiles() throws Exception {
        AtomicInteger logCounter = new AtomicInteger(0);
        Flux<LogEntry> receive = TestsUtils.receive(workerTaskLogQueue, logEntry -> {
            String message = logEntry.getLeft().getMessage();
            if (message.contains("First container succeeded") || message.contains("Second container failing")) {
                logCounter.incrementAndGet();
            }
        });

        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .outputFiles(Property.ofValue(List.of("result.txt")))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(10)))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: container-success",
                "  image: debian:stable-slim",
                "  command: ",
                "    - 'bash' ",
                "    - '-c'",
                "    - 'echo \"First container succeeded\" && exit 0'",
                "- name: container-failure",
                "  image: debian:stable-slim",
                "  command: ",
                "    - 'bash' ",
                "    - '-c'",
                "    - 'echo \"Second container failing\" && exit 1'",
                "restartPolicy: Never"
            ))
            .build();

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        RunContext runContextFinal = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(TestsUtils.mockTaskRun(execution, task)).build());

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> task.run(runContextFinal));
        assertThat(exception.getMessage(), containsString("container-failure"));
        assertThat(exception.getMessage(), containsString("exit code 1"));

        // Wait for logs from both containers to be collected
        Await.until(
            () -> logCounter.get() >= 2,
            Duration.ofMillis(100),
            Duration.ofSeconds(5)
        );

        List<LogEntry> logs = receive.collectList().block();

        // Verify logs from both containers were collected
        assertThat(logs.stream()
            .filter(logEntry -> logEntry.getMessage().contains("First container succeeded"))
            .count(),
            greaterThan(0L));
        assertThat(logs.stream()
            .filter(logEntry -> logEntry.getMessage().contains("Second container failing"))
            .count(),
            greaterThan(0L));
    }

    @Test
    void completeLogCollectionAfterQuickTermination() throws Exception {
        AtomicInteger expectedLogCounter = new AtomicInteger(0);
        Flux<LogEntry> receive = TestsUtils.receive(workerTaskLogQueue, logEntry -> {
            String message = logEntry.getLeft().getMessage();
            if (message.startsWith("Log line ") || message.equals("FINAL")) {
                expectedLogCounter.incrementAndGet();
            }
        });

        // Generate exactly 20 identifiable log lines in quick succession, then fail
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .outputFiles(Property.ofValue(List.of("result.txt")))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(10)))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: unittest",
                "  image: debian:stable-slim",
                "  command:",
                "    - 'bash'",
                "    - '-c'",
                "    - 'for i in {1..20}; do echo \"Log line $i\"; done; echo \"FINAL\" && exit 1'",
                "restartPolicy: Never"
            ))
            .build();

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        TaskRun taskRun = TestsUtils.mockTaskRun(execution, task);
        RunContext runContextFinal = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(taskRun).build());
        assertThrows(IllegalStateException.class, () -> task.run(runContextFinal));

        // Wait for all logs to be collected with retry mechanism (expect 20 numbered + 1 FINAL = 21 logs)
        Await.until(
            () -> expectedLogCounter.get() >= 21,
            Duration.ofMillis(100),
            Duration.ofSeconds(5)
        );

        List<LogEntry> logs = receive.collectList().block();

        // Verify all 20 numbered logs were collected (no missing logs)
        for (int i = 1; i <= 20; i++) {
            String expected = "Log line " + i;
            long count = logs.stream()
                .filter(log -> log.getMessage().equals(expected))
                .count();
            assertThat("Missing or duplicate log: " + expected, count, is(1L));
        }

        // Verify final log before exit was captured
        assertThat(logs.stream()
            .filter(log -> log.getMessage().equals("FINAL"))
            .count(),
            is(1L));
    }

}
