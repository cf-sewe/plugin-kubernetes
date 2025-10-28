package io.kestra.plugin.kubernetes.services;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.LogWatch;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.kestra.core.models.tasks.runners.AbstractLogConsumer;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Await;
import io.kestra.core.utils.ThreadMainFactoryBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class PodLogService implements AutoCloseable {
    private final ThreadMainFactoryBuilder threadFactoryBuilder;
    private List<LogWatch> podLogs = new ArrayList<>();
    private ScheduledExecutorService scheduledExecutor;
    private ScheduledFuture<?> scheduledFuture;
    @Getter
    private LoggingOutputStream outputStream;
    private Thread thread;

    public PodLogService(ThreadMainFactoryBuilder threadFactoryBuilder) {
        this.threadFactoryBuilder = threadFactoryBuilder;
    }

    public void setLogConsumer(AbstractLogConsumer logConsumer) {
        if (outputStream == null) {
            outputStream = new LoggingOutputStream(logConsumer);
        }
    }

    public final void watch(KubernetesClient client, Pod pod, AbstractLogConsumer logConsumer, RunContext runContext) {
        runContext.logger().debug("[PodLogService.watch] ENTER: Creating watch for pod '{}'", pod.getMetadata().getName());

        scheduledExecutor = Executors.newSingleThreadScheduledExecutor(threadFactoryBuilder.build("k8s-log"));
        setLogConsumer(logConsumer);
        AtomicBoolean started = new AtomicBoolean(false);

        runContext.logger().debug("[PodLogService.watch] Starting scheduledExecutor with 30s fixed rate");

        scheduledFuture = scheduledExecutor.scheduleAtFixedRate(
            () -> {
                Instant lastTimestamp = outputStream.getLastTimestamp() == null ? null : Instant.from(outputStream.getLastTimestamp());

                runContext.logger().debug("[PodLogService.watch] Scheduled task executing: started={}, lastTimestamp={}",
                    started.get(), lastTimestamp);

                if (!started.get() || lastTimestamp == null || lastTimestamp.isBefore(Instant.now().minus(Duration.ofMinutes(10)))) {
                    if (!started.get()) {
                        started.set(true);
                        runContext.logger().debug("[PodLogService.watch] First execution - setting up watchLog() streams");
                    } else {
                        runContext.logger().trace("No log since '{}', reconnecting", lastTimestamp == null ? "unknown" : lastTimestamp.toString());
                    }

                    if (podLogs != null) {
                        podLogs.forEach(LogWatch::close);
                        podLogs = new ArrayList<>();
                    }

                    PodResource podResource = PodService.podRef(client, pod);

                    try {
                        pod
                            .getSpec()
                            .getContainers()
                            .forEach(container -> {
                                try {
                                    String sinceTimeParam = lastTimestamp != null ?
                                        lastTimestamp.plusNanos(1).toString() :
                                        null;
                                    runContext.logger().debug("[PodLogService.watch] Creating watchLog() for container '{}': sinceTime={}, lastTimestamp={}",
                                        container.getName(), sinceTimeParam, lastTimestamp);

                                    podLogs.add(podResource
                                        .inContainer(container.getName())
                                        .usingTimestamps()
                                        .sinceTime(sinceTimeParam)
                                        .watchLog(outputStream)
                                    );

                                    runContext.logger().debug("[PodLogService.watch] watchLog() created for container '{}'", container.getName());
                                } catch (KubernetesClientException e) {
                                    if (e.getCode() == 404) {
                                        runContext.logger().info("Pod no longer exists, stopping log collection");
                                        scheduledFuture.cancel(false);
                                    } else {
                                        throw e;
                                    }
                                }
                            });
                    } catch (KubernetesClientException e) {
                        if (e.getCode() == 404) {
                            runContext.logger().info("Pod no longer exists, stopping log collection");
                            scheduledFuture.cancel(false);
                        } else {
                            throw e;
                        }
                    }

                    runContext.logger().debug("[PodLogService.watch] Scheduled task completed setup. Active watchLog streams: {}", podLogs.size());
                } else {
                    runContext.logger().debug("[PodLogService.watch] Scheduled task - watchLog still active, lastTimestamp recently updated: {}", lastTimestamp);
                }
            },
            0,
            30,
            TimeUnit.SECONDS
        );

        runContext.logger().debug("[PodLogService.watch] EXIT: Watch setup complete, scheduledExecutor started");

        // look at exception on the main thread
        thread = Thread.ofVirtual().name("k8s-listener").start(
            () -> {
                try {
                    Await.until(scheduledFuture::isDone);
                } catch (RuntimeException e) {
                    if (!e.getMessage().contains("Can't sleep")) {
                        log.error("{} exception", this.getClass().getName(), e);
                    } else {
                        log.debug("{} exception", this.getClass().getName(), e);
                    }
                }

                try {
                    scheduledFuture.get();
                } catch (CancellationException e) {
                    log.debug("{} cancelled", this.getClass().getName(), e);
                } catch (ExecutionException | InterruptedException e) {
                    log.error("{} exception", this.getClass().getName(), e);
                }
            }
        );
    }

    public void fetchFinalLogs(KubernetesClient client, Pod pod, RunContext runContext) throws IOException {
        if (outputStream == null) {
            return;
        }

        Instant lastTimestamp = outputStream.getLastTimestamp();
        PodResource podResource = PodService.podRef(client, pod);

        runContext.logger().debug("[PodLogService.fetchFinalLogs] Starting final log fetch with lastTimestamp: {}", lastTimestamp);

        pod.getSpec()
            .getContainers()
            .forEach(container -> {
                try {
                    // Use sinceTime as a hint to K8s API (may return more than requested, so we filter locally)
                    String sinceTimeParam = lastTimestamp != null ?
                        lastTimestamp.plusNanos(1).toString() :
                        null;

                    runContext.logger().debug("[PodLogService.fetchFinalLogs] Fetching logs for container '{}' with sinceTime={} (will filter locally by lastTimestamp: {})",
                        container.getName(), sinceTimeParam, lastTimestamp);

                    String logs = podResource
                        .inContainer(container.getName())
                        .usingTimestamps()
                        .sinceTime(sinceTimeParam)
                        .getLog();

                    if (logs != null && !logs.isEmpty()) {
                        int totalLineCount = logs.split("\n").length;
                        runContext.logger().debug("[PodLogService.fetchFinalLogs] Received {} total lines from K8s API for container '{}'",
                            totalLineCount, container.getName());

                        // Parse and filter logs to only write lines after lastTimestamp
                        BufferedReader reader = new BufferedReader(new StringReader(logs));
                        String line;
                        int linesWritten = 0;
                        int linesSkipped = 0;

                        while ((line = reader.readLine()) != null) {
                            // Extract timestamp from log line to compare
                            String[] parts = line.split("\\s+", 2);
                            if (parts.length >= 1 && lastTimestamp != null) {
                                try {
                                    Instant lineTimestamp = Instant.parse(parts[0]);

                                    // Only write logs that are AFTER lastTimestamp
                                    if (lineTimestamp.isAfter(lastTimestamp)) {
                                        runContext.logger().debug("[PodLogService.fetchFinalLogs] Writing new log line (timestamp {}): '{}'",
                                            lineTimestamp, line.substring(0, Math.min(100, line.length())));
                                        outputStream.write((line + "\n").getBytes());
                                        linesWritten++;
                                    } else {
                                        linesSkipped++;
                                        runContext.logger().debug("[PodLogService.fetchFinalLogs] Skipping already-streamed log (timestamp {} <= {})",
                                            lineTimestamp, lastTimestamp);
                                    }
                                } catch (DateTimeParseException e) {
                                    // If timestamp parsing fails, write the line (better to duplicate than miss)
                                    runContext.logger().debug("[PodLogService.fetchFinalLogs] Could not parse timestamp, writing line: '{}'",
                                        line.substring(0, Math.min(100, line.length())));
                                    outputStream.write((line + "\n").getBytes());
                                    linesWritten++;
                                }
                            } else {
                                // No lastTimestamp or no timestamp in line, write it
                                outputStream.write((line + "\n").getBytes());
                                linesWritten++;
                            }
                        }

                        outputStream.flush();
                        runContext.logger().debug("[PodLogService.fetchFinalLogs] Wrote {} new lines, skipped {} already-streamed lines for container '{}'",
                            linesWritten, linesSkipped, container.getName());
                    } else {
                        runContext.logger().debug("[PodLogService.fetchFinalLogs] No logs fetched for container '{}' (logs was {})",
                            container.getName(), logs == null ? "null" : "empty");
                    }
                } catch (IOException e) {
                    runContext.logger().error("Error fetching final logs for container {}", container.getName(), e);
                }
            });

        runContext.logger().debug("[PodLogService.fetchFinalLogs] Completed. Final lastTimestamp: {}", outputStream.getLastTimestamp());
    }

    @Override
    public void close() throws IOException {
        if (outputStream != null) {
            outputStream.flush();
            outputStream.close();
        }

        // Ensure the scheduled task reaches a terminal state to avoid blocking on future.get() in the listener
        if (scheduledFuture != null) {
            try {
                scheduledFuture.cancel(true);
            } catch (Exception ignore) {
                // best-effort cancellation
            }
        }

        if (thread != null) {
            thread.interrupt();
            thread = null;
        }

        if (podLogs != null) {
            podLogs.forEach(LogWatch::close);
        }

        if (scheduledExecutor != null) {
            scheduledExecutor.shutdownNow();
        }
    }
}
