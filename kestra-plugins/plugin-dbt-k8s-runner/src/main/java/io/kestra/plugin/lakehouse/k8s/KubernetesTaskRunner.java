package io.kestra.plugin.lakehouse.k8s;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.dsl.LogWatch;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.runners.*;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Runs every task command inside a Pod created in a single, fixed, tenant-owned Kubernetes
 * namespace — instead of the platform worker's own OS process (core {@code Process} runner) or
 * a cluster-wide connection an operator points anywhere (EE {@code Kubernetes} runner).
 *
 * <p>The tenant-isolation property lives entirely in the RBAC granted to the Kestra worker's own
 * ServiceAccount: it is bound, via a Role + RoleBinding <em>in the tenant namespace</em>, to
 * create/get/list/watch/delete {@code pods} and get/watch {@code pods/log} there and only there.
 * The platform-layer worker never holds a standing credential into any other tenant resource —
 * it can start and watch a Pod it just created in the one namespace it was granted, nothing more.
 *
 * <p><b>Phase 1 scope:</b> this runner does not sync the local working directory into the Pod the
 * way the EE runner's init-container/sidecar pair does. The container image is responsible for
 * having everything it needs (bake the dbt project into the image, or {@code git clone}/pull it
 * as the first step of the command). Input/output file sync via {@link RemoteRunnerInterface} is
 * a deliberate phase 2, once this phase 1 mechanics — create Pod in tenant namespace, stream logs,
 * capture exit code, clean up — is proven end to end.
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run tasks in a Pod inside a tenant-owned Kubernetes namespace.",
    description = """
        Every Pod this runner creates lands in the single `namespace` configured below — that \
        namespace is the tenant's own, not the platform namespace Kestra itself runs in. The \
        worker's ServiceAccount only needs pod-scoped RBAC in that one namespace; it never reaches \
        into any other tenant-owned resource, and it never runs task code as the worker's own \
        process the way the core `Process` runner does.

        Phase 1 scope: no working-directory file sync yet. Give the container image everything \
        it needs — bake the project in, or have the first command clone/pull it — until file sync \
        is added."""
)
@Plugin(
    examples = {
        @Example(
            title = "Run dbt inside the `tenant-data-eng` namespace, against a project already baked into the image.",
            full = true,
            code = """
                id: dbt_run_k8s
                namespace: tenant.data_eng

                tasks:
                  - id: dbt_run
                    type: io.kestra.plugin.scripts.shell.Commands
                    taskRunner:
                      type: io.kestra.plugin.lakehouse.k8s.KubernetesTaskRunner
                      namespace: tenant-data-eng
                      containerImage: ghcr.io/yourorg/dbt-runner:latest
                    commands:
                      - dbt run --project-dir /workspace/dbt --profiles-dir /workspace/dbt
                """
        )
    }
)
public class KubernetesTaskRunner extends TaskRunner<TaskRunnerDetailResult> {

    @Schema(
        title = "Target Kubernetes namespace.",
        description = "The tenant-owned namespace every Pod is created in. This is the only namespace the runner's RBAC needs access to — it is deliberately not overridable per-task-run from flow input, only set here in the flow/taskRunner definition an operator controls."
    )
    private Property<String> namespace;

    @Schema(
        title = "Container image to run the task command in.",
        description = "No default — the image is what makes the task runnable at all (dbt + deps, or however you built it)."
    )
    private Property<String> containerImage;

    @Schema(title = "Kubernetes ServiceAccount the Pod itself runs as, inside the target namespace.")
    @Builder.Default
    private Property<String> podServiceAccount = Property.of("default");

    @Schema(title = "CPU / memory requests and limits for the task container.")
    private PodResources resources;

    @Schema(title = "How long to wait for the Pod to reach Running before failing the task.")
    @Builder.Default
    private Property<Duration> waitUntilRunning = Property.of(Duration.ofMinutes(5));

    @Schema(title = "How long to wait for the Pod to reach a terminal phase before failing the task.")
    @Builder.Default
    private Property<Duration> waitUntilCompletion = Property.of(Duration.ofHours(1));

    @Schema(title = "Delete the Pod after completion. Set to `false` to leave it behind for `kubectl logs`/`describe` debugging.")
    @Builder.Default
    private Property<Boolean> delete = Property.of(true);

    @Override
    public TaskRunnerResult<TaskRunnerDetailResult> run(RunContext runContext, TaskCommands taskCommands, List<String> filesToDownload) throws Exception {
        Logger logger = runContext.logger();
        AbstractLogConsumer defaultLogConsumer = taskCommands.getLogConsumer();

        String renderedNamespace = runContext.render(this.namespace).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("`namespace` is required — this runner refuses to guess a tenant namespace"));
        String renderedImage = runContext.render(this.containerImage).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("`containerImage` is required"));
        String serviceAccount = runContext.render(this.podServiceAccount).as(String.class).orElse("default");
        boolean deletePod = runContext.render(this.delete).as(Boolean.class).orElse(true);
        Duration runningTimeout = runContext.render(this.waitUntilRunning).as(Duration.class).orElse(Duration.ofMinutes(5));
        Duration completionTimeout = runContext.render(this.waitUntilCompletion).as(Duration.class).orElse(Duration.ofHours(1));

        List<String> renderedCommands = runContext.render(taskCommands.getCommands()).asList(String.class);
        Map<String, String> env = this.env(runContext, taskCommands);

        String podName = "kestra-" + UUID.randomUUID().toString().substring(0, 8);

        logger.debug(
            "Starting Pod {} in namespace {} (image={}, working dir on worker={} — not synced in phase 1)",
            podName, renderedNamespace, renderedImage, taskCommands.getWorkingDirectory()
        );

        // in-cluster config is used automatically when the worker pod's own mounted
        // ServiceAccount token is present, i.e. when this runs inside the k8s cluster
        // as it will on satya-homelab. No kubeconfig fields on this runner by design —
        // the only identity it should ever act as is the worker's own bound ServiceAccount.
        try (KubernetesClient client = new KubernetesClientBuilder().build()) {
            Pod pod = buildPod(runContext, podName, renderedNamespace, renderedImage, serviceAccount, renderedCommands, env);

            client.pods().inNamespace(renderedNamespace).resource(pod).create();

            try {
                client.pods().inNamespace(renderedNamespace).withName(podName)
                    .waitUntilCondition(p -> p != null && p.getStatus() != null && p.getStatus().getPhase() != null
                            && !p.getStatus().getPhase().equals("Pending"),
                        runningTimeout.toMillis(), TimeUnit.MILLISECONDS);

                streamLogs(client, renderedNamespace, podName, defaultLogConsumer);

                Pod finished = client.pods().inNamespace(renderedNamespace).withName(podName)
                    .waitUntilCondition(p -> p != null && p.getStatus() != null && p.getStatus().getPhase() != null
                            && (p.getStatus().getPhase().equals("Succeeded") || p.getStatus().getPhase().equals("Failed")),
                        completionTimeout.toMillis(), TimeUnit.MILLISECONDS);

                int exitCode = extractExitCode(finished);

                if (exitCode != 0) {
                    throw new TaskException(exitCode, defaultLogConsumer);
                }

                logger.debug("Pod {} succeeded with exit code {}", podName, exitCode);
                return new TaskRunnerResult<>(exitCode, defaultLogConsumer);
            /*} catch (InterruptedException e) {
                logger.warn("Deleting Pod {} for InterruptedException", podName);
                client.pods().inNamespace(renderedNamespace).withName(podName).delete();
                throw e;*/
            } finally {
		// fabric8's waitUntilCondition() doesn't declare a checked InterruptedException
                // the way java.lang.Process.waitFor() does — worker-thread interruption during a
                // wait, a wait timeout, and any other fabric8 client failure all surface here as
                // one of fabric8's own unchecked exceptions instead. This one finally block
                // covers cleanup for all of them uniformly rather than needing a separate catch
                // per exception type.    
                if (deletePod) {
                    client.pods().inNamespace(renderedNamespace).withName(podName).delete();
                } else {
                    logger.warn("delete=false — leaving Pod {}/{} behind for debugging", renderedNamespace, podName);
                }
            }
        }
    }

    private Pod buildPod(RunContext runContext, String podName, String namespace, String image, String serviceAccount,
                          List<String> commands, Map<String, String> env) {
        List<EnvVar> envVars = new ArrayList<>();
        env.forEach((k, v) -> envVars.add(new EnvVar(k, v, null)));

        ContainerBuilder containerBuilder = new ContainerBuilder()
            .withName("task")
            .withImage(image)
            .withCommand("sh", "-c", String.join(" && ", commands))
            .withEnv(envVars);

        if (this.resources != null) {
            ResourceRequirementsBuilder rb = new ResourceRequirementsBuilder();
            Map<String, Quantity> requests = new HashMap<>();
            Map<String, Quantity> limits = new HashMap<>();
            Optional.ofNullable(this.resources.getCpuRequest()).flatMap(p -> renderProp(runContext, p))
                .ifPresent(v -> requests.put("cpu", new Quantity(v)));
            Optional.ofNullable(this.resources.getMemoryRequest()).flatMap(p -> renderProp(runContext, p))
                .ifPresent(v -> requests.put("memory", new Quantity(v)));
            Optional.ofNullable(this.resources.getCpuLimit()).flatMap(p -> renderProp(runContext, p))
                .ifPresent(v -> limits.put("cpu", new Quantity(v)));
            Optional.ofNullable(this.resources.getMemoryLimit()).flatMap(p -> renderProp(runContext, p))
                .ifPresent(v -> limits.put("memory", new Quantity(v)));
            containerBuilder.withResources(rb.withRequests(requests).withLimits(limits).build());
        }

        return new PodBuilder()
            .withNewMetadata()
                .withName(podName)
                .withNamespace(namespace)
                .withLabels(Map.of(
                    "app.kubernetes.io/managed-by", "kestra",
                    "kestra.io/runner", "plugin-dbt-k8s-runner"
                ))
            .endMetadata()
            .withNewSpec()
                .withServiceAccountName(serviceAccount)
                .withRestartPolicy("Never")
                .withContainers(containerBuilder.build())
            .endSpec()
            .build();
    }

    private Optional<String> renderProp(RunContext runContext, Property<String> property) {
        try {
            return runContext.render(property).as(String.class);
        } catch (io.kestra.core.exceptions.IllegalVariableEvaluationException e) {
            throw new RuntimeException(e);
        }
    }

    private void streamLogs(KubernetesClient client, String namespace, String podName, AbstractLogConsumer logConsumer) {
        try (LogWatch watch = client.pods().inNamespace(namespace).withName(podName)
                .inContainer("task").watchLog()) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(watch.getOutput(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    // Kubernetes pod logs interleave stdout/stderr into a single stream —
                    // there is no reliable way to split them back out from `kubectl logs`
                    // (or the equivalent client-go/fabric8 API), so everything is reported
                    // as stdout (isStdErr=false). Unlike the core Process runner, which
                    // reads the two OS-level streams separately and can report accurately.
                    logConsumer.accept(line, false);
                }
            }
        } catch (Exception e) {
            try {
                logConsumer.accept("log stream error: " + e.getMessage(), true);
            } catch (Exception ignored) {
                // do nothing if we cannot send the error message to the log consumer
            }
        }
    }

    private int extractExitCode(Pod pod) {
        if (pod == null || pod.getStatus() == null || pod.getStatus().getContainerStatuses() == null) {
            return -1;
        }
        return pod.getStatus().getContainerStatuses().stream()
            .filter(cs -> "task".equals(cs.getName()))
            .findFirst()
            .map(ContainerStatus::getState)
            .map(ContainerState::getTerminated)
            .map(ContainerStateTerminated::getExitCode)
            .orElse(-1);
    }

    @Override
    protected Map<String, Object> runnerAdditionalVars(RunContext runContext, TaskCommands taskCommands) {
        // No shared working dir with the worker in phase 1 — the Pod's own filesystem is
        // whatever the image provides. /workspace is just a documented convention for
        // images built for this runner, not something this runner creates or mounts itself.
        Map<String, Object> vars = new HashMap<>();
        vars.put("workingDir", "/workspace");
        return vars;
    }

    @Builder
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PodResources {
        @Schema(title = "CPU request, e.g. `500m`.")
        private Property<String> cpuRequest;

        @Schema(title = "CPU limit, e.g. `1`.")
        private Property<String> cpuLimit;

        @Schema(title = "Memory request, e.g. `512Mi`.")
        private Property<String> memoryRequest;

        @Schema(title = "Memory limit, e.g. `1Gi`.")
        private Property<String> memoryLimit;
    }
}
