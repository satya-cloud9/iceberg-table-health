# plugin-dbt-k8s-runner

A Kestra `TaskRunner` that creates a Pod in a single, fixed **tenant-owned**
Kubernetes namespace to run a task's command — instead of the core `Process`
runner (always the platform worker's own OS process, every task type,
always in the platform namespace, as `helm-values/kestra-values.yaml`'s
standalone deployment guarantees today).

Why this exists, and why `kubectl exec` was rejected instead, is written up
in the Lakehouse Series article ("who owns the pipeline" section) and in
this session's design discussion — short version: the platform layer
(Kestra) must never hold a standing credential that reaches into a specific
pre-existing tenant resource. `kubectl exec`-ing into a tenant's own pod
would be exactly that, backwards from every other dependency direction this
whole homelab enforces. This runner instead only ever *creates its own new
Pod* inside a namespace it holds narrow, Pod-scoped RBAC for — nothing it
didn't create itself, nothing outside that one namespace.

## Scope: what phase 1 is and isn't

**Is:** create a Pod in `namespace`, run the task's rendered shell commands
in it, stream logs back into the Kestra UI via `pods/log`, capture the real
container exit code, delete the Pod. RBAC scoped to exactly one tenant
namespace per deployment of this Role/RoleBinding pair.

**Isn't (yet):** working-directory file sync. The EE Kubernetes runner
solves "get input files into the Pod / output files back out" with an
init-container + sidecar pair; this phase 1 build deliberately skips that
and instead expects the container image to already have what it needs
(dbt project baked in via `docker/dbt-runner.Dockerfile`, or a `git
clone`/`pull` as the task's own first command). File sync is real, useful
phase 2 work — but proving the Pod-in-tenant-namespace mechanics cheaply
first, the same "smallest thing that proves the point" discipline this
whole rollout route has followed, matters more right now than building the
harder half before the easy half is even confirmed working.

## Layout

```
build.gradle, settings.gradle, gradle.properties   Gradle plugin project
lombok.config                                       lombok codegen settings (copied from kestra-io/plugin-template — needed, not optional)
gradlew, gradlew.bat, gradle/wrapper/               Gradle 9.7.1 wrapper — generate once (step 1 below), then commit it
src/main/java/.../KubernetesTaskRunner.java         the runner itself
rbac/tenant-namespace-rbac.yaml                     per-tenant RBAC (apply once per tenant)
docker/Dockerfile                                   bakes the plugin jar into a Kestra image
docker/dbt-runner.Dockerfile                        bakes dbt + the project into a task image
examples/01-smoke-test.yaml                         busybox sanity check — run this first
examples/02-dbt-run.yaml                            the real dbt flow — run this second
```

## Build & test steps

Run these from `kestra-plugins/plugin-dbt-k8s-runner/` unless noted.

### 1. Build the plugin jar

```bash
./gradlew shadowJar
# -> build/libs/plugin-dbt-k8s-runner-0.1.0-SNAPSHOT-all.jar (approx name;
#    shadowJar's archiveClassifier is nulled out in build.gradle, so check
#    the actual filename it produces)
```

If `gradlew` isn't present yet, generate it once with a local Gradle
install. Use **Gradle 9.7.1** specifically — that's what
`kestra-io/plugin-template` itself pins in its own
`gradle/wrapper/gradle-wrapper.properties`, and the
`repository-conventions`/`shadow` plugins this build.gradle uses are built
against that Gradle API. An older Gradle (e.g. Ubuntu's `apt install
gradle`, which lands around 4.4.1, or an arbitrarily-picked newer-but-still-wrong
version like 8.10) fails with a `NoSuchMethodError` on
`AdhocComponentWithVariants` or similar during plugin configuration,
before your code is ever compiled — confirmed the hard way once already,
don't repeat it:

```bash
gradle wrapper --gradle-version 9.7.1
```

If your system `gradle` is too old to even run that command cleanly,
bootstrap once from a real Gradle 9.7.1 binary instead:

```bash
cd /tmp
wget https://services.gradle.org/distributions/gradle-9.7.1-bin.zip
unzip gradle-9.7.1-bin.zip
export PATH="/tmp/gradle-9.7.1/bin:$PATH"
cd -   # back to plugin-dbt-k8s-runner/
gradle wrapper --gradle-version 9.7.1
```

Once `gradlew`/`gradlew.bat`/`gradle/wrapper/` exist and work, **commit
them** — that's the entire point of a Gradle wrapper: the next person (or
CI) gets the exact right Gradle version without repeating any of the above.

**Confirmed working as of this repo's first build**, against real Kestra
`1.3.37` jars, with Gradle 9.7.1: `./gradlew compileJava` succeeds and
`./gradlew shadowJar` produces the jar. Two real issues turned up and are
already fixed in this source, not hypothetical — worth knowing if you're
diffing against an older copy of this plugin:
- `lombok.config` (above) was missing; without it, `@EqualsAndHashCode`
  throws a lombok warning on every class using it.
- `KubernetesTaskRunner.run()`'s cleanup path originally had a
  `catch (InterruptedException e)` around the two `waitUntilCondition`
  calls, copying `Process.java`'s pattern too literally — fabric8's
  `waitUntilCondition` (unlike `java.lang.Process.waitFor()`) doesn't
  declare a checked `InterruptedException`, so that catch doesn't compile.
  Fixed by moving pod cleanup into the `finally` block alone, which covers
  interruption, timeout, and any other fabric8 failure uniformly.

### 2. Build the two Docker images

```bash
# from repo root
docker build -f kestra-plugins/plugin-dbt-k8s-runner/docker/dbt-runner.Dockerfile \
  -t ghcr.io/yourorg/dbt-runner:latest .

# from kestra-plugins/plugin-dbt-k8s-runner/
docker build -f docker/Dockerfile -t kestra-lakehouse:v1.3.37-dbt-k8s-runner .
```

Load both into whatever your homelab cluster uses to pull images (a local
registry, or `k3s ctr images import` / `kind load docker-image` depending
on how satya-homelab's k3s is set up) — this repo's existing Terraform
doesn't yet know about either image, so for now this is a manual step.

### 3. Point the Kestra deployment at the new image

Edit `helm-values/kestra-values.yaml`'s image tag to
`kestra-lakehouse:v1.3.37-dbt-k8s-runner` and `terraform apply` the
`platform` module (or `kubectl set image` directly against the deployment
for a quick manual test before touching Terraform).

### 4. Apply the tenant RBAC

```bash
kubectl get sa -n platform          # confirm the worker's real SA name first
kubectl apply -f rbac/tenant-namespace-rbac.yaml
```

### 5. Register and run the smoke test

```bash
# from wherever 06-register-flows.sh already points at your Kestra instance
kestra flow validate examples/01-smoke-test.yaml
kestra flow namespace update tenant.data_eng examples/01-smoke-test.yaml
```

(Or paste it into the Kestra UI's flow editor and hit Execute — same as
every other flow in this repo so far.)

Confirm in the UI: the log output shows the `hostname` from inside the
Pod (not the worker's own hostname), the flow succeeds, and
`kubectl get pods -n tenant-data-eng` shows nothing left behind once it's
done (or shows the Pod, if you set `delete: false` to debug).

### 6. Run the real dbt flow

Same steps as 5, with `examples/02-dbt-run.yaml`. This is the actual
target: dbt executing physically inside `tenant-data-eng`, not the
platform pod — closing the gap the article's "who owns the pipeline"
section calls out.

## Known gaps, honestly

- **No file sync** (see Scope above) — phase 2.
- **`Process` runner can't be excluded** even after this plugin exists —
  it's core to Kestra, not a separately-installable plugin, so nothing
  stops a flow author from just not setting `taskRunner:` and getting the
  old platform-pod behavior back. Enforcing "dbt only runs via this
  runner" stays a CI/lint-gate convention in OSS, not a structural
  guarantee — Policies that could make it structural are Enterprise-only.
  This matches the two-option plan already agreed: disabling script-based
  plugins via selective plugin installation is the other half of that
  plan, tracked separately.
- **Log stream doesn't distinguish stdout/stderr** — Kubernetes interleaves
  them by the time `pods/log` returns anything, unlike the core `Process`
  runner reading two real OS streams.
- **Resource `Property<String>` values aren't templated against flow
  inputs in a tested way yet** — `renderProp()` calls `runContext.render()`
  the same way `namespace`/`containerImage` do, but this path hasn't been
  exercised against an actual flow input.

