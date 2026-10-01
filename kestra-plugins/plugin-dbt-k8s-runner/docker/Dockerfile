# Bakes plugin-dbt-k8s-runner's shaded jar into the same Kestra image
# already deployed on satya-homelab (see helm-values/kestra-values.yaml,
# image tag v1.3.37 -- keep this pinned version and gradle.properties'
# kestraVersion in sync when either one changes).
#
# Build (from kestra-plugins/plugin-dbt-k8s-runner/):
#   ./gradlew shadowJar
#   docker build -f docker/Dockerfile -t kestra-lakehouse:v1.3.37-dbt-k8s-runner .

FROM kestra/kestra:v1.3.37

# Kestra auto-loads any jar dropped in /app/plugins on startup -- no
# `kestra plugins install` step needed for a jar that's already local.
COPY build/libs/plugin-dbt-k8s-runner-*.jar /app/plugins/
