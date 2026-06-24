#!/usr/bin/env bash
# Shared Java agent flags for DIFC processing-policy capture (difc-for-kafka policy-agent).

policy_agent_jar() {
  local module_jar="${KAFKA_HOME}/security/policy-agent/build/libs/policy-agent-1.0.0.jar"
  local root_jar="${KAFKA_HOME}/build/libs/policy-agent-1.0.0.jar"
  if [[ -f "${module_jar}" ]]; then
    echo "${module_jar}"
  else
    echo "${root_jar}"
  fi
}

policy_trusted_ca_path() {
  local mkcert_ca="${KAFKA_HOME}/security/policy-agent/mkcert-ca/rootCA.pem"
  if [[ -f "${mkcert_ca}" ]]; then
    echo "${mkcert_ca}"
  else
    echo "${KAFKA_HOME}/security/policy-agent/src/main/resources/trusted-ca.pem"
  fi
}

grantor_policy_trust_java_opts() {
  local trusted_ca state_dir
  trusted_ca="$(policy_trusted_ca_path)"
  state_dir="${1:-${STATE_DIR:-/tmp/sbk-kafka-demo}}"
  cat <<EOF
-Dpolicy.grantor.trusted.ca.path=${trusted_ca}
-Dpolicy.registry.dir=${state_dir}/policy
-Dpolicy.agent.kafka.bootstrap=${BOOTSTRAP_SERVERS:-localhost:9092,localhost:9094,localhost:9096,localhost:9098}
EOF
}

policy_agent_java_opts() {
  local principal="$1"
  local app_id="$2"
  local state_dir="${3:-${STATE_DIR:-/tmp/sbk-kafka-demo}}"
  local jar trusted_ca
  jar="$(policy_agent_jar)"
  trusted_ca="$(policy_trusted_ca_path)"
  if [[ ! -f "${jar}" ]]; then
    echo "WARN: policy-agent jar missing at ${jar}" >&2
    return 0
  fi
  local policy_dir="${state_dir}/policy/${principal}"
  local signing_dir="${KAFKA_HOME}/security/policy-agent/policy-signing"
  mkdir -p "${policy_dir}"
  cat <<EOF
-javaagent:${jar}
-Dpolicy.agent.network.enforcement=false
-Dpolicy.agent.allowed.external.hosts=${POLICY_AGENT_ALLOWED_EXTERNAL_HOSTS:-}
-Dpolicy.agent.kafka.bootstrap=${BOOTSTRAP_SERVERS}
-Dpolicy.agent.trusted.ca.path=${trusted_ca}
-Dpolicy.grantor.trusted.ca.path=${trusted_ca}
-Dpolicy.agent.signing.key.path=${signing_dir}/policy-signing-key.pem
-Dpolicy.agent.signing.cert.path=${signing_dir}/policy-signing-cert.pem
-Dpolicy.app.principal=${principal}
-Dpolicy.app.id=${app_id}
-Dpolicy.registry.dir=${state_dir}/policy
-Dpolicy.dsl.json.path=${policy_dir}/processing-policy.json
-Dpolicy.dsl.dot.path=${policy_dir}/dsl-topology.dot
EOF
}
