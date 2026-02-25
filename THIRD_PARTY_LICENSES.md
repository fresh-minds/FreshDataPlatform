# Third-Party Licenses and Legal Risk Notes

Last reviewed: 2026-02-24

This document tracks major third-party software licenses used by this platform stack.
It complements (but does not replace) your project license in [LICENSE](LICENSE).

This is an engineering compliance guide, not legal advice.

## Do You Need To Change Your MIT License?

No, for your own repository code.

- Keep your project `LICENSE` as MIT.
- Third-party components keep their own licenses.
- Your obligation is to comply with those third-party licenses when you run, modify, distribute, or offer related services.

## Stack License Map (Major Runtime Components)

The list below maps the core third-party applications in `docker-compose.yml` to their expected license family and risk profile.

| Component(s) in this stack | Expected license family | Risk level | Why it matters / required action | Primary source |
| --- | --- | --- | --- | --- |
| Airflow, Superset, DataHub, Keycloak, Prometheus, Alertmanager, OpenTelemetry Collector, statsd-exporter, postgres-exporter | Apache-2.0 (per project upstreams) | Low | Preserve notices when redistributing binaries/images; keep attribution records. | [Airflow](https://github.com/apache/airflow), [Superset](https://github.com/apache/superset), [DataHub](https://github.com/datahub-project/datahub), [Keycloak](https://github.com/keycloak/keycloak), [Prometheus](https://github.com/prometheus/prometheus), [Alertmanager](https://github.com/prometheus/alertmanager), [OpenTelemetry Collector](https://github.com/open-telemetry/opentelemetry-collector) |
| PostgreSQL | PostgreSQL License | Low | Permissive license; keep attribution in distribution artifacts. | [PostgreSQL License](https://www.postgresql.org/about/licence/) |
| NGINX | BSD-2-Clause | Low | Permissive license; keep attribution in distribution artifacts. | [nginx repo](https://github.com/nginx/nginx) |
| prometheus-msteams | MIT | Low | Permissive license; keep attribution in distribution artifacts. | [prometheus-msteams repo](https://github.com/prometheus-msteams/prometheus-msteams) |
| MinIO (`minio/minio`), MinIO client (`minio/mc`) | AGPL-3.0-or-later / commercial licensing model | High | Copyleft/network-use obligations can apply depending on deployment and modification model. Review AGPL/commercial terms before commercial distribution. | [MinIO compliance](https://www.min.io/compliance), [mc repo](https://github.com/minio/mc), [minio repo](https://github.com/minio/minio) |
| Grafana, Loki, Tempo | AGPL-3.0-or-later | High | AGPL obligations can be triggered, especially when modified software is offered over a network. Track source-offer and modification process. | [Grafana licensing](https://grafana.com/licensing/), [loki repo](https://github.com/grafana/loki), [tempo repo](https://github.com/grafana/tempo), [grafana repo](https://github.com/grafana/grafana) |
| MySQL Community (`mysql:8.2`) | GPL-2.0 (dual-licensed by Oracle) | High | GPL obligations may apply to redistribution/modification scenarios. Confirm commercial/OEM terms if needed. | [MySQL Community](https://www.mysql.com/products/community/), [MySQL OEM licensing](https://www.mysql.com/about/legal/licensing/oem/) |
| Confluent Schema Registry / ZooKeeper images | Confluent Community License (source-available) | High | Not a standard permissive OSS license; includes field-of-use/commercial restrictions. Legal review required before redistribution/commercial use. | [Confluent Community License FAQ](https://www.confluent.io/confluent-community-license-faq/), [Confluent license docs](https://docs.confluent.io/platform/current/installation/license.html) |
| Confluent Kafka image (`cp-kafka`) | Primarily Apache-2.0 Kafka distribution, but verify exact image terms/version docs | Medium | Keep per-version verification because Confluent images can mix packages with different terms. | [Confluent image reference](https://docs.confluent.io/platform/6.2/installation/docker/image-reference.html), [Confluent Kafka package docs](https://docs.confluent.io/platform/current/installation/license.html) |
| Elasticsearch image (`docker.elastic.co/elasticsearch/elasticsearch:7.10.2`) | Elastic License 2.0 / source-available terms | High | Not standard permissive OSS; review restrictions for managed-service and redistribution scenarios. | [Elastic licensing FAQ](https://www.elastic.co/pricing/faq/licensing), [Elastic licensing change](https://www.elastic.co/blog/licensing-change) |

## Practical Compliance Controls

Use this checklist before shipping outside internal/dev usage:

1. Keep [LICENSE](LICENSE) as MIT for your own code.
2. Keep this file up to date when images/dependencies change.
3. Maintain a release artifact bundle with:
   - your MIT license
   - third-party notices/license texts for distributed binaries/images
4. For AGPL/GPL/source-available components (MinIO, Grafana family, MySQL, Confluent CCL, Elastic), perform a legal review before:
   - commercial offering
   - redistribution to customers
   - offering modified versions as a network service
5. Pin runtime image digests for production releases and keep a versioned SBOM/license snapshot per release.

## Repository Guardrail

Use the built-in risk check to flag potentially restrictive runtime images:

```bash
make license-risk-check
```

To fail CI when high-risk families are present:

```bash
FAIL_ON_RESTRICTIVE=true ./scripts/quality/check_license_risk.sh docker-compose.yml
```

This command is a triage guardrail, not a substitute for legal counsel.
