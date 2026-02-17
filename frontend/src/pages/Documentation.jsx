import React from 'react';
import { Link } from 'react-router-dom';
import {
    Activity,
    ArrowUpRight,
    Boxes,
    Cloud,
    Database,
    HardDrive,
    Layers,
    LineChart,
    ShieldCheck,
    Terminal,
    Workflow
} from 'lucide-react';
import { hasServiceUrl, isLocalEnvironment, serviceUrls } from '../config/serviceUrls';

const QUICK_LINKS = [
    { label: 'System map', href: '#system-map' },
    { label: 'Data journey', href: '#data-journey' },
    { label: 'Interaction loops', href: '#interaction-loops' },
    { label: 'Repo map', href: '#repo-map' }
];

const SYSTEM_COLUMNS = [
    {
        title: 'Front-end surfaces',
        icon: Terminal,
        items: [
            {
                name: 'Launchpad dashboard',
                detail: 'Single entry point that routes operators to every UI and reports service reachability.'
            },
            {
                name: 'Airflow UI',
                detail: 'Trigger, monitor, and debug pipelines with task-level visibility.'
            },
            {
                name: 'DataHub UI',
                detail: 'Searchable catalog, lineage, ownership, and governance workflows.'
            },
            {
                name: 'Superset UI',
                detail: 'Self-serve analytics, dashboards, and SQL exploration.'
            },
            {
                name: 'MinIO Console',
                detail: 'Inspect object storage, buckets, and file lineage at rest.'
            }
        ]
    },
    {
        title: 'Control plane',
        icon: Workflow,
        items: [
            {
                name: 'Airflow scheduler + workers',
                detail: 'Executes DAGs, coordinates retries, and publishes task state.'
            },
            {
                name: 'Airflow webserver',
                detail: 'Serves the UI/API, task logs, and DAG metadata.'
            },
            {
                name: 'Metadata emission',
                detail: 'Tasks push dataset and lineage updates into DataHub.'
            },
            {
                name: 'Quality checks',
                detail: 'Validation and profiling steps gate each pipeline stage.'
            }
        ]
    },
    {
        title: 'Data plane',
        icon: Database,
        items: [
            {
                name: 'MinIO object storage',
                detail: 'Bronze, silver, and gold buckets store raw, refined, and curated assets.'
            },
            {
                name: 'Warehouse (Postgres)',
                detail: 'Analytics-ready tables for fast BI queries and modeling.'
            },
            {
                name: 'DataHub storage',
                detail: 'MySQL, Kafka, and Elasticsearch power metadata state and search.'
            },
            {
                name: 'Operational databases',
                detail: 'Airflow + Superset metadata stores for app configuration.'
            }
        ]
    }
];

const DATA_JOURNEY = [
    {
        title: 'Ingest & land',
        summary: 'Sources arrive as files or extracts and are stored immutably.',
        details: 'Landing zone is the MinIO bronze bucket, preserving raw payloads for replay.'
    },
    {
        title: 'Validate & standardize',
        summary: 'Airflow tasks clean data, enforce schema, and log quality signals.',
        details: 'Outputs move into the silver layer for consistent naming and typing.'
    },
    {
        title: 'Transform & model',
        summary: 'Business logic, joins, and aggregates create trusted datasets.',
        details: 'Gold outputs land in MinIO and curated tables land in the warehouse.'
    },
    {
        title: 'Catalog & document',
        summary: 'Metadata is emitted to DataHub for discovery and lineage tracing.',
        details: 'Owners, domains, and schemas stay in sync with pipeline runs.'
    },
    {
        title: 'Serve & analyze',
        summary: 'Superset queries the warehouse for dashboards and ad-hoc insights.',
        details: 'Analysts work from trusted models with lineage back to source.'
    }
];

const INTERACTION_LOOPS = [
    {
        title: 'Analytics loop',
        icon: LineChart,
        steps: [
            'User explores a Superset dashboard or SQL lab.',
            'Superset sends queries to the warehouse Postgres service.',
            'Results render in charts and dashboards for fast iteration.'
        ]
    },
    {
        title: 'Orchestration loop',
        icon: Activity,
        steps: [
            'Operator triggers or schedules a DAG in Airflow.',
            'Scheduler runs tasks that read/write MinIO and the warehouse.',
            'Task state, logs, and retries flow back to the Airflow UI.'
        ]
    },
    {
        title: 'Metadata loop',
        icon: Boxes,
        steps: [
            'Pipelines emit lineage and dataset metadata during execution.',
            'DataHub services persist metadata and index it for search.',
            'Catalog updates surface instantly for governance workflows.'
        ]
    }
];

const STORAGE_LAYERS = [
    {
        name: 'Bronze (raw)',
        detail: 'Immutable landing zone for source extracts and raw files.',
        emphasis: 'Replayable, auditable inputs.'
    },
    {
        name: 'Silver (refined)',
        detail: 'Validated, standardized datasets with consistent schemas.',
        emphasis: 'Quality gates live here.'
    },
    {
        name: 'Gold (curated)',
        detail: 'Business-ready models powering dashboards and reporting.',
        emphasis: 'Trusted, consumption-ready data.'
    }
];

const SERVICE_PORTS = [
    { name: 'Airflow UI', url: serviceUrls.airflow, purpose: 'Orchestration & monitoring' },
    { name: 'DataHub UI', url: serviceUrls.datahub, purpose: 'Catalog & lineage' },
    { name: 'Superset UI', url: serviceUrls.superset, purpose: 'BI & dashboards' },
    { name: 'Grafana', url: serviceUrls.grafana, purpose: 'Monitoring dashboards' },
    { name: 'Prometheus', url: serviceUrls.prometheus, purpose: 'Metrics store & alerting' },
    { name: 'MinIO Console', url: serviceUrls.minioConsole, purpose: 'Object storage management' },
    { name: 'MinIO S3 API', url: serviceUrls.minioApi, purpose: 'Programmatic storage access' }
].filter((service) => hasServiceUrl(service.url));

const REPO_MAP = [
    { path: 'dags/', detail: 'Airflow DAG definitions and scheduling logic.' },
    { path: 'pipelines/', detail: 'Pipeline code that reads, transforms, and loads data.' },
    { path: 'scripts/', detail: 'Utility scripts, bootstrap tasks, and data helpers.' },
    { path: 'dbt_parallel/', detail: 'dbt models for analytics transformations.' },
    { path: 'frontend/', detail: 'Launchpad UI and documentation experience.' },
    { path: 'docs/', detail: 'Long-form documentation and design notes.' },
    { path: 'guides/', detail: 'Operational playbooks and walkthroughs.' },
    { path: 'docker-compose.yml', detail: 'Local environment service topology.' }
];

function Documentation() {
    return (
        <div className="docs-page">
            <div className="docs-container">
                <header className="docs-hero reveal" style={{ '--delay': '0s' }}>
                    <div className="docs-nav">
                        <Link to="/platform" className="docs-back">← Back to dashboard</Link>
                        <span className="docs-tag">System documentation</span>
                    </div>

                    <div className="docs-hero-grid">
                        <div className="docs-hero-copy">
                            <p className="docs-eyebrow">Open Data Platform</p>
                            <h1>How the application works, end to end.</h1>
                            <p className="docs-lead">
                                This guide connects the UI surfaces to the backend services and explains how data moves
                                from source to dashboard. It is designed as an operator-ready map of the full stack.
                            </p>
                            <div className="docs-hero-actions">
                                {QUICK_LINKS.map((link) => (
                                    <a key={link.href} href={link.href} className="docs-chip">
                                        {link.label}
                                    </a>
                                ))}
                            </div>
                        </div>

                        <div className="docs-hero-panel">
                            <div className="panel-header">
                                <Cloud size={20} />
                                <h3>{isLocalEnvironment ? 'Local stack summary' : 'AKS stack summary'}</h3>
                            </div>
                            <div className="panel-list">
                                <div className="panel-item">
                                    <span>Orchestration</span>
                                    <strong>Airflow webserver + scheduler</strong>
                                </div>
                                <div className="panel-item">
                                    <span>Storage</span>
                                    <strong>MinIO (bronze / silver / gold)</strong>
                                </div>
                                <div className="panel-item">
                                    <span>Warehouse</span>
                                    <strong>Postgres analytics database</strong>
                                </div>
                                <div className="panel-item">
                                    <span>Catalog</span>
                                    <strong>DataHub metadata services</strong>
                                </div>
                                <div className="panel-item">
                                    <span>BI</span>
                                    <strong>Superset dashboards</strong>
                                </div>
                            </div>
                            <div className="panel-footer">
                                <span>Primary UI endpoints</span>
                                <div className="panel-tags">
                                    {SERVICE_PORTS.slice(0, 4).map((service) => (
                                        <a
                                            key={service.name}
                                            href={service.url}
                                            target="_blank"
                                            rel="noreferrer"
                                        >
                                            {service.name}
                                            <ArrowUpRight size={14} />
                                        </a>
                                    ))}
                                </div>
                            </div>
                        </div>
                    </div>
                </header>

                <section id="system-map" className="docs-section reveal" style={{ '--delay': '0.1s' }}>
                    <div className="section-head">
                        <p className="section-kicker">System map</p>
                        <h2>Front-end surfaces and backend planes</h2>
                        <p>
                            The platform is organized into three planes: the UI surfaces operators touch, the control
                            plane that schedules and validates work, and the data plane that stores assets and metadata.
                        </p>
                    </div>
                    <div className="system-grid">
                        {SYSTEM_COLUMNS.map((column) => (
                            <div key={column.title} className="system-column">
                                <div className="system-title">
                                    <column.icon size={18} />
                                    <h3>{column.title}</h3>
                                </div>
                                <div className="system-items">
                                    {column.items.map((item) => (
                                        <div key={item.name} className="system-item">
                                            <strong>{item.name}</strong>
                                            <p>{item.detail}</p>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        ))}
                    </div>
                </section>

                <section id="data-journey" className="docs-section reveal" style={{ '--delay': '0.2s' }}>
                    <div className="section-head">
                        <p className="section-kicker">Data journey</p>
                        <h2>From source to dashboard</h2>
                        <p>
                            Every dataset follows the same backbone: land, validate, transform, catalog, and serve. This
                            keeps lineage consistent and ensures every dashboard traces back to the original source.
                        </p>
                    </div>
                    <div className="flow-list">
                        {DATA_JOURNEY.map((step, index) => (
                            <div key={step.title} className="flow-step">
                                <div className="flow-index">0{index + 1}</div>
                                <div>
                                    <h3>{step.title}</h3>
                                    <p>{step.summary}</p>
                                    <span>{step.details}</span>
                                </div>
                            </div>
                        ))}
                    </div>
                </section>

                <section id="interaction-loops" className="docs-section reveal" style={{ '--delay': '0.3s' }}>
                    <div className="section-head">
                        <p className="section-kicker">Interaction loops</p>
                        <h2>How front-end actions reach the backend</h2>
                        <p>
                            Each UI has a tight feedback loop with backend services. These loops keep operators informed
                            and allow safe iteration without breaking data guarantees.
                        </p>
                    </div>
                    <div className="loop-grid">
                        {INTERACTION_LOOPS.map((loop) => (
                            <div key={loop.title} className="loop-card">
                                <div className="loop-header">
                                    <loop.icon size={20} />
                                    <h3>{loop.title}</h3>
                                </div>
                                <ul>
                                    {loop.steps.map((step) => (
                                        <li key={step}>{step}</li>
                                    ))}
                                </ul>
                            </div>
                        ))}
                    </div>
                </section>

                <section className="docs-section reveal" style={{ '--delay': '0.4s' }}>
                    <div className="section-head">
                        <p className="section-kicker">Storage layers</p>
                        <h2>Bronze, silver, gold: a shared contract</h2>
                        <p>
                            Storage layers keep the platform predictable. Each layer has clear rules for mutability and
                            data quality, which makes both debugging and governance straightforward.
                        </p>
                    </div>
                    <div className="layer-grid">
                        {STORAGE_LAYERS.map((layer) => (
                            <div key={layer.name} className="layer-card">
                                <Layers size={18} />
                                <h3>{layer.name}</h3>
                                <p>{layer.detail}</p>
                                <span>{layer.emphasis}</span>
                            </div>
                        ))}
                    </div>
                </section>

                <section className="docs-section reveal" style={{ '--delay': '0.5s' }}>
                    <div className="section-head">
                        <p className="section-kicker">Interfaces</p>
                        <h2>Key services and endpoints</h2>
                        <p>
                            These endpoints are the primary collaboration surfaces. They map cleanly to the service
                            cards on the dashboard and provide direct access for operators.
                        </p>
                    </div>
                    <div className="interface-grid">
                        {SERVICE_PORTS.map((service) => (
                            <div key={service.name} className="interface-card">
                                <div>
                                    <h3>{service.name}</h3>
                                    <p>{service.purpose}</p>
                                </div>
                                <a href={service.url} target="_blank" rel="noreferrer">
                                    {service.url}
                                    <ArrowUpRight size={14} />
                                </a>
                            </div>
                        ))}
                    </div>
                </section>

                <section id="repo-map" className="docs-section reveal" style={{ '--delay': '0.6s' }}>
                    <div className="section-head">
                        <p className="section-kicker">Repository map</p>
                        <h2>Where the work lives</h2>
                        <p>
                            These paths anchor the system. They connect configuration, orchestration, transformation,
                            and documentation into a single working workspace.
                        </p>
                    </div>
                    <div className="repo-grid">
                        {REPO_MAP.map((item) => (
                            <div key={item.path} className="repo-card">
                                <div className="repo-path">
                                    <HardDrive size={16} />
                                    <span>{item.path}</span>
                                </div>
                                <p>{item.detail}</p>
                            </div>
                        ))}
                    </div>
                </section>

                <section className="docs-section reveal" style={{ '--delay': '0.7s' }}>
                    <div className="section-head">
                        <p className="section-kicker">Trust & governance</p>
                        <h2>Operational safeguards that keep data reliable</h2>
                        <p>
                            Governance is embedded in execution. Each pipeline stage emits metadata, quality checks,
                            and audit signals to keep the catalog and dashboards trustworthy.
                        </p>
                    </div>
                    <div className="guardrail-grid">
                        <div className="guardrail-card">
                            <ShieldCheck size={20} />
                            <h3>Access control</h3>
                            <p>Credentials are managed centrally and injected through environment configuration.</p>
                        </div>
                        <div className="guardrail-card">
                            <Database size={20} />
                            <h3>Lineage visibility</h3>
                            <p>DataHub captures dataset owners, schemas, and upstream dependencies per run.</p>
                        </div>
                        <div className="guardrail-card">
                            <Activity size={20} />
                            <h3>Operational health</h3>
                            <p>Airflow task states, logs, and retries provide immediate feedback on failures.</p>
                        </div>
                    </div>
                </section>

                <footer className="docs-footer reveal" style={{ '--delay': '0.8s' }}>
                    <div>
                        <h2>Ready to dive deeper?</h2>
                        <p>Return to the dashboard to launch any service or continue exploring the stack.</p>
                    </div>
                    <Link to="/platform" className="docs-back primary">
                        Back to dashboard
                    </Link>
                </footer>
            </div>
        </div>
    );
}

export default Documentation;
