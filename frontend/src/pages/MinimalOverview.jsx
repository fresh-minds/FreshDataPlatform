import React from 'react';
import { ArrowUpRight, Database, Activity, Layers, LineChart, ShieldCheck } from 'lucide-react';
import platformIllustration from '../assets/platform-illustration.png';
import { hasServiceUrl, serviceUrls } from '../config/serviceUrls';

const PRIMARY_LINKS = [
    { label: 'Airflow', href: serviceUrls.airflow },
    { label: 'DataHub', href: serviceUrls.datahub },
    { label: 'Superset', href: serviceUrls.superset },
    { label: 'Grafana', href: serviceUrls.grafana }
].filter((link) => hasServiceUrl(link.href));

const PLATFORM_PILLARS = [
    {
        title: 'Ingest & Orchestrate',
        icon: Activity,
        detail: 'Scheduled DAGs pull sources into the lakehouse and validate arrivals.'
    },
    {
        title: 'Model & Serve',
        icon: Layers,
        detail: 'Bronze → Silver → Gold transformations publish analytics-ready models.'
    },
    {
        title: 'Govern & Discover',
        icon: ShieldCheck,
        detail: 'Catalog, lineage, and ownership live in DataHub for auditability.'
    },
    {
        title: 'Observe & Act',
        icon: LineChart,
        detail: 'Metrics, logs, and traces roll up into Grafana dashboards.'
    }
];

const DATA_FLOW = [
    'Sources → Bronze (raw)',
    'Bronze → Silver (clean)',
    'Silver → Gold (curated)',
    'Gold → Warehouse → Dashboards'
];

function MinimalOverview() {
    return (
        <div className="minimal-page">
            <div className="minimal-shell">
                <header className="minimal-header">
                    <div>
                        <p className="minimal-eyebrow">Open Data Platform</p>
                        <h1>One-screen platform overview.</h1>
                        <p className="minimal-subtitle">
                            Everything you need to understand the stack, the flow, and the monitoring surface at a glance.
                        </p>
                    </div>
                    <div className="minimal-cta">
                        <a href="/architecture">
                            Architecture
                            <ArrowUpRight size={16} />
                        </a>
                        <a href="/docs">
                            Documentation
                            <ArrowUpRight size={16} />
                        </a>
                        {PRIMARY_LINKS.map((link) => (
                            <a key={link.href} href={link.href} target="_blank" rel="noreferrer">
                                {link.label}
                                <ArrowUpRight size={16} />
                            </a>
                        ))}
                    </div>
                </header>

                <section className="minimal-hero-art">
                    <img src={platformIllustration} alt="Warm editorial illustration of the platform" />
                </section>

                <section className="minimal-grid">
                    <div className="minimal-card minimal-stack">
                        <div className="card-title">
                            <Database size={18} />
                            <h2>Platform Map</h2>
                        </div>
                        <div className="stack-map">
                            <div className="stack-col">
                                <span className="stack-label">Sources</span>
                                <div className="stack-node">CRM / Finance</div>
                                <div className="stack-node">HR / Time</div>
                                <div className="stack-node">Email Intake</div>
                            </div>
                            <div className="stack-rail">
                                <span />
                                <span />
                                <span />
                            </div>
                            <div className="stack-col">
                                <span className="stack-label">Lakehouse</span>
                                <div className="stack-node emphasis">Bronze</div>
                                <div className="stack-node">Silver</div>
                                <div className="stack-node">Gold</div>
                            </div>
                            <div className="stack-rail">
                                <span />
                                <span />
                                <span />
                            </div>
                            <div className="stack-col">
                                <span className="stack-label">Consumption</span>
                                <div className="stack-node">Warehouse</div>
                                <div className="stack-node">Superset</div>
                                <div className="stack-node">Exports</div>
                            </div>
                        </div>
                    </div>

                    <div className="minimal-card minimal-pillars">
                        <div className="card-title">
                            <Layers size={18} />
                            <h2>What Happens</h2>
                        </div>
                        <div className="pillar-grid">
                            {PLATFORM_PILLARS.map((pillar) => (
                                <div key={pillar.title} className="pillar">
                                    <pillar.icon size={18} />
                                    <div>
                                        <h3>{pillar.title}</h3>
                                        <p>{pillar.detail}</p>
                                    </div>
                                </div>
                            ))}
                        </div>
                    </div>

                    <div className="minimal-card minimal-flow">
                        <div className="card-title">
                            <Activity size={18} />
                            <h2>Data Journey</h2>
                        </div>
                        <div className="flow-steps">
                            {DATA_FLOW.map((step) => (
                                <span key={step}>{step}</span>
                            ))}
                        </div>
                    </div>

                    <div className="minimal-card minimal-observe">
                        <div className="card-title">
                            <LineChart size={18} />
                            <h2>Monitoring Coverage</h2>
                        </div>
                        <div className="observe-grid">
                            <div>
                                <strong>Metrics</strong>
                                <p>Airflow stats, pipeline counters, warehouse health in Prometheus.</p>
                            </div>
                            <div>
                                <strong>Logs</strong>
                                <p>Airflow + platform logs aggregated in Loki.</p>
                            </div>
                            <div>
                                <strong>Traces</strong>
                                <p>Pipeline spans published via OTEL → Tempo.</p>
                            </div>
                        </div>
                    </div>
                </section>

                <footer className="minimal-footer">
                    <span>Need the full walkthrough?</span>
                    <a href="/architecture">View architecture diagram</a>
                </footer>
            </div>
        </div>
    );
}

export default MinimalOverview;
