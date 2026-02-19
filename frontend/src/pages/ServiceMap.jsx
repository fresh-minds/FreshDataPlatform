import React from 'react';
import { Database, Activity, LayoutDashboard, HardDrive, Terminal, LineChart } from 'lucide-react';
import ServiceCard from '../components/ServiceCard';
import { hasServiceUrl, isLocalEnvironment, serviceUrls } from '../config/serviceUrls';

const SERVICES = [
    {
        name: "Airflow Orchestrator",
        description: "Workflow orchestration and pipeline management.",
        url: serviceUrls.airflow,
        icon: Activity,
        color: "#007BFF"
    },
    {
        name: "DataHub Catalog",
        description: "Modern data discovery, catalog, and lineage.",
        url: serviceUrls.datahub,
        icon: Database,
        color: "#0F766E"
    },
    {
        name: "Superset BI",
        description: "Business intelligence and data visualization.",
        url: serviceUrls.superset,
        icon: LayoutDashboard,
        color: "#F97316"
    },
    {
        name: "JupyterLab",
        description: "Interactive notebooks with direct MinIO and Postgres access.",
        url: serviceUrls.jupyter,
        icon: Terminal,
        color: "#4F46E5"
    },
    {
        name: "Monitoring (Grafana)",
        description: "Metrics, logs, and traces in a unified dashboard.",
        url: serviceUrls.grafana,
        icon: LineChart,
        color: "#16A34A"
    },
    {
        name: "MinIO S3",
        description: "Object storage browser and management.",
        url: serviceUrls.minioUi,
        icon: HardDrive,
        color: "#334155"
    },
    {
        name: "Documentation",
        description: "System walkthroughs, architecture, and data flow guides.",
        url: "/docs",
        icon: Terminal,
        color: "#C2410C"
    },
].filter((service) => hasServiceUrl(service.url));

function ServiceMap() {
    return (
        <div className="docs-page docs-dashboard">
            <div className="docs-container">
                <header className="docs-hero reveal" style={{ '--delay': '0s' }}>
                    <div className="docs-nav">
                        <span className="docs-tag">Service map</span>
                        <div className="environment-badge">{isLocalEnvironment ? 'LOCAL ENV' : 'AKS ENV'}</div>
                    </div>
                    <div className="docs-hero-grid">
                        <div className="docs-hero-copy">
                            <p className="docs-eyebrow">Open Data Platform</p>
                            <h1>Everything you can open from here.</h1>
                            <p className="docs-lead">
                                Each card maps to a service in this environment. Status indicators show whether the
                                UI is reachable right now.
                            </p>
                            <div className="docs-hero-actions">
                                <a href="/platform" className="docs-chip">Back to launchpad</a>
                                <a href="/overview" className="docs-chip">One-screen overview</a>
                            </div>
                        </div>
                    </div>
                </header>

                <section className="docs-section reveal" style={{ '--delay': '0.15s' }}>
                    <div className="section-head">
                        <p className="section-kicker">Service map</p>
                        <h2>Launch any surface</h2>
                        <p>
                            Use these entry points to jump directly to the tools, dashboards, and documentation.
                        </p>
                    </div>
                    <div className="dashboard-grid docs-grid">
                        {SERVICES.map((svc) => (
                            <ServiceCard key={svc.name} {...svc} />
                        ))}
                    </div>
                </section>
            </div>
        </div>
    );
}

export default ServiceMap;
