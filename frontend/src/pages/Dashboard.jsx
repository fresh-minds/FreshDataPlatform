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
        name: "Monitoring (Grafana)",
        description: "Metrics, logs, and traces in a unified dashboard.",
        url: serviceUrls.grafana,
        icon: LineChart,
        color: "#16A34A"
    },
    {
        name: "MinIO S3",
        description: "Object storage browser and management.",
        url: serviceUrls.minioConsole,
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

const QUICK_ACTIONS = [
    { label: 'Open analytics', href: serviceUrls.superset },
    { label: 'Open monitoring', href: serviceUrls.grafana }
].filter((link) => hasServiceUrl(link.href));

const PRIMARY_ENDPOINTS = [
    { label: 'Airflow UI', href: serviceUrls.airflow },
    { label: 'DataHub', href: serviceUrls.datahub },
    { label: 'Grafana', href: serviceUrls.grafana },
    { label: 'MinIO Console', href: serviceUrls.minioConsole }
].filter((link) => hasServiceUrl(link.href));

function Dashboard() {
    return (
        <div className="docs-page docs-dashboard">
            <div className="docs-container">
                <header className="docs-hero reveal" style={{ '--delay': '0s' }}>
                    <div className="docs-nav">
                        <span className="docs-tag">Launchpad</span>
                        <div className="environment-badge">{isLocalEnvironment ? 'LOCAL ENV' : 'AKS ENV'}</div>
                    </div>
                    <div className="docs-hero-grid">
                        <div className="docs-hero-copy">
                            <p className="docs-eyebrow">Open Data Platform</p>
                            <h1>Choose your workspace.</h1>
                            <p className="docs-lead">
                                This dashboard is the front door to the platform. Launch each surface to
                                orchestrate pipelines, trace lineage, explore data, and monitor operations.
                            </p>
                            <div className="docs-hero-actions">
                                <a href="/docs" className="docs-chip">How the system works</a>
                                <a href="/overview" className="docs-chip">One-screen overview</a>
                                <a href="#service-grid" className="docs-chip">Service map</a>
                                {QUICK_ACTIONS.map((action) => (
                                    <a key={action.label} href={action.href} className="docs-chip" target="_blank" rel="noreferrer">
                                        {action.label}
                                    </a>
                                ))}
                            </div>
                        </div>
                        <div className="docs-hero-panel">
                            <div className="panel-header">
                                <Terminal size={20} />
                                <h3>At-a-glance status</h3>
                            </div>
                            <div className="panel-list">
                                <div className="panel-item">
                                    <span>Orchestration</span>
                                    <strong>Airflow + DAG scheduling</strong>
                                </div>
                                <div className="panel-item">
                                    <span>Catalog & lineage</span>
                                    <strong>DataHub services</strong>
                                </div>
                                <div className="panel-item">
                                    <span>Storage & warehouse</span>
                                    <strong>MinIO + Postgres</strong>
                                </div>
                                <div className="panel-item">
                                    <span>BI & reporting</span>
                                    <strong>Superset dashboards</strong>
                                </div>
                            </div>
                            <div className="panel-footer">
                                <span>Helpful entry points</span>
                                <div className="panel-tags">
                                    {PRIMARY_ENDPOINTS.map((link) => (
                                        <a key={link.label} href={link.href} target="_blank" rel="noreferrer">
                                            {link.label}
                                        </a>
                                    ))}
                                </div>
                            </div>
                        </div>
                    </div>
                </header>

                <section id="service-grid" className="docs-section reveal" style={{ '--delay': '0.15s' }}>
                    <div className="section-head">
                        <p className="section-kicker">Service map</p>
                        <h2>Everything you can open from here</h2>
                        <p>
                            Each card maps to a service in this environment. Status indicators show whether the
                            UI is reachable right now.
                        </p>
                    </div>
                    <div className="dashboard-grid docs-grid">
                        {SERVICES.map((svc) => (
                            <ServiceCard key={svc.name} {...svc} />
                        ))}
                    </div>
                </section>

                <footer className="docs-footer reveal" style={{ '--delay': '0.3s' }}>
                    <div>
                        <h2>Need the full architecture?</h2>
                        <p>Open the documentation page for the end-to-end system walkthrough.</p>
                    </div>
                    <a href="/docs" className="docs-back primary">Open documentation</a>
                </footer>
            </div>
        </div>
    );
}

export default Dashboard;
