import React from 'react';
import {
    Activity,
    BarChart3,
    Database,
    Eye,
    LogOut,
    Network,
    Terminal,
    Users
} from 'lucide-react';
import { hasServiceUrl, isLocalEnvironment, serviceUrls } from '../config/serviceUrls';
import useAuth from '../auth/useAuth';

const OBSERVABILITY_LINKS = [
    { label: 'Grafana', href: serviceUrls.grafana },
    { label: 'Prometheus', href: serviceUrls.prometheus },
    { label: 'Alertmanager', href: serviceUrls.alertmanager }
].filter((link) => hasServiceUrl(link.href));

const OVERVIEW_ACTION = { label: 'One-screen overview', href: '/overview', icon: Eye };
const DIRECTORY_ACTION = { label: 'User directory', href: '/directory', icon: Users };

const DESTINATIONS = [
    {
        subject: 'Orchestration',
        detail: 'Airflow + DAG scheduling',
        label: 'Airflow UI',
        href: serviceUrls.airflow,
        icon: Network
    },
    {
        subject: 'Storage',
        detail: 'MinIO',
        label: 'MinIO Console',
        href: serviceUrls.minioUi,
        icon: Database
    },
    {
        subject: 'Analytics',
        detail: 'Superset workspaces',
        label: 'Open analytics',
        href: serviceUrls.superset,
        icon: BarChart3
    },
    {
        subject: 'Notebook workspace',
        detail: 'Ad hoc analysis with platform data',
        label: 'JupyterLab',
        href: serviceUrls.jupyter,
        icon: Terminal
    },
    {
        subject: 'Catalog & lineage',
        detail: 'DataHub services',
        label: 'DataHub',
        href: serviceUrls.datahub,
        icon: Database
    },
].filter((item) => hasServiceUrl(item.href));

const OVERVIEW_ITEM = {
    subject: 'Overview',
    detail: 'Single-page platform view',
    href: OVERVIEW_ACTION.href,
    label: OVERVIEW_ACTION.label,
    icon: OVERVIEW_ACTION.icon
};

const PEOPLE_ITEM = {
    subject: 'People',
    detail: 'Platform users',
    href: DIRECTORY_ACTION.href,
    label: DIRECTORY_ACTION.label,
    icon: Users
};

const getLinkProps = (href) => (
    href.startsWith('/')
        ? {}
        : { target: '_blank', rel: 'noreferrer' }
);

const LaunchpadListItem = ({ item, title }) => {
    const Icon = item.icon ?? Terminal;

    return (
        <a key={item.label} href={item.href} className="launchpad-item" {...getLinkProps(item.href)}>
            <Icon size={18} aria-hidden="true" />
            <div className="launchpad-text">
                <span>{title ?? item.subject}</span>
                <strong>{item.detail}</strong>
            </div>
        </a>
    );
};

function Dashboard() {
    const { user, logout } = useAuth();
    const isAdmin = user?.roles?.includes('admin') ?? false;

    return (
        <div className="docs-page docs-dashboard">
            <div className="docs-container">
                <header className="docs-hero reveal launchpad-hero" style={{ '--delay': '0s' }}>
                    <div className="docs-nav">
                        <span className="docs-tag">Launchpad</span>
                        <div className="environment-badge">{isLocalEnvironment ? 'LOCAL ENV' : 'AKS ENV'}</div>
                        {user && (
                            <div className="user-badge">
                                <span className="user-badge-name">{user.fullName || user.username}</span>
                                <button className="user-badge-logout" onClick={logout} type="button" title="Sign out">
                                    <LogOut size={14} aria-hidden="true" />
                                </button>
                            </div>
                        )}
                    </div>
                    <div className="docs-hero-grid">
                        <div className="docs-hero-copy launchpad-copy">
                            <p className="docs-eyebrow">Open Data Platform</p>
                            <h1></h1>
                            <p className="docs-lead">
                                This dashboard is the front door to the platform. Launch each surface to
                                orchestrate pipelines, trace lineage, explore data, and monitor operations.
                            </p>
                            <div className="launchpad-shell">
                                <div className="launchpad-section">
                                    <p className="launchpad-section-title">Primary</p>
                                    {OVERVIEW_ITEM ? (
                                        <a
                                            href={OVERVIEW_ITEM.href}
                                            className="launchpad-primary"
                                            {...getLinkProps(OVERVIEW_ITEM.href)}
                                        >
                                            <div className="launchpad-primary-text">
                                                <span>{OVERVIEW_ITEM.subject}</span>
                                                <strong>{OVERVIEW_ITEM.detail}</strong>
                                            </div>
                                            <span className="launchpad-primary-meta">Opens overview</span>
                                        </a>
                                    ) : (
                                        <div className="launchpad-empty">
                                            No overview is configured yet.
                                        </div>
                                    )}
                                </div>
                                <div className="launchpad-section">
                                    <p className="launchpad-section-title">Destinations</p>
                                    {DESTINATIONS.length ? (
                                        <div className="launchpad-list">
                                            {DESTINATIONS.map((item) => (
                                                <LaunchpadListItem key={item.label} item={item} />
                                            ))}
                                        </div>
                                    ) : (
                                        <div className="launchpad-empty">
                                            No services are available yet. Add service URLs to enable links.
                                        </div>
                                    )}
                                </div>
                                <div className="launchpad-section">
                                    <p className="launchpad-section-title">Logging, monitoring and tracing</p>
                                    {OBSERVABILITY_LINKS.length ? (
                                        <div className="launchpad-list">
                                            {OBSERVABILITY_LINKS.map((link) => (
                                                <LaunchpadListItem
                                                    key={link.label}
                                                    item={{
                                                        label: link.label,
                                                        href: link.href,
                                                        subject: 'Logging, monitoring and tracing',
                                                        detail: link.label,
                                                        icon: Activity
                                                    }}
                                                />
                                            ))}
                                        </div>
                                    ) : (
                                        <div className="launchpad-empty">
                                            No observability links are configured yet.
                                        </div>
                                    )}
                                </div>
                                {isAdmin ? (
                                    <div className="launchpad-section">
                                        <p className="launchpad-section-title">People</p>
                                        <div className="launchpad-list">
                                            <LaunchpadListItem item={PEOPLE_ITEM} />
                                        </div>
                                    </div>
                                ) : null}
                            </div>
                        </div>
                    </div>
                </header>

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
