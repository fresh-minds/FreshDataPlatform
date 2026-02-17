import React from 'react';
import {
    Activity,
    BarChart3,
    Database,
    Eye,
    Network,
    Terminal
} from 'lucide-react';
import { hasServiceUrl, isLocalEnvironment, serviceUrls } from '../config/serviceUrls';

const QUICK_ACTIONS = [
    { label: 'Open analytics', href: serviceUrls.superset },
    { label: 'Open monitoring', href: serviceUrls.grafana }
].filter((link) => hasServiceUrl(link.href));

const OVERVIEW_ACTION = { label: 'One-screen overview', href: '/overview', icon: Eye };

const PRIMARY_ENDPOINTS = [
    { label: 'Airflow UI', href: serviceUrls.airflow, icon: Network },
    { label: 'DataHub', href: serviceUrls.datahub, icon: Database },
    { label: 'Grafana', href: serviceUrls.grafana, icon: Activity },
    { label: 'MinIO Console', href: serviceUrls.minioConsole, icon: Database }
].filter((link) => hasServiceUrl(link.href));

const STATUS_SUBJECTS = {
    'Airflow UI': {
        subject: 'Orchestration',
        detail: 'Airflow + DAG scheduling'
    },
    DataHub: {
        subject: 'Catalog & lineage',
        detail: 'DataHub services'
    },
    Grafana: {
        subject: 'BI & reporting',
        detail: 'Grafana dashboards'
    },
    'MinIO Console': {
        subject: 'Storage & warehouse',
        detail: 'MinIO + Postgres'
    }
};

const STATUS_ITEMS = PRIMARY_ENDPOINTS.map((link) => {
    const mapping = STATUS_SUBJECTS[link.label];

    if (mapping) {
        return { ...mapping, href: link.href, label: link.label, icon: link.icon ?? Terminal };
    }

    return {
        subject: link.label,
        detail: `Open ${link.label}`,
        href: link.href,
        label: link.label,
        icon: link.icon ?? Terminal
    };
});

const QUICK_ACTION_SUBJECTS = {
    'One-screen overview': {
        subject: 'Overview',
        detail: 'Single-page platform view'
    },
    'Open analytics': {
        subject: 'Analytics',
        detail: 'Superset workspaces',
        icon: BarChart3
    },
    'Open monitoring': {
        subject: 'Monitoring',
        detail: 'Grafana operations',
        icon: Activity
    }
};

const QUICK_ACTION_ITEMS = [OVERVIEW_ACTION, ...QUICK_ACTIONS].map((action) => {
    const mapping = QUICK_ACTION_SUBJECTS[action.label];

    if (mapping) {
        return {
            ...mapping,
            href: action.href,
            label: action.label,
            icon: mapping.icon ?? action.icon
        };
    }

    return {
        subject: action.label,
        detail: `Open ${action.label}`,
        href: action.href,
        label: action.label,
        icon: action.icon ?? Terminal
    };
});

function Dashboard() {
    return (
        <div className="docs-page docs-dashboard">
            <div className="docs-container">
                <header className="docs-hero reveal launchpad-hero" style={{ '--delay': '0s' }}>
                    <div className="docs-nav">
                        <span className="docs-tag">Launchpad</span>
                        <div className="environment-badge">{isLocalEnvironment ? 'LOCAL ENV' : 'AKS ENV'}</div>
                    </div>
                    <div className="docs-hero-grid">
                        <div className="docs-hero-copy launchpad-copy">
                            <p className="docs-eyebrow">Open Data Platform</p>
                            <h1></h1>
                            <p className="docs-lead">
                                This dashboard is the front door to the platform. Launch each surface to
                                orchestrate pipelines, trace lineage, explore data, and monitor operations.
                            </p>
                            <div className="panel-list launchpad-list">
                                {QUICK_ACTION_ITEMS.map((item) => {
                                    const Icon = item.icon ?? Terminal;
                                    const isOverview = item.label === 'One-screen overview';
                                    return (
                                    <a
                                        key={item.label}
                                        href={item.href}
                                        className={`panel-item launchpad-item${isOverview ? ' launchpad-item--featured' : ''}`}
                                        target={item.href.startsWith('/') ? undefined : '_blank'}
                                        rel={item.href.startsWith('/') ? undefined : 'noreferrer'}
                                    >
                                        <Icon size={18} />
                                        <div className="launchpad-text">
                                            <span>{item.subject}</span>
                                            <strong>{item.detail}</strong>
                                        </div>
                                    </a>
                                    );
                                })}
                                {STATUS_ITEMS.map((item) => {
                                    const Icon = item.icon ?? Terminal;
                                    return (
                                    <a
                                        key={item.label}
                                        href={item.href}
                                        className="panel-item launchpad-item"
                                        target="_blank"
                                        rel="noreferrer"
                                    >
                                        <Icon size={18} />
                                        <div className="launchpad-text">
                                            <span>{item.subject}</span>
                                            <strong>{item.detail}</strong>
                                        </div>
                                    </a>
                                    );
                                })}
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
