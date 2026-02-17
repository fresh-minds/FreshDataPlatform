import React from 'react';
import { ArrowUpRight } from 'lucide-react';
import { hasServiceUrl, serviceUrls } from '../config/serviceUrls';

const LINKS = [
    { label: 'Open overview', href: '/overview' },
    { label: 'Open documentation', href: '/docs' },
    { label: 'Airflow', href: serviceUrls.airflow },
    { label: 'Grafana', href: serviceUrls.grafana }
].filter((link) => link.href.startsWith('/') || hasServiceUrl(link.href));

function Architecture() {
    return (
        <div className="arch-page">
            <div className="arch-shell">
                <header className="arch-header">
                    <div>
                        <p className="arch-eyebrow">System architecture</p>
                        <h1>Open Data Platform connections</h1>
                        <p className="arch-subtitle">
                            This diagram maps every major component and how data, metadata, and telemetry flow
                            through the stack.
                        </p>
                    </div>
                    <div className="arch-actions">
                        {LINKS.map((link) => (
                            <a key={link.href} href={link.href} className="arch-link">
                                {link.label}
                                <ArrowUpRight size={16} />
                            </a>
                        ))}
                    </div>
                </header>

                <div className="arch-diagram">
                    <svg viewBox="0 0 1200 720" role="img" aria-label="Architecture diagram">
                        <defs>
                            <marker id="arrow" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto">
                                <path d="M0,0 L10,3 L0,6 Z" fill="#1f2933" />
                            </marker>
                        </defs>

                        <rect x="40" y="40" width="300" height="150" rx="18" className="arch-group" />
                        <text x="60" y="70" className="arch-group-title">User Surface</text>
                        <rect x="60" y="95" width="120" height="60" rx="12" className="arch-node" />
                        <text x="120" y="130" className="arch-node-text">Browser</text>
                        <rect x="200" y="95" width="120" height="60" rx="12" className="arch-node accent" />
                        <text x="260" y="125" className="arch-node-text">Portal</text>
                        <text x="260" y="145" className="arch-node-sub">:3000</text>

                        <rect x="380" y="40" width="360" height="150" rx="18" className="arch-group" />
                        <text x="400" y="70" className="arch-group-title">Orchestration & Pipelines</text>
                        <rect x="400" y="95" width="140" height="60" rx="12" className="arch-node" />
                        <text x="470" y="123" className="arch-node-text">Airflow UI</text>
                        <text x="470" y="145" className="arch-node-sub">:8080</text>
                        <rect x="560" y="95" width="160" height="60" rx="12" className="arch-node" />
                        <text x="640" y="123" className="arch-node-text">Scheduler</text>
                        <text x="640" y="145" className="arch-node-sub">DAG runs</text>

                        <rect x="760" y="40" width="380" height="150" rx="18" className="arch-group" />
                        <text x="780" y="70" className="arch-group-title">Governance</text>
                        <rect x="780" y="95" width="130" height="60" rx="12" className="arch-node" />
                        <text x="845" y="123" className="arch-node-text">DataHub</text>
                        <text x="845" y="145" className="arch-node-sub">:9002</text>
                        <rect x="930" y="95" width="190" height="60" rx="12" className="arch-node" />
                        <text x="1025" y="123" className="arch-node-text">GMS + Search</text>
                        <text x="1025" y="145" className="arch-node-sub">Kafka / ES</text>

                        <rect x="40" y="240" width="520" height="190" rx="18" className="arch-group" />
                        <text x="60" y="270" className="arch-group-title">Storage & Data Plane</text>
                        <rect x="60" y="295" width="140" height="60" rx="12" className="arch-node" />
                        <text x="130" y="323" className="arch-node-text">MinIO S3</text>
                        <text x="130" y="345" className="arch-node-sub">:9000</text>
                        <rect x="220" y="295" width="140" height="60" rx="12" className="arch-node" />
                        <text x="290" y="323" className="arch-node-text">Lakehouse</text>
                        <text x="290" y="345" className="arch-node-sub">Bronze/Silver/Gold</text>
                        <rect x="380" y="295" width="160" height="60" rx="12" className="arch-node" />
                        <text x="460" y="323" className="arch-node-text">Warehouse</text>
                        <text x="460" y="345" className="arch-node-sub">Postgres</text>
                        <rect x="60" y="365" width="480" height="50" rx="12" className="arch-band" />
                        <text x="300" y="395" className="arch-node-text">Pipelines (Spark / Python)</text>

                        <rect x="600" y="240" width="260" height="190" rx="18" className="arch-group" />
                        <text x="620" y="270" className="arch-group-title">Analytics & BI</text>
                        <rect x="620" y="295" width="220" height="60" rx="12" className="arch-node" />
                        <text x="730" y="323" className="arch-node-text">Superset</text>
                        <text x="730" y="345" className="arch-node-sub">:8088</text>

                        <rect x="880" y="240" width="280" height="190" rx="18" className="arch-group" />
                        <text x="900" y="270" className="arch-group-title">Observability</text>
                        <rect x="900" y="295" width="120" height="60" rx="12" className="arch-node" />
                        <text x="960" y="323" className="arch-node-text">Grafana</text>
                        <text x="960" y="345" className="arch-node-sub">:3001</text>
                        <rect x="1040" y="295" width="120" height="60" rx="12" className="arch-node" />
                        <text x="1100" y="323" className="arch-node-text">Prometheus</text>
                        <text x="1100" y="345" className="arch-node-sub">:9090</text>
                        <rect x="900" y="365" width="120" height="50" rx="12" className="arch-node" />
                        <text x="960" y="395" className="arch-node-text">Loki</text>
                        <rect x="1040" y="365" width="120" height="50" rx="12" className="arch-node" />
                        <text x="1100" y="395" className="arch-node-text">Tempo</text>

                        <path d="M180 125 L200 125" className="arch-line" markerEnd="url(#arrow)" />
                        <path d="M320 125 L400 125" className="arch-line" markerEnd="url(#arrow)" />
                        <path d="M540 125 L560 125" className="arch-line" markerEnd="url(#arrow)" />
                        <path d="M640 155 L640 240" className="arch-line" markerEnd="url(#arrow)" />
                        <path d="M530 320 L620 320" className="arch-line" markerEnd="url(#arrow)" />
                        <path d="M320 335 L380 335" className="arch-line" markerEnd="url(#arrow)" />
                        <path d="M220 335 L260 335" className="arch-line" markerEnd="url(#arrow)" />
                        <path d="M300 365 L300 295" className="arch-line" markerEnd="url(#arrow)" />
                        <path d="M640 155 L780 155" className="arch-line" markerEnd="url(#arrow)" />
                        <path d="M910 325 L900 325" className="arch-line" markerEnd="url(#arrow)" />
                        <path d="M1040 325 L1020 325" className="arch-line" markerEnd="url(#arrow)" />
                        <path d="M920 365 L900 365" className="arch-line" markerEnd="url(#arrow)" />
                        <path d="M1040 365 L1020 365" className="arch-line" markerEnd="url(#arrow)" />
                        <path d="M300 425 L960 365" className="arch-line dotted" markerEnd="url(#arrow)" />
                    </svg>
                </div>
            </div>
        </div>
    );
}

export default Architecture;
