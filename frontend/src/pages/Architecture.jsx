import React from 'react';
import { ArrowUpRight } from 'lucide-react';
import { hasServiceUrl, serviceUrls } from '../config/serviceUrls';

const isNavigableLink = (href = '') => href.startsWith('/') || hasServiceUrl(href);
const ARCH_VIEWBOX = { width: 1200, height: 720 };

const ARCH_CONNECTIONS = [
    { d: 'M180 125 L200 125' },
    { d: 'M320 125 L400 125' },
    { d: 'M540 125 L560 125' },
    { d: 'M640 155 L640 240' },
    { d: 'M530 320 L620 320' },
    { d: 'M320 335 L380 335' },
    { d: 'M220 335 L260 335' },
    { d: 'M300 365 L300 295' },
    { d: 'M640 155 L780 155' },
    { d: 'M910 325 L900 325' },
    { d: 'M1040 325 L1020 325' },
    { d: 'M920 365 L900 365' },
    { d: 'M1040 365 L1020 365' },
    { d: 'M300 425 L960 365', dotted: true }
];

const LINKS = [
    { label: 'Open overview', href: '/overview' },
    { label: 'Open documentation', href: '/docs' },
    { label: 'Airflow', href: serviceUrls.airflow },
    { label: 'Jupyter', href: serviceUrls.jupyter },
    { label: 'Grafana', href: serviceUrls.grafana },
    { label: 'Prometheus', href: serviceUrls.prometheus },
    { label: 'Alertmanager', href: serviceUrls.alertmanager }
].filter((link) => isNavigableLink(link.href));

const OBSERVABILITY_NODES = [
    {
        key: 'grafana',
        label: 'Grafana',
        href: serviceUrls.grafana,
        x: 900,
        y: 295,
        width: 120,
        height: 60,
        textX: 960,
        textY: 323,
        subLabel: ':3001',
        subTextY: 345,
        linkMarkX: 1006,
        linkMarkY: 312
    },
    {
        key: 'prometheus',
        label: 'Prometheus',
        href: serviceUrls.prometheus,
        x: 1040,
        y: 295,
        width: 120,
        height: 60,
        textX: 1100,
        textY: 323,
        subLabel: ':9090',
        subTextY: 345,
        linkMarkX: 1146,
        linkMarkY: 312
    },
    {
        key: 'alertmanager',
        label: 'Alertmanager',
        href: serviceUrls.alertmanager,
        x: 900,
        y: 365,
        width: 120,
        height: 50,
        textX: 960,
        textY: 388,
        linkMarkX: 1006,
        linkMarkY: 382
    }
];

function NodeSubLabel({ x, y, value }) {
    if (!value || !y) {
        return null;
    }

    return (
        <text x={x} y={y} className="arch-node-sub">{value}</text>
    );
}

function ObservabilityNode({ node }) {
    const hasExternalServiceLink = hasServiceUrl(node.href);

    const content = (
        <>
            <rect x={node.x} y={node.y} width={node.width} height={node.height} rx="12" className="arch-node" />
            <text x={node.textX} y={node.textY} className="arch-node-text">{node.label}</text>
            <NodeSubLabel x={node.textX} y={node.subTextY} value={node.subLabel} />
            {hasExternalServiceLink ? (
                <text x={node.linkMarkX} y={node.linkMarkY} className="arch-node-link-mark" aria-hidden="true">↗</text>
            ) : null}
        </>
    );

    if (hasExternalServiceLink) {
        return (
            <a href={node.href} target="_blank" rel="noreferrer" className="arch-node-link">
                {content}
            </a>
        );
    }

    return <g>{content}</g>;
}

function DiagramGroup({ x, y, width, height, title, titleX, titleY, children }) {
    return (
        <>
            <rect x={x} y={y} width={width} height={height} rx="18" className="arch-group" />
            <text x={titleX} y={titleY} className="arch-group-title">{title}</text>
            {children}
        </>
    );
}

function DiagramNode({
    x,
    y,
    width,
    height,
    label,
    labelX,
    labelY,
    subLabel,
    subLabelY,
    rectClassName = 'arch-node'
}) {
    return (
        <>
            <rect x={x} y={y} width={width} height={height} rx="12" className={rectClassName} />
            <text x={labelX} y={labelY} className="arch-node-text">{label}</text>
            <NodeSubLabel x={labelX} y={subLabelY} value={subLabel} />
        </>
    );
}

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
                    <svg viewBox={`0 0 ${ARCH_VIEWBOX.width} ${ARCH_VIEWBOX.height}`} role="img" aria-label="Architecture diagram">
                        <defs>
                            <marker id="arrow" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto">
                                <path d="M0,0 L10,3 L0,6 Z" fill="#1f2933" />
                            </marker>
                        </defs>

                        <DiagramGroup x="40" y="40" width="300" height="150" title="User Surface" titleX="60" titleY="70">
                            <DiagramNode x="60" y="95" width="120" height="60" label="Browser" labelX="120" labelY="130" />
                            <DiagramNode x="200" y="95" width="120" height="60" label="Portal" labelX="260" labelY="125" subLabel=":3000" subLabelY="145" rectClassName="arch-node accent" />
                        </DiagramGroup>

                        <DiagramGroup x="380" y="40" width="360" height="150" title="Orchestration & Pipelines" titleX="400" titleY="70">
                            <DiagramNode x="400" y="95" width="140" height="60" label="Airflow UI" labelX="470" labelY="123" subLabel=":8080" subLabelY="145" />
                            <DiagramNode x="560" y="95" width="160" height="60" label="Scheduler" labelX="640" labelY="123" subLabel="DAG runs" subLabelY="145" />
                        </DiagramGroup>

                        <DiagramGroup x="760" y="40" width="380" height="150" title="Governance" titleX="780" titleY="70">
                            <DiagramNode x="780" y="95" width="130" height="60" label="DataHub" labelX="845" labelY="123" subLabel=":9002" subLabelY="145" />
                            <DiagramNode x="930" y="95" width="190" height="60" label="GMS + Search" labelX="1025" labelY="123" subLabel="Kafka / ES" subLabelY="145" />
                        </DiagramGroup>

                        <DiagramGroup x="40" y="240" width="520" height="190" title="Storage & Data Plane" titleX="60" titleY="270">
                            <DiagramNode x="60" y="295" width="140" height="60" label="MinIO S3" labelX="130" labelY="323" subLabel=":9000" subLabelY="345" />
                            <DiagramNode x="220" y="295" width="140" height="60" label="Lakehouse" labelX="290" labelY="323" subLabel="Bronze/Silver/Gold" subLabelY="345" />
                            <DiagramNode x="380" y="295" width="160" height="60" label="Warehouse" labelX="460" labelY="323" subLabel="Postgres" subLabelY="345" />
                            <DiagramNode x="60" y="365" width="480" height="50" label="Pipelines (Spark / Python)" labelX="300" labelY="395" rectClassName="arch-band" />
                        </DiagramGroup>

                        <DiagramGroup x="600" y="240" width="260" height="190" title="Analytics & BI" titleX="620" titleY="270">
                            <DiagramNode x="620" y="295" width="220" height="60" label="Superset" labelX="730" labelY="323" subLabel=":8088" subLabelY="345" />
                            <DiagramNode x="620" y="365" width="220" height="50" label="Jupyter" labelX="730" labelY="388" subLabel=":8888" subLabelY="408" />
                        </DiagramGroup>

                        <DiagramGroup x="880" y="240" width="280" height="190" title="Observability" titleX="900" titleY="270">
                            {OBSERVABILITY_NODES.map((node) => (
                                <ObservabilityNode key={node.key} node={node} />
                            ))}
                            <DiagramNode x="1040" y="365" width="120" height="50" label="Loki + Tempo" labelX="1100" labelY="388" subLabel=":3100 / :3200" subLabelY="408" />
                        </DiagramGroup>

                        {ARCH_CONNECTIONS.map((connection) => (
                            <path
                                key={connection.d}
                                d={connection.d}
                                className={`arch-line${connection.dotted ? ' dotted' : ''}`}
                                markerEnd="url(#arrow)"
                            />
                        ))}
                    </svg>
                </div>
            </div>
        </div>
    );
}

export default Architecture;
