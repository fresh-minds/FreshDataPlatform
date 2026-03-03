import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { Navigate } from 'react-router-dom';
import { LogOut, RefreshCw, Users } from 'lucide-react';
import { isLocalEnvironment, serviceUrls } from '../config/serviceUrls';
import useAuth from '../auth/useAuth';

function formatDateTime(value) {
    if (!value) return '';
    try {
        return new Date(value).toLocaleString();
    } catch {
        return '';
    }
}

function LoginMetadata() {
    const { user, logout, keycloak } = useAuth();
    const isAdmin = user?.roles?.includes('admin') ?? false;

    const [summary, setSummary] = useState({
        totalHomeVisits: 0,
        totalPageVisits: 0,
        totalApiEndpointHits: 0,
        pendingHomeVisits: 0,
        totalLoginEvents: 0
    });
    const [events, setEvents] = useState([]);
    const [pageCounts, setPageCounts] = useState([]);
    const [endpointCounts, setEndpointCounts] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState('');
    const [clearing, setClearing] = useState(false);

    const fetchMetadata = useCallback(async () => {
        setLoading(true);
        setError('');
        try {
            await keycloak.updateToken(30);
            const response = await fetch(`${serviceUrls.portalApi}/api/admin/login-metadata`, {
                headers: { Authorization: `Bearer ${keycloak.token}` }
            });
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }
            const data = await response.json();
            setSummary({
                totalHomeVisits: data.totalHomeVisits ?? 0,
                totalPageVisits: data.totalPageVisits ?? 0,
                totalApiEndpointHits: data.totalApiEndpointHits ?? 0,
                pendingHomeVisits: data.pendingHomeVisits ?? 0,
                totalLoginEvents: data.totalLoginEvents ?? 0
            });
            setEvents(Array.isArray(data.loginEvents) ? data.loginEvents : []);
            setPageCounts(Array.isArray(data.pageVisitCounts) ? data.pageVisitCounts : []);
            setEndpointCounts(Array.isArray(data.apiEndpointCounts) ? data.apiEndpointCounts : []);
        } catch (err) {
            setError(err.message || 'Failed to load login metadata');
        } finally {
            setLoading(false);
        }
    }, [keycloak]);

    useEffect(() => {
        fetchMetadata();
    }, [fetchMetadata]);

    const rows = useMemo(() => events, [events]);

    const handleClearMetadata = useCallback(async () => {
        if (clearing) {
            return;
        }

        const confirmed = window.confirm('Clear all login metadata and homepage visit counters?');
        if (!confirmed) {
            return;
        }

        setClearing(true);
        setError('');
        try {
            await keycloak.updateToken(30);
            const response = await fetch(`${serviceUrls.portalApi}/api/admin/login-metadata`, {
                method: 'DELETE',
                headers: { Authorization: `Bearer ${keycloak.token}` }
            });
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }
            await fetchMetadata();
        } catch (err) {
            setError(err.message || 'Failed to clear metadata');
        } finally {
            setClearing(false);
        }
    }, [clearing, fetchMetadata, keycloak]);

    if (!isAdmin) {
        return <Navigate to="/platform" replace />;
    }

    return (
        <div className="docs-page docs-dashboard">
            <div className="docs-container">
                <header className="docs-hero reveal" style={{ '--delay': '0s' }}>
                    <div className="docs-nav">
                        <span className="docs-tag">Admin</span>
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
                            <h1>Login Metadata</h1>
                            <p className="docs-lead">
                                Review homepage visits and metadata for users who completed login.
                            </p>
                            <div className="launchpad-shell">
                                <div className="launchpad-section">
                                    <div className="launchpad-list">
                                        <div className="launchpad-item launchpad-item--user">
                                            <Users size={18} aria-hidden="true" />
                                            <div className="launchpad-text">
                                                <span>Total homepage visits</span>
                                                <strong>{summary.totalHomeVisits}</strong>
                                            </div>
                                        </div>
                                        <div className="launchpad-item launchpad-item--user">
                                            <Users size={18} aria-hidden="true" />
                                            <div className="launchpad-text">
                                                <span>Total unique page visitors</span>
                                                <strong>{summary.totalPageVisits}</strong>
                                            </div>
                                        </div>
                                        <div className="launchpad-item launchpad-item--user">
                                            <Users size={18} aria-hidden="true" />
                                            <div className="launchpad-text">
                                                <span>Total API endpoint hits</span>
                                                <strong>{summary.totalApiEndpointHits}</strong>
                                            </div>
                                        </div>
                                        <div className="launchpad-item launchpad-item--user">
                                            <Users size={18} aria-hidden="true" />
                                            <div className="launchpad-text">
                                                <span>Pending (not yet linked to login)</span>
                                                <strong>{summary.pendingHomeVisits}</strong>
                                            </div>
                                        </div>
                                        <div className="launchpad-item launchpad-item--user">
                                            <Users size={18} aria-hidden="true" />
                                            <div className="launchpad-text">
                                                <span>Recorded login events</span>
                                                <strong>{summary.totalLoginEvents}</strong>
                                            </div>
                                        </div>
                                    </div>
                                </div>

                                <div className="launchpad-section">
                                    <div className="directory-meta-row" style={{ marginBottom: 12 }}>
                                        <p className="directory-meta">Latest login events</p>
                                        <div style={{ display: 'flex', gap: 8 }}>
                                            <button className="directory-retry" onClick={fetchMetadata} type="button">
                                                <RefreshCw size={14} aria-hidden="true" /> Refresh
                                            </button>
                                            <button
                                                className="directory-retry"
                                                onClick={handleClearMetadata}
                                                type="button"
                                                disabled={clearing}
                                            >
                                                {clearing ? 'Clearing...' : 'Clear metadata'}
                                            </button>
                                        </div>
                                    </div>

                                    {loading ? <div className="launchpad-empty">Loading metadata...</div> : null}

                                    {!loading && error ? (
                                        <div className="launchpad-empty">
                                            <p>Failed to load metadata: {error}</p>
                                        </div>
                                    ) : null}

                                    {!loading && !error ? (
                                        rows.length > 0 ? (
                                            <div className="launchpad-list">
                                                {rows.map((event) => (
                                                    <div key={event.eventId} className="launchpad-item launchpad-item--user">
                                                        <Users size={18} aria-hidden="true" />
                                                        <div className="launchpad-text">
                                                            <span>
                                                                {event.email || event.username || event.subject || 'unknown user'}
                                                                {' \u00B7 '}
                                                                {formatDateTime(event.loggedInAt)}
                                                            </span>
                                                            <strong>
                                                                roles: {(event.roles || []).join(', ') || 'none'}
                                                                {' \u00B7 ip: '}
                                                                {event.ipAddress || 'n/a'}
                                                                {event.homeVisit?.visitedAt
                                                                    ? ` \u00B7 home visit: ${formatDateTime(event.homeVisit.visitedAt)}`
                                                                    : ''}
                                                            </strong>
                                                        </div>
                                                    </div>
                                                ))}
                                            </div>
                                        ) : (
                                            <div className="launchpad-empty">No login events recorded yet.</div>
                                        )
                                    ) : null}
                                </div>

                                <div className="launchpad-section">
                                    <p className="directory-meta">Page unique visitor counters</p>
                                    {loading ? null : pageCounts.length > 0 ? (
                                        <div className="launchpad-list">
                                            {pageCounts.map((entry) => (
                                                <div key={entry.page} className="launchpad-item launchpad-item--user">
                                                    <Users size={18} aria-hidden="true" />
                                                    <div className="launchpad-text">
                                                        <span>{entry.page}</span>
                                                        <strong>{entry.count} unique visitors</strong>
                                                    </div>
                                                </div>
                                            ))}
                                        </div>
                                    ) : (
                                        <div className="launchpad-empty">No unique page visitors recorded yet.</div>
                                    )}
                                </div>

                                <div className="launchpad-section">
                                    <p className="directory-meta">API endpoint counters</p>
                                    {loading ? null : endpointCounts.length > 0 ? (
                                        <div className="launchpad-list">
                                            {endpointCounts.map((entry) => (
                                                <div key={entry.endpoint} className="launchpad-item launchpad-item--user">
                                                    <Users size={18} aria-hidden="true" />
                                                    <div className="launchpad-text">
                                                        <span>{entry.endpoint}</span>
                                                        <strong>{entry.count} hits</strong>
                                                    </div>
                                                </div>
                                            ))}
                                        </div>
                                    ) : (
                                        <div className="launchpad-empty">No API endpoint hits recorded yet.</div>
                                    )}
                                </div>
                            </div>
                        </div>
                    </div>
                </header>

                <footer className="docs-footer reveal" style={{ '--delay': '0.3s' }}>
                    <div>
                        <h2>Need user directory details too?</h2>
                        <p>Open the user directory for full account-level information.</p>
                    </div>
                    <a href="/directory" className="docs-back primary">
                        Open user directory
                    </a>
                </footer>
            </div>
        </div>
    );
}

export default LoginMetadata;
