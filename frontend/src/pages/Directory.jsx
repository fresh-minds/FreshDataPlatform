import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { Navigate } from 'react-router-dom';
import { LogOut, RefreshCw, Users } from 'lucide-react';
import { isLocalEnvironment, serviceUrls } from '../config/serviceUrls';
import useAuth from '../auth/useAuth';

function formatTimestamp(ts) {
    if (!ts) return '';
    try {
        return new Date(ts).toLocaleDateString(undefined, {
            year: 'numeric',
            month: 'short',
            day: 'numeric'
        });
    } catch {
        return '';
    }
}

function Directory() {
    const { user, logout, keycloak } = useAuth();
    const isAdmin = user?.roles?.includes('admin') ?? false;
    const [users, setUsers] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [search, setSearch] = useState('');

    const fetchUsers = useCallback(async () => {
        setLoading(true);
        setError(null);
        try {
            await keycloak.updateToken(30);
            const res = await fetch(`${serviceUrls.portalApi}/api/users`, {
                headers: { Authorization: `Bearer ${keycloak.token}` }
            });
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const data = await res.json();
            setUsers(data.users ?? []);
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    }, [keycloak]);

    useEffect(() => {
        fetchUsers();
    }, [fetchUsers]);

    const filtered = useMemo(() => {
        if (!search.trim()) return users;
        const q = search.toLowerCase();
        return users.filter(
            (u) =>
                (u.username || '').toLowerCase().includes(q) ||
                (u.email || '').toLowerCase().includes(q) ||
                (u.firstName || '').toLowerCase().includes(q) ||
                (u.lastName || '').toLowerCase().includes(q)
        );
    }, [users, search]);

    if (!isAdmin) {
        return <Navigate to="/platform" replace />;
    }

    return (
        <div className="docs-page docs-dashboard">
            <div className="docs-container">
                <header className="docs-hero reveal" style={{ '--delay': '0s' }}>
                    <div className="docs-nav">
                        <span className="docs-tag">Directory</span>
                        <div className="environment-badge">
                            {isLocalEnvironment ? 'LOCAL ENV' : 'AKS ENV'}
                        </div>
                        {user && (
                            <div className="user-badge">
                                <span className="user-badge-name">
                                    {user.fullName || user.username}
                                </span>
                                <button
                                    className="user-badge-logout"
                                    onClick={logout}
                                    type="button"
                                    title="Sign out"
                                >
                                    <LogOut size={14} aria-hidden="true" />
                                </button>
                            </div>
                        )}
                    </div>

                    <div className="docs-hero-grid">
                        <div className="docs-hero-copy launchpad-copy">
                            <p className="docs-eyebrow">Open Data Platform</p>
                            <h1>User Directory</h1>
                            <p className="docs-lead">
                                Browse all registered users on the platform.
                            </p>

                            <div className="launchpad-shell">
                                <div className="launchpad-section">
                                    <input
                                        type="text"
                                        className="directory-search"
                                        placeholder="Search by name or email..."
                                        value={search}
                                        onChange={(e) => setSearch(e.target.value)}
                                    />

                                    {loading && (
                                        <div className="launchpad-empty">Loading users...</div>
                                    )}

                                    {error && (
                                        <div className="launchpad-empty">
                                            <p>Failed to load users: {error}</p>
                                            <button
                                                className="directory-retry"
                                                onClick={fetchUsers}
                                                type="button"
                                            >
                                                <RefreshCw size={14} aria-hidden="true" /> Retry
                                            </button>
                                        </div>
                                    )}

                                    {!loading && !error && (
                                        <>
                                            <p className="directory-meta">
                                                {filtered.length}{' '}
                                                {filtered.length === 1 ? 'user' : 'users'}
                                                {search.trim() &&
                                                    ` matching "${search.trim()}"`}
                                            </p>

                                            {filtered.length > 0 ? (
                                                <div className="launchpad-list">
                                                    {filtered.map((u) => {
                                                        const displayName = [
                                                            u.firstName,
                                                            u.lastName
                                                        ]
                                                            .filter(Boolean)
                                                            .join(' ') || u.username;

                                                        return (
                                                            <div
                                                                key={u.id}
                                                                className="launchpad-item launchpad-item--user"
                                                            >
                                                                <Users
                                                                    size={18}
                                                                    aria-hidden="true"
                                                                />
                                                                <div className="launchpad-text">
                                                                    <span>
                                                                        {u.email || u.username}
                                                                        {u.createdTimestamp && (
                                                                            <>
                                                                                {' \u00B7 Joined '}
                                                                                {formatTimestamp(
                                                                                    u.createdTimestamp
                                                                                )}
                                                                            </>
                                                                        )}
                                                                    </span>
                                                                    <strong>{displayName}</strong>
                                                                </div>
                                                            </div>
                                                        );
                                                    })}
                                                </div>
                                            ) : (
                                                <div className="launchpad-empty">
                                                    {search.trim()
                                                        ? 'No users match your search.'
                                                        : 'No users registered yet.'}
                                                </div>
                                            )}
                                        </>
                                    )}
                                </div>
                            </div>
                        </div>
                    </div>
                </header>

                <footer className="docs-footer reveal" style={{ '--delay': '0.3s' }}>
                    <div>
                        <h2>Back to the launchpad?</h2>
                        <p>Return to the main platform dashboard.</p>
                    </div>
                    <a href="/platform" className="docs-back primary">
                        Open launchpad
                    </a>
                </footer>
            </div>
        </div>
    );
}

export default Directory;
