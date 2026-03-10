import React, { createContext, useCallback, useEffect, useRef, useState } from 'react';
import Keycloak from 'keycloak-js';
import { serviceUrls } from '../config/serviceUrls';

export const AuthContext = createContext({
    authenticated: false,
    user: null,
    keycloak: null,
    logout: () => {},
});

const TOKEN_REFRESH_MARGIN_SECONDS = 60;
const HOME_VISIT_STORAGE_KEY = 'portal_home_visit_id';
const LOGIN_EVENT_MARKER_KEY = 'portal_login_event_marker';
const demoAutoAdminEnabled = (import.meta.env.VITE_DEMO_AUTO_ADMIN || 'false').toLowerCase() === 'true';
const demoAdminUsername = import.meta.env.VITE_DEMO_USERNAME || 'odp-admin';

function getDefaultKeycloakUrl() {
    const host = window.location.hostname;
    if (host === 'localhost' || host === '127.0.0.1') {
        return 'http://localhost:8090';
    }
    const rootHost = host.replace(/^www\./, '');
    return `https://keycloak.${rootHost}`;
}

const buildKeycloakConfig = () => ({
    url: import.meta.env.VITE_KEYCLOAK_URL || getDefaultKeycloakUrl(),
    realm: import.meta.env.VITE_KEYCLOAK_REALM || 'odp',
    clientId: import.meta.env.VITE_KEYCLOAK_CLIENT_ID || 'portal',
});

function extractUser(kc) {
    if (!kc.tokenParsed) {
        return null;
    }
    const { preferred_username, email, given_name, family_name, name, realm_access } = kc.tokenParsed;
    return {
        username: preferred_username,
        email,
        firstName: given_name,
        lastName: family_name,
        fullName: name || [given_name, family_name].filter(Boolean).join(' '),
        roles: realm_access?.roles || [],
    };
}

async function registerAnonymousHomeVisit() {
    if (typeof window === 'undefined' || window.location.pathname !== '/') {
        return;
    }

    const existingVisitId = window.sessionStorage.getItem(HOME_VISIT_STORAGE_KEY);
    if (existingVisitId) {
        return;
    }

    try {
        const response = await fetch(`${serviceUrls.portalApi}/api/home-visit`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ page: window.location.pathname || '/' })
        });
        if (!response.ok) {
            return;
        }
        const payload = await response.json();
        if (payload?.visitId) {
            window.sessionStorage.setItem(HOME_VISIT_STORAGE_KEY, payload.visitId);
        }
    } catch (_error) {
        return;
    }
}

async function registerLoginMetadata(kc) {
    if (!kc?.token || !kc?.tokenParsed) {
        return;
    }

    const visitId = typeof window !== 'undefined' ? window.sessionStorage.getItem(HOME_VISIT_STORAGE_KEY) : null;
    if (!visitId) {
        return;
    }

    const subject = kc.tokenParsed.sub || 'unknown';
    const authTime = kc.tokenParsed.auth_time || kc.tokenParsed.iat || '0';
    const marker = `${subject}:${authTime}`;

    if (typeof window !== 'undefined') {
        const existingMarker = window.sessionStorage.getItem(LOGIN_EVENT_MARKER_KEY);
        if (existingMarker === marker) {
            return;
        }
    }

    try {
        const response = await fetch(`${serviceUrls.portalApi}/api/login-metadata`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                Authorization: `Bearer ${kc.token}`
            },
            body: JSON.stringify({ visitId })
        });
        if (!response.ok || typeof window === 'undefined') {
            return;
        }
        window.sessionStorage.setItem(LOGIN_EVENT_MARKER_KEY, marker);
        if (visitId) {
            window.sessionStorage.removeItem(HOME_VISIT_STORAGE_KEY);
        }
    } catch (_error) {
        return;
    }
}

export default function KeycloakProvider({ children }) {
    const [state, setState] = useState({
        ready: false,
        authenticated: false,
        user: null,
        error: null,
    });

    const kcRef = useRef(null);
    const initCalled = useRef(false);

    useEffect(() => {
        if (initCalled.current) {
            return;
        }
        initCalled.current = true;

        registerAnonymousHomeVisit();

        const kc = new Keycloak(buildKeycloakConfig());
        kcRef.current = kc;

        kc.onTokenExpired = () => {
            kc.updateToken(TOKEN_REFRESH_MARGIN_SECONDS).catch(() => {
                kc.login();
            });
        };

        kc.init({
            // Use check-sso so we can supply login hints before redirecting.
            // We still enforce login immediately when unauthenticated below.
            onLoad: 'check-sso',
            pkceMethod: 'S256',
            checkLoginIframe: false,
        })
            .then((authenticated) => {
                if (!authenticated) {
                    const loginOptions = demoAutoAdminEnabled
                        ? {
                              loginHint: demoAdminUsername,
                          }
                        : undefined;
                    kc.login(loginOptions);
                    return;
                }
                setState({
                    ready: true,
                    authenticated,
                    user: authenticated ? extractUser(kc) : null,
                    error: null,
                });
                registerLoginMetadata(kc);
            })
            .catch((err) => {
                console.error('Keycloak init failed', err);
                setState({ ready: true, authenticated: false, user: null, error: err });
            });
    }, []);

    const logout = useCallback(() => {
        if (kcRef.current) {
            kcRef.current.logout({ redirectUri: window.location.origin });
        }
    }, []);

    if (state.error) {
        return (
            <div className="kc-loading">
                <p>Authentication service is unavailable.</p>
                <button onClick={() => window.location.reload()}>Retry</button>
            </div>
        );
    }

    if (!state.ready) {
        return (
            <div className="kc-loading">
                <p>Signing in&hellip;</p>
            </div>
        );
    }

    return (
        <AuthContext.Provider
            value={{
                authenticated: state.authenticated,
                user: state.user,
                keycloak: kcRef.current,
                logout,
            }}
        >
            {children}
        </AuthContext.Provider>
    );
}
