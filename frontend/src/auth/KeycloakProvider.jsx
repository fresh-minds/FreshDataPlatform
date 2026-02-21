import React, { createContext, useCallback, useEffect, useRef, useState } from 'react';
import Keycloak from 'keycloak-js';

export const AuthContext = createContext({
    authenticated: false,
    user: null,
    keycloak: null,
    logout: () => {},
});

const TOKEN_REFRESH_MARGIN_SECONDS = 60;

const buildKeycloakConfig = () => ({
    url: import.meta.env.VITE_KEYCLOAK_URL || 'http://localhost:8090',
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

        const kc = new Keycloak(buildKeycloakConfig());
        kcRef.current = kc;

        kc.onTokenExpired = () => {
            kc.updateToken(TOKEN_REFRESH_MARGIN_SECONDS).catch(() => {
                kc.login();
            });
        };

        kc.init({
            onLoad: 'login-required',
            pkceMethod: 'S256',
            checkLoginIframe: false,
        })
            .then((authenticated) => {
                setState({
                    ready: true,
                    authenticated,
                    user: authenticated ? extractUser(kc) : null,
                    error: null,
                });
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
