import React, { useEffect, useRef } from 'react';
import { Routes, Route, useLocation } from 'react-router-dom';
import Home from './pages/Home';
import Dashboard from './pages/Dashboard';
import Directory from './pages/Directory';
import LoginMetadata from './pages/LoginMetadata';
import ServiceMap from './pages/ServiceMap';
import Documentation from './pages/Documentation';
import MinimalOverview from './pages/MinimalOverview';
import Architecture from './pages/Architecture';
import { serviceUrls } from './config/serviceUrls';

function PageVisitTracker() {
    const location = useLocation();
    const lastTrackedRef = useRef('');

    useEffect(() => {
        const page = `${location.pathname || '/'}${location.search || ''}`;
        if (
            page === '/' ||
            location.pathname.startsWith('/admin') ||
            lastTrackedRef.current === page
        ) {
            return;
        }

        lastTrackedRef.current = page;
        fetch(`${serviceUrls.portalApi}/api/page-visit`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ page })
        }).catch(() => null);
    }, [location.pathname, location.search]);

    return null;
}

function App() {
    return (
        <>
            <PageVisitTracker />
            <Routes>
                <Route path="/" element={<Home />} />
                <Route path="/platform" element={<Dashboard />} />
                <Route path="/directory" element={<Directory />} />
                <Route path="/admin/login-metadata" element={<LoginMetadata />} />
                <Route path="/service-map" element={<ServiceMap />} />
                <Route path="/overview" element={<MinimalOverview />} />
                <Route path="/architecture" element={<Architecture />} />
                <Route path="/docs" element={<Documentation />} />
                <Route path="*" element={<Home />} />
            </Routes>
        </>
    );
}

export default App;
