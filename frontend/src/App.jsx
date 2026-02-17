import React from 'react';
import { Routes, Route } from 'react-router-dom';
import Home from './pages/Home';
import Dashboard from './pages/Dashboard';
import ServiceMap from './pages/ServiceMap';
import Documentation from './pages/Documentation';
import MinimalOverview from './pages/MinimalOverview';
import Architecture from './pages/Architecture';

function App() {
    return (
        <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/platform" element={<Dashboard />} />
            <Route path="/service-map" element={<ServiceMap />} />
            <Route path="/overview" element={<MinimalOverview />} />
            <Route path="/architecture" element={<Architecture />} />
            <Route path="/docs" element={<Documentation />} />
            <Route path="*" element={<Home />} />
        </Routes>
    );
}

export default App;
