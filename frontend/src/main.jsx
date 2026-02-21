import React from 'react';
import ReactDOM from 'react-dom/client';
import { BrowserRouter } from 'react-router-dom';
import KeycloakProvider from './auth/KeycloakProvider.jsx';
import App from './App.jsx';
import './index.css';

ReactDOM.createRoot(document.getElementById('root')).render(
    <React.StrictMode>
        <KeycloakProvider>
            <BrowserRouter>
                <App />
            </BrowserRouter>
        </KeycloakProvider>
    </React.StrictMode>
);
