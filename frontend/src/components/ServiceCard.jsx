import React, { useState, useEffect } from 'react';
import { ExternalLink, CheckCircle, XCircle, Loader2, ArrowRight } from 'lucide-react';
import { Link } from 'react-router-dom';

const ServiceCard = ({ name, description, url, healthUrl, icon: Icon, color }) => {
    const [status, setStatus] = useState('loading'); // loading, healthy, unhealthy
    const isInternal = url?.startsWith('/');

    useEffect(() => {
        if (isInternal) {
            setStatus('healthy');
            return undefined;
        }

        const checkHealth = async () => {
            try {
                const controller = new AbortController();
                const timeoutId = setTimeout(() => controller.abort(), 2000);

                await fetch(url, { mode: 'no-cors', signal: controller.signal });

                clearTimeout(timeoutId);
                setStatus('healthy');
            } catch (error) {
                console.warn(`Health check failed for ${name}:`, error);
                setStatus('unhealthy');
            }
        };

        checkHealth();
        const interval = setInterval(checkHealth, 30000);
        return () => clearInterval(interval);
    }, [url, name, isInternal]);

    const getStatusColor = () => {
        switch (status) {
            case 'healthy':
                return 'text-green-400';
            case 'unhealthy':
                return 'text-red-400';
            default:
                return 'text-gray-400';
        }
    };

    return (
        <div className="service-card" style={{ '--accent-color': color }}>
            <div className="card-header">
                <div className="icon-wrapper" style={{ backgroundColor: `${color}20`, color: color }}>
                    <Icon size={24} />
                </div>
                <div className="status-indicator">
                    {status === 'loading' && <Loader2 size={16} className="animate-spin text-gray-400" />}
                    {status === 'healthy' && <CheckCircle size={16} className="text-green-400" />}
                    {status === 'unhealthy' && <XCircle size={16} className="text-red-400" />}
                </div>
            </div>

            <div className="card-content">
                <h3>{name}</h3>
                <p>{description}</p>
            </div>

            <div className="card-footer">
                {isInternal ? (
                    <Link to={url} className="launch-btn">
                        View Docs <ArrowRight size={14} />
                    </Link>
                ) : (
                    <a href={url} target="_blank" rel="noopener noreferrer" className="launch-btn">
                        Launch App <ExternalLink size={14} />
                    </a>
                )}
            </div>
        </div>
    );
};

export default ServiceCard;
