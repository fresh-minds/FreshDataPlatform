const LOCAL_HOSTS = new Set(['localhost', '127.0.0.1', '0.0.0.0']);

const LOCAL_DEFAULTS = {
    airflow: 'http://localhost:8080',
    datahub: 'http://localhost:9002',
    superset: 'http://localhost:8088',
    grafana: 'http://localhost:3001',
    minioConsole: 'http://localhost:9001',
    minioApi: 'http://localhost:9000',
    prometheus: 'http://localhost:9090'
};

const trimTrailingSlash = (url) => url.replace(/\/+$/, '');
const normalizeHost = (hostname) => hostname.replace(/^www\./i, '');

const readEnv = (key) => {
    const value = import.meta.env[key];
    return typeof value === 'string' ? trimTrailingSlash(value.trim()) : '';
};

const buildSubdomainUrl = (subdomain) => {
    if (typeof window === 'undefined') {
        return '';
    }

    const protocol = window.location.protocol === 'https:' ? 'https:' : 'http:';
    const baseHost = normalizeHost(window.location.hostname);
    return `${protocol}//${subdomain}.${baseHost}`;
};

const getIsLocalEnvironment = () => {
    if (typeof window === 'undefined') {
        return true;
    }

    return LOCAL_HOSTS.has(window.location.hostname);
};

export const isLocalEnvironment = getIsLocalEnvironment();

const cloudDefaults = {
    airflow: buildSubdomainUrl('airflow'),
    datahub: '',
    superset: '',
    grafana: '',
    minioConsole: buildSubdomainUrl('minio'),
    minioApi: buildSubdomainUrl('minio-api'),
    prometheus: ''
};

const fallbackDefaults = isLocalEnvironment ? LOCAL_DEFAULTS : cloudDefaults;

export const serviceUrls = {
    airflow: readEnv('VITE_AIRFLOW_URL') || fallbackDefaults.airflow,
    datahub: readEnv('VITE_DATAHUB_URL') || fallbackDefaults.datahub,
    superset: readEnv('VITE_SUPERSET_URL') || fallbackDefaults.superset,
    grafana: readEnv('VITE_GRAFANA_URL') || fallbackDefaults.grafana,
    minioConsole: readEnv('VITE_MINIO_CONSOLE_URL') || fallbackDefaults.minioConsole,
    minioApi: readEnv('VITE_MINIO_API_URL') || fallbackDefaults.minioApi,
    prometheus: readEnv('VITE_PROMETHEUS_URL') || fallbackDefaults.prometheus
};

export const hasServiceUrl = (url) => typeof url === 'string' && url.length > 0;
