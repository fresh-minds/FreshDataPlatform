import React from 'react';
import { LineChart, Map, Briefcase } from 'lucide-react';
import { jobMarketSample } from '../data/jobMarketSample';
import { hasServiceUrl, serviceUrls } from '../config/serviceUrls';

const formatNumber = (value) => new Intl.NumberFormat('en-US').format(value);

function JobMarketNL() {
  const { snapshot, skills, regions } = jobMarketSample;

  return (
    <div className="job-market-page">
      <div className="job-market-hero">
        <div>
          <p className="job-market-eyebrow">Netherlands IT Job Market</p>
          <h1>Pulse of Dutch tech hiring.</h1>
          <p className="job-market-lead">
            A live snapshot of demand, skills, and regional momentum. This view is driven by
            CBS vacancy data and job ads ingestion. Replace the sample data with the exported
            warehouse tables to go fully live.
          </p>
          <div className="job-market-actions">
            <a href="/platform" className="docs-chip">Back to launchpad</a>
            {hasServiceUrl(serviceUrls.superset) && (
              <a href={serviceUrls.superset} className="docs-chip" target="_blank" rel="noreferrer">
                Open Superset
              </a>
            )}
            {hasServiceUrl(serviceUrls.airflow) && (
              <a href={serviceUrls.airflow} className="docs-chip" target="_blank" rel="noreferrer">
                View Airflow
              </a>
            )}
          </div>
        </div>
        <div className="job-market-summary">
          <div className="summary-card">
            <div className="summary-icon">
              <Briefcase size={18} />
            </div>
            <div>
              <p>Open vacancies</p>
              <h2>{formatNumber(snapshot.vacancies)}</h2>
              <span>{snapshot.periodLabel}</span>
            </div>
          </div>
          <div className="summary-card">
            <div className="summary-icon">
              <LineChart size={18} />
            </div>
            <div>
              <p>Vacancy rate</p>
              <h2>{snapshot.vacancyRate}%</h2>
              <span>{snapshot.sectorName}</span>
            </div>
          </div>
          <div className="summary-card">
            <div className="summary-icon">
              <Map size={18} />
            </div>
            <div>
              <p>Job ads tracked</p>
              <h2>{formatNumber(snapshot.jobAdsCount)}</h2>
              <span>Adzuna sample</span>
            </div>
          </div>
        </div>
      </div>

      <section className="job-market-grid">
        <div className="job-market-panel">
          <div className="panel-header">
            <h3>Top skills in demand</h3>
            <p>Derived from job ad text.</p>
          </div>
          <div className="panel-body">
            {skills.map((skill) => (
              <div key={skill.skill} className="bar-row">
                <span>{skill.skill}</span>
                <div className="bar-track">
                  <div className="bar-fill" style={{ width: `${(skill.count / skills[0].count) * 100}%` }} />
                </div>
                <strong>{formatNumber(skill.count)}</strong>
              </div>
            ))}
          </div>
        </div>

        <div className="job-market-panel">
          <div className="panel-header">
            <h3>Regional concentration</h3>
            <p>Share of ads by province (sample).</p>
          </div>
          <div className="panel-body">
            {regions.map((region) => (
              <div key={region.name} className="bar-row">
                <span>{region.name}</span>
                <div className="bar-track">
                  <div className="bar-fill alt" style={{ width: `${region.share}%` }} />
                </div>
                <strong>{region.share}%</strong>
              </div>
            ))}
          </div>
        </div>
      </section>
    </div>
  );
}

export default JobMarketNL;
