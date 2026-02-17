import React from 'react';
import { Link } from 'react-router-dom';
import PlatformGlyph from '../components/PlatformGlyph';

function Home() {
    return (
        <main className="home-page">
            <section className="home-shell" aria-labelledby="home-title">
                <p className="home-eyebrow">Open Data Platform</p>
                <h1 id="home-title">Operate your data platform from one place.</h1>

                <div
                    className="home-icon-frame"
                    role="img"
                    aria-label="Connected data nodes representing an AI and analytics platform"
                >
                    <PlatformGlyph />
                </div>

                <p className="home-copy">
                    Monitor pipelines, explore trusted datasets, and access analytics without switching contexts.
                </p>

                <Link to="/platform" className="home-cta">
                    Open Platform
                </Link>
            </section>
        </main>
    );
}

export default Home;
