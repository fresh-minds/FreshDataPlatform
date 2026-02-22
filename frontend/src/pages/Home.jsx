import React, { useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import upgradedPlatformImage from '../assets/upgraded_platform.png';
import { MessageSquare, Send, X } from 'lucide-react';
import { serviceUrls } from '../config/serviceUrls';
import useAuth from '../auth/useAuth';

function Home() {
    const { user, logout, keycloak } = useAuth();
    const [chatOpen, setChatOpen] = useState(false);
    const [messages, setMessages] = useState([]);
    const [draft, setDraft] = useState('');
    const [isSending, setIsSending] = useState(false);
    const [chatError, setChatError] = useState('');

    const canSend = useMemo(() => draft.trim().length > 0 && !isSending, [draft, isSending]);

    const handleResetChat = () => {
        setMessages([]);
        setDraft('');
        setChatError('');
    };

    const handleSend = async (event) => {
        event.preventDefault();
        const trimmed = draft.trim();
        if (!trimmed || isSending) {
            return;
        }

        const nextMessages = [...messages, { role: 'user', content: trimmed }];
        setMessages(nextMessages);
        setDraft('');
        setIsSending(true);
        setChatError('');

        try {
            await keycloak.updateToken(30);
            const response = await fetch(`${serviceUrls.portalApi}/api/chat`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    Authorization: `Bearer ${keycloak.token}`
                },
                body: JSON.stringify({
                    message: trimmed,
                    history: messages
                })
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }

            const data = await response.json();
            const reply = (data.reply || '').trim();
            if (!reply) {
                throw new Error('Empty response from chat API');
            }
            setMessages((current) => [...current, { role: 'assistant', content: reply }]);
        } catch (err) {
            setChatError(err.message || 'Failed to send message');
        } finally {
            setIsSending(false);
        }
    };

    return (
        <main className="home-page">
            {user && (
                <div className="home-user-bar">
                    <span className="home-user-name">{user.fullName || user.username}</span>
                    <button className="home-logout-btn" onClick={logout} type="button">
                        Sign out
                    </button>
                </div>
            )}
            <div className="home-layout">
                <div className="home-layout-main">
                    <section className="home-shell" aria-labelledby="home-title">
                        <h1 id="home-title">Operate your data platform from one place.</h1>

                        <button
                            type="button"
                            className="home-icon-button"
                            onClick={() => setChatOpen(true)}
                            aria-label="Open chat assistant"
                        >
                            <div
                                className="home-icon-frame"
                                role="img"
                                aria-label="Connected data nodes representing an AI and analytics platform"
                            >
                                <img src={upgradedPlatformImage} alt="Upgraded Open Data Platform diagram" />
                            </div>
                        </button>

                        <button
                            type="button"
                            className="home-qa-tag"
                            onClick={() => setChatOpen(true)}
                            aria-label="Open chat assistant"
                        >
                            QA this platform!
                        </button>

                        <p className="home-copy">
                            Monitor pipelines, explore trusted datasets, and access analytics without switching contexts.
                        </p>

                        <Link to="/platform" className="home-cta">
                            Open Platform
                        </Link>
                    </section>

                    
                </div>

                {chatOpen ? (
                    <aside className="home-chat-panel" aria-label="Assistant chat panel">
                        <div className="home-chat-header">
                            <div className="home-chat-title">
                                <MessageSquare size={16} aria-hidden="true" />
                                <span>Platform Assistant</span>
                            </div>
                            <div className="home-chat-header-actions">
                                <button
                                    type="button"
                                    className="home-chat-reset"
                                    onClick={handleResetChat}
                                    aria-label="Reset chat"
                                >
                                    Reset chat
                                </button>
                                <button
                                    type="button"
                                    className="home-chat-close"
                                    onClick={() => setChatOpen(false)}
                                    aria-label="Close chat panel"
                                >
                                    <X size={16} aria-hidden="true" />
                                </button>
                            </div>
                        </div>

                        <div className="home-chat-messages">
                            {messages.length === 0 ? (
                                <p className="home-chat-empty">Ask anything about the platform to get started.</p>
                            ) : (
                                messages.map((message, index) => (
                                    <div
                                        key={`${message.role}-${index}`}
                                        className={`home-chat-bubble home-chat-bubble--${message.role}`}
                                    >
                                        {message.content}
                                    </div>
                                ))
                            )}
                            {isSending ? <p className="home-chat-empty">Thinking...</p> : null}
                            {chatError ? <p className="home-chat-error">{chatError}</p> : null}
                        </div>

                        <form className="home-chat-form" onSubmit={handleSend}>
                            <input
                                type="text"
                                value={draft}
                                onChange={(event) => setDraft(event.target.value)}
                                placeholder="Type your message..."
                                className="home-chat-input"
                            />
                            <button type="submit" className="home-chat-send" disabled={!canSend}>
                                <Send size={16} aria-hidden="true" />
                            </button>
                        </form>
                    </aside>
                ) : null}
            </div>
        </main>
    );
}

export default Home;
