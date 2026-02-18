import { Routes, Route } from 'react-router-dom';
import SearchPage from './pages/SearchPage';
import PrivacyPage from './pages/PrivacyPage';
import CookieBanner from './components/CookieBanner';
import AttributionFooter from './components/AttributionFooter';
import type { Attribution } from './types';
import { useState } from 'react';

function App() {
    const [activeAttributions, setActiveAttributions] = useState<Attribution[]>([]);

    return (
        <>
            <header className="header">
                <div className="container header-inner">
                    <div className="logo">
                        <span className="logo-icon">🚂</span>
                        <div className="logo-text">
                            Rail<span>Router</span>
                        </div>
                    </div>
                    <nav>
                        <ul className="nav-links">
                            <li><a href="/">Suche</a></li>
                            <li><a href="/datenschutz">Datenschutz</a></li>
                        </ul>
                    </nav>
                </div>
            </header>

            <main>
                <Routes>
                    <Route
                        path="/"
                        element={<SearchPage onAttributionsChange={setActiveAttributions} />}
                    />
                    <Route path="/datenschutz" element={<PrivacyPage />} />
                </Routes>
            </main>

            <AttributionFooter attributions={activeAttributions} />
            <CookieBanner />
        </>
    );
}

export default App;
