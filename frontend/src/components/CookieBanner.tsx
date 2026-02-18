import { useState, useEffect } from 'react';

export default function CookieBanner() {
    const [visible, setVisible] = useState(false);

    useEffect(() => {
        const accepted = localStorage.getItem('cookies_accepted');
        if (!accepted) {
            setVisible(true);
        }
    }, []);

    const accept = () => {
        localStorage.setItem('cookies_accepted', 'true');
        setVisible(false);
    };

    if (!visible) return null;

    return (
        <div className="cookie-banner">
            <p>
                Diese Website verwendet ausschließlich technisch notwendige Cookies
                für die grundlegende Funktionalität. Es werden keine Tracking- oder
                Werbe-Cookies eingesetzt.{' '}
                <a href="/datenschutz">Datenschutzerklärung</a>
            </p>
            <button className="btn btn-primary" onClick={accept}>
                Verstanden
            </button>
        </div>
    );
}
