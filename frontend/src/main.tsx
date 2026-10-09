import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App';
import './styles.css';

if ('serviceWorker' in navigator && import.meta.env.PROD) {
  window.addEventListener('load', () => navigator.serviceWorker.register('/sw.js').catch(() => undefined));
}

ReactDOM.createRoot(document.getElementById('root')!).render(<App />);
