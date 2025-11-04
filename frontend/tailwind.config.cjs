/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    './index.html',
    './src/**/*.{js,ts,jsx,tsx}'
  ],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        primary: '#1976d2',
        success: '#4caf50',
        warning: '#ff9800',
        error: '#f44336',
        background: '#f5f5f5',
        darkBg: '#121212'
      }
    }
  },
  plugins: []
};