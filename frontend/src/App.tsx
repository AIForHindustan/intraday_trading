import React, { useState, useMemo } from 'react';
import { Routes, Route, Link, Navigate, useLocation } from 'react-router-dom';
import { createTheme, ThemeProvider, CssBaseline } from '@mui/material';
import AlertDetail from './components/AlertDetail';
import SummaryStats from './components/SummaryStats';
import NewsFeed from './components/NewsFeed';
import MarketIndices from './components/MarketIndices';
import Dashboard from './components/Dashboard';
import { Box, AppBar, Toolbar, IconButton, Typography } from '@mui/material';
import { WbSunny, DarkMode } from '@mui/icons-material';

const App: React.FC = () => {
  // Dark mode toggle
  const [darkMode, setDarkMode] = useState(false);

  const theme = useMemo(() =>
    createTheme({
      palette: {
        mode: darkMode ? 'dark' : 'light',
        primary: {
          main: '#1976d2'
        },
        success: {
          main: '#4caf50'
        },
        warning: {
          main: '#ff9800'
        },
        error: {
          main: '#f44336'
        }
      }
    }), [darkMode]);

  const location = useLocation();
  const isLoginPage = location.pathname === '/login';

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      {!isLoginPage && (
        <AppBar position="static">
          <Toolbar>
            <Typography variant="h6" component={Link} to="/" sx={{ flexGrow: 1, textDecoration: 'none', color: 'inherit' }}>
              Intraday Trading Dashboard
            </Typography>
            <MarketIndices />
            <IconButton color="inherit" onClick={() => setDarkMode(!darkMode)}>
              {darkMode ? <WbSunny /> : <DarkMode />}
            </IconButton>
          </Toolbar>
        </AppBar>
      )}
      <Box sx={{ padding: isLoginPage ? 0 : 2 }}>
        <Routes>
          <Route path="/login" element={<Navigate to="/" replace />} />
          <Route path="/" element={<Dashboard />} />
          <Route path="/alerts/:alertId" element={<AlertDetail />} />
          <Route path="/summary" element={<SummaryStats />} />
          <Route path="/news" element={<NewsFeed />} />
        </Routes>
      </Box>
    </ThemeProvider>
  );
};

export default App;