import React, { useEffect } from 'react';
import { Routes, Route, Link, Navigate, useLocation } from 'react-router-dom';
import { createTheme, ThemeProvider, CssBaseline } from '@mui/material';
import { useState, useMemo } from 'react';
import AlertList from './components/AlertList';
import AlertDetail from './components/AlertDetail';
import SummaryStats from './components/SummaryStats';
import NewsFeed from './components/NewsFeed';
import MarketIndices from './components/MarketIndices';
import Login from './components/Login';
import Dashboard from './components/Dashboard';
import { useAuth } from './store/auth';
import { Box, AppBar, Toolbar, IconButton, Typography, Switch as MuiSwitch } from '@mui/material';
import { WbSunny, DarkMode } from '@mui/icons-material';

function PrivateRoute({ children }: { children: JSX.Element }) {
  const { user, hydrate } = useAuth();
  const loc = useLocation();
  
  useEffect(() => { 
    hydrate(); 
  }, [hydrate]);

  if (!user) {
    return <Navigate to="/login" state={{ from: loc }} replace />;
  }

  return children;
}

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

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
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
      <Box sx={{ padding: 2 }}>
        <Routes>
          <Route path="/login" element={<Login />} />
          <Route path="/" element={<PrivateRoute><Dashboard /></PrivateRoute>} />
          <Route path="/alerts/:alertId" element={<PrivateRoute><AlertDetail /></PrivateRoute>} />
          <Route path="/summary" element={<PrivateRoute><SummaryStats /></PrivateRoute>} />
          <Route path="/news" element={<PrivateRoute><NewsFeed /></PrivateRoute>} />
        </Routes>
      </Box>
    </ThemeProvider>
  );
};

export default App;