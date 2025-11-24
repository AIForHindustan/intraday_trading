import React, { useState, useMemo, useEffect } from 'react';
import { Routes, Route, Link, Navigate, useLocation, useNavigate } from 'react-router-dom';
import { createTheme, ThemeProvider, CssBaseline } from '@mui/material';
import AlertDetail from './components/AlertDetail';
import SummaryStats from './components/SummaryStats';
import NewsFeed from './components/NewsFeed';
import MarketIndices from './components/MarketIndices';
import Dashboard from './components/Dashboard';
import InstrumentCharts from './components/InstrumentCharts';
import BrokerConnections from './components/BrokerConnections';
import AutoTradeSettings from './components/AutoTradeSettings';
import Login from './components/Login';
import Register from './components/Register';
import { useAuth } from './store/auth';
import { Box, AppBar, Toolbar, IconButton, Typography, Button } from '@mui/material';
import { WbSunny, DarkMode, BarChart, Login as LoginIcon, Logout } from '@mui/icons-material';

// Protected Route Component
const ProtectedRoute: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const { user } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();
  
  useEffect(() => {
    // Only redirect if we're sure there's no user (after hydration)
    if (user === null) {
      // Check if token exists in localStorage as fallback
      const token = localStorage.getItem('access_token');
      if (!token) {
        console.log('🔒 ProtectedRoute: No user and no token, redirecting to login');
        navigate('/login', { replace: true, state: { from: location.pathname } });
      }
    }
  }, [user, navigate, location]);
  
  // If user is explicitly null (not just undefined during hydration), redirect
  if (user === null) {
    const token = localStorage.getItem('access_token');
    if (!token) {
      return null; // Will redirect to login
    }
  }
  
  // Show children if user exists or if we're still checking (during hydration)
  return <>{children}</>;
};

const App: React.FC = () => {
  // Dark mode toggle
  const [darkMode, setDarkMode] = useState(false);
  const { user, logout, hydrate } = useAuth();
  const location = useLocation();
  const navigate = useNavigate();
  const isLoginPage = location.pathname === '/login' || location.pathname === '/register';

  // Hydrate auth state on mount
  useEffect(() => {
    hydrate();
  }, [hydrate]);

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

  const handleLogout = () => {
    logout();
    navigate('/login');
  };

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      {!isLoginPage && (
        <AppBar position="static" sx={{ zIndex: (theme) => theme.zIndex.drawer + 1 }}>
          <Toolbar sx={{ position: 'relative', justifyContent: 'space-between', minHeight: '64px !important' }}>
            <Box sx={{ width: 200, flexShrink: 0 }} /> {/* Spacer for left side */}
            <Typography 
              variant="h6" 
              component={Link} 
              to="/" 
              sx={{ 
                position: 'absolute',
                left: '50%',
                transform: 'translateX(-50%)',
                textDecoration: 'none', 
                color: 'inherit',
                fontWeight: 'bold',
                fontSize: '1.333rem', // h6 is 1.25rem, adding ~2pt
                zIndex: 1,
                pointerEvents: 'auto',
              }}
            >
              Intraday Trading Dashboard
            </Typography>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, flexShrink: 0, zIndex: 2 }}>
              <MarketIndices />
              {user ? (
                <>
                  <Typography variant="body2" sx={{ mr: 1 }}>
                    {user.username}
                  </Typography>
                  <IconButton color="inherit" onClick={handleLogout} sx={{ position: 'relative', zIndex: 2 }} title="Logout">
                    <Logout />
                  </IconButton>
                </>
              ) : (
                <Button
                  color="inherit"
                  component={Link}
                  to="/login"
                  startIcon={<LoginIcon />}
                  sx={{ textTransform: 'none' }}
                >
                  Login
                </Button>
              )}
              <IconButton color="inherit" onClick={() => setDarkMode(!darkMode)} sx={{ position: 'relative', zIndex: 2 }}>
                {darkMode ? <WbSunny /> : <DarkMode />}
              </IconButton>
            </Box>
          </Toolbar>
        </AppBar>
      )}
      {!isLoginPage && (
        <Box 
          sx={{ 
            bgcolor: 'background.paper', 
            borderBottom: 1, 
            borderColor: 'divider',
            px: 2,
            py: 1,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            boxShadow: 1
          }}
        >
          <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
            <Button
              color={location.pathname === '/' ? 'primary' : 'inherit'}
              component={Link}
              to="/"
              sx={{ 
                textTransform: 'none',
                fontWeight: location.pathname === '/' ? 'bold' : 'medium',
                px: 2
              }}
            >
              Home
            </Button>
            <Button
              variant={location.pathname === '/instruments' ? 'contained' : 'text'}
              color={location.pathname === '/instruments' ? 'primary' : 'inherit'}
              component={Link}
              to="/instruments"
              startIcon={<BarChart />}
              sx={{ 
                textTransform: 'none',
                fontWeight: location.pathname === '/instruments' ? 'bold' : 'medium',
                px: 3
              }}
            >
              Instruments
            </Button>
            <Button
              color={location.pathname === '/brokers' ? 'primary' : 'inherit'}
              component={Link}
              to="/brokers"
              sx={{ 
                textTransform: 'none',
                fontWeight: location.pathname === '/brokers' ? 'bold' : 'medium',
                px: 2
              }}
            >
              Brokers
            </Button>
            <Button
              color={location.pathname === '/auto-trade' ? 'primary' : 'inherit'}
              component={Link}
              to="/auto-trade"
              sx={{ 
                textTransform: 'none',
                fontWeight: location.pathname === '/auto-trade' ? 'bold' : 'medium',
                px: 2
              }}
            >
              My Algos
            </Button>
          </Box>
        </Box>
      )}
      <Box sx={{ padding: isLoginPage ? 0 : 2 }}>
        <Routes>
          <Route path="/login" element={user ? <Navigate to="/" replace /> : <Login />} />
          <Route path="/register" element={user ? <Navigate to="/" replace /> : <Register />} />
          <Route path="/" element={<ProtectedRoute><Dashboard /></ProtectedRoute>} />
          <Route path="/alerts/:alertId" element={<ProtectedRoute><AlertDetail /></ProtectedRoute>} />
          <Route path="/summary" element={<ProtectedRoute><SummaryStats /></ProtectedRoute>} />
          <Route path="/news" element={<ProtectedRoute><NewsFeed /></ProtectedRoute>} />
          <Route path="/instruments" element={<ProtectedRoute><InstrumentCharts /></ProtectedRoute>} />
          <Route path="/brokers" element={<ProtectedRoute><BrokerConnections /></ProtectedRoute>} />
          <Route path="/auto-trade" element={<ProtectedRoute><AutoTradeSettings /></ProtectedRoute>} />
        </Routes>
      </Box>
    </ThemeProvider>
  );
};

export default App;