import React from 'react';
import { Routes, Route, Link } from 'react-router-dom';
import { createTheme, ThemeProvider, CssBaseline } from '@mui/material';
import { useState, useMemo } from 'react';
import AlertList from './components/AlertList';
import AlertDetail from './components/AlertDetail';
import SummaryStats from './components/SummaryStats';
import NewsFeed from './components/NewsFeed';
import MarketIndices from './components/MarketIndices';
import { Box, AppBar, Toolbar, IconButton, Typography, Switch as MuiSwitch } from '@mui/material';
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
          <Route path="/" element={<AlertList />} />
          <Route path="/alerts/:alertId" element={<AlertDetail />} />
          <Route path="/summary" element={<SummaryStats />} />
          <Route path="/news" element={<NewsFeed />} />
        </Routes>
      </Box>
    </ThemeProvider>
  );
};

export default App;