import { useState, useEffect } from 'react';
import { Box, Paper, TextField, Button, Typography, CircularProgress, Dialog, DialogTitle, DialogContent, DialogActions, DialogContentText } from '@mui/material';
import { useAuth } from '../store/auth';
import { useNavigate, Link } from 'react-router-dom';
import { brokersAPI } from '../services/api';

export default function Login() {
  const [accountNumber, setAccountNumber] = useState('');
  const [showBrokerPrompt, setShowBrokerPrompt] = useState(false);
  const [checkingBrokers, setCheckingBrokers] = useState(false);
  const [justLoggedIn, setJustLoggedIn] = useState(false);
  const { login, loading, error, user } = useAuth();
  const nav = useNavigate();

  // Check broker connection status after successful login
  useEffect(() => {
    if (justLoggedIn && user && !showBrokerPrompt && !checkingBrokers) {
      checkBrokerConnection();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [justLoggedIn, user]);

  const checkBrokerConnection = async () => {
    setCheckingBrokers(true);
    try {
      const statuses = await brokersAPI.getStatus();
      const hasConnectedBroker = statuses.some(s => s.status === 'ACTIVE');
      
      if (!hasConnectedBroker) {
        // No broker connected, show prompt
        setShowBrokerPrompt(true);
      } else {
        // Broker already connected, go to dashboard
        nav('/');
      }
    } catch (err) {
      // If check fails, just go to dashboard
      console.error('Failed to check broker status:', err);
      nav('/');
    } finally {
      setCheckingBrokers(false);
      setJustLoggedIn(false);
    }
  };

  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      // Account number login (the account number IS the login key)
      await login(accountNumber.replace(/\s/g, '')); // Remove spaces
      setJustLoggedIn(true); // Flag that we just logged in
      // Don't navigate here - let useEffect handle it after checking broker status
    } catch (err) {
      // Error already set in store
      setJustLoggedIn(false);
    }
  };

  const handleConnectBroker = () => {
    setShowBrokerPrompt(false);
    nav('/brokers');
  };

  const handleSkipBroker = () => {
    setShowBrokerPrompt(false);
    nav('/');
  };

  return (
    <Box sx={{ minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center', p: 2 }}>
      <Paper sx={{ p: 4, width: '100%', maxWidth: 400 }}>
        <Typography variant="h6" gutterBottom>Sign in</Typography>

        <form onSubmit={onSubmit}>
          <TextField 
            label="Account Number" 
            fullWidth 
            margin="dense" 
            value={accountNumber} 
            onChange={e => {
              // Format as user types: 1234 5678 9012
              const value = e.target.value.replace(/\s/g, '').replace(/\D/g, '');
              if (value.length <= 12) {
                const formatted = value.match(/.{1,4}/g)?.join(' ') || value;
                setAccountNumber(formatted);
              }
            }}
            placeholder="1234 5678 9012"
            helperText="Enter your account number (spaces optional)"
            inputProps={{ maxLength: 14 }} // 12 digits + 2 spaces
            sx={{ mt: 2 }}
          />
          
          {error && (
            <Typography color="error" sx={{ mt: 1 }}>{error}</Typography>
          )}
          
          <Button 
            type="submit" 
            variant="contained" 
            fullWidth 
            sx={{ mt: 2 }} 
            disabled={loading}
          >
            {loading ? <CircularProgress size={20} /> : 'Log in'}
          </Button>
        </form>

        <Typography variant="body2" color="text.secondary" sx={{ mt: 2, textAlign: 'center' }}>
          Don't have an account?{' '}
          <Link to="/register" style={{ color: 'inherit', textDecoration: 'underline' }}>
            Register
          </Link>
        </Typography>
      </Paper>

      {/* Optional Broker Connection Prompt */}
      <Dialog open={showBrokerPrompt} onClose={handleSkipBroker}>
        <DialogTitle>Connect Your Broker Account</DialogTitle>
        <DialogContent>
          <DialogContentText>
            You haven't connected a broker account yet. Would you like to connect Zerodha or Angel One now?
            <br /><br />
            You can also skip this step and connect your broker later from the dashboard.
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleSkipBroker} color="inherit">
            Skip for Now
          </Button>
          <Button onClick={handleConnectBroker} variant="contained" color="primary">
            Connect Broker
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}

