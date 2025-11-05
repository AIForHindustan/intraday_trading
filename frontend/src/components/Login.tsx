import { useState } from 'react';
import { Box, Paper, TextField, Button, Typography, CircularProgress } from '@mui/material';
import { useAuth } from '../store/auth';
import { useNavigate } from 'react-router-dom';

export default function Login() {
  const [username, setU] = useState('');
  const [password, setP] = useState('');
  const { login, loading, error } = useAuth();
  const nav = useNavigate();

  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      await login(username, password);
      nav('/');
    } catch (err) {
      // Error already set in store
    }
  };

  return (
    <Box sx={{ minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
      <Paper sx={{ p: 4, width: 360 }}>
        <Typography variant="h6" gutterBottom>Sign in</Typography>
        <form onSubmit={onSubmit}>
          <TextField 
            label="Username" 
            fullWidth 
            margin="dense" 
            value={username} 
            onChange={e => setU(e.target.value)} 
            autoComplete="username"
          />
          <TextField 
            label="Password" 
            type="password" 
            fullWidth 
            margin="dense" 
            value={password} 
            onChange={e => setP(e.target.value)} 
            autoComplete="current-password"
          />
          {error && <Typography color="error" sx={{ mt: 1 }}>{error}</Typography>}
          <Button type="submit" variant="contained" fullWidth sx={{ mt: 2 }} disabled={loading}>
            {loading ? <CircularProgress size={20} /> : 'Login'}
          </Button>
        </form>
      </Paper>
    </Box>
  );
}

