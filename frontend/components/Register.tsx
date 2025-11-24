import { useState } from 'react';
import { Box, Paper, Button, Typography, Alert, CircularProgress, IconButton, Checkbox, FormControlLabel, Link as MuiLink } from '@mui/material';
import { ContentCopy, CheckCircle, Download } from '@mui/icons-material';
import { authAPI } from '../services/api';
import { useNavigate, Link } from 'react-router-dom';

type RegistrationStep = 'generate' | 'confirm' | 'terms';

export default function Register() {
  const [step, setStep] = useState<RegistrationStep>('generate');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [accountNumber, setAccountNumber] = useState<string | null>(null);
  const [accountNumberRaw, setAccountNumberRaw] = useState<string | null>(null);
  const [confirmed, setConfirmed] = useState(false);
  const [termsAccepted, setTermsAccepted] = useState(false);
  const [copied, setCopied] = useState(false);

  const navigate = useNavigate();

  const handleGenerate = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError(null);

    try {
      console.log('🔄 Starting registration...');
      const response = await authAPI.register();
      console.log('✅ Registration response:', response);
      console.log('✅ Response data:', response.data);
      console.log('✅ Response status:', response.status);
      
      // Check response structure - axios wraps the response
      const responseData = response.data;
      if (!responseData) {
        console.error('❌ No response data received');
        throw new Error('No response data from server');
      }
      
      if (!responseData.account_number) {
        console.error('❌ Missing account_number in response:', responseData);
        throw new Error('Invalid response: missing account number');
      }
      
      setAccountNumber(responseData.account_number);
      setAccountNumberRaw(responseData.account_number_raw);
      setStep('confirm');
      console.log('✅ Account number set:', responseData.account_number);
    } catch (err: any) {
      console.error('❌ Registration error:', err);
      console.error('❌ Error response:', err?.response);
      console.error('❌ Error data:', err?.response?.data);
      const errorMessage = 
        err?.response?.data?.detail ||
        err?.response?.data?.message ||
        err?.message ||
        'Registration failed. Please try again.';
      setError(errorMessage);
    } finally {
      setLoading(false);
    }
  };

  const handleConfirm = () => {
    if (confirmed) {
      setStep('terms');
    }
  };

  const handleComplete = () => {
    if (termsAccepted) {
      navigate('/login');
    }
  };

  const copyToClipboard = async (text: string) => {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch (err) {
      console.error('Failed to copy:', err);
    }
  };

  const downloadAccountNumber = () => {
    if (!accountNumber) return;
    const fileContent = `AION ALGO TRADING SYSTEM INC\n\naccount number: ${accountNumber}`;
    const blob = new Blob([fileContent], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `account_number_${accountNumber.replace(/\s/g, '')}.txt`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  // Step 1: Generate Account Number
  if (step === 'generate') {
    return (
      <Box sx={{ minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center', p: 2, bgcolor: 'background.default' }}>
        <Paper sx={{ p: 4, width: '100%', maxWidth: 500 }}>
          <Typography variant="h5" gutterBottom sx={{ mb: 3, textAlign: 'center' }}>
            Create Account
          </Typography>
          
          <Typography variant="body1" color="text.secondary" sx={{ mb: 3, textAlign: 'center' }}>
            Generate your account number to get started. No email or phone number required.
          </Typography>

          {error && (
            <Alert severity="error" sx={{ mb: 2 }}>
              {error}
            </Alert>
          )}

          <form onSubmit={handleGenerate}>
            <Button 
              type="submit" 
              variant="contained" 
              fullWidth 
              sx={{ mt: 2, mb: 2, py: 1.5 }} 
              disabled={loading}
            >
              {loading ? <CircularProgress size={20} /> : 'Generate Account Number'}
            </Button>
          </form>

          <Typography variant="body2" color="text.secondary" sx={{ mt: 2, textAlign: 'center' }}>
            Already have an account?{' '}
            <Link to="/login" style={{ color: 'inherit', textDecoration: 'underline' }}>
              Sign in
            </Link>
          </Typography>
        </Paper>
      </Box>
    );
  }

  // Step 2: Show Account Number and Confirmation
  if (step === 'confirm') {
    return (
      <Box sx={{ minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center', p: 2, bgcolor: 'background.default' }}>
        <Paper sx={{ p: 4, width: '100%', maxWidth: 600 }}>
          <Typography 
            variant="h6" 
            sx={{ 
              mb: 4, 
              textAlign: 'center',
              fontFamily: 'monospace',
              textTransform: 'uppercase',
              color: 'text.primary',
              fontWeight: 'bold'
            }}
          >
            THIS IS YOUR ACCOUNT NUMBER. SAVE IT — YOU'LL NEED IT TO LOG IN.
          </Typography>

          <Box 
            sx={{ 
              p: 4, 
              mb: 3, 
              bgcolor: 'grey.50', 
              border: '2px dashed',
              borderColor: 'grey.400',
              borderRadius: 2,
              textAlign: 'center'
            }}
          >
            <Typography 
              variant="h3" 
              sx={{ 
                fontFamily: 'monospace',
                fontWeight: 'bold',
                color: 'text.primary',
                letterSpacing: 2
              }}
            >
              {accountNumber}
            </Typography>
          </Box>

          <Box sx={{ display: 'flex', gap: 2, justifyContent: 'center', mb: 4 }}>
            <Button
              variant="outlined"
              startIcon={copied ? <CheckCircle /> : <ContentCopy />}
              onClick={() => accountNumber && copyToClipboard(accountNumber)}
              sx={{ textTransform: 'none' }}
            >
              COPY
            </Button>
            <Button
              variant="outlined"
              startIcon={<Download />}
              onClick={downloadAccountNumber}
              sx={{ textTransform: 'none' }}
            >
              DOWNLOAD
            </Button>
          </Box>

          <Alert severity="warning" sx={{ mb: 3 }}>
            <Typography variant="body2">
              <strong>Important:</strong> Your account number is the only identifier you need to use our service. 
              For security and privacy reasons we cannot show it again after login.
            </Typography>
          </Alert>

          <Box sx={{ mb: 3 }}>
            <FormControlLabel
              control={
                <Checkbox
                  checked={confirmed}
                  onChange={(e) => setConfirmed(e.target.checked)}
                />
              }
              label={
                <Typography variant="body1">
                  I confirm that I've saved my account number.
                </Typography>
              }
            />
          </Box>

          <Button
            variant="contained"
            fullWidth
            onClick={handleConfirm}
            disabled={!confirmed}
            sx={{ py: 1.5, textTransform: 'none' }}
          >
            Continue
          </Button>
        </Paper>
      </Box>
    );
  }

  // Step 3: Terms of Service
  if (step === 'terms') {
    return (
      <Box sx={{ minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center', p: 2, bgcolor: 'background.default' }}>
        <Paper sx={{ p: 4, width: '100%', maxWidth: 600, maxHeight: '90vh', overflow: 'auto' }}>
          <Typography variant="h5" gutterBottom sx={{ mb: 3 }}>
            Terms of Service
          </Typography>

          <Box 
            sx={{ 
              p: 3, 
              mb: 3, 
              bgcolor: 'grey.50', 
              borderRadius: 1,
              maxHeight: '400px',
              overflow: 'auto'
            }}
          >
            <Typography variant="body2" sx={{ whiteSpace: 'pre-line' }}>
              TERMS OF SERVICE (STUB - TO BE UPDATED PER INDIAN LAW AND TRADING TERMINAL COMPLIANCE)

              This is a placeholder for the terms of service. The full terms will be prepared
              according to Indian law and trading terminal compliance requirements.

              By using this service, you agree to:
              - Use the service responsibly
              - Not share your account number with others
              - Comply with all applicable laws and regulations
              - Understand that trading involves risk
              - Accept full responsibility for your trading decisions
            </Typography>
          </Box>

          <Box sx={{ mb: 3 }}>
            <FormControlLabel
              control={
                <Checkbox
                  checked={termsAccepted}
                  onChange={(e) => setTermsAccepted(e.target.checked)}
                />
              }
              label={
                <Typography variant="body1">
                  I have read and agree to the terms of service
                </Typography>
              }
            />
          </Box>

          <Box sx={{ display: 'flex', gap: 2 }}>
            <Button
              variant="outlined"
              fullWidth
              onClick={() => setStep('confirm')}
              sx={{ textTransform: 'none' }}
            >
              Back
            </Button>
            <Button
              variant="contained"
              fullWidth
              onClick={handleComplete}
              disabled={!termsAccepted}
              sx={{ textTransform: 'none' }}
            >
              Complete Registration
            </Button>
          </Box>
        </Paper>
      </Box>
    );
  }

  return null;
}
