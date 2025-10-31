# 🔐 ZERODHA AUTHENTICATION STEPS

## Your API Key: `v7ettoea5czh2l8g`

## Step 1: Get API Secret
1. Go to https://developers.kite.trade/apps
2. Login with your Zerodha credentials
3. Find your app and copy the API Secret

## Step 2: Generate Login URL
Visit this URL in your browser:
```
https://kite.zerodha.com/connect/login?api_key=v7ettoea5czh2l8g&v=3
```

## Step 3: After Login
After successful login, you'll be redirected to a URL like:
```
https://yourapp.com?request_token=XXXXXXXXXX&action=login&status=success
```

Copy the `request_token` value (the XXXXXXXXXX part)

## Step 4: Provide Both Values

Please provide:
1. **API Secret**: (from developers console)
2. **Request Token**: (from redirect URL)

## Quick Command
Once you have both, I'll run a command to save your access token.

---

**Note**: The token expires daily and needs to be refreshed each trading day.