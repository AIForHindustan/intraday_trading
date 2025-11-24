// Normalize alerts coming from different backends
export function adaptAlert(row: any) {
  const price =
    row.last_price ??
    row.price ??
    row.entry_price ??
    row.ltp ??
    row.underlying_price ??
    null;

  // Extract base_symbol from symbol if not provided
  let base_symbol = row.base_symbol;
  if (!base_symbol && row.symbol) {
    const symbolStr = row.symbol.toUpperCase();
    // Handle options contracts: NIFTY25NOV25950CE -> NIFTY 50
    // Handle futures: NIFTY25NOV -> NIFTY 50
    const indexMatch = symbolStr.match(/^(NIFTY|BANKNIFTY|FINNIFTY|INDIA VIX)/);
    if (indexMatch) {
      const indexName = indexMatch[1];
      // Check if there are digits after index name (options/futures)
      const afterIndex = symbolStr.substring(indexMatch[0].length).trim();
      if (/^\d/.test(afterIndex)) {
        // Has digits after - it's an options/futures contract
        base_symbol = indexName === 'NIFTY' ? 'NIFTY 50' : (indexName === 'BANKNIFTY' ? 'NIFTY BANK' : indexName);
      } else {
        // Pure index
        base_symbol = symbolStr.includes('NIFTY 50') ? 'NIFTY 50' : 
                     (symbolStr.includes('NIFTY BANK') || symbolStr.includes('BANKNIFTY') ? 'NIFTY BANK' : 
                     symbolStr);
      }
    } else {
      // Not a recognized index, use symbol as-is
      base_symbol = row.symbol;
    }
  }

  return {
    alert_id: row.alert_id ?? row.id ?? `${row.symbol}_${row.timestamp}`,
    symbol: row.symbol ?? base_symbol ?? 'UNKNOWN',
    pattern: row.pattern ?? row.pattern_type ?? row.pattern_label ?? 'unknown',
    pattern_label: row.pattern_label ?? row.pattern ?? row.pattern_type ?? 'unknown',
    confidence: Number(row.confidence ?? row.validation_confidence ?? 0),
    signal: (row.signal && row.signal !== 'NEUTRAL' && row.signal.toUpperCase() in ['BUY', 'SELL', 'HOLD', 'WATCH']) 
      ? row.signal.toUpperCase() 
      : null, // Only valid signals: BUY, SELL, HOLD, WATCH. Don't use NEUTRAL.
    action: row.action ?? row.signal ?? '', // Trading action (e.g., SELL_STRADDLE, BUY_CALL)
    last_price: price,
    timestamp: row.timestamp ?? row.time ?? row.created_at,
    base_symbol: base_symbol ?? row.symbol,
  };
}

// Charts: accept {ohlc:[...]} or {data:[...]} and fix ms/seconds
export function adaptOhlcPayload(p: any) {
  const rows = (p?.ohlc ?? p?.data ?? []) as any[];
  return rows
    .map((r) => ({
      timestamp: typeof r.timestamp === 'number'
        ? (String(r.timestamp).length <= 10 ? r.timestamp * 1000 : r.timestamp)
        : r.timestamp
        ? Date.parse(r.timestamp)
        : r.time
        ? Date.parse(r.time)
        : undefined,
      open: r.open ?? r.o,
      high: r.high ?? r.h,
      low: r.low ?? r.l,
      close: r.close ?? r.c,
      volume: r.volume ?? r.v ?? 0,
    }))
    .filter(
      (x) =>
        Number.isFinite(x?.timestamp) &&
        Number.isFinite(x?.open) &&
        Number.isFinite(x?.high) &&
        Number.isFinite(x?.low) &&
        Number.isFinite(x?.close)
    );
}

export function adaptOverlays(p: any) {
  const o = p?.indicators_overlay ?? {};
  return {
    ema_5: o.ema_5 ?? [],
    ema_10: o.ema_10 ?? [],
    ema_20: o.ema_20 ?? [],
    ema_50: o.ema_50 ?? [],
    ema_100: o.ema_100 ?? [],
    ema_200: o.ema_200 ?? [],
    vwap: o.vwap ?? [],
    bb_upper: o.bollinger_bands?.upper ?? [],
    bb_lower: o.bollinger_bands?.lower ?? [],
  };
}

