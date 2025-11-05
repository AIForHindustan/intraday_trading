export function istNow() {
  const now = new Date();
  const istOffset = 5.5 * 60; // minutes
  return new Date(now.getTime() + (istOffset - now.getTimezoneOffset()) * 60000);
}

export function istStartOfToday() {
  const d = istNow();
  d.setHours(0, 0, 0, 0);
  return d;
}

export function istEndOfToday() {
  const d = istNow();
  d.setHours(23, 59, 59, 999);
  return d;
}

