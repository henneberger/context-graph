export function metricKey(m) { return `${m.entity_id}|${m.metric}|${m.window_start}|${m.window_end}`; }
export function mergeMetrics(current, incoming, limit=2000) {
  const map = new Map(current.map(m => [metricKey(m), m]));
  for (const m of incoming) map.set(metricKey(m), m);
  return [...map.values()].sort((a,b) => String(a.window_end).localeCompare(String(b.window_end))).slice(-limit);
}
export function mediaUrl(uri) {
  // Only serve relative media paths through the same-origin proxy.
  if (typeof uri !== 'string' || !uri.startsWith('/media/') || uri.includes('..') || uri.includes('\\')) return null;
  return uri;
}
export function websocketUrl(location) { return `${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.host}/graphql`; }
