// Observation time is application time, independent of receive time and HLC ordering.
export function freshness(seen, ticks, lifetime = 45) {
  const age = Math.max(0, ticks - seen) * .5;
  const amount = Math.min(1, age / lifetime);
  return { age, amount, stale: age >= lifetime, alpha: 1 - amount * .45 };
}

// A local synthesis keeps the newest measurement and exposes every available source.
// It does not average asynchronous measurements or claim a sensor-fusion estimate.
export function knowledge(entries, ticks) {
  const terrain = new Map();
  const vehicles = [];
  const reports = [];
  const sectors = new Set();
  for (const [key, value] of entries) {
    if (value.Terrain) terrain.set(value.Terrain.x + ',' + value.Terrain.y, value.Terrain);
    if (value.Vehicle) vehicles.push({ ...value.Vehicle, ...freshness(value.Vehicle.seen, ticks, 30) });
    if (value.Contact) reports.push({ ...value.Contact, ...freshness(value.Contact.seen, ticks) });
    if (value.Sector) sectors.add(value.Sector.id);
  }
  reports.sort((a, b) => b.seen - a.seen || a.source - b.source);
  return { terrain, vehicles, reports, contact: reports[0] || null, sectors: sectors.size };
}
