// Observation time is application time, independent of receive time and HLC ordering.
export function freshness(seen, ticks, lifetime = 45) {
  const age = Math.max(0, ticks - seen) * .5;
  const amount = Math.min(1, age / lifetime);
  return { age, amount, stale: age >= lifetime, alpha: 1 - amount * .45 };
}

// A local synthesis keeps the newest measurement and exposes every available source.
// It does not average asynchronous measurements or claim a sensor-fusion estimate.
export function knowledge(entries, ticks) {
  const vehicles = [];
  const reports = [];
  const discoveries = [];
  const orders = [];
  const issued = [];
  const acknowledgements = new Map();
  const sectors = new Map();
  for (const [key, value] of entries) {
    // Keep latest-state registers separate from immutable event keys. All of
    // these are still ordinary LWW entries in the library; immutability is an
    // application-level guarantee from disjoint, single-writer key ownership.
    if (key.startsWith('vehicle/') && value.Vehicle) vehicles.push({ ...value.Vehicle, ...freshness(value.Vehicle.seen, ticks, 30) });
    else if (key.startsWith('contact/') && value.Contact) reports.push({ ...value.Contact, ...freshness(value.Contact.seen, ticks) });
    else if (key.startsWith('contact-first/') && value.Contact) discoveries.push(value.Contact);
    else if (key.startsWith('order/') && value.Order) orders.push(value.Order);
    else if (key.startsWith('order-issued/') && value.Order) issued.push(value.Order);
    else if (key.startsWith('order-ack/') && value.Acknowledgement) {
      acknowledgements.set(value.Acknowledgement.recipient + '/' + value.Acknowledgement.sequence, value.Acknowledgement);
    }
    else if (key.startsWith('sector/') && value.Sector) {
      const sector = value.Sector;
      if (!sectors.has(sector.id) || sector.scanned > sectors.get(sector.id).scanned) sectors.set(sector.id, sector);
    }
  }
  reports.sort((a, b) => b.seen - a.seen || a.source - b.source || (a.id ?? 0) - (b.id ?? 0));
  const byContact = new Map();
  for (const report of reports) {
    const id = report.id ?? 1;
    if (!byContact.has(id)) byContact.set(id, report);
  }
  const contacts = [...byContact.values()];
  const decorate = order => ({
    ...order,
    acknowledgement: acknowledgements.get(order.recipient + '/' + order.sequence) || null,
    expired: ticks > order.expires,
  });
  const commands = orders.map(decorate);
  const orderHistory = issued.sort((a, b) => b.sequence - a.sequence).map(decorate);
  const coverage = [...sectors.values()].map(sector => ({ ...sector, ...freshness(sector.scanned, ticks, 120) }));
  return { coverage, vehicles, reports, contacts, commands, orderHistory, discoveries,
    acknowledgements: [...acknowledgements.values()],
    contact: reports[0] || null, sectors: sectors.size };
}

// Connectivity is inferred only from this observer's direct ingress. Relayed reports
// do not make their sensor source a neighbor, or expose other peers' network edges.
export function directConnections(known, observer, receipts, center, coastalPosition) {
  const positions = new Map(known.vehicles.map(vehicle => [vehicle.source, vehicle.position]));
  positions.set(center, coastalPosition); // Fixed station location is mission configuration.
  return receipts.filter(receipt => receipt.peer !== observer).map(receipt => ({
    ...receipt,
    position: positions.get(receipt.peer) || null,
    active: receipt.age_ms < 5000,
  }));
}

// Hit testing receives exactly the primitives drawn in the active perspective.
// Unknown cells and peers therefore cannot become invisible click targets.
export function hitVisible(entities, point) {
  const distance = entity => Math.hypot(point.x - entity.position.x * 30, point.y - entity.position.y * 30);
  const markers=entities.filter(e=>e.kind==='peer'||e.kind==='contact').map(e=>({e,d:distance(e)})).sort((a,b)=>a.d-b.d);
  if(markers[0]?.d<18)return markers[0].e;
  const links=entities.filter(e=>e.kind==='link').map(e=>{
    const ax=e.from.x*30,ay=e.from.y*30,dx=(e.to.x-e.from.x)*30,dy=(e.to.y-e.from.y)*30;
    const length=dx*dx+dy*dy,t=length?Math.max(0,Math.min(1,((point.x-ax)*dx+(point.y-ay)*dy)/length)):0;
    return {e,d:Math.hypot(point.x-ax-t*dx,point.y-ay-t*dy)};
  }).sort((a,b)=>a.d-b.d);
  if(links[0]?.d<4)return links[0].e;
  const zone=entities.find(e=>e.kind==='zone'&&distance(e)<=e.radius*30);
  if(zone)return zone;
  return entities.find(e=>e.kind==='terrain'&&e.x===Math.floor(point.x/30)&&e.y===Math.floor(point.y/30))||null;
}
