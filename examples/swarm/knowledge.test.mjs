import { readFile } from 'node:fs/promises';
import test from 'node:test';
import assert from 'node:assert/strict';
const source = await readFile(new URL('./knowledge.js', import.meta.url), 'utf8');
const { freshness, knowledge, directConnections } = await import('data:text/javascript;base64,' + Buffer.from(source).toString('base64'));

test('late delivery preserves observation age; fading is bounded and monotonic', () => {
  let previous = -1;
  for (let ticks = 10; ticks < 500; ticks++) {
    const value = freshness(10, ticks);
    assert.ok(value.age > previous);
    assert.ok(value.alpha >= .35 && value.alpha <= 1);
    previous = value.age;
  }
  assert.equal(freshness(10, 110).age, 50);
  assert.equal(freshness(10, 110).stale, true);
  assert.equal(freshness(10, 5).age, 0);
});

test('local synthesis keeps source reports and orders by observation time, not write or delivery order', () => {
  const reports = [
    ['contact/01/00', { Contact: { source: 0, seen: 10, position: { x: 1, y: 2 } } }],
    ['contact/01/01', { Contact: { source: 1, seen: 30, position: { x: 9, y: 8 } } }],
  ];
  for (const entries of [reports, [...reports].reverse()]) {
    const result = knowledge(entries, 130);
    assert.equal(result.reports.length, 2);
    assert.equal(result.contact.source, 1);
    assert.equal(result.contact.age, 50);
    assert.deepEqual(result.contact.position, { x: 9, y: 8 });
  }
});

test('empty replica has no operational knowledge; chart data is outside the projection', () => {
  const empty = knowledge([], 1000);
  assert.equal(empty.contact, null);
  assert.equal(empty.vehicles.length, 0);
  assert.equal('coverage' in empty, false);
  assert.equal('terrain' in empty, false);
  assert.equal(knowledge([['map/1/1', { Terrain: { x: 1, y: 1 } }]], 1000).reports.length, 0);
});

test('sector visits do not imply inspected areas; fleet reports retain observation age', () => {
  const result=knowledge([
    ['vehicle/00',{Vehicle:{source:0,seen:0,position:{x:1,y:1},battery:70}}],
    ['sector/00/00',{Sector:{id:0,scanned:2}}],
  ],60);
  assert.equal(result.vehicles[0].age,30);
  assert.equal(result.vehicles[0].stale,true);
  assert.equal('coverage' in result,false);
});

test('bearing anchors ignore supplied positions and preserve first source across handover', () => {
  const bearing={origin:{x:5,y:3},direction_deg:90,half_angle_deg:12,range_km:1.5};
  const result=knowledge([
    ['contact-first/01/00',{Contact:{id:1,source:0,seen:0,bearing}}],
    ['contact/01/01',{Contact:{id:1,source:1,seen:20,bearing,position:{x:30,y:19}}}],
  ],40);
  assert.deepEqual(result.contact.position,{x:5.75,y:3});
  assert.equal(result.contact.firstSource,0);
  assert.equal(result.contact.source,1);
  assert.equal(result.contact.age,10);
  assert.deepEqual(result.contact.bearing,bearing);
});

test('direct neighbors require ingress, never relayed reports or simulator positions', () => {
  const known = knowledge([
    ['vehicle/01', { Vehicle: { source: 1, position: { x: 3, y: 4 }, seen: 10 } }],
    ['vehicle/02', { Vehicle: { source: 2, position: { x: 8, y: 9 }, seen: 10 } }],
  ], 10);
  const links = directConnections(known, 0, [{ peer: 1, age_ms: 100, datagrams: 2 }], 3, { x: 16, y: 1 });
  assert.equal(links.length, 1);
  assert.equal(links[0].peer, 1);
  assert.deepEqual(links[0].position, { x: 3, y: 4 });
  assert.equal(links[0].active, true);
  assert.equal(links.some(link => link.peer === 2), false);
});

test('silent links expire on real transport time and unknown locations stay unknown', () => {
  const links = directConnections(knowledge([], 1000), 0, [
    { peer: 1, age_ms: 5000, datagrams: 1 },
    { peer: 2, age_ms: 10, datagrams: 1 },
  ], 2, { x: 16, y: 1 });
  assert.equal(links[0].active, false);
  assert.equal(links[0].position, null);
  assert.equal(links[1].active, true);
  assert.deepEqual(links[1].position, { x: 16, y: 1 });
});

test('distinct contact identities are synthesized independently', () => {
  const result = knowledge([
    ['contact/01/00', {Contact:{id:1,kind:'civil',source:0,seen:5}}],
    ['contact/02/00', {Contact:{id:2,kind:'whale',source:0,seen:10}}],
    ['contact/01/01', {Contact:{id:1,kind:'civil',source:1,seen:20}}],
  ],30);
  assert.equal(result.contacts.length,2);
  assert.equal(result.contacts.find(r=>r.id===1).source,1);
  assert.equal(result.contacts.find(r=>r.id===2).kind,'whale');
});

test('order status requires a matching acknowledgement in the same replica', () => {
  const order=['order/00',{Order:{recipient:0,sequence:2,expires:240,action:'hold'}}];
  const old=['order-ack/00/0000000000000001',{Acknowledgement:{recipient:0,sequence:1,applied:true}}];
  assert.equal(knowledge([order,old],250).commands[0].acknowledgement,null);
  assert.equal(knowledge([order],250).commands[0].expired,true);
  const ack=['order-ack/00/0000000000000002',{Acknowledgement:{recipient:0,sequence:2,applied:true}}];
  assert.equal(knowledge([order,ack],100).commands[0].acknowledgement.applied,true);
});

test('immutable discovery and order journals never appear as live contacts or duplicate commands', () => {
  const discovery = { Contact: { id: 0, source: 0, seen: 2, position: { x: 1, y: 1 } } };
  const latest = { Contact: { id: 0, source: 0, seen: 10, position: { x: 2, y: 2 } } };
  const another = { Contact: { id: 1, source: 1, seen: 5, position: { x: 3, y: 3 } } };
  const oldOrder = { Order: { recipient: 0, sequence: 1, expires: 100, action: 'hold' } };
  const nextOrder = { Order: { recipient: 0, sequence: 2, expires: 100, action: 'patrol' } };
  const result = knowledge([
    ['contact-first/00/00', discovery],
    ['contact/00/00', latest],
    ['contact/01/01', another],
    ['order-issued/00/0000000000000001', oldOrder],
    ['order-issued/00/0000000000000002', nextOrder],
    ['order/00', nextOrder],
    ['order-ack/00/0000000000000001', { Acknowledgement: { recipient: 0, sequence: 1, applied: true } }],
    ['order-ack/00/0000000000000002', { Acknowledgement: { recipient: 0, sequence: 2, applied: false } }],
  ], 10);
  assert.equal(result.reports.length, 2);
  assert.equal(result.contacts.length, 2); // id=0 must not be confused with id=1
  assert.equal(result.contacts.find(r => r.id === 0).seen, 10);
  assert.equal(result.discoveries.length, 1);
  assert.equal(result.discoveries[0].seen, 2);
  assert.equal(result.commands.length, 1);
  assert.equal(result.commands[0].sequence, 2);
  assert.equal(result.commands[0].acknowledgement.applied, false);
  assert.deepEqual(result.orderHistory.map(o => o.sequence), [2, 1]);
  assert.equal(result.orderHistory[1].acknowledgement.applied, true);
});

test('hit testing covers visible primitives without hidden truth targets', async () => {
  const { hitVisible } = await import('data:text/javascript;base64,' + Buffer.from(source).toString('base64'));
  const cell={kind:'terrain',x:1,y:1};
  const contact={kind:'contact',id:2,position:{x:1.5,y:1.5}};
  const peer={kind:'peer',source:3,position:{x:4,y:2}};
  const link={kind:'link',from:{x:4,y:2},to:{x:8,y:2}};
  const zone={kind:'zone',position:{x:10,y:10},radius:2};
  const visible=[cell,contact,peer,link,zone];
  assert.equal(hitVisible(visible,{x:45,y:45}),contact);
  assert.equal(hitVisible(visible,{x:120,y:60}),peer);
  assert.equal(hitVisible(visible,{x:180,y:61}),link);
  assert.equal(hitVisible(visible,{x:330,y:300}),zone);
  assert.equal(hitVisible([cell],{x:45,y:45}),cell);
  assert.equal(hitVisible([cell],{x:120,y:60}),null);
  assert.equal(hitVisible(visible,{x:-1,y:-1}),null);
});
