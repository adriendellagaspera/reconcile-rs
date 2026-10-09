import { readFile } from 'node:fs/promises';
import test from 'node:test';
import assert from 'node:assert/strict';
const source = await readFile(new URL('./knowledge.js', import.meta.url), 'utf8');
const { freshness, knowledge } = await import('data:text/javascript;base64,' + Buffer.from(source).toString('base64'));

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

test('empty command-center replica has no contacts, fleet positions or terrain; terrain does not fade', () => {
  const empty = knowledge([], 1000);
  assert.equal(empty.contact, null);
  assert.equal(empty.vehicles.length, 0);
  assert.equal(empty.terrain.size, 0);
  const cell = { x: 1, y: 1, land: true };
  assert.deepEqual(knowledge([['map/1/1', { Terrain: cell }]], 10000).terrain.get('1,1'), cell);
});

test('fleet reports have their own freshness horizon and coverage deduplicates source reports', () => {
  const result = knowledge([
    ['vehicle/00', { Vehicle: { source: 0, seen: 0, position: { x: 1, y: 1 }, battery: 70 } }],
    ['sector/00/00', { Sector: { id: 0 } }],
    ['sector/00/01', { Sector: { id: 0 } }],
  ], 60);
  assert.equal(result.vehicles[0].stale, true);
  assert.equal(result.vehicles[0].age, 30);
  assert.equal(result.sectors, 1);
});
