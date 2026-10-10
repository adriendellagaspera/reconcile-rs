use super::*;
use serde::{Deserialize, Serialize};

pub const BASE: Point = Point { x: 4.0, y: 10.0 };
pub const FOOTPRINT_KM: f64 = 0.1;
pub const CRUISE_KM_S: f64 = 0.02;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PoiKind {
    People,
    Bridge,
    Road,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Finding {
    Suspected,
    Confirmed,
    Dismissed,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Search,
    Verify,
    Survey,
    Monitor,
    Return,
}
#[derive(Clone, Copy, Serialize)]
pub struct TruthPoi {
    pub id: usize,
    pub kind: PoiKind,
    pub position: Point,
    pub genuine: bool,
}
#[derive(Serialize)]
pub struct Aircraft {
    pub home: Point,
    pub waypoint: usize,
    pub target: Point,
    pub role: Role,
    pub poi: Option<usize>,
    pub distance_km: f64,
}
pub struct Mission {
    pub aircraft: Vec<Aircraft>,
    pub pois: Vec<TruthPoi>,
}
impl Mission {
    pub fn new(count: usize) -> Self {
        let aircraft = (0..count)
            .map(|id| {
                let home = Point {
                    x: 10.0 + ((id / 2) % 3) as f64 * 6.0 + (id % 2) as f64 * 1.5,
                    y: 6.0 + ((id / 2) % 3) as f64 * 3.5,
                };
                Aircraft {
                    home,
                    waypoint: 0,
                    target: Point {
                        x: home.x + 1.2,
                        y: home.y,
                    },
                    role: Role::Search,
                    poi: None,
                    distance_km: 0.0,
                }
            })
            .collect();
        Self {
            aircraft,
            pois: vec![
                TruthPoi {
                    id: 1,
                    kind: PoiKind::People,
                    position: Point { x: 10.6, y: 6.0 },
                    genuine: true,
                },
                TruthPoi {
                    id: 2,
                    kind: PoiKind::Bridge,
                    position: Point { x: 16.6, y: 9.5 },
                    genuine: true,
                },
                TruthPoi {
                    id: 3,
                    kind: PoiKind::People,
                    position: Point { x: 17.9, y: 9.5 },
                    genuine: false,
                },
                TruthPoi {
                    id: 4,
                    kind: PoiKind::Road,
                    position: Point { x: 22.6, y: 13.0 },
                    genuine: true,
                },
            ],
        }
    }
}
impl Cluster {
    pub fn start_flood(&mut self) {
        for node in &self.nodes {
            node.clear();
        }
        self.world.ticks = 0;
        self.world.scripted = false;
        self.world.playing = true;
        let mission = Mission::new(self.center());
        self.world.positions = mission.aircraft.iter().map(|a| a.home).collect();
        self.world.held.fill(false);
        self.flood = Some(mission);
        self.heal();
        self.network.topology.write().coastal_position = None;
        self.network.topology.write().positions[self.center()] = BASE;
        self.network.topology.write().range = 12.0;
        self.refresh_network();
        self.observe();
    }
    pub(super) fn advance_flood(&mut self) {
        self.process_orders();
        if !self.world.playing {
            return;
        }
        self.world.ticks += 1;
        let tick = self.world.ticks;
        if self.world.scripted {
            if tick == 40 {
                self.set_peer(self.center(), PeerState::Offline).unwrap();
                let n = self.nodes.len();
                let mut topology = self.network.topology.write();
                for a in 0..self.center() {
                    for b in 0..self.center() {
                        if (a < 3) != (b < 3) {
                            topology.blocked[a * n + b] = true;
                        }
                    }
                }
            }
            if tick == 600 {
                self.heal();
            }
            if tick >= 1800 {
                if self.divergent_keys() == 0 {
                    self.world.playing = false;
                }
                return;
            }
        }
        let peers = self.network.topology.read().peers.clone();
        for (id, peer) in peers.iter().enumerate().take(self.center()) {
            if *peer == PeerState::Stopped {
                continue;
            }
            let snapshot = self.nodes[id].to_vec();
            let mut reports = std::collections::BTreeMap::new();
            for (_, value) in &snapshot {
                if let Observation::Poi {
                    id: poi,
                    seen,
                    source,
                    ..
                } = value
                {
                    let replace =
                        reports
                            .get(poi)
                            .is_none_or(|previous: &&Observation| match previous {
                                Observation::Poi {
                                    seen: old,
                                    source: old_source,
                                    ..
                                } => {
                                    (evidence(value), *seen, std::cmp::Reverse(*source))
                                        > (evidence(previous), *old, std::cmp::Reverse(*old_source))
                                }
                                _ => false,
                            });
                    if replace {
                        reports.insert(*poi, value);
                    }
                }
            }
            let position = self.world.positions[id];
            let a = &mut self.flood.as_mut().unwrap().aircraft[id];
            let reserve = 900.0;
            let remaining = 3600.0 - self.world.seconds();
            let returning = remaining <= reserve + position.distance(BASE) / CRUISE_KM_S
                || a.distance_km >= 45.0;
            let desired = reports.values().find_map(|value| match value {
                Observation::Poi {
                    id: poi,
                    kind: PoiKind::People,
                    finding,
                    position,
                    seen,
                    ..
                } if *finding != Finding::Dismissed => {
                    let role = match (id, *finding) {
                        (1, Finding::Suspected) => Some(Role::Verify),
                        (3, Finding::Suspected) if *poi == 3 => Some(Role::Verify),
                        (2, _) if tick.saturating_sub(*seen) < 360 => Some(Role::Survey),
                        (0, Finding::Confirmed) => Some(Role::Monitor),
                        _ => None,
                    };
                    role.map(|role| (*poi, *position, role))
                }
                _ => None,
            });
            let (poi, role) = if returning {
                (None, Role::Return)
            } else if let Some((poi, p, role)) = desired {
                a.target = if role == Role::Survey {
                    Point {
                        x: p.x + 0.6 * (tick as f64 * 0.5 * CRUISE_KM_S / 0.6).cos(),
                        y: p.y + 0.6 * (tick as f64 * 0.5 * CRUISE_KM_S / 0.6).sin(),
                    }
                } else {
                    p
                };
                (Some(poi), role)
            } else {
                (None, Role::Search)
            };
            if a.poi != poi || a.role != role {
                a.poi = poi;
                a.role = role;
                self.nodes[id].insert(
                    format!("intent/{id:02}"),
                    Observation::Intent {
                        recipient: id,
                        poi,
                        role,
                        seen: tick,
                    },
                );
            }
            if role == Role::Return {
                a.target = BASE;
            }
            if role == Role::Search {
                let corners = [
                    Point {
                        x: a.home.x + 1.2,
                        y: a.home.y,
                    },
                    Point {
                        x: a.home.x + 1.2,
                        y: a.home.y + 0.8,
                    },
                    Point {
                        x: a.home.x,
                        y: a.home.y + 0.8,
                    },
                    a.home,
                ];
                a.target = corners[a.waypoint];
                if position.distance(a.target) < 0.02 {
                    a.waypoint = (a.waypoint + 1) % 4;
                    a.target = corners[a.waypoint];
                }
            }
            // Hold is an aircraft loiter; this kinematic model draws it stationary.
            if !self.world.held[id] {
                let distance = position.distance(a.target);
                let step = (CRUISE_KM_S * super::super::world::STEP_SECONDS).min(distance);
                if distance > 0.0 {
                    self.world.positions[id] = Point {
                        x: position.x + (a.target.x - position.x) * step / distance,
                        y: position.y + (a.target.y - position.y) * step / distance,
                    };
                    a.distance_km += step;
                }
            }
        }
        self.refresh_network();
        self.observe();
    }
    pub(super) fn observe_flood(&mut self, id: usize, force: bool) {
        if self.network.topology.read().peers[id] == PeerState::Stopped {
            return;
        }
        if !force && self.world.ticks % 20 != 0 {
            return;
        }
        let tick = self.world.ticks;
        let p = self.world.positions[id];
        let battery = (100.0 * (1.0 - self.world.seconds() / 3600.0)).clamp(0.0, 100.0) as u8;
        let mut updates = vec![(
            format!("vehicle/{id:02}"),
            Observation::Vehicle {
                position: p,
                seen: tick,
                source: id,
                battery,
            },
        )];
        for poi in &self.flood.as_ref().unwrap().pois {
            if p.distance(poi.position) > FOOTPRINT_KM {
                continue;
            }
            let key = format!("poi/{:02}/{id:02}", poi.id);
            let previous = self.nodes[id].get_cloned(&key);
            let finding = if poi.kind != PoiKind::People {
                Finding::Confirmed
            } else if id % 2 == 1 && previous.is_some() {
                if poi.genuine {
                    Finding::Confirmed
                } else {
                    Finding::Dismissed
                }
            } else {
                Finding::Suspected
            };
            // A sensor keeps its own confirmation; receiving a report never creates a measurement.
            let finding = match previous {
                Some(Observation::Poi {
                    finding: Finding::Confirmed,
                    ..
                }) => Finding::Confirmed,
                Some(Observation::Poi {
                    finding: Finding::Dismissed,
                    ..
                }) => Finding::Dismissed,
                _ => finding,
            };
            let report = Observation::Poi {
                id: poi.id,
                kind: poi.kind,
                finding,
                position: poi.position,
                seen: tick,
                source: id,
            };
            let first = format!("poi-first/{:02}/{id:02}", poi.id);
            if !self.nodes[id].contains_key(&first) {
                updates.push((first, report.clone()));
            }
            updates.push((key, report));
        }
        self.nodes[id].insert_bulk(&updates);
    }
}

fn evidence(value: &Observation) -> u8 {
    match value {
        Observation::Poi {
            finding: Finding::Suspected,
            ..
        } => 0,
        _ => 1,
    }
}

#[cfg(test)]
#[path = "flood/tests.rs"]
mod tests;
