use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::{Deserialize, Serialize};

pub const WIDTH: usize = 32;
pub const HEIGHT: usize = 20;
pub const STEP_SECONDS: f64 = 0.5;
pub const COMMAND_POSITION: Point = Point { x: 15.0, y: 8.0 };

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PeerState {
    Active,
    Offline,
    Stopped,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Point {
    pub x: f64,
    pub y: f64,
}

impl Point {
    pub fn distance(self, other: Self) -> f64 {
        (self.x - other.x).hypot(self.y - other.y)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderAction {
    Scan,
    Hold,
    Patrol,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContactKind {
    Civil,
    Hostile,
    Whale,
    SpermWhale,
    OceanFront,
}

#[derive(Clone, Copy, Serialize)]
pub struct ContactPosition {
    pub id: usize,
    pub kind: ContactKind,
    pub position: Point,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Observation {
    Terrain {
        x: usize,
        y: usize,
        land: bool,
        detail: [u8; 8],
    },
    Contact {
        id: usize,
        kind: ContactKind,
        position: Point,
        seen: u64,
        source: usize,
    },
    Vehicle {
        position: Point,
        seen: u64,
        source: usize,
        battery: u8,
    },
    Order {
        recipient: usize,
        sequence: u64,
        issued: u64,
        expires: u64,
        action: OrderAction,
    },
    Acknowledgement {
        recipient: usize,
        sequence: u64,
        received: u64,
        applied: bool,
    },
    Sector {
        id: usize,
        scanned: u64,
    },
}

pub fn terrain(x: usize, y: usize) -> bool {
    terrain_at(Point {
        x: x as f64 + 0.5,
        y: y as f64 + 0.5,
    })
}

pub fn terrain_at(p: Point) -> bool {
    let shore =
        2.8 + (p.y * 0.42).sin() * 1.2 + (p.y * 1.7).sin() * 0.35 + (p.y * 4.3).cos() * 0.12;
    p.x < shore
        || [(15.0, 8.0, 3.8, 2.8), (26.0, 15.0, 2.8, 1.9)]
            .iter()
            .any(|&(x, y, rx, ry)| {
                let dx = (p.x - x) / rx;
                let dy = (p.y - y) / ry;
                let a = dy.atan2(dx);
                dx.hypot(dy) < 1.0 + 0.11 * (a * 5.0).sin() + 0.05 * (a * 9.0).cos()
            })
}

pub fn terrain_detail(x: usize, y: usize) -> [u8; 8] {
    std::array::from_fn(|row| {
        (0..8).fold(0, |mask, col| {
            mask | (u8::from(terrain_at(Point {
                x: x as f64 + (col as f64 + 0.5) / 8.0,
                y: y as f64 + (row as f64 + 0.5) / 8.0,
            })) << col)
        })
    })
}

pub struct World {
    pub ticks: u64,
    pub playing: bool,
    pub scripted: bool,
    pub positions: Vec<Point>,
    contact_origin: Option<ContactTruth>,
    contacts: Vec<(ContactKind, Point, f64)>,
    pub held: Vec<bool>,
}

struct ContactTruth {
    origin: Point,
    born_tick: u64,
}

impl World {
    pub fn new(nodes: usize) -> Self {
        let mut world = Self {
            ticks: 0,
            playing: false,
            scripted: false,
            positions: vec![Point { x: 0.0, y: 0.0 }; nodes],
            contact_origin: None,
            contacts: Vec::new(),
            held: vec![false; nodes],
        };
        let mut rng = StdRng::seed_from_u64(42);
        for kind in [
            ContactKind::Civil,
            ContactKind::Hostile,
            ContactKind::Whale,
            ContactKind::SpermWhale,
            ContactKind::OceanFront,
        ] {
            let origin = loop {
                let p = Point {
                    x: rng.gen_range(5.0..29.0),
                    y: rng.gen_range(3.0..17.0),
                };
                if (0..64).all(|i| {
                    !terrain_at(Point {
                        x: p.x + (i as f64 * 0.1).sin(),
                        y: p.y + (i as f64 * 0.1).cos(),
                    })
                }) {
                    break p;
                }
            };
            world.contacts.push((kind, origin, rng.gen_range(0.0..6.0)));
        }
        world.move_vehicles();
        world
    }

    pub fn seconds(&self) -> f64 {
        self.ticks as f64 * STEP_SECONDS
    }

    pub fn move_vehicles(&mut self) {
        self.move_active(&vec![PeerState::Active; self.positions.len()]);
    }

    pub fn move_active(&mut self, peers: &[PeerState]) {
        let half = self.positions.len() / 2;
        let count = self.positions.len();
        let seconds = self.seconds();
        let exploration = ((seconds - 30.0) / 30.0).clamp(0.0, 1.0);
        for (id, position) in self.positions.iter_mut().enumerate() {
            if peers[id] == PeerState::Stopped || self.held[id] {
                continue;
            }
            let group = usize::from(id >= half);
            let group_size = if group == 0 { half } else { count - half };
            let slot = if group == 0 { id } else { id - half };
            let phase = slot as f64 * std::f64::consts::TAU / group_size as f64 + seconds * 0.025;
            // Fixed offshore patrol loops: motion is cosmetic, observation writes are not.
            let mut next = Point {
                x: if group == 0 {
                    8.5 - 3.0 * exploration + 2.5 * phase.cos()
                } else {
                    21.0 + 6.0 * exploration + 1.5 * phase.cos()
                },
                y: 10.0 + 6.5 * phase.sin(),
            };
            // A deterministic cosmetic detour around islands and the shoreline.
            while terrain_at(next) {
                next.x += 0.5;
            }
            *position = next;
        }
    }

    pub fn reveal_contact(&mut self) {
        let vehicle = self.positions[0];
        self.contact_origin = [(1.5, 0.0), (0.0, 1.5), (0.0, -1.5), (-1.5, 0.0)]
            .into_iter()
            .map(|(dx, dy)| Point {
                x: vehicle.x + dx,
                y: vehicle.y + dy,
            })
            .find(|point| {
                point.x >= 0.0
                    && point.y >= 0.0
                    && point.x < WIDTH as f64
                    && point.y < HEIGHT as f64
                    && !terrain(point.x as usize, point.y as usize)
            })
            .map(|origin| ContactTruth {
                origin,
                born_tick: self.ticks,
            });
    }

    pub fn contact(&self) -> Option<Point> {
        self.contact_origin.as_ref().map(|contact| {
            let origin = contact.origin;
            let angle = self.ticks.saturating_sub(contact.born_tick) as f64 * STEP_SECONDS * 0.03;
            Point {
                x: origin.x + angle.sin(),
                y: origin.y + (angle.cos() - 1.0) * 0.7,
            }
        })
    }

    pub fn contacts(&self) -> Vec<ContactPosition> {
        let mut contacts: Vec<_> = self
            .contacts
            .iter()
            .enumerate()
            .map(|(i, &(kind, origin, phase))| {
                let a = self.seconds() * 0.025 + phase;
                ContactPosition {
                    id: i + 2,
                    kind,
                    position: Point {
                        x: origin.x + a.sin(),
                        y: origin.y + a.cos(),
                    },
                }
            })
            .collect();
        if let Some(position) = self.contact() {
            contacts.push(ContactPosition {
                id: 1,
                kind: ContactKind::Hostile,
                position,
            });
        }
        contacts
    }

    pub fn phase(&self, converged: bool) -> &'static str {
        if !self.scripted {
            return "Manual exploration";
        }
        match self.ticks {
            0..60 => "1 / Explore — distance-based peer links",
            60..120 => "2 / Drifting storm — command center disconnected",
            120..180 => "3 / G1 modem offline — local discoveries continue",
            180..240 => "3 / G1 reconnects — G7 halted with state retained",
            240..300 => "4 / All peers restored — observations frozen for repair",
            _ if converged => "5 / Converged — every dated entry agrees",
            _ => "4 / Waiting for actual convergence",
        }
    }
}
