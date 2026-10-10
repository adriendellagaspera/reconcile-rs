use serde::{Deserialize, Serialize};

pub const WIDTH: usize = 32;
pub const HEIGHT: usize = 20;
pub const STEP_SECONDS: f64 = 0.5;
pub const KM_PER_UNIT: f64 = 1.0;
pub const LISTENING_RANGE_KM: f64 = 1.5;
pub const CORRIDOR: [Point; 2] = [Point { x: 5.0, y: 3.0 }, Point { x: 29.0, y: 3.0 }];
pub const COMMAND_POSITION: Point = Point { x: 1.0, y: 10.0 };
pub const COASTAL_POSITION: Point = Point { x: 3.8, y: 10.0 };

#[derive(Serialize)]
pub struct ReferenceMap {
    pub version: &'static str,
    pub width: usize,
    pub height: usize,
    pub km_per_unit: f64,
    pub detail: Vec<[u8; 8]>,
}

impl ReferenceMap {
    pub fn new() -> Self {
        Self {
            version: "synthetic-coast-v1",
            width: WIDTH,
            height: HEIGHT,
            km_per_unit: KM_PER_UNIT,
            detail: (0..HEIGHT)
                .flat_map(|y| (0..WIDTH).map(move |x| terrain_detail(x, y)))
                .collect(),
        }
    }
}

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
    Mechanical,
    Biological,
}

#[derive(Clone, Copy, Serialize)]
pub struct ContactPosition {
    pub id: usize,
    pub kind: ContactKind,
    pub position: Point,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct AcousticBearing {
    pub origin: Point,
    pub direction_deg: f64,
    pub half_angle_deg: f64,
    pub range_km: f64,
}

impl AcousticBearing {
    // The midpoint is a drawing anchor, never a measured or fused target position.
    pub fn anchor(self) -> Point {
        let a = self.direction_deg.to_radians();
        Point {
            x: self.origin.x + a.sin() * self.range_km * 0.5,
            y: self.origin.y - a.cos() * self.range_km * 0.5,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Observation {
    Contact {
        id: usize,
        kind: ContactKind,
        position: Point,
        bearing: Option<AcousticBearing>,
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
    pub speed: u32,
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
            speed: 1,
            positions: vec![Point { x: 0.0, y: 0.0 }; nodes],
            contact_origin: None,
            contacts: Vec::new(),
            held: vec![false; nodes],
        };
        world.contacts = vec![
            (ContactKind::Civil, Point { x: 6.2, y: 3.1 }, 0.008),
            (ContactKind::Hostile, Point { x: 17.0, y: 3.4 }, 0.006),
            (ContactKind::Whale, Point { x: 12.0, y: 2.6 }, 0.003),
            (ContactKind::SpermWhale, Point { x: 23.0, y: 3.6 }, 0.002),
            (ContactKind::OceanFront, Point { x: 10.0, y: 16.0 }, 0.0),
        ];
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
        let count = self.positions.len();
        let seconds = self.seconds();
        for (id, position) in self.positions.iter_mut().enumerate() {
            if peers[id] == PeerState::Stopped || self.held[id] {
                continue;
            }
            let phase = id as f64 * 1.7 + seconds * 0.0016;
            let mut next = Point {
                x: 5.5 + 23.0 * id as f64 / (count - 1) as f64 + 0.3 * phase.cos(),
                y: 3.0 + 0.3 * phase.sin(),
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
        self.contact_origin = [(0.4, 0.0), (0.0, 0.4), (0.0, -0.4), (-0.4, 0.0)]
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
            let ticks = if self.scripted {
                self.ticks.min(239)
            } else {
                self.ticks
            };
            let elapsed = ticks.saturating_sub(contact.born_tick) as f64 * STEP_SECONDS;
            Point {
                x: route_x(origin.x, elapsed * 0.008),
                y: origin.y,
            }
        })
    }

    pub fn contacts(&self) -> Vec<ContactPosition> {
        let mut contacts: Vec<_> = self
            .contacts
            .iter()
            .enumerate()
            .map(|(i, &(kind, origin, speed))| {
                let elapsed = if self.scripted {
                    self.ticks.min(239) as f64 * STEP_SECONDS
                } else {
                    self.seconds()
                };
                ContactPosition {
                    id: i + 2,
                    kind,
                    position: Point {
                        x: route_x(origin.x, elapsed * speed),
                        y: origin.y,
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
            return "Coastal watch / passive acoustic bearings";
        }
        match self.ticks {
            0..60 => "1 / Watch corridor — collect acoustic bearings",
            60..120 => "2 / Shore uplink lost — fleet retains reports",
            120..180 => "3 / G1 modem offline — new contact remains local",
            180..240 => "3 / G1 sensor halted — G2 continues contact watch",
            240..300 => "4 / All peers restored — observations frozen for repair",
            _ if converged => "5 / Converged — every dated entry agrees",
            _ => "4 / Waiting for actual convergence",
        }
    }
}

fn route_x(origin: f64, distance: f64) -> f64 {
    let phase = (origin - 5.0 + distance).rem_euclid(48.0);
    5.0 + if phase <= 24.0 { phase } else { 48.0 - phase }
}
