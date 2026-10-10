#![forbid(unsafe_code)]
#![cfg(all(target_arch = "wasm32", target_os = "unknown"))]

#[path = "../bandwidth.rs"]
mod bandwidth;
#[path = "../cluster.rs"]
mod cluster;
#[path = "../controls.rs"]
mod controls;
#[path = "../telemetry.rs"]
mod telemetry;
#[path = "../transport.rs"]
mod transport;
#[path = "../world.rs"]
mod world;

mod probe;

use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Fleet {
    cluster: cluster::Cluster,
}

#[wasm_bindgen]
impl Fleet {
    #[wasm_bindgen(constructor)]
    pub fn new(
        loss: f64,
        drones: usize,
        mtu: usize,
        bandwidth_kbps: usize,
    ) -> Result<Fleet, JsValue> {
        console_error_panic_hook::set_once();
        if !loss.is_finite() || !(0.0..=100.0).contains(&loss) {
            return Err(JsValue::from_str("loss must be between 0 and 100 percent"));
        }
        telemetry::install().map_err(js_error)?;
        let mut cluster =
            cluster::Cluster::with_limits(loss, drones, mtu, bandwidth_kbps).map_err(js_error)?;
        cluster.world.playing = true;
        Ok(Fleet { cluster })
    }

    pub fn step(&mut self) {
        self.cluster.advance();
    }

    pub fn state(&self) -> String {
        self.cluster.state().to_string()
    }

    pub fn command(&mut self, action: &str) -> Result<String, JsValue> {
        controls::apply(&mut self.cluster, action).map_err(js_error)?;
        Ok(self.state())
    }
}

fn js_error(error: std::io::Error) -> JsValue {
    JsValue::from_str(&error.to_string())
}
