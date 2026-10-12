#![forbid(unsafe_code)]
#![cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[path = "../cluster.rs"]
mod cluster;
#[path = "../controls.rs"]
mod controls;
mod probe;
#[path = "../transport.rs"]
mod transport;
use wasm_bindgen::prelude::*;
#[wasm_bindgen]
pub struct Lab {
    cluster: cluster::Cluster,
}
#[wasm_bindgen]
impl Lab {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Result<Lab, JsValue> {
        console_error_panic_hook::set_once();
        Ok(Lab {
            cluster: cluster::Cluster::new().map_err(js_error)?,
        })
    }
    pub fn state(&self) -> String {
        self.cluster.state().to_string()
    }
    pub fn command(&mut self, action: &str) -> Result<String, JsValue> {
        controls::apply(&mut self.cluster, action).map_err(js_error)?;
        Ok(self.state())
    }
}
impl Default for Lab {
    fn default() -> Self {
        Self::new().expect("valid lab configuration")
    }
}
fn js_error(error: std::io::Error) -> JsValue {
    JsValue::from_str(&error.to_string())
}
