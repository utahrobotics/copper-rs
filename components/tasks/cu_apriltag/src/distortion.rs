use serde::{Deserialize, Serialize};

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Deserialize, Serialize)]
pub enum Rs2Distortion {
    None                 = 0,
    ModifiedBrownConrady = 1,
    InverseBrownConrady  = 2,
    Ftheta               = 3,
    BrownConrady         = 4,
    /// t265 distortion model
    KannalaBrandt4       = 5,
    Count                = 6,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct Rs2Intrinsics {
    /// Width of the image in pixels
    pub width: i32,
    /// Height of the image in pixels
    pub height: i32,
    /// Horizontal coordinate of the principal point, as a pixel offset from the left edge
    pub ppx: f32,
    /// Vertical coordinate of the principal point, as a pixel offset from the top edge
    pub ppy: f32,
    /// Focal length of the image plane, as a multiple of pixel width
    pub fx: f32,
    /// Focal length of the image plane, as a multiple of pixel height
    pub fy: f32,
    /// Distortion model of the image
    pub model: Rs2Distortion,
    /// Distortion coefficients.
    /// Brown-Conrady order: [k1, k2, p1, p2, k3]
    /// F-Theta fish-eye order: [k1, k2, k3, k4, 0]
    pub coeffs: [f32; 5],
}


pub fn intrinsics_from_file(path: &str) -> Result<Rs2Intrinsics, Box<dyn std::error::Error>> {
    let ron_string = std::fs::read_to_string(path)?;
    Ok(ron::from_str(&ron_string)?)
}