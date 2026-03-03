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

/// Returns true if all distortion coefficients are zero.
fn is_distortion_zero(intr: &Rs2Intrinsics) -> bool {
    intr.coeffs.iter().all(|&c| c == 0.0)
}

pub fn intrinsics_from_file(path: &str) -> Result<Rs2Intrinsics, Box<dyn std::error::Error>> {
    let ron_string = std::fs::read_to_string(path)?;
    Ok(ron::from_str(&ron_string)?)
}

/// Deprojects a pixel coordinate to normalized camera coordinates (focal length = 1,
/// principal point at origin), removing lens distortion.
/// Equivalent to `rs2_deproject_pixel_to_point` with depth=1.0.
pub fn deproject_pixel_to_point(intr: &Rs2Intrinsics, pixel: [f64; 2]) -> [f64; 2] {
    debug_assert!(
        intr.model != Rs2Distortion::ModifiedBrownConrady,
        "Cannot deproject from a forward-distorted image (ModifiedBrownConrady)"
    );

    let mut x = (pixel[0] as f32 - intr.ppx) / intr.fx;
    let mut y = (pixel[1] as f32 - intr.ppy) / intr.fy;
    let xo = x;
    let yo = y;

    if !is_distortion_zero(intr) {
        if intr.model == Rs2Distortion::InverseBrownConrady {
            for _ in 0..10 {
                let r2 = x * x + y * y;
                let icdist = 1.0
                    / (1.0
                        + ((intr.coeffs[4] * r2 + intr.coeffs[1]) * r2 + intr.coeffs[0]) * r2);
                let xq = x / icdist;
                let yq = y / icdist;
                let delta_x =
                    2.0 * intr.coeffs[2] * xq * yq + intr.coeffs[3] * (r2 + 2.0 * xq * xq);
                let delta_y =
                    2.0 * intr.coeffs[3] * xq * yq + intr.coeffs[2] * (r2 + 2.0 * yq * yq);
                x = (xo - delta_x) * icdist;
                y = (yo - delta_y) * icdist;
            }
        }
        if intr.model == Rs2Distortion::BrownConrady {
            for _ in 0..10 {
                let r2 = x * x + y * y;
                let icdist = 1.0
                    / (1.0
                        + ((intr.coeffs[4] * r2 + intr.coeffs[1]) * r2 + intr.coeffs[0]) * r2);
                let delta_x =
                    2.0 * intr.coeffs[2] * x * y + intr.coeffs[3] * (r2 + 2.0 * x * x);
                let delta_y =
                    2.0 * intr.coeffs[3] * x * y + intr.coeffs[2] * (r2 + 2.0 * y * y);
                x = (xo - delta_x) * icdist;
                y = (yo - delta_y) * icdist;
            }
        }
    }

    if intr.model == Rs2Distortion::KannalaBrandt4 {
        let mut rd = (x * x + y * y).sqrt();
        if rd < f32::EPSILON {
            rd = f32::EPSILON;
        }
        let mut theta = rd;
        let mut theta2 = rd * rd;
        for _ in 0..4 {
            let f = theta
                * (1.0
                    + theta2
                        * (intr.coeffs[0]
                            + theta2
                                * (intr.coeffs[1]
                                    + theta2
                                        * (intr.coeffs[2] + theta2 * intr.coeffs[3]))))
                - rd;
            if f.abs() < f32::EPSILON {
                break;
            }
            let df = 1.0
                + theta2
                    * (3.0 * intr.coeffs[0]
                        + theta2
                            * (5.0 * intr.coeffs[1]
                                + theta2
                                    * (7.0 * intr.coeffs[2]
                                        + 9.0 * theta2 * intr.coeffs[3])));
            theta -= f / df;
            theta2 = theta * theta;
        }
        let r = theta.tan();
        x *= r / rd;
        y *= r / rd;
    }

    if intr.model == Rs2Distortion::Ftheta {
        let mut rd = (x * x + y * y).sqrt();
        if rd < f32::EPSILON {
            rd = f32::EPSILON;
        }
        let r = if intr.coeffs[0].abs() < f32::EPSILON {
            0.0
        } else {
            (intr.coeffs[0] * rd).tan() / (2.0 * (intr.coeffs[0] / 2.0).tan()).atan()
        };
        x *= r / rd;
        y *= r / rd;
    }

    [x as f64, y as f64]
}

/// Computes a 3x3 homography from 4 point correspondences using DLT
/// with Gaussian elimination. `corr[i] = [ideal_x, ideal_y, observed_x, observed_y]`.
/// Result is written row-major into `h_data` (must have length >= 9).
fn homography_compute2(corr: &[[f64; 4]; 4], h_data: &mut [f64]) {
    debug_assert!(h_data.len() >= 9);

    let mut a = [0.0f64; 72];
    for (i, c) in corr.iter().enumerate() {
        let r0 = i * 2;
        let r1 = r0 + 1;
        a[r0 * 9] = c[0];
        a[r0 * 9 + 1] = c[1];
        a[r0 * 9 + 2] = 1.0;
        a[r0 * 9 + 6] = -c[0] * c[2];
        a[r0 * 9 + 7] = -c[1] * c[2];
        a[r0 * 9 + 8] = c[2];
        a[r1 * 9 + 3] = c[0];
        a[r1 * 9 + 4] = c[1];
        a[r1 * 9 + 5] = 1.0;
        a[r1 * 9 + 6] = -c[0] * c[3];
        a[r1 * 9 + 7] = -c[1] * c[3];
        a[r1 * 9 + 8] = c[3];
    }

    for col in 0..8 {
        let mut max_val = 0.0f64;
        let mut max_idx = col;
        for row in col..8 {
            let val = a[row * 9 + col].abs();
            if val > max_val {
                max_val = val;
                max_idx = row;
            }
        }
        if max_val < 1e-10 {
            eprintln!("WRN: Matrix is singular.");
        }
        if max_idx != col {
            for i in col..9 {
                a.swap(col * 9 + i, max_idx * 9 + i);
            }
        }
        for i in (col + 1)..8 {
            let f = a[i * 9 + col] / a[col * 9 + col];
            a[i * 9 + col] = 0.0;
            for j in (col + 1)..9 {
                a[i * 9 + j] -= f * a[col * 9 + j];
            }
        }
    }

    for col in (0..8).rev() {
        let mut sum = 0.0;
        for i in (col + 1)..8 {
            sum += a[col * 9 + i] * a[i * 9 + 8];
        }
        a[col * 9 + 8] = (a[col * 9 + 8] - sum) / a[col * 9 + col];
    }

    for i in 0..8 {
        h_data[i] = a[i * 9 + 8];
    }
    h_data[8] = 1.0;
}

/// Safety
/// `det` must be a valid, non-null pointer to an `apriltag_detection_t`.
pub unsafe fn undistort_detection(
    det: *mut apriltag_sys::apriltag_detection_t,
    intr: &Rs2Intrinsics,
) {
    let det = unsafe { &mut *det };

    det.c = deproject_pixel_to_point(intr, det.c);

    let mut corr = [[0.0f64; 4]; 4];
    for c in 0..4 {
        det.p[c] = deproject_pixel_to_point(intr, det.p[c]);
        corr[c][0] = if c == 0 || c == 3 { -1.0 } else { 1.0 };
        corr[c][1] = if c == 0 || c == 1 { -1.0 } else { 1.0 };
        corr[c][2] = det.p[c][0];
        corr[c][3] = det.p[c][1];
    }

    if det.H.is_null() {
        det.H = unsafe { apriltag_sys::matd_create(3, 3) };
    }
    let h_data = unsafe { std::slice::from_raw_parts_mut((*det.H).data, 9) };
    homography_compute2(&corr, h_data);
}