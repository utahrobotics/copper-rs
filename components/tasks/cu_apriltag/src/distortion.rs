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
        // The apriltag library computes det->p[i] using a Y-flipped convention
        // relative to the quad's original homography (see apriltag.c line ~1000):
        //   p[0] = H * (-1, +1),  p[1] = H * (+1, +1),
        //   p[2] = H * (+1, -1),  p[3] = H * (-1, -1)
        // We must use the same mapping when recomputing H from undistorted corners,
        // so that the pose estimator (which assumes this convention) gets a correct H.
        corr[c][1] = if c < 2 { 1.0 } else { -1.0 };
        corr[c][2] = det.p[c][0];
        corr[c][3] = det.p[c][1];
    }

    if det.H.is_null() {
        det.H = unsafe { apriltag_sys::matd_create(3, 3) };
    }
    let h_data = unsafe { std::slice::from_raw_parts_mut((*det.H).data, 9) };
    homography_compute2(&corr, h_data);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression test: fisheye undistortion + H recomputation must produce
    /// a pose with positive t_z (tag in front of camera).
    ///
    /// Root cause: the apriltag library computes det->p[i] using a Y-flipped
    /// tag coordinate convention (see apriltag.c ~line 1005):
    ///   p[0] = H*(-1,+1), p[1] = H*(+1,+1), p[2] = H*(+1,-1), p[3] = H*(-1,-1)
    /// When recomputing H from undistorted corners, we must use the same mapping.
    /// Using the quad's original convention (Y not flipped) produces an incorrect
    /// homography that causes the pose estimator to output negative t_z.
    ///
    /// This test uses corner coordinates from an actual failing detection
    /// (tag_id=16 on a T265 fisheye camera, KannalaBrandt4 model).
    #[test]
    fn test_undistort_detection_produces_positive_tz() {
        let intr = Rs2Intrinsics {
            width: 848,
            height: 800,
            ppx: 420.073,
            ppy: 403.880,
            fx: 285.883,
            fy: 286.195,
            model: Rs2Distortion::KannalaBrandt4,
            coeffs: [-0.00567, 0.04139, -0.03887, 0.00694, 0.0],
        };

        let pixel_corners: [[f64; 2]; 4] = [
            [330.8242, 297.6857],
            [373.7998, 291.7668],
            [370.6774, 248.0146],
            [327.4177, 254.8688],
        ];

        unsafe {
            // Build the initial H using the detection convention:
            //   p[0]↔tag(-1,+1), p[1]↔tag(+1,+1), p[2]↔tag(+1,-1), p[3]↔tag(-1,-1)
            let h_mat = apriltag_sys::matd_create(3, 3);
            let mut corr_pixel = [[0.0f64; 4]; 4];
            for c in 0..4 {
                corr_pixel[c][0] = if c == 0 || c == 3 { -1.0 } else { 1.0 };
                corr_pixel[c][1] = if c < 2 { 1.0 } else { -1.0 };
                corr_pixel[c][2] = pixel_corners[c][0];
                corr_pixel[c][3] = pixel_corners[c][1];
            }
            let h_data = std::slice::from_raw_parts_mut((*h_mat).data, 9);
            homography_compute2(&corr_pixel, h_data);

            let mut det = apriltag_sys::apriltag_detection_t {
                family: std::ptr::null_mut(),
                id: 16,
                hamming: 0,
                decision_margin: 108.7,
                H: h_mat,
                c: [350.47, 273.20],
                p: pixel_corners,
            };

            undistort_detection(&mut det as *mut _, &intr);

            println!("Undistorted center = [{:.6}, {:.6}]", det.c[0], det.c[1]);
            for i in 0..4 {
                println!(
                    "Undistorted corner[{}] = [{:.6}, {:.6}]",
                    i, det.p[i][0], det.p[i][1]
                );
            }

            let new_h = std::slice::from_raw_parts((*det.H).data, 9);
            println!(
                "New H = [{:.6}, {:.6}, {:.6}; {:.6}, {:.6}, {:.6}; {:.6}, {:.6}, {:.6}]",
                new_h[0], new_h[1], new_h[2],
                new_h[3], new_h[4], new_h[5],
                new_h[6], new_h[7], new_h[8],
            );

            // Run pose estimation with fx=1, fy=1, cx=0, cy=0 (normalized coords)
            let tagsize = 0.145;
            let info = apriltag_sys::apriltag_detection_info_t {
                det: &mut det as *mut _,
                tagsize,
                fx: 1.0,
                fy: 1.0,
                cx: 0.0,
                cy: 0.0,
            };

            let mut pose1 = std::mem::MaybeUninit::<apriltag_sys::apriltag_pose_t>::uninit();
            let mut err1 = std::mem::MaybeUninit::<f64>::uninit();
            let mut pose2 = std::mem::MaybeUninit::<apriltag_sys::apriltag_pose_t>::uninit();
            let mut err2 = std::mem::MaybeUninit::<f64>::uninit();

            apriltag_sys::estimate_tag_pose_orthogonal_iteration(
                &info as *const _ as *mut _,
                err1.as_mut_ptr(),
                pose1.as_mut_ptr(),
                err2.as_mut_ptr(),
                pose2.as_mut_ptr(),
                50,
            );

            let pose1 = pose1.assume_init();
            let err1 = err1.assume_init();
            let pose2 = pose2.assume_init();
            let err2 = err2.assume_init();

            let best_is_1 = err1 <= err2 || pose2.R.is_null();
            if !pose1.R.is_null() {
                let t1 = std::slice::from_raw_parts((*pose1.t).data, 3);
                println!(
                    "Pose 1: t=[{:.6}, {:.6}, {:.6}] err={:.6} {}",
                    t1[0], t1[1], t1[2], err1,
                    if best_is_1 { "<-- best" } else { "" }
                );
                if best_is_1 {
                    assert!(
                        t1[2] > 0.0,
                        "Best solution t_z must be positive (tag in front of camera), got {:.6}",
                        t1[2]
                    );
                }
                apriltag_sys::matd_destroy(pose1.R);
                apriltag_sys::matd_destroy(pose1.t);
            }

            if !pose2.R.is_null() {
                let t2 = std::slice::from_raw_parts((*pose2.t).data, 3);
                println!(
                    "Pose 2: t=[{:.6}, {:.6}, {:.6}] err={:.6} {}",
                    t2[0], t2[1], t2[2], err2,
                    if !best_is_1 { "<-- best" } else { "" }
                );
                if !best_is_1 {
                    assert!(
                        t2[2] > 0.0,
                        "Best solution t_z must be positive (tag in front of camera), got {:.6}",
                        t2[2]
                    );
                }
                apriltag_sys::matd_destroy(pose2.R);
                apriltag_sys::matd_destroy(pose2.t);
            }

            apriltag_sys::matd_destroy(det.H);
        }
    }

    /// Verify that the WRONG Y convention reproduces the original bug (negative t_z),
    /// and the CORRECT convention produces positive t_z.
    #[test]
    fn test_wrong_y_convention_causes_negative_tz() {
        let undist_corners: [[f64; 2]; 4] = [
            [-0.331993, -0.378354],
            [-0.166493, -0.392824],
            [-0.187458, -0.583990],
            [-0.362695, -0.567848],
        ];

        unsafe {
            // WRONG convention (old bug): Y = (c==0||c==1)?-1:1
            let h_wrong = apriltag_sys::matd_create(3, 3);
            let mut corr_wrong = [[0.0f64; 4]; 4];
            for c in 0..4 {
                corr_wrong[c][0] = if c == 0 || c == 3 { -1.0 } else { 1.0 };
                corr_wrong[c][1] = if c == 0 || c == 1 { -1.0 } else { 1.0 }; // WRONG
                corr_wrong[c][2] = undist_corners[c][0];
                corr_wrong[c][3] = undist_corners[c][1];
            }
            let h_w = std::slice::from_raw_parts_mut((*h_wrong).data, 9);
            homography_compute2(&corr_wrong, h_w);

            // CORRECT convention: Y = (c<2)?1:-1
            let h_correct = apriltag_sys::matd_create(3, 3);
            let mut corr_correct = [[0.0f64; 4]; 4];
            for c in 0..4 {
                corr_correct[c][0] = if c == 0 || c == 3 { -1.0 } else { 1.0 };
                corr_correct[c][1] = if c < 2 { 1.0 } else { -1.0 }; // CORRECT
                corr_correct[c][2] = undist_corners[c][0];
                corr_correct[c][3] = undist_corners[c][1];
            }
            let h_c = std::slice::from_raw_parts_mut((*h_correct).data, 9);
            homography_compute2(&corr_correct, h_c);

            let tagsize = 0.145;

            let mut det_wrong = apriltag_sys::apriltag_detection_t {
                family: std::ptr::null_mut(),
                id: 16,
                hamming: 0,
                decision_margin: 100.0,
                H: h_wrong,
                c: [-0.265, -0.481],
                p: undist_corners,
            };
            let mut det_correct = apriltag_sys::apriltag_detection_t {
                family: std::ptr::null_mut(),
                id: 16,
                hamming: 0,
                decision_margin: 100.0,
                H: h_correct,
                c: [-0.265, -0.481],
                p: undist_corners,
            };

            // Pose from WRONG H
            let info_w = apriltag_sys::apriltag_detection_info_t {
                det: &mut det_wrong as *mut _,
                tagsize,
                fx: 1.0,
                fy: 1.0,
                cx: 0.0,
                cy: 0.0,
            };
            let mut p1_w = std::mem::MaybeUninit::<apriltag_sys::apriltag_pose_t>::uninit();
            let mut e1_w = std::mem::MaybeUninit::<f64>::uninit();
            let mut p2_w = std::mem::MaybeUninit::<apriltag_sys::apriltag_pose_t>::uninit();
            let mut e2_w = std::mem::MaybeUninit::<f64>::uninit();
            apriltag_sys::estimate_tag_pose_orthogonal_iteration(
                &info_w as *const _ as *mut _,
                e1_w.as_mut_ptr(),
                p1_w.as_mut_ptr(),
                e2_w.as_mut_ptr(),
                p2_w.as_mut_ptr(),
                50,
            );
            let p1_w = p1_w.assume_init();
            let e1_w = e1_w.assume_init();
            let p2_w = p2_w.assume_init();

            let t1_w = std::slice::from_raw_parts((*p1_w.t).data, 3);
            println!(
                "WRONG  -> Pose 1: t=[{:.6}, {:.6}, {:.6}] err={:.6}",
                t1_w[0], t1_w[1], t1_w[2], e1_w
            );
            let wrong_tz = t1_w[2];
            apriltag_sys::matd_destroy(p1_w.R);
            apriltag_sys::matd_destroy(p1_w.t);
            if !p2_w.R.is_null() {
                apriltag_sys::matd_destroy(p2_w.R);
                apriltag_sys::matd_destroy(p2_w.t);
            }

            // Pose from CORRECT H
            let info_c = apriltag_sys::apriltag_detection_info_t {
                det: &mut det_correct as *mut _,
                tagsize,
                fx: 1.0,
                fy: 1.0,
                cx: 0.0,
                cy: 0.0,
            };
            let mut p1_c = std::mem::MaybeUninit::<apriltag_sys::apriltag_pose_t>::uninit();
            let mut e1_c = std::mem::MaybeUninit::<f64>::uninit();
            let mut p2_c = std::mem::MaybeUninit::<apriltag_sys::apriltag_pose_t>::uninit();
            let mut e2_c = std::mem::MaybeUninit::<f64>::uninit();
            apriltag_sys::estimate_tag_pose_orthogonal_iteration(
                &info_c as *const _ as *mut _,
                e1_c.as_mut_ptr(),
                p1_c.as_mut_ptr(),
                e2_c.as_mut_ptr(),
                p2_c.as_mut_ptr(),
                50,
            );
            let p1_c = p1_c.assume_init();
            let e1_c = e1_c.assume_init();
            let p2_c = p2_c.assume_init();

            let t1_c = std::slice::from_raw_parts((*p1_c.t).data, 3);
            println!(
                "CORRECT -> Pose 1: t=[{:.6}, {:.6}, {:.6}] err={:.6}",
                t1_c[0], t1_c[1], t1_c[2], e1_c
            );
            let correct_tz = t1_c[2];
            apriltag_sys::matd_destroy(p1_c.R);
            apriltag_sys::matd_destroy(p1_c.t);
            if !p2_c.R.is_null() {
                apriltag_sys::matd_destroy(p2_c.R);
                apriltag_sys::matd_destroy(p2_c.t);
            }

            apriltag_sys::matd_destroy(h_wrong);
            apriltag_sys::matd_destroy(h_correct);

            assert!(
                wrong_tz < 0.0,
                "WRONG Y convention should produce negative t_z (reproducing the bug), got {:.6}",
                wrong_tz
            );
            assert!(
                correct_tz > 0.0,
                "CORRECT Y convention should produce positive t_z, got {:.6}",
                correct_tz
            );
        }
    }
}
