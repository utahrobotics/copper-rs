#[cfg(unix)]
use std::mem::ManuallyDrop;

#[cfg(unix)]
use apriltag::{Detector, DetectorBuilder, Family, Image, TagParams};
#[cfg(unix)]
use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError};
#[cfg(unix)]
use std::thread;
#[cfg(unix)]
use std::time::Instant;
#[cfg(unix)]
use libc;

#[cfg(unix)]
use apriltag_sys::image_u8_t;

use bincode::de::Decoder;
use bincode::error::DecodeError;
use cu29::bincode::{Decode, Encode};
use cu29::prelude::*;
use cu_sensor_payloads::CuImage;
#[cfg(unix)]
use cu_sensor_payloads::CuImageBufferFormat;
use cu_spatial_payloads::Pose as CuPose;
use serde::ser::SerializeTuple;
use serde::{Deserialize, Deserializer, Serialize};

// the maximum number of detections that can be returned by the detector
const MAX_DETECTIONS: usize = 16;

// Defaults
#[cfg(not(windows))]
const TAG_SIZE: f64 = 0.14;
#[cfg(not(windows))]
const FX: f64 = 2600.0;
#[cfg(not(windows))]
const FY: f64 = 2600.0;
#[cfg(not(windows))]
const CX: f64 = 900.0;
#[cfg(not(windows))]
const CY: f64 = 520.0;
#[cfg(not(windows))]
const FAMILY: &str = "tag16h5";

#[derive(Default, Debug, Clone, Encode)]
pub struct AprilTagDetections {
    pub camera_id: Box<String>,
    pub ids: CuArrayVec<usize, MAX_DETECTIONS>,
    pub poses: CuArrayVec<CuPose<f32>, MAX_DETECTIONS>,
    pub decision_margins: CuArrayVec<f32, MAX_DETECTIONS>,
}

impl Decode<()> for AprilTagDetections {
    fn decode<D: Decoder<Context = ()>>(decoder: &mut D) -> Result<Self, DecodeError> {
        let ids = CuArrayVec::<usize, MAX_DETECTIONS>::decode(decoder)?;
        let poses = CuArrayVec::<CuPose<f32>, MAX_DETECTIONS>::decode(decoder)?;
        let decision_margins = CuArrayVec::<f32, MAX_DETECTIONS>::decode(decoder)?;
        let camera_id: Box<String> = Box::decode(decoder)?;
        Ok(AprilTagDetections {
            camera_id,
            ids,
            poses,
            decision_margins,
        })
    }
}

// implement serde support for AprilTagDetections
// This is so it can be logged with debug!.
impl Serialize for AprilTagDetections {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let CuArrayVec(ids) = &self.ids;
        let CuArrayVec(poses) = &self.poses;
        let CuArrayVec(decision_margins) = &self.decision_margins;
        let camera_id= &self.camera_id;

        let mut tup = serializer.serialize_tuple(ids.len() + 1)?;

        ids.iter()
            .zip(poses.iter())
            .zip(decision_margins.iter())
            .map(|((id, pose), margin)| (id, pose, margin))
            .for_each(|(id, pose, margin)| {
                tup.serialize_element(&(id, pose, margin)).unwrap();
            });
        tup.serialize_element(camera_id)?;
        tup.end()
    }
}

impl<'de> Deserialize<'de> for AprilTagDetections {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct AprilTagDetectionsVisitor;

        impl<'de> serde::de::Visitor<'de> for AprilTagDetectionsVisitor {
            type Value = AprilTagDetections;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a tuple of (id, pose, decision_margin) and camera_id")     
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut detections = AprilTagDetections::new();
                while let Some((id, pose, decision_margin)) = seq.next_element()? {
                    let CuArrayVec(ids) = &mut detections.ids;
                    ids.push(id);
                    let CuArrayVec(poses) = &mut detections.poses;
                    poses.push(pose);
                    let CuArrayVec(decision_margins) = &mut detections.decision_margins;
                    decision_margins.push(decision_margin);
                }
                let camera_id = seq.next_element()?.ok_or_else(|| serde::de::Error::invalid_length(MAX_DETECTIONS, &self))?;

                detections.camera_id = camera_id;
                Ok(detections)
            }
        }

        deserializer.deserialize_tuple(MAX_DETECTIONS + 1, AprilTagDetectionsVisitor)
    }
}

impl AprilTagDetections {
    fn new() -> Self {
        Self::default()
    }
    pub fn filtered_by_decision_margin(
        &self,
        threshold: f32,
    ) -> impl Iterator<Item = (usize, &CuPose<f32>, f32)> {
        let CuArrayVec(ids) = &self.ids;
        let CuArrayVec(poses) = &self.poses;
        let CuArrayVec(decision_margins) = &self.decision_margins;

        ids.iter()
            .zip(poses.iter())
            .zip(decision_margins.iter())
            .filter_map(move |((id, pose), margin)| {
                (*margin > threshold).then_some((*id, pose, *margin))
            })
    }
}

#[cfg(unix)]
#[derive(Clone)]
struct ImageJob {
    format: CuImageBufferFormat,
    buffer: CuHandle<Vec<u8>>,
}

#[cfg(unix)]
pub struct AprilTags {
    img_tx: Sender<ImageJob>,
    det_rx: Receiver<AprilTagDetections>,
    camera_id: Box<String>,
    process_counter: u32,
    #[allow(dead_code)]
    worker: thread::JoinHandle<()>,
}

#[cfg(not(unix))]
pub struct AprilTags {}

#[cfg(not(windows))]
fn image_from_cuimage<A>(cu_image: &CuImage<A>) -> ManuallyDrop<Image>
where
    A: ArrayLike<Element = u8>,
{
    unsafe {
        // Try to emulate what the C code is doing on the heap to avoid double free
        let buffer_ptr = cu_image.buffer_handle.with_inner(|inner| inner.as_ptr());
        let low_level_img = Box::new(image_u8_t {
            buf: buffer_ptr as *mut u8,
            width: cu_image.format.width as i32,
            height: cu_image.format.height as i32,
            stride: cu_image.format.stride as i32,
        });
        let ptr = Box::into_raw(low_level_img);
        ManuallyDrop::new(Image::from_raw(ptr))
    }
}

impl Freezable for AprilTags {}

#[cfg(windows)]
impl<'cl> CuTask<'cl> for AprilTags {
    type Input = input_msg!('cl, CuImage<Vec<u8>>);
    type Output = output_msg!('cl, AprilTagDetections);

    fn new(_config: Option<&ComponentConfig>) -> CuResult<Self>
    where
        Self: Sized,
    {
        Ok(Self {})
    }

    fn process(
        &mut self,
        _clock: &RobotClock,
        _input: Self::Input,
        _output: Self::Output,
    ) -> CuResult<()> {
        Ok(())
    }
}

#[cfg(not(windows))]
impl<'cl> CuTask<'cl> for AprilTags {
    type Input = input_msg!('cl, CuImage<Vec<u8>>);
    type Output = output_msg!('cl, AprilTagDetections);

    fn new(_config: Option<&ComponentConfig>) -> CuResult<Self>
    where
        Self: Sized,
    {
        if let Some(config) = _config {
            let family_cfg: String = config.get("family").unwrap_or(FAMILY.to_string());
            let bits_corrected: u32 = config.get("bits_corrected").unwrap_or(1);
            let tagsize = config.get("tag_size").unwrap_or(TAG_SIZE);
            let fx = config.get("fx").unwrap_or(FX);
            let fy = config.get("fy").unwrap_or(FY);
            let cx = config.get("cx").unwrap_or(CX);
            let cy = config.get("cy").unwrap_or(CY);
            let tag_params = TagParams {
                fx,
                fy,
                cx,
                cy,
                tagsize,
            };

            let camera_id = if let Some(id) = config.get::<String>("camera_id") {
                Box::new(id)
            } else {
                Box::new(String::new())
            };

            // Create lock-free channels
            let (img_tx, img_rx) = bounded::<ImageJob>(2);
            let (det_tx, det_rx) = bounded::<AprilTagDetections>(2);

            // Spawn worker thread running the detection loop
            let worker_family_cfg = family_cfg.clone();
            let worker_camera_id = camera_id.clone();
            let worker = thread::spawn(move || {
                // lower priority of this worker thread (nice +5)
                #[allow(unused_unsafe)]
                unsafe {
                    libc::setpriority(libc::PRIO_PROCESS, 0, 5);
                }
                // Build detector inside the thread without capturing non-Send types
                let family: Family = worker_family_cfg.parse().unwrap();
                let mut detector = DetectorBuilder::default()
                    .add_family_bits(family, bits_corrected as usize)
                    .build()
                    .unwrap();

                // Performance settings
                detector.set_decimation(2.0);
                detector.set_refine_edges(false);
                detector.set_thread_number(1);

                for job in img_rx {
                    let job_start = Instant::now();
                    let mut result = AprilTagDetections::new();
                    // Recreate Image from buffer
                    let cu_tmp = CuImage {
                        seq: 0,
                        format: job.format,
                        buffer_handle: job.buffer.clone(),
                    };
                    let image = image_from_cuimage(&cu_tmp);
                    let start = Instant::now();
                    let detections = detector.detect(&image);
                    let elapsed_ms = start.elapsed().as_millis();
                    if elapsed_ms > 6 {
                        error!("APRILTAG detect() slow: {} ms", elapsed_ms);
                    }
                    for detection in detections {
                        if let Some(aprilpose) = detection.estimate_tag_pose(&tag_params) {
                            let translation = aprilpose.translation();
                            let rotation = aprilpose.rotation();
                            let mut mat: [[f32; 4]; 4] = [[0.0, 0.0, 0.0, 0.0]; 4];
                            mat[0][3] = translation.data()[0] as f32;
                            mat[1][3] = translation.data()[1] as f32;
                            mat[2][3] = translation.data()[2] as f32;
                            mat[0][0] = rotation.data()[0] as f32;
                            mat[0][1] = rotation.data()[3] as f32;
                            mat[0][2] = rotation.data()[2 * 3] as f32;
                            mat[1][0] = rotation.data()[1] as f32;
                            mat[1][1] = rotation.data()[1 + 3] as f32;
                            mat[1][2] = rotation.data()[1 + 2 * 3] as f32;
                            mat[2][0] = rotation.data()[2] as f32;
                            mat[2][1] = rotation.data()[2 + 3] as f32;
                            mat[2][2] = rotation.data()[2 + 2 * 3] as f32;

                            let pose = CuPose::<f32>::from_matrix(mat);
                            let CuArrayVec(poses) = &mut result.poses;
                            poses.push(pose);
                            let CuArrayVec(decision_margins) = &mut result.decision_margins;
                            decision_margins.push(detection.decision_margin());
                            let CuArrayVec(ids) = &mut result.ids;
                            ids.push(detection.id());
                        }
                    }
                    result.camera_id = worker_camera_id.clone();

                    // Ignore send errors (main thread may have gone away)
                    let _ = det_tx.try_send(result);
                    let total_ms = job_start.elapsed().as_millis();
                    if total_ms > 5 {
                        error!("APRILTAG worker total job time {} ms", total_ms);
                    }
                }
            });

            return Ok(Self {
                img_tx,
                det_rx,
                camera_id,
                process_counter: 0,
                worker,
            });
        }
        // Default configuration path
        let (img_tx, img_rx) = bounded::<ImageJob>(2);
        let (det_tx, det_rx) = bounded::<AprilTagDetections>(2);

        let tag_params = TagParams {
                fx: FX,
                fy: FY,
                cx: CX,
                cy: CY,
                tagsize: TAG_SIZE,
        };

        let default_family_cfg = FAMILY.to_string();
        let default_camera_id = Box::new(String::new());

        let worker_family_cfg = default_family_cfg.clone();
        let worker_camera_id = default_camera_id.clone();
        let worker = thread::spawn(move || {
            // lower priority
            #[allow(unused_unsafe)]
            unsafe {
                libc::setpriority(libc::PRIO_PROCESS, 0, 5);
            }
            let family: Family = worker_family_cfg.parse().unwrap();
            let mut detector = DetectorBuilder::default()
                .add_family_bits(family, 1)
                .build()
                .unwrap();

            detector.set_decimation(2.0);
            detector.set_refine_edges(false);
            detector.set_thread_number(1);

            for job in img_rx {
        let job_start = Instant::now();
        let mut result = AprilTagDetections::new();
                let cu_tmp = CuImage {
                    seq: 0,
                    format: job.format,
                    buffer_handle: job.buffer.clone(),
                };
                let image = image_from_cuimage(&cu_tmp);
                let start = Instant::now();
                let detections = detector.detect(&image);
                let elapsed_ms = start.elapsed().as_millis();
                if elapsed_ms > 5 {
                    error!("APRILTAG detect() slow: {} ms", elapsed_ms);
                }
            for detection in detections {
                    if let Some(aprilpose) = detection.estimate_tag_pose(&tag_params) {
                    let translation = aprilpose.translation();
                    let rotation = aprilpose.rotation();
                    let mut mat: [[f32; 4]; 4] = [[0.0, 0.0, 0.0, 0.0]; 4];
                    mat[0][3] = translation.data()[0] as f32;
                    mat[1][3] = translation.data()[1] as f32;
                    mat[2][3] = translation.data()[2] as f32;
                    mat[0][0] = rotation.data()[0] as f32;
                    mat[0][1] = rotation.data()[3] as f32;
                    mat[0][2] = rotation.data()[2 * 3] as f32;
                    mat[1][0] = rotation.data()[1] as f32;
                    mat[1][1] = rotation.data()[1 + 3] as f32;
                    mat[1][2] = rotation.data()[1 + 2 * 3] as f32;
                    mat[2][0] = rotation.data()[2] as f32;
                    mat[2][1] = rotation.data()[2 + 3] as f32;
                    mat[2][2] = rotation.data()[2 + 2 * 3] as f32;

                    let pose = CuPose::<f32>::from_matrix(mat);
                        let CuArrayVec(poses) = &mut result.poses;
                        poses.push(pose);
                        let CuArrayVec(decision_margins) = &mut result.decision_margins;
                        decision_margins.push(detection.decision_margin());
                    let CuArrayVec(ids) = &mut result.ids;
                    ids.push(detection.id());
                }
            }
                result.camera_id = worker_camera_id.clone();
                let _ = det_tx.try_send(result);
                let total_ms = job_start.elapsed().as_millis();
                if total_ms > 5 {
                    error!("APRILTAG worker total job time {} ms", total_ms);
                }
            }
        });

        Ok(Self {
            img_tx,
            det_rx,
            camera_id: default_camera_id,
            process_counter: 0,
            worker,
        })
    }

    fn process(
        &mut self,
        clock: &RobotClock,
        input: Self::Input,
        output: Self::Output,
    ) -> CuResult<()> {
        use std::time::Instant;
        let p_start = Instant::now();
        self.process_counter += 1;
        if self.process_counter % 300 == 0 {
            info!("CU_APRILTAG_LATENCY: counter: {}, clock.now(): {} us", self.process_counter, clock.now().as_nanos() /1000);
        }
        if let Some(img) = input.payload() {
            // log backlog size when queue is not empty
            let backlog = self.img_tx.len();
            if backlog > 0 {
                error!("APRILTAG backlog len={}", backlog);
            }
            let job = ImageJob {
                format: img.format,
                buffer: img.buffer_handle.clone(),
            };
            if let Err(err) = self.img_tx.try_send(job) {
                error!("APRILTAG process drop frame: {:?}", err);
            }
        }

        // Try to get detection result.
        match self.det_rx.try_recv() {
            Ok(mut det) => {
                det.camera_id = self.camera_id.clone();
        output.tov = input.tov;
                output.set_payload(det);
            }
            Err(TryRecvError::Empty) => {
                output.clear_payload();
            }
            Err(TryRecvError::Disconnected) => {
                return Err(CuError::from("AprilTag worker thread disconnected"));
            }
        }
        let p_elapsed = p_start.elapsed().as_micros();
        if p_elapsed > 100 {
            error!("APRILTAG process() slow: {} µs", p_elapsed);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;
    use anyhow::Context;
    use anyhow::Result;
    use image::{imageops::crop, imageops::resize, imageops::FilterType, Luma};
    use image::{ImageBuffer, ImageReader};

    #[cfg(not(windows))]
    use cu_sensor_payloads::CuImageBufferFormat;

    #[allow(dead_code)]
    fn process_image(path: &str) -> Result<ImageBuffer<Luma<u8>, Vec<u8>>> {
        let reader = ImageReader::open(path).with_context(|| "Failed to open image")?;
        let mut img = reader
            .decode()
            .context("Failed to decode image")?
            .into_luma8();
        let (orig_w, orig_h) = img.dimensions();

        let new_h = (orig_w as f32 * 9.0 / 16.0) as u32;
        let crop_y = (orig_h - new_h) / 2; // Center crop

        let cropped = crop(&mut img, 0, crop_y, orig_w, new_h).to_image();
        Ok(resize(&cropped, 1920, 1080, FilterType::Lanczos3))
    }

    #[test]
    #[cfg(not(windows))]
    fn test_end2end_apriltag() -> Result<()> {
        let img = process_image("tests/data/simple.jpg")?;
        let format = CuImageBufferFormat {
            width: img.width(),
            height: img.height(),
            stride: img.width(),
            pixel_format: "GRAY".as_bytes().try_into()?,
        };
        let buffer_handle = CuHandle::new_detached(img.into_raw());
        let cuimage = CuImage::new(format, buffer_handle);

        let mut config = ComponentConfig::default();
        config.set("tag_size", 0.14);
        config.set("fx", 2600.0);
        config.set("fy", 2600.0);
        config.set("cx", 900.0);
        config.set("cy", 520.0);
        config.set("family", "tag16h5".to_string());

        let mut task = AprilTags::new(Some(&config))?;
        let input = CuStampedData::<CuImage<Vec<u8>>, CuMsgMetadata>::new(Some(cuimage));
        let mut output = CuStampedData::<AprilTagDetections, CuMsgMetadata>::default();

        let clock = RobotClock::new();
        let result = task.process(&clock, &input, &mut output);
        assert!(result.is_ok());

        if let Some(detections) = output.payload() {
            let detections = detections
                .filtered_by_decision_margin(150.0)
                .collect::<Vec<_>>();

            assert_eq!(detections.len(), 1);
            assert_eq!(detections[0].0, 4);
            return Ok(());
        }
        Err(anyhow::anyhow!("No output"))
    }
}