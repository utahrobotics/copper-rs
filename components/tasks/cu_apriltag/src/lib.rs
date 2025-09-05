#[cfg(target_os = "linux")]
use std::cell::RefCell;
#[cfg(target_os = "linux")]
use std::mem::ManuallyDrop;

#[cfg(target_os = "linux")]
use apriltag::{Detector, DetectorBuilder, Family, Image, TagParams};

#[cfg(target_os = "linux")]
use apriltag_sys::image_u8_t;

use bincode::de::Decoder;
use bincode::error::DecodeError;
use cu29::bincode::{Decode, Encode};
use cu29::prelude::*;
use cu_sensor_payloads::CuImage;
use cu_spatial_payloads::EncodableIsometry;
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
    pub ids: CuArrayVec<usize, MAX_DETECTIONS>,
    pub poses: CuArrayVec<EncodableIsometry, MAX_DETECTIONS>,
    pub decision_margins: CuArrayVec<f32, MAX_DETECTIONS>,
    pub camera_id: Box<String>,
}

impl Decode<()> for AprilTagDetections {
    fn decode<D: Decoder<Context = ()>>(decoder: &mut D) -> Result<Self, DecodeError> {
        let ids = CuArrayVec::<usize, MAX_DETECTIONS>::decode(decoder)?;
        let poses = CuArrayVec::<EncodableIsometry, MAX_DETECTIONS>::decode(decoder)?;
        let decision_margins = CuArrayVec::<f32, MAX_DETECTIONS>::decode(decoder)?;
        let camera_id = Box::<String>::decode(decoder)?;
        Ok(AprilTagDetections {
            ids,
            poses,
            decision_margins,
            camera_id,
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
        let camera_id = &self.camera_id;

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
                let camera_id = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(MAX_DETECTIONS, &self))?;

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
    ) -> impl Iterator<Item = (usize, &EncodableIsometry, f32)> {
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

// Thread-local storage for detector configuration
#[cfg(target_os = "linux")]
#[derive(Clone)]
struct DetectorConfig {
    family_str: String, // Store as string to avoid Clone/PartialEq issues
    bits_corrected: usize,
    tag_params: TagParams,
}

#[cfg(target_os = "linux")]
thread_local! {
    static DETECTOR: RefCell<Option<Detector>> = RefCell::new(None);
    static DETECTOR_CONFIG: RefCell<Option<DetectorConfig>> = RefCell::new(None);
}

#[cfg(target_os = "linux")]
pub struct AprilTags {
    tag_params: TagParams,
    camera_id: Box<String>,
    // Store configuration for detector creation
    family_str: String, // Store as string instead of Family enum
    bits_corrected: usize,
}

#[cfg(not(target_os = "linux"))]
pub struct AprilTags {}

#[cfg(target_os = "linux")]
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

#[cfg(target_os = "linux")]
impl AprilTags {
    fn ensure_detector_initialized(&self) -> CuResult<()> {
        DETECTOR_CONFIG.with(|config_cell| {
            let mut config = config_cell.borrow_mut();
            let current_config = DetectorConfig {
                family_str: self.family_str.clone(),
                bits_corrected: self.bits_corrected,
                tag_params: self.tag_params.clone(),
            };

            // Check if we need to reinitialize the detector
            let needs_init = match config.as_ref() {
                None => true,
                Some(existing) => {
                    existing.family_str != current_config.family_str
                        || existing.bits_corrected != current_config.bits_corrected
                }
            };

            if needs_init {
                DETECTOR.with(|detector_cell| {
                    let mut detector = detector_cell.borrow_mut();
                    let family: Family = self
                        .family_str
                        .parse()
                        .map_err(|e| CuError::new_with_cause("Failed to parse family", e))?;
                    *detector = Some(
                        DetectorBuilder::default()
                            .add_family_bits(family, self.bits_corrected)
                            .build()
                            .map_err(|e| CuError::new_with_cause("Failed to build detector", e))?,
                    );
                    *config = Some(current_config);
                    Ok(())
                })
            } else {
                Ok(())
            }
        })
    }

    fn with_detector<F, R>(&self, f: F) -> CuResult<R>
    where
        F: FnOnce(&mut Detector) -> R,
    {
        self.ensure_detector_initialized()?;

        DETECTOR.with(|detector_cell| {
            let mut detector = detector_cell.borrow_mut();
            match detector.as_mut() {
                Some(d) => Ok(f(d)),
                None => Err(CuError::new_with_cause(
                    "Detector not initialized",
                    std::io::Error::new(std::io::ErrorKind::Other, "detector not found"),
                )),
            }
        })
    }
}

impl Freezable for AprilTags {}

#[cfg(not(target_os = "linux"))]
impl CuTask for AprilTags {
    type Input<'m> = input_msg!(CuImage<Vec<u8>>);
    type Output<'m> = output_msg!(AprilTagDetections);

    fn new(_config: Option<&ComponentConfig>) -> CuResult<Self>
    where
        Self: Sized,
    {
        Ok(Self {})
    }

    fn process(
        &mut self,
        _clock: &RobotClock,
        _input: &Self::Input<'_>,
        _output: &mut Self::Output<'_>,
    ) -> CuResult<()> {
        Ok(())
    }
}

#[cfg(target_os = "linux")]
impl CuTask for AprilTags {
    type Input<'m> = input_msg!(CuImage<Vec<u8>>);
    type Output<'m> = output_msg!(AprilTagDetections);

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
            let camera_id = config.get("camera_id").expect("provide camera id");

            return Ok(Self {
                tag_params,
                camera_id: Box::new(camera_id),
                family_str: family_cfg,
                bits_corrected: bits_corrected as usize,
            });
        }
        Ok(Self {
            tag_params: TagParams {
                fx: FX,
                fy: FY,
                cx: CX,
                cy: CY,
                tagsize: TAG_SIZE,
            },
            camera_id: Box::new("cam_front".to_string()),
            family_str: FAMILY.to_string(),
            bits_corrected: 1,
        })
    }

    fn process(
        &mut self,
        _clock: &RobotClock,
        input: &Self::Input<'_>,
        output: &mut Self::Output<'_>,
    ) -> CuResult<()> {
        let mut result = AprilTagDetections::new();
        result.camera_id = self.camera_id.clone();

        if let Some(payload) = input.payload() {
            let image = image_from_cuimage(payload);

            let detections = self.with_detector(|detector| detector.detect(&image))?;

            for detection in detections {
                if let Some(aprilpose) = detection.estimate_tag_pose(&self.tag_params) {
                    use apriltag_nalgebra::PoseExt;

                    let pose_na = aprilpose.to_na();
                    let encodable_pose = EncodableIsometry::from_na(&pose_na);

                    let CuArrayVec(detections) = &mut result.poses;
                    detections.push(encodable_pose);
                    let CuArrayVec(decision_margin) = &mut result.decision_margins;
                    decision_margin.push(detection.decision_margin());
                    let CuArrayVec(ids) = &mut result.ids;
                    ids.push(detection.id());
                }
            }
        }

        output.tov = input.tov;
        output.set_payload(result);
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
        let input = CuMsg::<CuImage<Vec<u8>>>::new(Some(cuimage));
        let mut output = CuMsg::<AprilTagDetections>::default();

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
