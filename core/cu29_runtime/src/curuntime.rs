//! CuRuntime is the heart of what copper is running on the robot.
//! It is exposed to the user via the `copper_runtime` macro injecting it as a field in their application struct.
//!

use crate::config::{ComponentConfig, CuDirection, DEFAULT_KEYFRAME_INTERVAL, Node};
use crate::config::{CuConfig, CuGraph, NodeId, RuntimeConfig};
use crate::copperlist::{CopperList, CopperListState, CuListZeroedInit, CuListsManager};
use crate::cutask::{BincodeAdapter, Freezable};
use crate::monitoring::{CuMonitor, build_monitor_topology};
use crate::resource::ResourceManager;
use cu29_clock::{ClockProvider, CuTime, RobotClock};
use cu29_traits::CuResult;
use cu29_traits::WriteStream;
use cu29_traits::{CopperListTuple, CuError};

#[cfg(target_os = "none")]
#[allow(unused_imports)]
use cu29_log::{ANONYMOUS, CuLogEntry, CuLogLevel};
#[cfg(target_os = "none")]
#[allow(unused_imports)]
use cu29_log_derive::info;
#[cfg(target_os = "none")]
#[allow(unused_imports)]
use cu29_log_runtime::log;
#[cfg(all(target_os = "none", debug_assertions))]
#[allow(unused_imports)]
use cu29_log_runtime::log_debug_mode;
#[cfg(target_os = "none")]
#[allow(unused_imports)]
use cu29_value::to_value;

use alloc::boxed::Box;
use alloc::collections::{BTreeSet, VecDeque};
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use bincode::enc::EncoderImpl;
use bincode::enc::write::{SizeWriter, SliceWriter};
use bincode::error::EncodeError;
use bincode::{Decode, Encode};
use core::fmt::Result as FmtResult;
use core::fmt::{Debug, Formatter};

#[cfg(feature = "std")]
use cu29_log_runtime::LoggerRuntime;
#[cfg(feature = "std")]
use cu29_unifiedlog::UnifiedLoggerWrite;
#[cfg(feature = "std")]
use alloc::alloc::{alloc_zeroed, handle_alloc_error};
#[cfg(feature = "std")]
use core::alloc::Layout;
#[cfg(feature = "std")]
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, sync_channel};
#[cfg(feature = "std")]
use std::sync::{Arc, Mutex};
#[cfg(feature = "std")]
use std::thread::JoinHandle;

/// Just a simple struct to hold the various bits needed to run a Copper application.
#[cfg(feature = "std")]
pub struct CopperContext {
    pub unified_logger: Arc<Mutex<UnifiedLoggerWrite>>,
    pub logger_runtime: LoggerRuntime,
    pub clock: RobotClock,
}

/// A payload type that can be sent across threads (required for async CL logging).
pub trait AsyncCopperListPayload: Send {}
impl<T: Send> AsyncCopperListPayload for T {}

/// Manages the lifecycle of the copper lists and logging (synchronous fallback for no_std).
pub struct SyncCopperListsManager<P: CopperListTuple + Default, const NBCL: usize> {
    pub inner: CuListsManager<P, NBCL>,
    /// Logger for the copper lists (messages between tasks)
    pub logger: Option<Box<dyn WriteStream<CopperList<P>>>>,
    /// Last encoded size returned by logger.log
    pub last_encoded_bytes: u64,
}

impl<P: CopperListTuple + Default, const NBCL: usize> SyncCopperListsManager<P, NBCL> {
    pub fn new(logger: Option<Box<dyn WriteStream<CopperList<P>>>>) -> CuResult<Self>
    where
        P: CuListZeroedInit,
    {
        Ok(Self {
            inner: CuListsManager::new(),
            logger,
            last_encoded_bytes: 0,
        })
    }

    pub fn next_cl_id(&self) -> u32 {
        self.inner.next_cl_id()
    }

    pub fn last_cl_id(&self) -> u32 {
        self.inner.last_cl_id()
    }

    pub fn create(&mut self) -> CuResult<&mut CopperList<P>> {
        self.inner
            .create()
            .ok_or_else(|| CuError::from("Ran out of space for copper lists"))
    }

    pub fn end_of_processing(&mut self, culistid: u32) -> CuResult<()> {
        let mut is_top = true;
        let mut nb_done = 0;
        for cl in self.inner.iter_mut() {
            if cl.id == culistid && cl.get_state() == CopperListState::Processing {
                cl.change_state(CopperListState::DoneProcessing);
            }
            if is_top && cl.get_state() == CopperListState::DoneProcessing {
                if let Some(logger) = &mut self.logger {
                    cl.change_state(CopperListState::BeingSerialized);
                    logger.log(cl)?;
                    self.last_encoded_bytes = logger.last_log_bytes().unwrap_or(0) as u64;
                }
                cl.change_state(CopperListState::Free);
                nb_done += 1;
            } else {
                is_top = false;
            }
        }
        for _ in 0..nb_done {
            let _ = self.inner.pop();
        }
        Ok(())
    }

    pub fn finish_pending(&mut self) -> CuResult<()> {
        Ok(())
    }

    pub fn available_copper_lists(&mut self) -> CuResult<usize> {
        Ok(NBCL - self.inner.len())
    }
}

#[cfg(feature = "std")]
struct AsyncCopperListCompletion<P: CopperListTuple> {
    culist: Box<CopperList<P>>,
    log_result: CuResult<u64>,
}

#[cfg(feature = "std")]
fn allocate_zeroed_copperlist<P>() -> Box<CopperList<P>>
where
    P: CopperListTuple + CuListZeroedInit,
{
    // SAFETY: We allocate zeroed memory and immediately initialize required fields.
    let mut culist = unsafe {
        let layout = Layout::new::<CopperList<P>>();
        let ptr = alloc_zeroed(layout) as *mut CopperList<P>;
        if ptr.is_null() {
            handle_alloc_error(layout);
        }
        Box::from_raw(ptr)
    };
    culist.msgs.init_zeroed();
    culist
}

/// Manages the lifecycle of the copper lists and logging on the asynchronous path.
/// Serialization is offloaded to a background thread; the hot path only sends a channel message.
#[cfg(feature = "std")]
pub struct AsyncCopperListsManager<P: CopperListTuple + Default, const NBCL: usize> {
    free_pool: Vec<Box<CopperList<P>>>,
    current: Option<Box<CopperList<P>>>,
    pending_count: usize,
    next_cl_id: u32,
    pending_sender: Option<SyncSender<Box<CopperList<P>>>>,
    completion_receiver: Option<Receiver<AsyncCopperListCompletion<P>>>,
    worker_handle: Option<JoinHandle<()>>,
    /// Last encoded size returned by logger.log
    pub last_encoded_bytes: u64,
}

#[cfg(feature = "std")]
impl<P: CopperListTuple + Default, const NBCL: usize> AsyncCopperListsManager<P, NBCL> {
    pub fn new(logger: Option<Box<dyn WriteStream<CopperList<P>>>>) -> CuResult<Self>
    where
        P: CuListZeroedInit + AsyncCopperListPayload + 'static,
    {
        let mut free_pool = Vec::with_capacity(NBCL);
        for _ in 0..NBCL {
            free_pool.push(allocate_zeroed_copperlist::<P>());
        }

        let (pending_sender, completion_receiver, worker_handle) = if let Some(mut logger) = logger
        {
            let (pending_sender, pending_receiver) = sync_channel::<Box<CopperList<P>>>(NBCL);
            let (completion_sender, completion_receiver) =
                sync_channel::<AsyncCopperListCompletion<P>>(NBCL);
            let worker_handle = std::thread::Builder::new()
                .name("cu-async-cl-io".to_string())
                .spawn(move || {
                    while let Ok(mut culist) = pending_receiver.recv() {
                        culist.change_state(CopperListState::BeingSerialized);
                        let log_result = logger
                            .log(&culist)
                            .map(|_| logger.last_log_bytes().unwrap_or(0) as u64);
                        let should_stop = log_result.is_err();
                        if completion_sender
                            .send(AsyncCopperListCompletion { culist, log_result })
                            .is_err()
                        {
                            break;
                        }
                        if should_stop {
                            break;
                        }
                    }
                })
                .map_err(|e| {
                    CuError::from("Failed to spawn async CopperList serializer thread")
                        .add_cause(e.to_string().as_str())
                })?;
            (
                Some(pending_sender),
                Some(completion_receiver),
                Some(worker_handle),
            )
        } else {
            (None, None, None)
        };

        Ok(Self {
            free_pool,
            current: None,
            pending_count: 0,
            next_cl_id: 0,
            pending_sender,
            completion_receiver,
            worker_handle,
            last_encoded_bytes: 0,
        })
    }

    pub fn next_cl_id(&self) -> u32 {
        self.next_cl_id
    }

    pub fn last_cl_id(&self) -> u32 {
        self.next_cl_id.saturating_sub(1)
    }

    pub fn create(&mut self) -> CuResult<&mut CopperList<P>> {
        if self.current.is_some() {
            return Err(CuError::from(
                "Attempted to create a CopperList while another one is still active",
            ));
        }
        self.reclaim_completed()?;
        if self.free_pool.is_empty() {
            while self.free_pool.is_empty() {
                self.wait_for_completion()?;
            }
        }
        let culist = self
            .free_pool
            .pop()
            .ok_or_else(|| CuError::from("Ran out of space for copper lists"))?;
        self.current = Some(culist);
        let current = self
            .current
            .as_mut()
            .expect("current CopperList is missing");
        current.id = self.next_cl_id;
        current.change_state(CopperListState::Initialized);
        self.next_cl_id += 1;
        Ok(current.as_mut())
    }

    pub fn end_of_processing(&mut self, culistid: u32) -> CuResult<()> {
        self.reclaim_completed()?;
        let mut culist = self.current.take().ok_or_else(|| {
            CuError::from("Attempted to finish processing without an active CopperList")
        })?;
        if culist.id != culistid {
            return Err(CuError::from(format!(
                "Attempted to finish CopperList #{culistid} while CopperList #{} is active",
                culist.id
            )));
        }
        culist.change_state(CopperListState::DoneProcessing);
        self.last_encoded_bytes = 0;
        if let Some(pending_sender) = &self.pending_sender {
            culist.change_state(CopperListState::QueuedForSerialization);
            pending_sender.send(culist).map_err(|e| {
                CuError::from("Failed to enqueue CopperList for async serialization")
                    .add_cause(e.to_string().as_str())
            })?;
            self.pending_count += 1;
            self.reclaim_completed()?;
        } else {
            culist.change_state(CopperListState::Free);
            self.free_pool.push(culist);
        }
        Ok(())
    }

    pub fn finish_pending(&mut self) -> CuResult<()> {
        if self.current.is_some() {
            return Err(CuError::from(
                "Cannot flush CopperList I/O while a CopperList is still active",
            ));
        }
        while self.pending_count > 0 {
            self.wait_for_completion()?;
        }
        Ok(())
    }

    pub fn available_copper_lists(&mut self) -> CuResult<usize> {
        self.reclaim_completed()?;
        Ok(self.free_pool.len())
    }

    fn reclaim_completed(&mut self) -> CuResult<()> {
        loop {
            let recv_result = {
                let Some(completion_receiver) = self.completion_receiver.as_ref() else {
                    return Ok(());
                };
                completion_receiver.try_recv()
            };
            let completion = match recv_result {
                Ok(completion) => completion,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    return Err(CuError::from(
                        "Async CopperList serializer thread disconnected unexpectedly",
                    ));
                }
            };
            self.handle_completion(completion)?;
        }
        Ok(())
    }

    fn wait_for_completion(&mut self) -> CuResult<()> {
        let completion = self
            .completion_receiver
            .as_ref()
            .ok_or_else(|| {
                CuError::from("No async CopperList serializer is active to return a free slot")
            })?
            .recv()
            .map_err(|e| {
                CuError::from("Failed to receive completion from async CopperList serializer")
                    .add_cause(e.to_string().as_str())
            })?;
        self.handle_completion(completion)
    }

    fn handle_completion(&mut self, mut completion: AsyncCopperListCompletion<P>) -> CuResult<()> {
        self.pending_count = self.pending_count.saturating_sub(1);
        completion.culist.change_state(CopperListState::Free);
        self.free_pool.push(completion.culist);
        completion.log_result.map(|_| ())
    }

    fn shutdown_worker(&mut self) -> CuResult<()> {
        self.finish_pending()?;
        self.pending_sender.take();
        if let Some(worker_handle) = self.worker_handle.take() {
            worker_handle.join().map_err(|_| {
                CuError::from("Async CopperList serializer thread panicked while joining")
            })?;
        }
        Ok(())
    }
}

#[cfg(feature = "std")]
impl<P: CopperListTuple + Default, const NBCL: usize> Drop for AsyncCopperListsManager<P, NBCL> {
    fn drop(&mut self) {
        let _ = self.shutdown_worker();
    }
}

#[cfg(feature = "std")]
pub type CopperListsManager<P, const NBCL: usize> = AsyncCopperListsManager<P, NBCL>;
#[cfg(not(feature = "std"))]
pub type CopperListsManager<P, const NBCL: usize> = SyncCopperListsManager<P, NBCL>;

/// Manages the frozen tasks state and logging.
pub struct KeyFramesManager {
    /// Where the serialized tasks are stored following the wave of execution of a CL.
    inner: KeyFrame,

    /// Optional override for the timestamp to stamp the next keyframe (used by deterministic replay).
    forced_timestamp: Option<CuTime>,

    /// If set, reuse this keyframe verbatim (e.g., during replay) instead of re-freezing state.
    locked: bool,

    /// Logger for the state of the tasks (frozen tasks)
    logger: Option<Box<dyn WriteStream<KeyFrame>>>,

    /// Capture a keyframe only each...
    keyframe_interval: u32,

    /// Bytes written by the last keyframe log
    pub last_encoded_bytes: u64,
}

impl KeyFramesManager {
    fn is_keyframe(&self, culistid: u32) -> bool {
        self.logger.is_some() && culistid.is_multiple_of(self.keyframe_interval)
    }

    pub fn reset(&mut self, culistid: u32, clock: &RobotClock) {
        if self.is_keyframe(culistid) {
            // If a recorded keyframe was preloaded for this CL, keep it as-is.
            if self.locked && self.inner.culistid == culistid {
                return;
            }
            let ts = self.forced_timestamp.take().unwrap_or_else(|| clock.now());
            self.inner.reset(culistid, ts);
            self.locked = false;
        }
    }

    /// Force the timestamp of the next keyframe to a given value.
    #[cfg(feature = "std")]
    pub fn set_forced_timestamp(&mut self, ts: CuTime) {
        self.forced_timestamp = Some(ts);
    }

    pub fn freeze_task(&mut self, culistid: u32, task: &impl Freezable) -> CuResult<usize> {
        if self.is_keyframe(culistid) {
            if self.locked {
                // We are replaying a recorded keyframe verbatim; don't mutate it.
                return Ok(0);
            }
            if self.inner.culistid != culistid {
                return Err(CuError::from(format!(
                    "Freezing task for culistid {} but current keyframe is {}",
                    culistid, self.inner.culistid
                )));
            }
            self.inner
                .add_frozen_task(task)
                .map_err(|e| CuError::from(format!("Failed to serialize task: {e}")))
        } else {
            Ok(0)
        }
    }

    /// Generic helper to freeze any `Freezable` state (task or bridge) into the current keyframe.
    pub fn freeze_any(&mut self, culistid: u32, item: &impl Freezable) -> CuResult<usize> {
        self.freeze_task(culistid, item)
    }

    pub fn end_of_processing(&mut self, culistid: u32) -> CuResult<()> {
        if self.is_keyframe(culistid) {
            let logger = self.logger.as_mut().unwrap();
            logger.log(&self.inner)?;
            self.last_encoded_bytes = logger.last_log_bytes().unwrap_or(0) as u64;
            // Clear the lock so the next CL can rebuild normally unless re-locked.
            self.locked = false;
            Ok(())
        } else {
            // Not a keyframe for this CL; ensure we don't carry stale sizes forward.
            self.last_encoded_bytes = 0;
            Ok(())
        }
    }

    /// Preload a recorded keyframe so it is logged verbatim on the matching CL.
    #[cfg(feature = "std")]
    pub fn lock_keyframe(&mut self, keyframe: &KeyFrame) {
        self.inner = keyframe.clone();
        self.forced_timestamp = Some(keyframe.timestamp);
        self.locked = true;
    }
}

/// This is the main structure that will be injected as a member of the Application struct.
/// CT is the tuple of all the tasks in order of execution.
/// CL is the type of the copper list, representing the input/output messages for all the tasks.
pub struct CuRuntime<CT, CB, P: CopperListTuple, M: CuMonitor, const NBCL: usize> {
    /// The base clock the runtime will be using to record time.
    pub clock: RobotClock, // TODO: remove public at some point

    /// The tuple of all the tasks in order of execution.
    pub tasks: CT,

    /// Tuple of all instantiated bridges.
    pub bridges: CB,

    /// Resource registry kept alive for tasks borrowing shared handles.
    pub resources: ResourceManager,

    /// The runtime monitoring.
    pub monitor: M,

    /// The logger for the copper lists (messages between tasks)
    pub copperlists_manager: CopperListsManager<P, NBCL>,

    /// The logger for the state of the tasks (frozen tasks)
    pub keyframes_manager: KeyFramesManager,

    /// The runtime configuration controlling the behavior of the run loop
    pub runtime_config: RuntimeConfig,
}

/// To be able to share the clock we make the runtime a clock provider.
impl<
    CT,
    CB,
    P: CopperListTuple + CuListZeroedInit + Default + AsyncCopperListPayload,
    M: CuMonitor,
    const NBCL: usize,
> ClockProvider for CuRuntime<CT, CB, P, M, NBCL>
{
    fn get_clock(&self) -> RobotClock {
        self.clock.clone()
    }
}

/// A KeyFrame is recording a snapshot of the tasks state before a given copperlist.
/// It is a double encapsulation: this one recording the culistid and another even in
/// bincode in the serialized_tasks.
#[derive(Clone, Encode, Decode)]
pub struct KeyFrame {
    // This is the id of the copper list that this keyframe is associated with (recorded before the copperlist).
    pub culistid: u32,
    // This is the timestamp when the keyframe was created, using the robot clock.
    pub timestamp: CuTime,
    // This is the bincode representation of the tuple of all the tasks.
    pub serialized_tasks: Vec<u8>,
}

impl KeyFrame {
    fn new() -> Self {
        KeyFrame {
            culistid: 0,
            timestamp: CuTime::default(),
            serialized_tasks: Vec::new(),
        }
    }

    /// This is to be able to avoid reallocations
    fn reset(&mut self, culistid: u32, timestamp: CuTime) {
        self.culistid = culistid;
        self.timestamp = timestamp;
        self.serialized_tasks.clear();
    }

    /// We need to be able to accumulate tasks to the serialization as they are executed after the step.
    fn add_frozen_task(&mut self, task: &impl Freezable) -> Result<usize, EncodeError> {
        let cfg = bincode::config::standard();
        let mut sizer = EncoderImpl::<_, _>::new(SizeWriter::default(), cfg);
        BincodeAdapter(task).encode(&mut sizer)?;
        let need = sizer.into_writer().bytes_written as usize;

        let start = self.serialized_tasks.len();
        self.serialized_tasks.resize(start + need, 0);
        let mut enc =
            EncoderImpl::<_, _>::new(SliceWriter::new(&mut self.serialized_tasks[start..]), cfg);
        BincodeAdapter(task).encode(&mut enc)?;
        Ok(need)
    }
}

impl<
    CT,
    CB,
    P: CopperListTuple + CuListZeroedInit + Default + AsyncCopperListPayload + 'static,
    M: CuMonitor,
    const NBCL: usize,
> CuRuntime<CT, CB, P, M, NBCL>
{
    // FIXME(gbin): this became REALLY ugly with no-std
    #[allow(clippy::too_many_arguments)]
    #[cfg(feature = "std")]
    pub fn new(
        clock: RobotClock,
        config: &CuConfig,
        mission: Option<&str>,
        resources_instanciator: impl Fn(&CuConfig) -> CuResult<ResourceManager>,
        tasks_instanciator: impl for<'c> Fn(
            Vec<Option<&'c ComponentConfig>>,
            &mut ResourceManager,
        ) -> CuResult<CT>,
        monitor_instanciator: impl Fn(&CuConfig) -> M,
        bridges_instanciator: impl Fn(&CuConfig, &mut ResourceManager) -> CuResult<CB>,
        copperlists_logger: impl WriteStream<CopperList<P>> + 'static,
        keyframes_logger: impl WriteStream<KeyFrame> + 'static,
    ) -> CuResult<Self> {
        let resources = resources_instanciator(config)?;
        Self::new_with_resources(
            clock,
            config,
            mission,
            resources,
            tasks_instanciator,
            monitor_instanciator,
            bridges_instanciator,
            copperlists_logger,
            keyframes_logger,
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[cfg(feature = "std")]
    pub fn new_with_resources(
        clock: RobotClock,
        config: &CuConfig,
        mission: Option<&str>,
        mut resources: ResourceManager,
        tasks_instanciator: impl for<'c> Fn(
            Vec<Option<&'c ComponentConfig>>,
            &mut ResourceManager,
        ) -> CuResult<CT>,
        monitor_instanciator: impl Fn(&CuConfig) -> M,
        bridges_instanciator: impl Fn(&CuConfig, &mut ResourceManager) -> CuResult<CB>,
        copperlists_logger: impl WriteStream<CopperList<P>> + 'static,
        keyframes_logger: impl WriteStream<KeyFrame> + 'static,
    ) -> CuResult<Self> {
        let graph = config.get_graph(mission)?;
        let all_instances_configs: Vec<Option<&ComponentConfig>> = graph
            .get_all_nodes()
            .iter()
            .map(|(_, node)| node.get_instance_config())
            .collect();

        let tasks = tasks_instanciator(all_instances_configs, &mut resources)?;
        let mut monitor = monitor_instanciator(config);
        if let Ok(topology) = build_monitor_topology(config, mission) {
            monitor.set_topology(topology);
        }
        let bridges = bridges_instanciator(config, &mut resources)?;

        let (copperlists_logger, keyframes_logger, keyframe_interval) = match &config.logging {
            Some(logging_config) if logging_config.enable_task_logging => (
                Some(Box::new(copperlists_logger) as Box<dyn WriteStream<CopperList<P>>>),
                Some(Box::new(keyframes_logger) as Box<dyn WriteStream<KeyFrame>>),
                logging_config.keyframe_interval.unwrap(), // it is set to a default at parsing time
            ),
            Some(_) => (None, None, 0), // explicit no enable logging
            None => (
                // default
                Some(Box::new(copperlists_logger) as Box<dyn WriteStream<CopperList<P>>>),
                Some(Box::new(keyframes_logger) as Box<dyn WriteStream<KeyFrame>>),
                DEFAULT_KEYFRAME_INTERVAL,
            ),
        };

        let copperlists_manager = CopperListsManager::new(copperlists_logger)?;
        #[cfg(target_os = "none")]
        {
            let cl_size = core::mem::size_of::<CopperList<P>>();
            let total_bytes = cl_size.saturating_mul(NBCL);
            info!(
                "CuRuntime::new: copperlists count={} cl_size={} total_bytes={}",
                NBCL, cl_size, total_bytes
            );
        }

        let keyframes_manager = KeyFramesManager {
            inner: KeyFrame::new(),
            logger: keyframes_logger,
            keyframe_interval,
            last_encoded_bytes: 0,
            forced_timestamp: None,
            locked: false,
        };

        let runtime_config = config.runtime.clone().unwrap_or_default();

        let runtime = Self {
            tasks,
            bridges,
            resources,
            monitor,
            clock,
            copperlists_manager,
            keyframes_manager,
            runtime_config,
        };

        Ok(runtime)
    }

    #[allow(clippy::too_many_arguments)]
    #[cfg(not(feature = "std"))]
    pub fn new(
        clock: RobotClock,
        config: &CuConfig,
        mission: Option<&str>,
        resources_instanciator: impl Fn(&CuConfig) -> CuResult<ResourceManager>,
        tasks_instanciator: impl for<'c> Fn(
            Vec<Option<&'c ComponentConfig>>,
            &mut ResourceManager,
        ) -> CuResult<CT>,
        monitor_instanciator: impl Fn(&CuConfig) -> M,
        bridges_instanciator: impl Fn(&CuConfig, &mut ResourceManager) -> CuResult<CB>,
        copperlists_logger: impl WriteStream<CopperList<P>> + 'static,
        keyframes_logger: impl WriteStream<KeyFrame> + 'static,
    ) -> CuResult<Self> {
        #[cfg(target_os = "none")]
        info!("CuRuntime::new: resources instanciator");
        let resources = resources_instanciator(config)?;
        Self::new_with_resources(
            clock,
            config,
            mission,
            resources,
            tasks_instanciator,
            monitor_instanciator,
            bridges_instanciator,
            copperlists_logger,
            keyframes_logger,
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[cfg(not(feature = "std"))]
    pub fn new_with_resources(
        clock: RobotClock,
        config: &CuConfig,
        mission: Option<&str>,
        mut resources: ResourceManager,
        tasks_instanciator: impl for<'c> Fn(
            Vec<Option<&'c ComponentConfig>>,
            &mut ResourceManager,
        ) -> CuResult<CT>,
        monitor_instanciator: impl Fn(&CuConfig) -> M,
        bridges_instanciator: impl Fn(&CuConfig, &mut ResourceManager) -> CuResult<CB>,
        copperlists_logger: impl WriteStream<CopperList<P>> + 'static,
        keyframes_logger: impl WriteStream<KeyFrame> + 'static,
    ) -> CuResult<Self> {
        #[cfg(target_os = "none")]
        info!("CuRuntime::new: get graph");
        let graph = config.get_graph(mission)?;
        #[cfg(target_os = "none")]
        info!("CuRuntime::new: graph ok");
        let all_instances_configs: Vec<Option<&ComponentConfig>> = graph
            .get_all_nodes()
            .iter()
            .map(|(_, node)| node.get_instance_config())
            .collect();

        #[cfg(target_os = "none")]
        info!("CuRuntime::new: tasks instanciator");
        let tasks = tasks_instanciator(all_instances_configs, &mut resources)?;

        #[cfg(target_os = "none")]
        info!("CuRuntime::new: monitor instanciator");
        let mut monitor = monitor_instanciator(config);
        #[cfg(target_os = "none")]
        info!("CuRuntime::new: monitor instanciator ok");
        #[cfg(target_os = "none")]
        info!("CuRuntime::new: build monitor topology");
        if let Ok(topology) = build_monitor_topology(config, mission) {
            #[cfg(target_os = "none")]
            info!("CuRuntime::new: monitor topology ok");
            monitor.set_topology(topology);
            #[cfg(target_os = "none")]
            info!("CuRuntime::new: monitor topology set");
        }
        #[cfg(target_os = "none")]
        info!("CuRuntime::new: bridges instanciator");
        let bridges = bridges_instanciator(config, &mut resources)?;

        let (copperlists_logger, keyframes_logger, keyframe_interval) = match &config.logging {
            Some(logging_config) if logging_config.enable_task_logging => (
                Some(Box::new(copperlists_logger) as Box<dyn WriteStream<CopperList<P>>>),
                Some(Box::new(keyframes_logger) as Box<dyn WriteStream<KeyFrame>>),
                logging_config.keyframe_interval.unwrap(), // it is set to a default at parsing time
            ),
            Some(_) => (None, None, 0), // explicit no enable logging
            None => (
                // default
                Some(Box::new(copperlists_logger) as Box<dyn WriteStream<CopperList<P>>>),
                Some(Box::new(keyframes_logger) as Box<dyn WriteStream<KeyFrame>>),
                DEFAULT_KEYFRAME_INTERVAL,
            ),
        };

        let copperlists_manager = CopperListsManager::new(copperlists_logger)?;
        #[cfg(target_os = "none")]
        {
            let cl_size = core::mem::size_of::<CopperList<P>>();
            let total_bytes = cl_size.saturating_mul(NBCL);
            info!(
                "CuRuntime::new: copperlists count={} cl_size={} total_bytes={}",
                NBCL, cl_size, total_bytes
            );
        }

        let keyframes_manager = KeyFramesManager {
            inner: KeyFrame::new(),
            logger: keyframes_logger,
            keyframe_interval,
            last_encoded_bytes: 0,
            forced_timestamp: None,
            locked: false,
        };

        let runtime_config = config.runtime.clone().unwrap_or_default();

        let runtime = Self {
            tasks,
            bridges,
            resources,
            monitor,
            clock,
            copperlists_manager,
            keyframes_manager,
            runtime_config,
        };

        Ok(runtime)
    }
}

/// Copper tasks can be of 3 types:
/// - Source: only producing output messages (usually used for drivers)
/// - Regular: processing input messages and producing output messages, more like compute nodes.
/// - Sink: only consuming input messages (usually used for actuators)
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CuTaskType {
    Source,
    Regular,
    Sink,
}

#[derive(Debug, Clone)]
pub struct CuOutputPack {
    pub culist_index: u32,
    pub msg_types: Vec<String>,
    /// For each output slot, the edge IDs that fan out from it.
    /// Used to resolve `src_port` for a given incoming edge.
    pub slot_edges: Vec<Vec<usize>>,
}

#[derive(Debug, Clone)]
pub struct CuInputMsg {
    pub culist_index: u32,
    pub msg_type: String,
    pub src_port: usize,
    pub edge_id: usize,
}

/// This structure represents a step in the execution plan.
pub struct CuExecutionStep {
    /// NodeId: node id of the task to execute
    pub node_id: NodeId,
    /// Node: node instance
    pub node: Node,
    /// CuTaskType: type of the task
    pub task_type: CuTaskType,

    /// the indices in the copper list of the input messages and their types
    pub input_msg_indices_types: Vec<CuInputMsg>,

    /// the index in the copper list of the output message and its type
    pub output_msg_pack: Option<CuOutputPack>,
}

impl Debug for CuExecutionStep {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.write_str(format!("   CuExecutionStep: Node Id: {}\n", self.node_id).as_str())?;
        f.write_str(format!("                  task_type: {:?}\n", self.node.get_type()).as_str())?;
        f.write_str(format!("                       task: {:?}\n", self.task_type).as_str())?;
        f.write_str(
            format!(
                "              input_msg_types: {:?}\n",
                self.input_msg_indices_types
            )
            .as_str(),
        )?;
        f.write_str(format!("       output_msg_pack: {:?}\n", self.output_msg_pack).as_str())?;
        Ok(())
    }
}

/// This structure represents a loop in the execution plan.
/// It is used to represent a sequence of Execution units (loop or steps) that are executed
/// multiple times.
/// if loop_count is None, the loop is infinite.
pub struct CuExecutionLoop {
    pub steps: Vec<CuExecutionUnit>,
    pub loop_count: Option<u32>,
}

impl Debug for CuExecutionLoop {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.write_str("CuExecutionLoop:\n")?;
        for step in &self.steps {
            match step {
                CuExecutionUnit::Step(step) => {
                    step.fmt(f)?;
                }
                CuExecutionUnit::Loop(l) => {
                    l.fmt(f)?;
                }
            }
        }

        f.write_str(format!("   count: {:?}", self.loop_count).as_str())?;
        Ok(())
    }
}

/// This structure represents a step in the execution plan.
#[derive(Debug)]
pub enum CuExecutionUnit {
    Step(CuExecutionStep),
    Loop(CuExecutionLoop),
}

fn find_output_pack_from_nodeid(
    node_id: NodeId,
    steps: &Vec<CuExecutionUnit>,
) -> Option<CuOutputPack> {
    for step in steps {
        match step {
            CuExecutionUnit::Loop(loop_unit) => {
                if let Some(output_pack) = find_output_pack_from_nodeid(node_id, &loop_unit.steps) {
                    return Some(output_pack);
                }
            }
            CuExecutionUnit::Step(step) => {
                if step.node_id == node_id {
                    return step.output_msg_pack.clone();
                }
            }
        }
    }
    None
}

pub fn find_task_type_for_id(graph: &CuGraph, node_id: NodeId) -> CuTaskType {
    if graph.incoming_neighbor_count(node_id) == 0 {
        CuTaskType::Source
    } else if graph.outgoing_neighbor_count(node_id) == 0 {
        CuTaskType::Sink
    } else {
        CuTaskType::Regular
    }
}

/// The connection id used here is the index of the config graph edge that equates to the wanted
/// connection.
fn sort_inputs_by_cnx_id(input_msg_indices_types: &mut [CuInputMsg]) {
    input_msg_indices_types.sort_by_key(|input| input.edge_id);
}

fn collect_output_msg_types(graph: &CuGraph, node_id: NodeId) -> (Vec<String>, Vec<Vec<usize>>) {
    let mut src_edge_ids = graph.get_src_edges(node_id).unwrap_or_default();
    src_edge_ids.sort();

    let mut msg_types: Vec<String> = Vec::new();
    // For each output slot, the list of edge IDs that map to it.
    let mut slot_edges: Vec<Vec<usize>> = Vec::new();

    for edge_id in src_edge_ids {
        if let Some(edge) = graph.edge(edge_id) {
            // If src_port is explicitly set on the connection, use it to create/extend slots
            if let Some(explicit_port) = edge.src_port {
                // Extend slots to accommodate the explicit port index
                while slot_edges.len() <= explicit_port {
                    msg_types.push(String::new());
                    slot_edges.push(Vec::new());
                }
                if msg_types[explicit_port].is_empty() {
                    msg_types[explicit_port] = edge.msg.clone();
            }
                slot_edges[explicit_port].push(edge_id);
            } else {
                // Default: fan-out, deduplicate by msg type
                if let Some(pos) = msg_types.iter().position(|msg| msg == &edge.msg) {
                    slot_edges[pos].push(edge_id);
                } else {
            msg_types.push(edge.msg.clone());
                    slot_edges.push(vec![edge_id]);
        }
    }
        }
    }
    (msg_types, slot_edges)
}
/// Explores a subbranch and build the partial plan out of it.
fn plan_tasks_tree_branch(
    graph: &CuGraph,
    mut next_culist_output_index: u32,
    starting_point: NodeId,
    plan: &mut Vec<CuExecutionUnit>,
) -> (u32, bool) {
    #[cfg(all(feature = "std", feature = "macro_debug"))]
    eprintln!("-- starting branch from node {starting_point}");

    let mut handled = false;

    for id in graph.bfs_nodes(starting_point) {
        let node_ref = graph.get_node(id).unwrap();
        #[cfg(all(feature = "std", feature = "macro_debug"))]
        eprintln!("  Visiting node: {node_ref:?}");

        let mut input_msg_indices_types: Vec<CuInputMsg> = Vec::new();
        let output_msg_pack: Option<CuOutputPack>;
        let task_type = find_task_type_for_id(graph, id);

        match task_type {
            CuTaskType::Source => {
                #[cfg(all(feature = "std", feature = "macro_debug"))]
                eprintln!("    → Source node, assign output index {next_culist_output_index}");
                let (msg_types, slot_edges) = collect_output_msg_types(graph, id);
                if msg_types.is_empty() {
                    panic!(
                        "Source node '{}' has no outgoing connections",
                        node_ref.get_id()
                    );
                }
                output_msg_pack = Some(CuOutputPack {
                    culist_index: next_culist_output_index,
                    msg_types,
                    slot_edges,
                });
                next_culist_output_index += 1;
            }
            CuTaskType::Sink => {
                let mut edge_ids = graph.get_dst_edges(id).unwrap_or_default();
                edge_ids.sort();
                #[cfg(all(feature = "std", feature = "macro_debug"))]
                eprintln!("    → Sink with incoming edges: {edge_ids:?}");
                for edge_id in edge_ids {
                    let edge = graph
                        .edge(edge_id)
                        .unwrap_or_else(|| panic!("Missing edge {edge_id} for node {id}"));
                    let pid = graph
                        .get_node_id_by_name(edge.src.as_str())
                        .unwrap_or_else(|| {
                            panic!("Missing source node '{}' for edge {edge_id}", edge.src)
                        });
                    let output_pack = find_output_pack_from_nodeid(pid, plan);
                    if let Some(output_pack) = output_pack {
                        #[cfg(all(feature = "std", feature = "macro_debug"))]
                        eprintln!("      ✓ Input from {pid} ready: {output_pack:?}");
                        let msg_type = edge.msg.as_str();
                        let src_port = output_pack
                            .slot_edges
                            .iter()
                            .position(|edges| edges.contains(&edge_id))
                            .unwrap_or_else(|| {
                                panic!(
                                    "Missing output port for edge {edge_id} (msg type '{msg_type}') on node {pid}"
                                )
                            });
                        input_msg_indices_types.push(CuInputMsg {
                            culist_index: output_pack.culist_index,
                            msg_type: msg_type.to_string(),
                            src_port,
                            edge_id,
                        });
                    } else {
                        #[cfg(all(feature = "std", feature = "macro_debug"))]
                        eprintln!("      ✗ Input from {pid} not ready, returning");
                        return (next_culist_output_index, handled);
                    }
                }
                output_msg_pack = Some(CuOutputPack {
                    culist_index: next_culist_output_index,
                    msg_types: Vec::from(["()".to_string()]),
                    slot_edges: Vec::new(),
                });
                next_culist_output_index += 1;
            }
            CuTaskType::Regular => {
                let mut edge_ids = graph.get_dst_edges(id).unwrap_or_default();
                edge_ids.sort();
                #[cfg(all(feature = "std", feature = "macro_debug"))]
                eprintln!("    → Regular task with incoming edges: {edge_ids:?}");
                for edge_id in edge_ids {
                    let edge = graph
                        .edge(edge_id)
                        .unwrap_or_else(|| panic!("Missing edge {edge_id} for node {id}"));
                    let pid = graph
                        .get_node_id_by_name(edge.src.as_str())
                        .unwrap_or_else(|| {
                            panic!("Missing source node '{}' for edge {edge_id}", edge.src)
                        });
                    let output_pack = find_output_pack_from_nodeid(pid, plan);
                    if let Some(output_pack) = output_pack {
                        #[cfg(all(feature = "std", feature = "macro_debug"))]
                        eprintln!("      ✓ Input from {pid} ready: {output_pack:?}");
                        let msg_type = edge.msg.as_str();
                        let src_port = output_pack
                            .slot_edges
                            .iter()
                            .position(|edges| edges.contains(&edge_id))
                            .unwrap_or_else(|| {
                                panic!(
                                    "Missing output port for edge {edge_id} (msg type '{msg_type}') on node {pid}"
                                )
                            });
                        input_msg_indices_types.push(CuInputMsg {
                            culist_index: output_pack.culist_index,
                            msg_type: msg_type.to_string(),
                            src_port,
                            edge_id,
                        });
                    } else {
                        #[cfg(all(feature = "std", feature = "macro_debug"))]
                        eprintln!("      ✗ Input from {pid} not ready, returning");
                        return (next_culist_output_index, handled);
                    }
                }
                let (msg_types, slot_edges) = collect_output_msg_types(graph, id);
                if msg_types.is_empty() {
                    panic!(
                        "Regular node '{}' has no outgoing connections",
                        node_ref.get_id()
                    );
                }
                output_msg_pack = Some(CuOutputPack {
                    culist_index: next_culist_output_index,
                    msg_types,
                    slot_edges,
                });
                next_culist_output_index += 1;
            }
        }

        sort_inputs_by_cnx_id(&mut input_msg_indices_types);

        if let Some(pos) = plan
            .iter()
            .position(|step| matches!(step, CuExecutionUnit::Step(s) if s.node_id == id))
        {
            #[cfg(all(feature = "std", feature = "macro_debug"))]
            eprintln!("    → Already in plan, modifying existing step");
            let mut step = plan.remove(pos);
            if let CuExecutionUnit::Step(ref mut s) = step {
                s.input_msg_indices_types = input_msg_indices_types;
            }
            plan.push(step);
        } else {
            #[cfg(all(feature = "std", feature = "macro_debug"))]
            eprintln!("    → New step added to plan");
            let step = CuExecutionStep {
                node_id: id,
                node: node_ref.clone(),
                task_type,
                input_msg_indices_types,
                output_msg_pack,
            };
            plan.push(CuExecutionUnit::Step(step));
        }

        handled = true;
    }

    #[cfg(all(feature = "std", feature = "macro_debug"))]
    eprintln!("-- finished branch from node {starting_point} with handled={handled}");
    (next_culist_output_index, handled)
}

/// This is the main heuristics to compute an execution plan at compilation time.
/// TODO(gbin): Make that heuristic pluggable.
pub fn compute_runtime_plan(graph: &CuGraph) -> CuResult<CuExecutionLoop> {
    #[cfg(all(feature = "std", feature = "macro_debug"))]
    eprintln!("[runtime plan]");
    let mut plan = Vec::new();
    let mut next_culist_output_index = 0u32;

    let mut queue: VecDeque<NodeId> = graph
        .node_ids()
        .into_iter()
        .filter(|&node_id| find_task_type_for_id(graph, node_id) == CuTaskType::Source)
        .collect();

    #[cfg(all(feature = "std", feature = "macro_debug"))]
    eprintln!("Initial source nodes: {queue:?}");

    while let Some(start_node) = queue.pop_front() {
        #[cfg(all(feature = "std", feature = "macro_debug"))]
        eprintln!("→ Starting BFS from source {start_node}");
        for node_id in graph.bfs_nodes(start_node) {
            let already_in_plan = plan
                .iter()
                .any(|unit| matches!(unit, CuExecutionUnit::Step(s) if s.node_id == node_id));
            if already_in_plan {
                #[cfg(all(feature = "std", feature = "macro_debug"))]
                eprintln!("    → Node {node_id} already planned, skipping");
                continue;
            }

            #[cfg(all(feature = "std", feature = "macro_debug"))]
            eprintln!("    Planning from node {node_id}");
            let (new_index, handled) =
                plan_tasks_tree_branch(graph, next_culist_output_index, node_id, &mut plan);
            next_culist_output_index = new_index;

            if !handled {
                #[cfg(all(feature = "std", feature = "macro_debug"))]
                eprintln!("    ✗ Node {node_id} was not handled, skipping enqueue of neighbors");
                continue;
            }

            #[cfg(all(feature = "std", feature = "macro_debug"))]
            eprintln!("    ✓ Node {node_id} handled successfully, enqueueing neighbors");
            for neighbor in graph.get_neighbor_ids(node_id, CuDirection::Outgoing) {
                #[cfg(all(feature = "std", feature = "macro_debug"))]
                eprintln!("      → Enqueueing neighbor {neighbor}");
                queue.push_back(neighbor);
            }
        }
    }

    let mut planned_nodes = BTreeSet::new();
    for unit in &plan {
        if let CuExecutionUnit::Step(step) = unit {
            planned_nodes.insert(step.node_id);
        }
    }

    let mut missing = Vec::new();
    for node_id in graph.node_ids() {
        if !planned_nodes.contains(&node_id) {
            if let Some(node) = graph.get_node(node_id) {
                missing.push(node.get_id().to_string());
            } else {
                missing.push(format!("node_id_{node_id}"));
            }
        }
    }

    if !missing.is_empty() {
        missing.sort();
        return Err(CuError::from(format!(
            "Execution plan could not include all nodes. Missing: {}. Check for loopback or missing source connections.",
            missing.join(", ")
        )));
    }

    Ok(CuExecutionLoop {
        steps: plan,
        loop_count: None,
    })
}

//tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Node;
    use crate::cutask::CuSinkTask;
    use crate::cutask::{CuSrcTask, Freezable};
    use crate::monitoring::NoMonitor;
    use bincode::Encode;
    use cu29_traits::{ErasedCuStampedData, ErasedCuStampedDataSet, MatchingTasks};
    use serde_derive::{Deserialize, Serialize};

    pub struct TestSource {}

    impl Freezable for TestSource {}

    impl CuSrcTask for TestSource {
        type Resources<'r> = ();
        type Output<'m> = ();
        fn new(_config: Option<&ComponentConfig>, _resources: Self::Resources<'_>) -> CuResult<Self>
        where
            Self: Sized,
        {
            Ok(Self {})
        }

        fn process(
            &mut self,
            _clock: &RobotClock,
            _empty_msg: &mut Self::Output<'_>,
        ) -> CuResult<()> {
            Ok(())
        }
    }

    pub struct TestSink {}

    impl Freezable for TestSink {}

    impl CuSinkTask for TestSink {
        type Resources<'r> = ();
        type Input<'m> = ();

        fn new(_config: Option<&ComponentConfig>, _resources: Self::Resources<'_>) -> CuResult<Self>
        where
            Self: Sized,
        {
            Ok(Self {})
        }

        fn process(&mut self, _clock: &RobotClock, _input: &Self::Input<'_>) -> CuResult<()> {
            Ok(())
        }
    }

    // Those should be generated by the derive macro
    type Tasks = (TestSource, TestSink);

    #[derive(Debug, Encode, Decode, Serialize, Deserialize, Default)]
    struct Msgs(());

    impl ErasedCuStampedDataSet for Msgs {
        fn cumsgs(&self) -> Vec<&dyn ErasedCuStampedData> {
            Vec::new()
        }
    }

    impl MatchingTasks for Msgs {
        fn get_all_task_ids() -> &'static [&'static str] {
            &[]
        }
    }

    impl CuListZeroedInit for Msgs {
        fn init_zeroed(&mut self) {}
    }

    #[cfg(feature = "std")]
    fn tasks_instanciator(
        all_instances_configs: Vec<Option<&ComponentConfig>>,
        _resources: &mut ResourceManager,
    ) -> CuResult<Tasks> {
        Ok((
            TestSource::new(all_instances_configs[0], ())?,
            TestSink::new(all_instances_configs[1], ())?,
        ))
    }

    #[cfg(not(feature = "std"))]
    fn tasks_instanciator(
        all_instances_configs: Vec<Option<&ComponentConfig>>,
        _resources: &mut ResourceManager,
    ) -> CuResult<Tasks> {
        Ok((
            TestSource::new(all_instances_configs[0], ())?,
            TestSink::new(all_instances_configs[1], ())?,
        ))
    }

    fn monitor_instanciator(_config: &CuConfig) -> NoMonitor {
        NoMonitor {}
    }

    fn bridges_instanciator(_config: &CuConfig, _resources: &mut ResourceManager) -> CuResult<()> {
        Ok(())
    }

    fn resources_instanciator(_config: &CuConfig) -> CuResult<ResourceManager> {
        Ok(ResourceManager::new(&[]))
    }

    #[derive(Debug)]
    struct FakeWriter {}

    impl<E: Encode> WriteStream<E> for FakeWriter {
        fn log(&mut self, _obj: &E) -> CuResult<()> {
            Ok(())
        }
    }

    #[test]
    fn test_runtime_instantiation() {
        let mut config = CuConfig::default();
        let graph = config.get_graph_mut(None).unwrap();
        graph.add_node(Node::new("a", "TestSource")).unwrap();
        graph.add_node(Node::new("b", "TestSink")).unwrap();
        graph.connect(0, 1, "()").unwrap();
        let runtime = CuRuntime::<Tasks, (), Msgs, NoMonitor, 2>::new(
            RobotClock::default(),
            &config,
            None,
            resources_instanciator,
            tasks_instanciator,
            monitor_instanciator,
            bridges_instanciator,
            FakeWriter {},
            FakeWriter {},
        );
        assert!(runtime.is_ok());
    }

    #[test]
    fn test_copperlists_manager_lifecycle() {
        let mut config = CuConfig::default();
        let graph = config.get_graph_mut(None).unwrap();
        graph.add_node(Node::new("a", "TestSource")).unwrap();
        graph.add_node(Node::new("b", "TestSink")).unwrap();
        graph.connect(0, 1, "()").unwrap();

        let mut runtime = CuRuntime::<Tasks, (), Msgs, NoMonitor, 4>::new(
            RobotClock::default(),
            &config,
            None,
            resources_instanciator,
            tasks_instanciator,
            monitor_instanciator,
            bridges_instanciator,
            FakeWriter {},
            FakeWriter {},
        )
        .unwrap();

        // Emulates the generated runtime using the async manager API
        {
            let copperlists = &mut runtime.copperlists_manager;
            assert_eq!(copperlists.available_copper_lists().unwrap(), 4);
            let culist0 = copperlists.create().expect("Ran out of space for copper lists");
            let id = culist0.id;
            assert_eq!(id, 0);
            culist0.change_state(CopperListState::Processing);
            assert_eq!(copperlists.available_copper_lists().unwrap(), 3);
            let _ = copperlists.end_of_processing(0);
        }

        {
            let copperlists = &mut runtime.copperlists_manager;
            copperlists.finish_pending().unwrap();
            assert_eq!(copperlists.available_copper_lists().unwrap(), 4);
        }
    }

    #[test]
    fn test_runtime_task_input_order() {
        let mut config = CuConfig::default();
        let graph = config.get_graph_mut(None).unwrap();
        let src1_id = graph.add_node(Node::new("a", "Source1")).unwrap();
        let src2_id = graph.add_node(Node::new("b", "Source2")).unwrap();
        let sink_id = graph.add_node(Node::new("c", "Sink")).unwrap();

        assert_eq!(src1_id, 0);
        assert_eq!(src2_id, 1);

        // note that the source2 connection is before the source1
        let src1_type = "src1_type";
        let src2_type = "src2_type";
        graph.connect(src2_id, sink_id, src2_type).unwrap();
        graph.connect(src1_id, sink_id, src1_type).unwrap();

        let src1_edge_id = *graph.get_src_edges(src1_id).unwrap().first().unwrap();
        let src2_edge_id = *graph.get_src_edges(src2_id).unwrap().first().unwrap();
        // the edge id depends on the order the connection is created, not
        // on the node id, and that is what determines the input order
        assert_eq!(src1_edge_id, 1);
        assert_eq!(src2_edge_id, 0);

        let runtime = compute_runtime_plan(graph).unwrap();
        let sink_step = runtime
            .steps
            .iter()
            .find_map(|step| match step {
                CuExecutionUnit::Step(step) if step.node_id == sink_id => Some(step),
                _ => None,
            })
            .unwrap();

        // since the src2 connection was added before src1 connection, the src2 type should be
        // first
        assert_eq!(sink_step.input_msg_indices_types[0].msg_type, src2_type);
        assert_eq!(sink_step.input_msg_indices_types[1].msg_type, src1_type);
    }

    #[test]
    fn test_runtime_output_ports_unique_ordered() {
        let mut config = CuConfig::default();
        let graph = config.get_graph_mut(None).unwrap();
        let src_id = graph.add_node(Node::new("src", "Source")).unwrap();
        let dst_a_id = graph.add_node(Node::new("dst_a", "SinkA")).unwrap();
        let dst_b_id = graph.add_node(Node::new("dst_b", "SinkB")).unwrap();
        let dst_a2_id = graph.add_node(Node::new("dst_a2", "SinkA2")).unwrap();
        let dst_c_id = graph.add_node(Node::new("dst_c", "SinkC")).unwrap();

        graph.connect(src_id, dst_a_id, "msg::A").unwrap();
        graph.connect(src_id, dst_b_id, "msg::B").unwrap();
        graph.connect(src_id, dst_a2_id, "msg::A").unwrap();
        graph.connect(src_id, dst_c_id, "msg::C").unwrap();

        let runtime = compute_runtime_plan(graph).unwrap();
        let src_step = runtime
            .steps
            .iter()
            .find_map(|step| match step {
                CuExecutionUnit::Step(step) if step.node_id == src_id => Some(step),
                _ => None,
            })
            .unwrap();

        let output_pack = src_step.output_msg_pack.as_ref().unwrap();
        assert_eq!(output_pack.msg_types, vec!["msg::A", "msg::B", "msg::C"]);

        let dst_a_step = runtime
            .steps
            .iter()
            .find_map(|step| match step {
                CuExecutionUnit::Step(step) if step.node_id == dst_a_id => Some(step),
                _ => None,
            })
            .unwrap();
        let dst_b_step = runtime
            .steps
            .iter()
            .find_map(|step| match step {
                CuExecutionUnit::Step(step) if step.node_id == dst_b_id => Some(step),
                _ => None,
            })
            .unwrap();
        let dst_a2_step = runtime
            .steps
            .iter()
            .find_map(|step| match step {
                CuExecutionUnit::Step(step) if step.node_id == dst_a2_id => Some(step),
                _ => None,
            })
            .unwrap();
        let dst_c_step = runtime
            .steps
            .iter()
            .find_map(|step| match step {
                CuExecutionUnit::Step(step) if step.node_id == dst_c_id => Some(step),
                _ => None,
            })
            .unwrap();

        // dst_a and dst_a2 both fan out from the same "msg::A" slot (port 0)
        assert_eq!(dst_a_step.input_msg_indices_types[0].src_port, 0);
        assert_eq!(dst_b_step.input_msg_indices_types[0].src_port, 1);
        assert_eq!(dst_a2_step.input_msg_indices_types[0].src_port, 0);
        assert_eq!(dst_c_step.input_msg_indices_types[0].src_port, 2);
    }

    #[test]
    fn test_runtime_output_ports_fanout_single() {
        let mut config = CuConfig::default();
        let graph = config.get_graph_mut(None).unwrap();
        let src_id = graph.add_node(Node::new("src", "Source")).unwrap();
        let dst_a_id = graph.add_node(Node::new("dst_a", "SinkA")).unwrap();
        let dst_b_id = graph.add_node(Node::new("dst_b", "SinkB")).unwrap();

        graph.connect(src_id, dst_a_id, "i32").unwrap();
        graph.connect(src_id, dst_b_id, "i32").unwrap();

        let runtime = compute_runtime_plan(graph).unwrap();
        let src_step = runtime
            .steps
            .iter()
            .find_map(|step| match step {
                CuExecutionUnit::Step(step) if step.node_id == src_id => Some(step),
                _ => None,
            })
            .unwrap();

        let output_pack = src_step.output_msg_pack.as_ref().unwrap();
        assert_eq!(output_pack.msg_types, vec!["i32"]);
    }

    #[test]
    fn test_runtime_plan_diamond_case1() {
        // more complex topology that tripped the scheduler
        let mut config = CuConfig::default();
        let graph = config.get_graph_mut(None).unwrap();
        let cam0_id = graph
            .add_node(Node::new("cam0", "tasks::IntegerSrcTask"))
            .unwrap();
        let inf0_id = graph
            .add_node(Node::new("inf0", "tasks::Integer2FloatTask"))
            .unwrap();
        let broadcast_id = graph
            .add_node(Node::new("broadcast", "tasks::MergingSinkTask"))
            .unwrap();

        // case 1 order
        graph.connect(cam0_id, broadcast_id, "i32").unwrap();
        graph.connect(cam0_id, inf0_id, "i32").unwrap();
        graph.connect(inf0_id, broadcast_id, "f32").unwrap();

        let edge_cam0_to_broadcast = *graph.get_src_edges(cam0_id).unwrap().first().unwrap();
        let edge_cam0_to_inf0 = graph.get_src_edges(cam0_id).unwrap()[1];

        assert_eq!(edge_cam0_to_inf0, 0);
        assert_eq!(edge_cam0_to_broadcast, 1);

        let runtime = compute_runtime_plan(graph).unwrap();
        let broadcast_step = runtime
            .steps
            .iter()
            .find_map(|step| match step {
                CuExecutionUnit::Step(step) if step.node_id == broadcast_id => Some(step),
                _ => None,
            })
            .unwrap();

        assert_eq!(broadcast_step.input_msg_indices_types[0].msg_type, "i32");
        assert_eq!(broadcast_step.input_msg_indices_types[1].msg_type, "f32");
    }

    #[test]
    fn test_runtime_plan_diamond_case2() {
        // more complex topology that tripped the scheduler variation 2
        let mut config = CuConfig::default();
        let graph = config.get_graph_mut(None).unwrap();
        let cam0_id = graph
            .add_node(Node::new("cam0", "tasks::IntegerSrcTask"))
            .unwrap();
        let inf0_id = graph
            .add_node(Node::new("inf0", "tasks::Integer2FloatTask"))
            .unwrap();
        let broadcast_id = graph
            .add_node(Node::new("broadcast", "tasks::MergingSinkTask"))
            .unwrap();

        // case 2 order
        graph.connect(cam0_id, inf0_id, "i32").unwrap();
        graph.connect(cam0_id, broadcast_id, "i32").unwrap();
        graph.connect(inf0_id, broadcast_id, "f32").unwrap();

        let edge_cam0_to_inf0 = *graph.get_src_edges(cam0_id).unwrap().first().unwrap();
        let edge_cam0_to_broadcast = graph.get_src_edges(cam0_id).unwrap()[1];

        assert_eq!(edge_cam0_to_broadcast, 0);
        assert_eq!(edge_cam0_to_inf0, 1);

        let runtime = compute_runtime_plan(graph).unwrap();
        let broadcast_step = runtime
            .steps
            .iter()
            .find_map(|step| match step {
                CuExecutionUnit::Step(step) if step.node_id == broadcast_id => Some(step),
                _ => None,
            })
            .unwrap();

        assert_eq!(broadcast_step.input_msg_indices_types[0].msg_type, "i32");
        assert_eq!(broadcast_step.input_msg_indices_types[1].msg_type, "f32");
    }
}