//! A file-transfer body governed by its org's byte-rate bucket and, for
//! uploads, a minimum body rate.

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker, ready};
use std::time::Duration;

use axum::body::{Body, Bytes};
use http_body::{Body as HttpBody, Frame, SizeHint};
use pplx_space_metrics::data_plane::TransferDirection;
use tokio::time::{Instant, Sleep};

use super::MinBodyRateConfig;
use super::byte_rate::ByteRateBucket;

/// The error a governed upload body ends with when the client stalls. The
/// handler learns of the stall from its [`StallSignal`], not from this error,
/// which reaches it wrapped differently by each body reader.
#[derive(Debug, thiserror::Error)]
#[error("upload body arrived slower than the minimum body rate")]
struct UploadBodyStalled;

/// Set once a governed upload body ends for arriving below the minimum rate.
#[derive(Debug, Clone, Default)]
pub(crate) struct StallSignal(Arc<AtomicBool>);

impl StallSignal {
    pub(crate) fn is_stalled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    fn trip(&self) {
        self.0.store(true, Ordering::Release);
    }
}

pub(super) struct GovernedBody {
    inner: Body,
    direction: TransferDirection,
    rate: Option<Arc<ByteRateBucket>>,
    /// A frame already debited, released once `throttle` fires.
    throttled: Option<Frame<Bytes>>,
    /// Reset for each frame that owes the bucket, rather than allocated anew.
    throttle: Pin<Box<Sleep>>,
    /// The debt `throttle` is armed to sleep off, added to `throttled_for`
    /// once that sleep completes.
    owed: Duration,
    /// Debt slept off in completed throttle sleeps, reported once when the
    /// body is dropped. A sleep cut short by the drop isn't counted.
    throttled_for: Duration,
    floor: Option<MinBodyRate>,
}

impl GovernedBody {
    pub(super) fn new(
        inner: Body,
        direction: TransferDirection,
        rate: Option<Arc<ByteRateBucket>>,
        floor: Option<(MinBodyRateConfig, StallSignal)>,
    ) -> Self {
        Self {
            inner,
            direction,
            rate,
            throttled: None,
            throttle: Box::pin(tokio::time::sleep(Duration::ZERO)),
            owed: Duration::ZERO,
            throttled_for: Duration::ZERO,
            floor: floor.map(|(config, stalled)| MinBodyRate::new(config, stalled)),
        }
    }
}

impl Drop for GovernedBody {
    fn drop(&mut self) {
        // A frame held by the throttle was debited but never handed on.
        if let (Some(rate), Some(frame)) = (&self.rate, &self.throttled) {
            let len = frame.data_ref().map_or(0, Bytes::len);
            rate.credit(len, Instant::now());
        }
        if !self.throttled_for.is_zero() {
            pplx_space_metrics::data_plane::transfer_throttled(self.direction, self.throttled_for);
        }
    }
}

impl HttpBody for GovernedBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        // Every field is `Unpin`, so the body can be borrowed mutably as a whole.
        let this = &mut *self;
        // A stall ends the body for good, so a reader that polls again can't
        // resume it after the handler has been told it stalled.
        if this
            .floor
            .as_ref()
            .is_some_and(|floor| floor.stalled.is_stalled())
        {
            return Poll::Ready(Some(Err(axum::Error::new(UploadBodyStalled))));
        }
        if this.throttled.is_some() {
            ready!(this.throttle.as_mut().poll(cx));
            this.throttled_for += std::mem::take(&mut this.owed);
            return Poll::Ready(this.throttled.take().map(Ok));
        }
        let (polled, woke_at) = match &this.floor {
            Some(floor) => floor
                .data_wake
                .poll_through(cx, |cx| Pin::new(&mut this.inner).poll_frame(cx)),
            None => (Pin::new(&mut this.inner).poll_frame(cx), None),
        };
        let Poll::Ready(next) = polled else {
            if let Some(floor) = &mut this.floor {
                floor.on_pending(Instant::now(), cx)?;
            }
            return Poll::Pending;
        };
        let Some(Ok(frame)) = next else {
            return Poll::Ready(next);
        };
        let now = Instant::now();
        let len = frame.data_ref().map_or(0, Bytes::len);
        if let Some(floor) = &mut this.floor {
            floor.on_ready(woke_at.unwrap_or(now).min(now), len)?;
        }
        if let Some(rate) = &this.rate {
            let debt = rate.debit(len, now);
            if !debt.is_zero() {
                this.throttle.as_mut().reset(now + debt);
                if this.throttle.as_mut().poll(cx).is_pending() {
                    this.owed = debt;
                    this.throttled = Some(frame);
                    return Poll::Pending;
                }
                this.throttled_for += debt;
            }
        }
        Poll::Ready(Some(Ok(frame)))
    }

    fn is_end_stream(&self) -> bool {
        let stalled = self
            .floor
            .as_ref()
            .is_some_and(|floor| floor.stalled.is_stalled());
        !stalled && self.throttled.is_none() && self.inner.is_end_stream()
    }

    /// The inner body's hint plus any frame held back by the throttle, which
    /// the inner body no longer counts.
    fn size_hint(&self) -> SizeHint {
        let held = self
            .throttled
            .as_ref()
            .and_then(Frame::data_ref)
            .map_or(0, |data| data.len() as u64);
        let inner = self.inner.size_hint();
        let mut hint = SizeHint::new();
        hint.set_lower(inner.lower().saturating_add(held));
        if let Some(upper) = inner.upper() {
            hint.set_upper(upper.saturating_add(held));
        }
        hint
    }
}

/// Records when the inner body wakes the task, which is when the client's
/// data arrived, and passes the wake on to the task.
#[derive(Default)]
struct DataWake {
    woke_at: Mutex<Option<Instant>>,
    task: Mutex<Option<Waker>>,
}

impl Wake for DataWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.woke_at
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_or_insert_with(Instant::now);
        if let Some(task) = &*self.task.lock().unwrap_or_else(|e| e.into_inner()) {
            task.wake_by_ref();
        }
    }
}

/// A `DataWake` and the waker made from it, built once per body.
struct DataWaker {
    wake: Arc<DataWake>,
    waker: Waker,
}

impl Default for DataWaker {
    fn default() -> Self {
        let wake = Arc::new(DataWake::default());
        let waker = Waker::from(Arc::clone(&wake));
        Self { wake, waker }
    }
}

impl DataWaker {
    /// Polls through this waker on behalf of the task `cx` belongs to, and
    /// returns when the inner body first woke it since the previous poll, if
    /// it has. Taken before polling, so a wake that comes while polling is
    /// kept for the next one. Any inner wake counts as data arriving, which
    /// can only shorten the client's measured wait.
    fn poll_through<T>(
        &self,
        cx: &Context<'_>,
        poll: impl FnOnce(&mut Context<'_>) -> T,
    ) -> (T, Option<Instant>) {
        {
            let mut task = self.wake.task.lock().unwrap_or_else(|e| e.into_inner());
            if !task.as_ref().is_some_and(|task| task.will_wake(cx.waker())) {
                *task = Some(cx.waker().clone());
            }
        }
        let woke_at = self
            .wake
            .woke_at
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        (poll(&mut Context::from_waker(&self.waker)), woke_at)
    }
}

/// Counts time spent waiting on the client, a window at a time: from a poll
/// that finds no data until the client's data arrives, as the inner body's
/// wake reports it. Time after that, such as a reader busy writing the spool
/// or the guest before it comes back for the data, and time sleeping off
/// byte-rate debt never count, so neither can condemn an upload.
struct MinBodyRate {
    window: Duration,
    min_bytes_per_window: u64,
    stalled: StallSignal,
    /// Client wait time and bytes received in the current window.
    waited: Duration,
    received: u64,
    /// Set while the body is pending on the client.
    pending_since: Option<Instant>,
    /// Fires when the current wait would fill the window.
    timer: Pin<Box<Sleep>>,
    data_wake: DataWaker,
}

impl MinBodyRate {
    fn new(config: MinBodyRateConfig, stalled: StallSignal) -> Self {
        // A zero window would judge every wait at once and never advance.
        let window = config.window.max(Duration::from_millis(1));
        // Rounded up, so at least one byte: a window with nothing received
        // always fails.
        let min_bytes_per_window = u64::try_from(
            (u128::from(config.bytes_per_second.get()) * window.as_millis()).div_ceil(1000),
        )
        .unwrap_or(u64::MAX);
        Self {
            window,
            min_bytes_per_window,
            stalled,
            waited: Duration::ZERO,
            received: 0,
            pending_since: None,
            timer: Box::pin(tokio::time::sleep(Duration::ZERO)),
            data_wake: DataWaker::default(),
        }
    }

    fn on_pending(&mut self, now: Instant, cx: &mut Context<'_>) -> Result<(), axum::Error> {
        let mut since = *self.pending_since.get_or_insert(now);
        loop {
            let window_ends = since + self.window.saturating_sub(self.waited);
            if self.timer.deadline() != window_ends {
                self.timer.as_mut().reset(window_ends);
            }
            if self.timer.as_mut().poll(cx).is_pending() {
                return Ok(());
            }
            // The window filled while waiting on the client: judge it, and
            // keep waiting in a fresh one.
            self.waited = self.window;
            self.close_window()?;
            since = window_ends;
            self.pending_since = Some(since);
        }
    }

    /// `arrived` is when the frame's data reached the body: its wake, or
    /// now if it was already there.
    fn on_ready(&mut self, arrived: Instant, len: usize) -> Result<(), axum::Error> {
        if let Some(mut since) = self.pending_since.take() {
            // Judge every window that ran out before the data arrived without
            // it, so a late frame can't rescue a window the client let lapse.
            loop {
                let window_ends = since + self.window.saturating_sub(self.waited);
                if arrived < window_ends {
                    self.waited += arrived.saturating_duration_since(since);
                    break;
                }
                self.waited = self.window;
                self.close_window()?;
                since = window_ends;
            }
        }
        self.received = self.received.saturating_add(len as u64);
        if self.waited >= self.window {
            self.close_window()?;
        }
        Ok(())
    }

    fn close_window(&mut self) -> Result<(), axum::Error> {
        if self.received < self.min_bytes_per_window {
            pplx_space_metrics::data_plane::upload_body_stalled();
            self.stalled.trip();
            return Err(axum::Error::new(UploadBodyStalled));
        }
        self.waited = Duration::ZERO;
        self.received = 0;
        Ok(())
    }
}
