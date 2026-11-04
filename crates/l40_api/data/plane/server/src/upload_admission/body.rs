//! A file-transfer body governed by its org's byte-rate bucket and, for
//! uploads, a minimum body rate or a time limit.

use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, Wake, Waker, ready};
use std::time::Duration;

use axum::body::{Body, Bytes};
use http_body::{Body as HttpBody, Frame, SizeHint};
use tokio::time::{Instant, Sleep};

use super::MinBodyRateConfig;
use super::byte_rate::ByteRateBucket;

/// Why the data plane ended a governed upload body early.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BodyEnded {
    /// It arrived below the minimum body rate.
    Stalled,
    /// A frame would have slept past the longest throttle sleep an upload
    /// may hold its permits through; `excess` is how long the org's debt
    /// then had to fall before a new upload would be admitted.
    OverRate { excess: Duration },
    /// An HTTP/2 upload ran past its time limit.
    TimedOut,
}

/// The error a governed upload body ends with when the data plane ends it.
/// The handler learns why from its [`BodyEnd`], not from this error, which
/// reaches it wrapped differently by each body reader.
#[derive(Debug, thiserror::Error)]
#[error("upload body ended by the data plane: {0:?}")]
struct UploadBodyEnded(BodyEnded);

/// Set once, when a governed upload body is ended early, to why.
#[derive(Debug, Clone, Default)]
pub(crate) struct BodyEnd(Arc<OnceLock<BodyEnded>>);

impl BodyEnd {
    pub(crate) fn ended(&self) -> Option<BodyEnded> {
        self.0.get().copied()
    }

    #[cfg(test)]
    pub(crate) fn is_stalled(&self) -> bool {
        self.ended() == Some(BodyEnded::Stalled)
    }

    /// Ends the body for `why`, unless it already ended, and returns the
    /// error to end it with: for the first reason it ended, so every poll
    /// afterwards reports the same one.
    fn end(&self, why: BodyEnded) -> axum::Error {
        axum::Error::new(UploadBodyEnded(*self.0.get_or_init(|| why)))
    }
}

/// How a governed body answers to its org's byte-rate bucket.
pub(super) enum Metering {
    /// Each frame that leaves the org in debt is held until the debt is
    /// slept off.
    Throttle(Arc<ByteRateBucket>),
    /// Each frame is charged and passed on at once. For a body that mustn't
    /// stop being read: an HTTP/2 stream, such as an HTTP/2 upload's or a
    /// download's from its pooled backend connection, shares its
    /// connection's flow-control window, so holding one stream's frames
    /// holds back every other stream's.
    Charge(Arc<ByteRateBucket>),
}

pub(super) struct GovernedBody {
    inner: Body,
    /// The bucket to sleep off debt in, when the body is throttled.
    rate: Option<Arc<ByteRateBucket>>,
    /// The bucket to charge without waiting, when the body is only charged.
    charged_to: Option<Arc<ByteRateBucket>>,
    /// A frame already debited, released once `throttle` fires.
    throttled: Option<Frame<Bytes>>,
    /// Reset for each frame that owes the bucket, rather than allocated anew.
    throttle: Pin<Box<Sleep>>,
    /// When the current throttle sleep began, while one is in progress.
    throttle_started: Option<Instant>,
    /// Time slept in the throttle not yet reported, flushed once it reaches
    /// `THROTTLE_REPORT_EVERY` and when the body is dropped.
    throttled_for: Duration,
    /// The longest a frame may sleep in the throttle; a longer debt ends the
    /// body instead.
    max_throttle: Duration,
    admitted_debt: Duration,
    floor: Option<MinBodyRate>,
    /// When the body must have ended by, for an upload with a time limit.
    deadline: Option<Pin<Box<Sleep>>>,
    /// Whether the deadline has been pushed back by `END_GRACE` already.
    end_grace_given: bool,
    ended: BodyEnd,
}

/// What a governed body is held to, past its metering.
pub(super) struct Limits {
    pub(super) floor: Option<MinBodyRateConfig>,
    pub(super) max_duration: Option<Duration>,
    pub(super) max_throttle: Duration,
    /// The most debt a new upload is admitted into, which an over-rate end's
    /// `excess` is measured down to.
    pub(super) admitted_debt: Duration,
}

/// No limits: no floor, no time limit, and throttle sleeps of any length.
impl Default for Limits {
    fn default() -> Self {
        Self {
            floor: None,
            max_duration: None,
            max_throttle: Duration::MAX,
            admitted_debt: Duration::ZERO,
        }
    }
}

impl GovernedBody {
    pub(super) fn new(
        inner: Body,
        metering: Option<Metering>,
        limits: Limits,
        ended: BodyEnd,
    ) -> Self {
        let (rate, charged_to) = match metering {
            Some(Metering::Throttle(bucket)) => (Some(bucket), None),
            Some(Metering::Charge(bucket)) => (None, Some(bucket)),
            None => (None, None),
        };
        Self {
            inner,
            rate,
            charged_to,
            throttled: None,
            throttle: Box::pin(tokio::time::sleep(Duration::ZERO)),
            throttle_started: None,
            throttled_for: Duration::ZERO,
            max_throttle: limits.max_throttle,
            admitted_debt: limits.admitted_debt,
            floor: limits
                .floor
                .map(|config| MinBodyRate::new(config, ended.clone())),
            deadline: limits
                .max_duration
                .map(|limit| Box::pin(tokio::time::sleep(limit))),
            end_grace_given: false,
            ended,
        }
    }
}

/// How long past its time limit a body whose declared bytes have all
/// arrived may take to send its end.
pub(super) const END_GRACE: Duration = Duration::from_secs(5);

/// How much throttled time a body accumulates before reporting it, so a long
/// transfer reports as it goes without a metric per throttled frame.
const THROTTLE_REPORT_EVERY: Duration = Duration::from_secs(1);

impl GovernedBody {
    /// Ends the throttle sleep in progress, if any, and adds the time it took:
    /// to its deadline when it ran out, or to `ended` when the body was
    /// dropped first. Time the reader took to come back after the deadline is
    /// the reader's, not the throttle's.
    fn end_throttle(&mut self, ended: Instant) {
        if let Some(started) = self.throttle_started.take() {
            let ended = ended.min(self.throttle.deadline());
            self.throttled_for += ended.saturating_duration_since(started);
        }
        if self.throttled_for >= THROTTLE_REPORT_EVERY {
            self.report_throttle();
        }
    }

    fn report_throttle(&mut self) {
        let throttled_for = std::mem::take(&mut self.throttled_for);
        if !throttled_for.is_zero() {
            pplx_space_metrics::data_plane::upload_throttled(throttled_for);
        }
    }
}

impl Drop for GovernedBody {
    fn drop(&mut self) {
        // A held frame already crossed the network, so it stays charged.
        self.end_throttle(Instant::now());
        self.report_throttle();
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
        // Ending a body is final, so a reader that polls again can't resume
        // it after the handler has been told why it ended.
        if let Some(why) = this.ended.ended() {
            return Poll::Ready(Some(Err(this.ended.end(why))));
        }
        if this.throttled.is_some() {
            ready!(this.throttle.as_mut().poll(cx));
            this.end_throttle(Instant::now());
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
            if let Some(deadline) = &mut this.deadline
                && deadline.as_mut().poll(cx).is_ready()
            {
                // A body whose declared bytes have all arrived is only waiting
                // for its end, which an HTTP/2 client can send in a frame of
                // its own: it gets a short grace for that once, not an
                // exemption, or withholding the end would hold its permits.
                if !this.end_grace_given && inner_complete(&this.inner) {
                    this.end_grace_given = true;
                    let extended = deadline.deadline() + END_GRACE;
                    deadline.as_mut().reset(extended);
                    if deadline.as_mut().poll(cx).is_pending() {
                        return Poll::Pending;
                    }
                }
                pplx_space_metrics::data_plane::upload_body_timed_out();
                return Poll::Ready(Some(Err(this.ended.end(BodyEnded::TimedOut))));
            }
            return Poll::Pending;
        };
        // The end of the body, or an error from it, passes straight through.
        // The end isn't judged against the floor: the client has sent all of
        // it, and the handler releases the upload's permits as it finishes, so
        // failing it would discard a complete upload to free nothing.
        let Some(Ok(frame)) = next else {
            return Poll::Ready(next);
        };
        let now = Instant::now();
        // A body that keeps arriving never waits, so the time limit is
        // judged on frames too, though not on the last: that would discard a
        // complete upload to free permits the handler is about to release.
        if this
            .deadline
            .as_ref()
            .is_some_and(|deadline| now >= deadline.deadline())
            && !inner_complete(&this.inner)
        {
            pplx_space_metrics::data_plane::upload_body_timed_out();
            return Poll::Ready(Some(Err(this.ended.end(BodyEnded::TimedOut))));
        }
        let len = frame.data_ref().map_or(0, Bytes::len);
        if let Some(floor) = &mut this.floor {
            floor.on_ready(woke_at.map(|woke_at| woke_at.min(now)), len)?;
        }
        if let Some(bucket) = this.charged_to.as_ref().filter(|_| len > 0) {
            bucket.charge(len, now);
        }
        // A frame with no data, such as trailers, moves nothing the bucket
        // meters, so it never waits behind other transfers' debt.
        let debt_ends = this
            .rate
            .as_ref()
            .filter(|_| len > 0)
            .and_then(|rate| rate.debit(len, now));
        if let Some(ends) = debt_ends {
            // The frame stays charged: it already crossed the network.
            let debt = ends.saturating_duration_since(now);
            if debt > this.max_throttle {
                // The body's last frame is handed on unslowed instead: ending
                // a complete upload would discard it to free permits the
                // handler is about to release, and its retry would be
                // charged again.
                if inner_complete(&this.inner) {
                    return Poll::Ready(Some(Ok(frame)));
                }
                pplx_space_metrics::data_plane::upload_body_over_rate();
                let excess = debt.saturating_sub(this.admitted_debt);
                return Poll::Ready(Some(Err(this.ended.end(BodyEnded::OverRate { excess }))));
            }
            this.throttle.as_mut().reset(ends);
            if this.throttle.as_mut().poll(cx).is_pending() {
                this.throttle_started = Some(now);
                this.throttled = Some(frame);
                return Poll::Pending;
            }
        }
        Poll::Ready(Some(Ok(frame)))
    }

    fn is_end_stream(&self) -> bool {
        self.ended.ended().is_none() && self.throttled.is_none() && self.inner.is_end_stream()
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

/// Whether `inner` has nothing left to deliver but its end: it says so, or
/// its declared length has all arrived. An HTTP/2 body can send END_STREAM
/// in a frame after its last data, so the length is the earlier signal.
fn inner_complete(inner: &Body) -> bool {
    inner.is_end_stream() || inner.size_hint().upper() == Some(0)
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
/// byte-rate debt never count, so neither can condemn an upload. Only
/// HTTP/1.x uploads get one: there, a body waits on nothing but its client.
struct MinBodyRate {
    window: Duration,
    min_bytes_per_window: u64,
    ended: BodyEnd,
    /// Client wait time and bytes received in the current window.
    waited: Duration,
    received: u64,
    /// Set while the body is pending on the client.
    pending_since: Option<Instant>,
    /// The latest poll that found no data, while the body is pending.
    last_found_empty: Option<Instant>,
    /// Fires when the current wait would fill the window.
    timer: Pin<Box<Sleep>>,
    data_wake: DataWaker,
}

impl MinBodyRate {
    fn new(config: MinBodyRateConfig, ended: BodyEnd) -> Self {
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
            ended,
            waited: Duration::ZERO,
            received: 0,
            pending_since: None,
            last_found_empty: None,
            timer: Box::pin(tokio::time::sleep(Duration::ZERO)),
            data_wake: DataWaker::default(),
        }
    }

    fn on_pending(&mut self, now: Instant, cx: &mut Context<'_>) -> Result<(), axum::Error> {
        let mut since = *self.pending_since.get_or_insert(now);
        self.last_found_empty = Some(now);
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

    /// `woke_at` is when the inner body's wake reported the frame's data.
    /// Without one, the data arrived after the last poll that found none and
    /// before its wake ran, so it's credited to that poll: otherwise a timer
    /// that fires in that gap would judge the window without data the client
    /// sent within it.
    fn on_ready(&mut self, woke_at: Option<Instant>, len: usize) -> Result<(), axum::Error> {
        if let Some(mut since) = self.pending_since.take() {
            // The recorded wake wins; without one, the last poll that found no
            // data; failing both, the start of the wait.
            let arrived = woke_at.or(self.last_found_empty.take()).unwrap_or(since);
            // Judge every window that ran out before the data arrived without
            // it, so a late frame can't rescue a window the client let lapse.
            // The one exception is the no-wake fallback above, which credits
            // a frame found with no wake recorded to the last poll that found
            // none, however late the reader comes back. hyper's body always
            // wakes, so with it that's only data that lands just after a
            // window ends, before its wake runs.
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
        // The loop leaves `waited` short of the window, so the frame always
        // counts toward the window still open.
        self.received = self.received.saturating_add(len as u64);
        Ok(())
    }

    fn close_window(&mut self) -> Result<(), axum::Error> {
        if self.received < self.min_bytes_per_window {
            pplx_space_metrics::data_plane::upload_body_stalled();
            return Err(self.ended.end(BodyEnded::Stalled));
        }
        self.waited = Duration::ZERO;
        self.received = 0;
        Ok(())
    }
}
