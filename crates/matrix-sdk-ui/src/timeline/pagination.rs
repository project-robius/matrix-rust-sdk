// Copyright 2023 The Matrix.org Foundation C.I.C.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use async_rx::StreamExt as _;
use async_stream::stream;
use futures_core::Stream;
use futures_util::{StreamExt as _, pin_mut};
use matrix_sdk::event_cache::{PaginationStatus, RoomPagination};
use tracing::instrument;

use std::sync::Arc;

use eyeball_im::VectorDiff;
use imbl::Vector;

use super::{Error, TimelineItem, subscriber::{TimelineSubscriber, TimelineWithDropHandle}};
use crate::timeline::{PaginationError::NotSupported, controller::TimelineFocusKind};

/// Which end of the timeline a pagination call should extend.
#[derive(Clone, Copy)]
enum PaginationDirection {
    /// Add older events at the start of the timeline.
    Backwards,
    /// Add newer events at the end of an event-focused timeline.
    Forwards
}

impl super::Timeline {
    /// Add more events to the start of the timeline.
    ///
    /// Returns whether we hit the start of the timeline. On success, the cache
    /// updates have been applied internally, even for empty or filtered pages.
    /// This does not mean an existing item subscriber has consumed its queued
    /// diffs; use [`Self::paginate_backwards_with_subscription`] when needed.
    ///
    /// A clear or replacement during pagination returns `false` rather than
    /// confirming an old start of history. A later call can confirm the new start.
    #[instrument(skip_all, fields(room_id = ?self.room().room_id()))]
    pub async fn paginate_backwards(&self, num_events: u16) -> Result<bool, Error> {
        self.paginate(PaginationDirection::Backwards, num_events, false).await.0
    }

    /// Add more events to the end of the timeline.
    ///
    /// Returns whether we hit the end of the timeline, with the same delivery
    /// guarantees as [`Self::paginate_backwards`].
    #[instrument(skip_all, fields(room_id = ?self.room().room_id()))]
    pub async fn paginate_forwards(&self, num_events: u16) -> Result<bool, Error> {
        self.paginate(PaginationDirection::Forwards, num_events, false).await.0
    }

    /// Add older events and return the result, current visible items, and a
    /// fresh item stream, in that order.
    ///
    /// The result, items, and new subscription are captured under one timeline
    /// lock after waiting for cache processing. The items use the same
    /// filtering and lazy pagination limits as [`Self::subscribe`], and may also
    /// include concurrent sync changes. The stream starts after that snapshot.
    /// Install the returned items, switch to the new stream, and discard queued
    /// diffs from any previous subscription.
    ///
    /// `Ok(true)` confirms the start of this timeline. A clear or replacement
    /// during pagination instead returns `Ok(false)`; another call can confirm
    /// the new start of history. Pinned timelines do not support backwards pagination.
    ///
    /// An `Err` still comes with the items already applied and a usable stream,
    /// including partial changes made before the error. If a cache-processing
    /// task stopped, some page changes may not have been applied. Like
    /// [`Self::subscribe`], the returned stream keeps timeline background tasks alive.
    ///
    /// Dropping this future before it returns cancels the remaining delivery
    /// wait and snapshot capture. Changes already applied stay in the timeline,
    /// and an underlying shared cache request may keep running.
    pub async fn paginate_backwards_with_subscription(
        &self,
        num_events: u16,
    ) -> (Result<bool, Error>, Vector<Arc<TimelineItem>>,
        impl Stream<Item = Vec<VectorDiff<Arc<TimelineItem>>>> + use<>)
    {
        self.paginate_with_subscription(PaginationDirection::Backwards, num_events).await
    }

    /// Add newer events and return the result, current visible items, and a
    /// fresh item stream, in that order.
    ///
    /// The snapshot, stream, errors, and cancellation behave as described in
    /// [`Self::paginate_backwards_with_subscription`]. Here, `Ok(true)` confirms
    /// the end of the timeline. Event-focused timelines can fetch newer events;
    /// live timelines are already at the end. Thread and pinned timelines do not
    /// support forwards pagination.
    pub async fn paginate_forwards_with_subscription(
        &self,
        num_events: u16,
    ) -> (Result<bool, Error>, Vector<Arc<TimelineItem>>,
        impl Stream<Item = Vec<VectorDiff<Arc<TimelineItem>>>> + use<>)
    {
        self.paginate_with_subscription(PaginationDirection::Forwards, num_events).await
    }

    /// Run pagination with a snapshot and wrap its stream to keep the timeline's
    /// background tasks alive, just as [`Self::subscribe`] does.
    async fn paginate_with_subscription(
        &self,
        direction: PaginationDirection,
        num_events: u16,
    ) -> (Result<bool, Error>, Vector<Arc<TimelineItem>>,
        impl Stream<Item = Vec<VectorDiff<Arc<TimelineItem>>>> + use<>)
    {
        let (result, subscription) = self.paginate(direction, num_events, true).await;
        let (items, stream) = subscription.expect("pagination requested a subscription");
        (result, items, TimelineWithDropHandle::new(stream, self.drop_handle.clone()))
    }

    /// Run a page, wait for cache processing, and check for a reset.
    ///
    /// `subscribe` requests current visible items and a fresh stream too. Both
    /// cache tasks are awaited even after an error; the pagination error takes
    /// precedence if cache processing also fails. Cancelling this future skips
    /// the remaining wait and final snapshot, not changes already applied.
    async fn paginate(
        &self,
        direction: PaginationDirection,
        num_events: u16,
        subscribe: bool,
    ) -> (Result<bool, Error>, Option<(Vector<Arc<TimelineItem>>, TimelineSubscriber)>) {
        let generation = self.controller.pagination_generation().await;
        let result = match direction {
            PaginationDirection::Backwards => self.backwards(num_events).await,
            PaginationDirection::Forwards => self.forwards(num_events).await,
        };
        // A failure can follow lazy expansion or partially applied events too.
        // Wait for both independent cache tasks even if either fence fails.
        let delivered = self.wait_for_cache_updates().await;
        let result = result.and_then(|reached_end| delivered.map(|()| reached_end));
        self.controller.finish_pagination(result, generation,
            matches!(direction, PaginationDirection::Backwards), subscribe).await
    }

    /// Do backwards pagination before waiting for the cache tasks.
    ///
    /// Live timelines reveal cached items first; event and thread timelines use
    /// their own caches. The result still needs to be checked for a reset.
    async fn backwards(&self, mut num_events: u16) -> Result<bool, Error> {
        let fully_paginated = match self.controller.focus() {
            TimelineFocusKind::Live { event_cache, .. } => {
                match self.controller.live_lazy_paginate_backwards(num_events).await {
                    Some(needed_num_events) => {
                        num_events = needed_num_events.try_into().expect(
                            "failed to cast `needed_num_events` (`usize`) into `num_events` (`usize`)",
                        );
                    }
                    None => {
                        // We could adjust the skip count to a lower value,
                        // while passing the requested number of events. We
                        // _may_ have reached the start of the timeline, but
                        // since we're fulfilling the caller's request, assume
                        // it's not the case and return false here. A subsequent
                        // call will go to the `Some()` arm of this match, and
                        // cause a call to the event cache's pagination.
                        return Ok(false);
                    }
                }

                Ok(self.live_paginate_backwards(&event_cache.pagination(), num_events).await?)
            }

            TimelineFocusKind::Event { event_cache, .. } => {
                Ok(event_cache.paginate_backwards(num_events).await?.hit_end_of_timeline)
            }

            TimelineFocusKind::Thread { event_cache, .. } => Ok(event_cache
                .pagination()
                .run_backwards_once(num_events)
                .await
                .map(|outcome| outcome.reached_start)?),

            TimelineFocusKind::PinnedEvents { .. } => Err(Error::PaginationError(NotSupported)),
        }?;
        Ok(fully_paginated)
    }

    /// Do forwards pagination before waiting for the cache tasks.
    ///
    /// Only event-focused timelines fetch a page. Live timelines are already at
    /// the end; thread and pinned timelines return an unsupported-pagination error.
    async fn forwards(&self, num_events: u16) -> Result<bool, Error> {
        let fully_paginated = match self.controller.focus() {
            TimelineFocusKind::Live { .. } => Ok(true),

            TimelineFocusKind::Event { event_cache, .. } => {
                Ok(event_cache.paginate_forwards(num_events).await?.hit_end_of_timeline)
            }

            TimelineFocusKind::Thread { .. } | TimelineFocusKind::PinnedEvents { .. } => {
                Err(Error::PaginationError(NotSupported))
            }
        }?;
        Ok(fully_paginated)
    }

    /// Wait for the room task and, when present, the event or thread task to
    /// finish the updates queued when they accept each request.
    ///
    /// Both tasks are awaited even if one stops. If both fail, return the room
    /// task's error; item subscribers do not need to consume their own queued diffs.
    async fn wait_for_cache_updates(&self) -> Result<(), Error> {
        let (room, focus) = futures_util::future::join(
            self.room_updates_barrier.wait(),
            async {
                if let Some(barrier) = &self.focus_updates_barrier {
                    barrier.wait().await
                } else {
                    Ok(())
                }
            },
        ).await;
        room.and(focus)
    }

    /// Paginate backwards in live mode.
    ///
    /// This can only be called when the timeline is in live mode, not focused
    /// on a specific event.
    ///
    /// Returns whether we hit the start of the timeline.
    async fn live_paginate_backwards(
        &self,
        event_cache_pagination: &RoomPagination,
        batch_size: u16,
    ) -> Result<bool, Error> {
        loop {
            match event_cache_pagination.run_backwards_once(batch_size).await {
                Ok(outcome) => {
                    if outcome.reached_start {
                        return Ok(true);
                    }

                    if !outcome.events.is_empty() {
                        return Ok(false);
                    }

                    // Fallthrough: as a special contract, restart pagination,
                    // if it returned 0 events.
                }

                // Propagate errors as such.
                Err(err) => return Err(err.into()),
            }
        }
    }
    /// Subscribe to the back-pagination status of a live timeline.
    ///
    /// This will return `None` if the timeline is in the focused mode.
    ///
    /// Note: this may send multiple Paginating/Idle sequences during a single
    /// call to [`Self::paginate_backwards()`].
    pub async fn live_back_pagination_status(
        &self,
    ) -> Option<(PaginationStatus, impl Stream<Item = PaginationStatus> + use<>)> {
        let TimelineFocusKind::Live { event_cache, .. } = self.controller.focus() else {
            return None;
        };

        let pagination = event_cache.pagination();

        let mut status = pagination.status();

        let current_value = self.controller.map_pagination_status(status.next_now()).await;

        let controller = self.controller.clone();
        let stream = Box::pin(stream! {
            let status_stream = status.dedup();

            pin_mut!(status_stream);

            while let Some(state) = status_stream.next().await {
                let state = controller.map_pagination_status(state).await;

                match state {
                    PaginationStatus::Idle { hit_timeline_start } => {
                        if hit_timeline_start {
                            controller.insert_timeline_start_if_missing().await;
                        }
                    }
                    PaginationStatus::Paginating => {}
                }

                yield state;
            }
        });

        Some((current_value, stream))
    }
}
