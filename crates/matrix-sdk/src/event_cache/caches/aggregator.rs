// Copyright 2026 The Matrix.org Foundation C.I.C.
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

use std::collections::{BTreeMap, HashMap};

use matrix_sdk_base::{
    deserialized_responses::TimelineEvent,
    serde_helpers::{extract_redaction_target, extract_relation, extract_thread_root},
    sync::Timeline,
};
use ruma::{
    EventId, OwnedEventId,
    events::{
        AnySyncEphemeralRoomEvent, AnySyncTimelineEvent,
        receipt::{ReceiptEventContent, ReceiptThread, Receipts},
        relation::RelationType,
    },
    room_version_rules::RedactionRules,
    serde::Raw,
};

use super::{
    super::{Result, states::StateLockReadGuard},
    read_receipts::MaybeReceiptEventContent,
    room::RoomEventCacheState,
    thread::ThreadEventCacheState,
};

pub fn aggregate_timeline_and_read_receipts_for_room(
    timeline: &Timeline,
    ephemeral: &[AnySyncEphemeralRoomEvent],
) -> (Timeline, MaybeReceiptEventContent) {
    (
        timeline.clone(),
        filter_read_receipts_and_group_by(ephemeral, |receipt_thread| match receipt_thread {
            ReceiptThread::Main | ReceiptThread::Unthreaded => Some(()),
            _ => None,
        })
        .map(|(_receipt_thread, (event_id, event_receipts))| {
            (event_id.clone(), event_receipts.clone())
        })
        .collect(),
    )
}

pub async fn aggregate_timeline_and_read_receipts_for_threads<'sync, 'state>(
    timeline: &'sync Timeline,
    ephemeral: &'sync [AnySyncEphemeralRoomEvent],
    existing_threads: StateLockReadGuard<'state, HashMap<OwnedEventId, ThreadEventCacheState>>,
    maybe_room: Option<StateLockReadGuard<'state, RoomEventCacheState>>,
    redaction_rules: &'sync RedactionRules,
) -> Result<HashMap<OwnedEventId, (Timeline, MaybeReceiptEventContent)>> {
    let mut new_events_by_thread = HashMap::new();

    let default_entry = || {
        (
            Timeline {
                limited: timeline.limited,
                prev_batch: timeline.prev_batch.clone(),
                events: Vec::new(),
            },
            MaybeReceiptEventContent::none(),
        )
    };

    // Look for in-thread events, i.e. events that are part of threads.
    for (nth, event) in timeline.events.iter().enumerate() {
        match extract_relation(event.raw()) {
            // Ohh, this event relates to another event!
            Some((relation_type, related_event_id)) => match relation_type {
                // `related_event` represents a thread root.
                RelationType::Thread => {
                    new_events_by_thread
                        .entry(related_event_id)
                        .or_insert_with(default_entry)
                        .0
                        .events
                        .push(event.clone());
                }

                // `event` represents an annotation (e.g. reactions), a
                // replacement (an edit), a reference or something custom. Let's
                // see if the `related_event_id` is an in-thread event.
                RelationType::Annotation
                | RelationType::Replacement
                | RelationType::Reference
                | _ => {
                    if let Some(thread_root) =
                        find_thread_root(&related_event_id, &timeline.events[..nth], &maybe_room)
                            .await?
                    {
                        new_events_by_thread
                            .entry(thread_root)
                            .or_insert_with(default_entry)
                            .0
                            .events
                            .push(event.clone());
                    }
                }
            },

            // No explicit relation, okay, but it can still be related to a thread!
            None => {
                // We previously found events that are part of a thread, but we
                // didn't see the thread root yet. And guess what? This might be
                // this event!
                if let Some(event_id) = event.event_id()
                    && existing_threads.contains_key(event_id)
                {
                    new_events_by_thread
                        .entry(event_id.to_owned())
                        .or_insert_with(default_entry)
                        .0
                        .events
                        .push(event.clone());
                }
                // Otherwise, this event might be a redaction that applies to a thread.
                else if let Some(redaction_target) =
                    extract_redaction_target(event.raw(), redaction_rules)
                {
                    // A redacted thread root is part of its own thread. Other
                    // targets are found before the room redacts them.
                    let thread_root = if existing_threads.contains_key(&redaction_target) {
                        Some(redaction_target)
                    } else {
                        find_thread_root(&redaction_target, &timeline.events[..nth], &maybe_room)
                            .await?
                    };

                    if let Some(thread_root) = thread_root {
                        new_events_by_thread
                            .entry(thread_root)
                            .or_insert_with(default_entry)
                            .0
                            .events
                            .push(event.clone());
                    }
                }
            }
        }
    }

    for (thread_root, (read_receipt_event_id, read_receipt_event)) in
        filter_read_receipts_and_group_by(ephemeral, |receipt_thread| match receipt_thread {
            ReceiptThread::Thread(thread_id) => Some(thread_id),
            _ => None,
        })
    {
        // 1. Create an empty `Timeline` if it doesn't exist so that it triggers
        //    the update for this thread in `Caches`. This is done by
        //    `default_entry`.
        // 2. Accumulate the read receipt event.
        new_events_by_thread
            .entry(thread_root.to_owned())
            .or_insert_with(default_entry)
            .1
            .get_or_insert_with(|| ReceiptEventContent(BTreeMap::new()))
            .insert(read_receipt_event_id.clone(), read_receipt_event.clone());
    }

    Ok(new_events_by_thread)
}

/// Finds the thread that an event is part of, looking at this sync's
/// `earlier_events` first, then the room. Reactions and edits are part of the
/// thread of the event they relate to.
async fn find_thread_root(
    event_id: &EventId,
    earlier_events: &[TimelineEvent],
    maybe_room: &Option<StateLockReadGuard<'_, RoomEventCacheState>>,
) -> Result<Option<OwnedEventId>> {
    let Some(event) = find_raw_event(event_id, earlier_events, maybe_room).await? else {
        return Ok(None);
    };

    if let Some(thread_root) = extract_thread_root(&event) {
        return Ok(Some(thread_root));
    }

    let Some((_relation_type, related_event_id)) = extract_relation(&event) else {
        return Ok(None);
    };

    Ok(find_raw_event(&related_event_id, earlier_events, maybe_room)
        .await?
        .and_then(|related_event| extract_thread_root(&related_event)))
}

/// Looks for an event in this sync's `earlier_events`, then in the room.
async fn find_raw_event(
    event_id: &EventId,
    earlier_events: &[TimelineEvent],
    maybe_room: &Option<StateLockReadGuard<'_, RoomEventCacheState>>,
) -> Result<Option<Raw<AnySyncTimelineEvent>>> {
    if let Some(event) =
        earlier_events.iter().rev().find(|event| event.event_id() == Some(event_id))
    {
        return Ok(Some(event.raw().clone()));
    }

    Ok(match maybe_room {
        Some(room) => room.find_event(event_id).await?.map(|(_location, event)| event.into_raw()),
        None => None,
    })
}

pub fn aggregate_timeline_for_pinned_events(
    timeline: &Timeline,
    pinned_event_ids: &[OwnedEventId],
    redaction_rules: &RedactionRules,
) -> Timeline {
    let mut new_timeline = Timeline {
        limited: timeline.limited,
        prev_batch: timeline.prev_batch.clone(),
        events: Vec::new(),
    };

    // No events are pinned? The `Timeline` must be empty.
    if pinned_event_ids.is_empty() {
        return new_timeline;
    }

    // Look for events that relate to pinned events. We already know the
    // pinned-events, we don't need to look for them. We are only interested by
    // related events.
    for event in &timeline.events {
        match extract_relation(event.raw()) {
            // Ohh, this event relates to another event!
            Some((relation_type, related_event_id)) => match relation_type {
                // `event` relates to a thread: not what we want.
                RelationType::Thread => {}

                // `event` represents an annotation (e.g. reactions), a
                // replacement (an edit), a reference or something custom. Let's
                // see if the `related_event_id` is a pinned-event.
                RelationType::Annotation
                | RelationType::Replacement
                | RelationType::Reference
                | _ => {
                    if pinned_event_ids.contains(&related_event_id) {
                        new_timeline.events.push(event.clone());
                    }
                }
            },

            // No explicit relation, but it can be a redaction of a pinned-event!
            None => {
                if let Some(redaction_target) =
                    extract_redaction_target(event.raw(), redaction_rules)
                    && pinned_event_ids.contains(&redaction_target)
                {
                    new_timeline.events.push(event.clone());
                }
            }
        }
    }

    new_timeline
}

/// Filter (deserialised) ephemeral events to only keep the read receipts
/// matching a particular predicate.
///
/// Read receipts have a deep structure (an entanglement of `BTreeMap`). The
/// returned iterator returns the tuple `(OwnedEventId, Receipts)`, which is the
/// second level. However, the `predicate` is applied on the fourth level,
/// directly on the leaf of the read receipts.
///
/// The predicate is used with [`Iterator::filter_map`], and thus can return a
/// group key (it can be anything). This group key is associated to the tuple
/// mentioned earlier, and should be used to “group” read receipts. This is
/// useful when one wants to group read receipts by their `ReceiptThread` for
/// example.
fn filter_read_receipts_and_group_by<'e, F, G>(
    events: &'e [AnySyncEphemeralRoomEvent],
    predicate: F,
) -> impl Iterator<Item = (G, (&'e OwnedEventId, &'e Receipts))>
where
    F: Fn(&'e ReceiptThread) -> Option<G>,
{
    events
        .iter()
        .filter_map(|ephemeral| match ephemeral {
            AnySyncEphemeralRoomEvent::Receipt(receipt_event) => Some(receipt_event),
            _ => None,
        })
        .flat_map(|receipt_event| receipt_event.content.iter())
        .filter_map(move |(event_id, event_receipts)| {
            Some((
                predicate(&event_receipts.first_key_value()?.1.first_key_value()?.1.thread)?,
                (event_id, event_receipts),
            ))
        })
}
