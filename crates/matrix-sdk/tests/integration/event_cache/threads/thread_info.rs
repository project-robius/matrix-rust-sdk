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

use std::sync::Arc;

use matrix_sdk::{
    assert_let_timeout,
    event_cache::{RoomEventCacheUpdate, ThreadEventCacheUpdate, TimelineVectorDiffs},
    linked_chunk::{ChunkIdentifier, LinkedChunkId, Position, Update},
    store::StoreConfig,
    test_utils::mocks::MatrixMockServer,
};
use matrix_sdk_base::event_cache::{
    store::{EventCacheStore, MemoryStore},
    thread::ThreadInfo,
};
use matrix_sdk_common::cross_process_lock::CrossProcessLockConfig;
use matrix_sdk_test::{ALICE, JoinedRoomBuilder, async_test, event_factory::EventFactory};
use ruma::{
    EventId, event_id,
    events::{
        relation::Thread,
        room::encrypted::{
            EncryptedEventScheme, MegolmV1AesSha2ContentInit, Relation, RoomEncryptedEventContent,
        },
    },
    owned_device_id, room_id,
};

/// A thread isn't counted until a reply shows up in its own timeline, even
/// though the latest reply bundled with its root gets saved on its own.
#[async_test]
async fn test_thread_info_is_not_counted_before_a_reply_shows_up() {
    let server = MatrixMockServer::new().await;
    let client = server.client_builder().build().await;

    let event_cache = client.event_cache();
    event_cache.subscribe().unwrap();

    let room_id = room_id!("!r");
    let thread_id = event_id!("$t");
    let bundled_reply_id = event_id!("$bundled_reply");
    let reply_id = event_id!("$reply");
    let f = EventFactory::new().room(room_id).sender(*ALICE);

    server.sync_joined_room(&client, room_id).await;
    let (thread, _drop_handles) = event_cache.thread(room_id, thread_id).await.unwrap();
    let (_, mut thread_stream) = thread.subscribe().await.unwrap();

    // The thread root shows up with its bundled summary, so there's nothing to
    // count yet.
    server
        .sync_room(
            &client,
            JoinedRoomBuilder::new(room_id).add_timeline_event(
                f.text_msg("thread root").event_id(thread_id).with_bundled_thread_summary(
                    f.text_msg("bundled reply")
                        .in_thread(thread_id, thread_id)
                        .event_id(bundled_reply_id)
                        .into(),
                    42,
                    false,
                ),
            ),
        )
        .await;
    assert_let_timeout!(
        Ok(ThreadEventCacheUpdate::UpdateTimelineEvents(TimelineVectorDiffs { .. })) =
            thread_stream.recv()
    );

    // Then a reply shows up, which is the first thing that gets counted.
    server
        .sync_room(
            &client,
            JoinedRoomBuilder::new(room_id).add_timeline_event(
                f.text_msg("reply").in_thread(thread_id, thread_id).event_id(reply_id),
            ),
        )
        .await;
    assert_let_timeout!(
        Ok(ThreadEventCacheUpdate::UpdateTimelineEvents(TimelineVectorDiffs { .. })) =
            thread_stream.recv()
    );
    // The local count includes the saved bundled reply too.
    assert_let_timeout!(Ok(ThreadEventCacheUpdate::UpdateSummary(summary)) = thread_stream.recv());
    assert_eq!(summary.num_replies, 2);
    assert_eq!(summary.latest_reply.as_deref(), Some(reply_id));

    let thread_info = event_cache.thread_info(room_id, thread_id).await.unwrap().unwrap();
    assert_eq!(thread_info.number_of_replies, Some(2));
}

/// A thread's root is never its own latest reply, even when none of the replies
/// can be shown, e.g. because they can't be decrypted.
#[async_test]
async fn test_thread_root_is_never_its_own_latest_reply() {
    let server = MatrixMockServer::new().await;
    let client = server.client_builder().build().await;

    let event_cache = client.event_cache();
    event_cache.subscribe().unwrap();

    let room_id = room_id!("!r");
    let thread_id = event_id!("$t");
    let f = EventFactory::new().room(room_id).sender(*ALICE);

    server.sync_joined_room(&client, room_id).await;
    let (thread, _drop_handles) = event_cache.thread(room_id, thread_id).await.unwrap();
    let (_, mut thread_stream) = thread.subscribe().await.unwrap();

    // The root lands in the thread's own timeline.
    server
        .sync_room(
            &client,
            JoinedRoomBuilder::new(room_id)
                .add_timeline_event(f.text_msg("thread root").event_id(thread_id)),
        )
        .await;
    assert_let_timeout!(
        Ok(ThreadEventCacheUpdate::UpdateTimelineEvents(TimelineVectorDiffs { .. })) =
            thread_stream.recv()
    );

    // Then a reply shows up, which we can't decrypt.
    let utd_reply = f
        .event(RoomEncryptedEventContent::new(
            EncryptedEventScheme::MegolmV1AesSha2(
                MegolmV1AesSha2ContentInit {
                    ciphertext: "ciphertext".to_owned(),
                    sender_key: "sender_key".to_owned(),
                    device_id: owned_device_id!("DEVICE"),
                    session_id: "session".to_owned(),
                }
                .into(),
            ),
            Some(Relation::Thread(Thread::plain(thread_id.to_owned(), thread_id.to_owned()))),
        ))
        .event_id(event_id!("$utd_reply"));
    server.sync_room(&client, JoinedRoomBuilder::new(room_id).add_timeline_event(utd_reply)).await;
    assert_let_timeout!(
        Ok(ThreadEventCacheUpdate::UpdateTimelineEvents(TimelineVectorDiffs { .. })) =
            thread_stream.recv()
    );

    assert_let_timeout!(Ok(ThreadEventCacheUpdate::UpdateSummary(summary)) = thread_stream.recv());
    assert_eq!(summary.num_replies, 1);
    assert!(summary.latest_reply.is_none());
}

/// A thread root redacted while its thread cache holds it keeps its thread
/// summary in the store.
#[async_test]
async fn test_redacted_thread_root_keeps_its_summary() {
    let server = MatrixMockServer::new().await;
    let client = server.client_builder().build().await;

    let event_cache = client.event_cache();
    event_cache.subscribe().unwrap();

    let room_id = room_id!("!r");
    let thread_id = event_id!("$t");
    let latest_reply_id = event_id!("$latest_reply");
    let f = EventFactory::new().room(room_id).sender(*ALICE);

    server.sync_joined_room(&client, room_id).await;
    let (thread, _drop_handles) = event_cache.thread(room_id, thread_id).await.unwrap();
    let (_, mut thread_stream) = thread.subscribe().await.unwrap();

    server
        .sync_room(
            &client,
            JoinedRoomBuilder::new(room_id).add_timeline_event(
                f.text_msg("thread root").event_id(thread_id).with_bundled_thread_summary(
                    f.text_msg("latest reply")
                        .in_thread(thread_id, thread_id)
                        .event_id(latest_reply_id)
                        .into(),
                    42,
                    false,
                ),
            ),
        )
        .await;
    assert_let_timeout!(
        Ok(ThreadEventCacheUpdate::UpdateTimelineEvents(TimelineVectorDiffs { .. })) =
            thread_stream.recv()
    );

    server
        .sync_room(
            &client,
            JoinedRoomBuilder::new(room_id).add_timeline_event(f.redaction(thread_id)),
        )
        .await;
    assert_let_timeout!(
        Ok(ThreadEventCacheUpdate::UpdateTimelineEvents(TimelineVectorDiffs { .. })) =
            thread_stream.recv()
    );

    let store = client.event_cache_store().lock().await.unwrap();
    let stored_root =
        store.as_clean().unwrap().find_event(room_id, thread_id).await.unwrap().unwrap();
    assert!(stored_root.raw().deserialize().unwrap().is_redacted());
    let summary = stored_root.thread_summary().unwrap();
    assert_eq!(summary.num_replies, 42);
    assert_eq!(summary.latest_reply.as_deref(), Some(latest_reply_id));
}

/// A redaction goes to its target's thread, even when other threads are loaded
/// too.
#[async_test]
async fn test_redaction_goes_to_its_own_thread() {
    let server = MatrixMockServer::new().await;
    let client = server.client_builder().build().await;

    let event_cache = client.event_cache();
    event_cache.subscribe().unwrap();

    let room_id = room_id!("!r");
    let f = EventFactory::new().room(room_id).sender(*ALICE);
    // With more threads loaded, a misrouted redaction is less likely to land in
    // the right one by chance.
    let thread_ids =
        (0..6).map(|i| EventId::parse(format!("$thread_{i}")).unwrap()).collect::<Vec<_>>();
    let reply_ids =
        (0..6).map(|i| EventId::parse(format!("$reply_{i}")).unwrap()).collect::<Vec<_>>();

    server.sync_joined_room(&client, room_id).await;
    let mut threads = Vec::new();
    for thread_id in &thread_ids {
        let (thread, drop_handles) = event_cache.thread(room_id, thread_id).await.unwrap();
        let (_, stream) = thread.subscribe().await.unwrap();
        threads.push((thread, drop_handles, stream));
    }

    // Each thread gets a reply, which counts it.
    let mut room = JoinedRoomBuilder::new(room_id);
    for (thread_id, reply_id) in thread_ids.iter().zip(&reply_ids) {
        room = room.add_timeline_event(
            f.text_msg("reply").in_thread(thread_id, thread_id).event_id(reply_id),
        );
    }
    server.sync_room(&client, room).await;
    for (_, _, stream) in &mut threads {
        assert_let_timeout!(
            Ok(ThreadEventCacheUpdate::UpdateTimelineEvents(TimelineVectorDiffs { .. })) =
                stream.recv()
        );
        assert_let_timeout!(Ok(ThreadEventCacheUpdate::UpdateSummary(summary)) = stream.recv());
        assert_eq!(summary.num_replies, 1);
    }

    // Redacting the last thread's reply only updates that thread.
    server
        .sync_room(
            &client,
            JoinedRoomBuilder::new(room_id).add_timeline_event(f.redaction(&reply_ids[5])),
        )
        .await;
    let (_, _, stream) = &mut threads[5];
    assert_let_timeout!(
        Ok(ThreadEventCacheUpdate::UpdateTimelineEvents(TimelineVectorDiffs { .. })) =
            stream.recv()
    );
    assert_let_timeout!(Ok(ThreadEventCacheUpdate::UpdateSummary(summary)) = stream.recv());
    assert_eq!(summary.num_replies, 0);

    for (_, _, stream) in &threads[..5] {
        assert!(stream.is_empty());
    }
}

/// A reply and its redaction in the same sync leave the reply redacted, even
/// with its thread loaded.
#[async_test]
async fn test_reply_and_its_redaction_in_one_sync() {
    let server = MatrixMockServer::new().await;
    let client = server.client_builder().build().await;

    let event_cache = client.event_cache();
    event_cache.subscribe().unwrap();

    let room_id = room_id!("!r");
    let thread_id = event_id!("$t");
    let reply_id = event_id!("$reply");
    let f = EventFactory::new().room(room_id).sender(*ALICE);

    server.sync_joined_room(&client, room_id).await;
    let (thread, _drop_handles) = event_cache.thread(room_id, thread_id).await.unwrap();
    let (_, mut thread_stream) = thread.subscribe().await.unwrap();

    server
        .sync_room(
            &client,
            JoinedRoomBuilder::new(room_id)
                .add_timeline_event(
                    f.text_msg("secret").in_thread(thread_id, thread_id).event_id(reply_id),
                )
                .add_timeline_event(f.redaction(reply_id)),
        )
        .await;
    assert_let_timeout!(
        Ok(ThreadEventCacheUpdate::UpdateTimelineEvents(TimelineVectorDiffs { .. })) =
            thread_stream.recv()
    );

    let store = client.event_cache_store().lock().await.unwrap();
    let stored_reply =
        store.as_clean().unwrap().find_event(room_id, reply_id).await.unwrap().unwrap();
    assert!(stored_reply.raw().deserialize().unwrap().is_redacted());
}

/// A redaction recounts its reply's thread even when that thread isn't loaded,
/// e.g. after a restart.
#[async_test]
async fn test_redaction_reaches_a_thread_that_is_not_loaded() {
    let room_id = room_id!("!r");
    let thread_id = event_id!("$t");
    let reply_id = event_id!("$reply");
    let f = EventFactory::new().room(room_id).sender(*ALICE);

    // An earlier session stored the room with a counted thread, whose reply it
    // only saw in the room's timeline.
    let event_cache_store = Arc::new(MemoryStore::new());
    let chunk_id = ChunkIdentifier::new(0);
    event_cache_store
        .handle_linked_chunk_updates(
            LinkedChunkId::Room(room_id),
            vec![
                Update::NewItemsChunk { previous: None, new: chunk_id, next: None },
                Update::PushItems {
                    at: Position::new(chunk_id, 0),
                    items: vec![
                        f.text_msg("thread root").event_id(thread_id).into_event(),
                        f.text_msg("reply")
                            .in_thread(thread_id, thread_id)
                            .event_id(reply_id)
                            .into_event(),
                    ],
                },
            ],
        )
        .await
        .unwrap();
    let thread_info = ThreadInfo {
        number_of_replies: Some(1),
        latest_event: Some(reply_id.to_owned()),
        ..ThreadInfo::new()
    };
    event_cache_store.load_thread_info(room_id, thread_id, true).await.unwrap();
    event_cache_store.update_thread_info(room_id, thread_id, &thread_info).await.unwrap();

    let server = MatrixMockServer::new().await;
    let client = server
        .client_builder()
        .on_builder(|builder| {
            builder.store_config(
                StoreConfig::new(CrossProcessLockConfig::multi_process("hodor"))
                    .event_cache_store(event_cache_store.clone()),
            )
        })
        .build()
        .await;

    let event_cache = client.event_cache();
    event_cache.subscribe().unwrap();

    server.sync_joined_room(&client, room_id).await;
    let (room_event_cache, _drop_handles) = event_cache.room(room_id).await.unwrap();
    let (_, mut room_stream) = room_event_cache.subscribe().await.unwrap();

    server
        .sync_room(
            &client,
            JoinedRoomBuilder::new(room_id).add_timeline_event(f.redaction(reply_id)),
        )
        .await;
    assert_let_timeout!(
        Ok(RoomEventCacheUpdate::UpdateTimelineEvents(TimelineVectorDiffs { .. })) =
            room_stream.recv()
    );
    assert_let_timeout!(
        Ok(RoomEventCacheUpdate::UpdateThreadSummary { thread_root, thread_summary }) =
            room_stream.recv()
    );
    assert_eq!(thread_root, thread_id);
    assert_eq!(thread_summary.num_replies, 0);

    let thread_info = event_cache.thread_info(room_id, thread_id).await.unwrap().unwrap();
    assert_eq!(thread_info.number_of_replies, Some(0));
    assert!(thread_info.latest_event.is_none());
}

/// Redacting a reaction to a reply reaches the reply's thread.
#[async_test]
async fn test_reaction_redaction_goes_to_its_thread() {
    let server = MatrixMockServer::new().await;
    let client = server.client_builder().build().await;

    let event_cache = client.event_cache();
    event_cache.subscribe().unwrap();

    let room_id = room_id!("!r");
    let thread_id = event_id!("$t");
    let reply_id = event_id!("$reply");
    let reaction_id = event_id!("$reaction");
    let f = EventFactory::new().room(room_id).sender(*ALICE);

    server.sync_joined_room(&client, room_id).await;
    let (thread, _drop_handles) = event_cache.thread(room_id, thread_id).await.unwrap();
    let (_, mut thread_stream) = thread.subscribe().await.unwrap();

    server
        .sync_room(
            &client,
            JoinedRoomBuilder::new(room_id)
                .add_timeline_event(
                    f.text_msg("reply").in_thread(thread_id, thread_id).event_id(reply_id),
                )
                .add_timeline_event(f.reaction(reply_id, "👍").event_id(reaction_id)),
        )
        .await;
    assert_let_timeout!(
        Ok(ThreadEventCacheUpdate::UpdateTimelineEvents(TimelineVectorDiffs { .. })) =
            thread_stream.recv()
    );
    assert_let_timeout!(Ok(ThreadEventCacheUpdate::UpdateSummary(_)) = thread_stream.recv());

    server
        .sync_room(
            &client,
            JoinedRoomBuilder::new(room_id).add_timeline_event(f.redaction(reaction_id)),
        )
        .await;
    assert_let_timeout!(
        Ok(ThreadEventCacheUpdate::UpdateTimelineEvents(TimelineVectorDiffs { .. })) =
            thread_stream.recv()
    );

    let (events, _) = thread.subscribe().await.unwrap();
    let reaction = events.iter().find(|event| event.event_id() == Some(reaction_id)).unwrap();
    assert!(reaction.raw().deserialize().unwrap().is_redacted());
}

/// A redaction in a thread that isn't counted yet counts it, since the summary
/// bundled with its root is stale now.
#[async_test]
async fn test_redaction_counts_an_uncounted_thread() {
    let server = MatrixMockServer::new().await;
    let client = server.client_builder().build().await;

    let event_cache = client.event_cache();
    event_cache.subscribe().unwrap();

    let room_id = room_id!("!r");
    let thread_id = event_id!("$t");
    let latest_reply_id = event_id!("$latest_reply");
    let f = EventFactory::new().room(room_id).sender(*ALICE);

    server.sync_joined_room(&client, room_id).await;
    let (thread, _drop_handles) = event_cache.thread(room_id, thread_id).await.unwrap();
    let (_, mut thread_stream) = thread.subscribe().await.unwrap();

    server
        .sync_room(
            &client,
            JoinedRoomBuilder::new(room_id).add_timeline_event(
                f.text_msg("thread root").event_id(thread_id).with_bundled_thread_summary(
                    f.text_msg("latest reply")
                        .in_thread(thread_id, thread_id)
                        .event_id(latest_reply_id)
                        .into(),
                    42,
                    false,
                ),
            ),
        )
        .await;
    assert_let_timeout!(
        Ok(ThreadEventCacheUpdate::UpdateTimelineEvents(TimelineVectorDiffs { .. })) =
            thread_stream.recv()
    );

    server
        .sync_room(
            &client,
            JoinedRoomBuilder::new(room_id).add_timeline_event(f.redaction(latest_reply_id)),
        )
        .await;
    assert_let_timeout!(
        Ok(ThreadEventCacheUpdate::UpdateTimelineEvents(TimelineVectorDiffs { .. })) =
            thread_stream.recv()
    );
    assert_let_timeout!(Ok(ThreadEventCacheUpdate::UpdateSummary(summary)) = thread_stream.recv());
    assert_eq!(summary.num_replies, 0);
    assert!(summary.latest_reply.is_none());

    let thread_info = event_cache.thread_info(room_id, thread_id).await.unwrap().unwrap();
    assert_eq!(thread_info.number_of_replies, Some(0));
}
