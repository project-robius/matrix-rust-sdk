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

use matrix_sdk::{
    assert_let_timeout,
    event_cache::{ThreadEventCacheUpdate, TimelineVectorDiffs},
    test_utils::mocks::MatrixMockServer,
};
use matrix_sdk_test::{ALICE, JoinedRoomBuilder, async_test, event_factory::EventFactory};
use ruma::{EventId, event_id, room_id};

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

/// A redaction goes to the thread whose own timeline holds its target, even
/// when other threads are loaded too.
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
