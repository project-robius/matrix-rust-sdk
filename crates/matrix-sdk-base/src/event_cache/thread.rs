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

//! Threads types related to the Event Cache.

use ruma::OwnedEventId;
use serde::{Deserialize, Serialize};

use crate::read_receipts::ReadReceipts;

/// All the information about a thread.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadInfo {
    /// The number of events in the thread.
    ///
    /// This doesn't include:
    /// - the thread root event itself
    /// - events sent by ignored users
    /// - redacted events.
    ///
    /// Thus, it can be zero!
    ///
    /// It's `None` until one of the thread's replies shows up in the thread's
    /// own timeline, e.g. while it only holds read receipts, or after a clear.
    // Older builds stored a plain count, with 0 for uncounted threads too, so
    // this uses a new key and their counts read back as `None`.
    #[serde(default, rename = "counted_replies")]
    pub number_of_replies: Option<u32>,

    /// The ID of the latest event in the thread, if any.
    #[serde(default)] // For backwards compatibility.
    pub latest_event: Option<OwnedEventId>,

    /// Read receipts for the current thread.
    pub read_receipts: ReadReceipts,
}

impl ThreadInfo {
    /// Create a new [`ThreadInfo`].
    pub fn new() -> Self {
        Self { number_of_replies: None, latest_event: None, read_receipts: ReadReceipts::default() }
    }
}

impl Default for ThreadInfo {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::ThreadInfo;

    #[test]
    fn test_older_number_of_replies_reads_back_as_uncounted() {
        let mut stored = serde_json::to_value(ThreadInfo::new()).unwrap();
        let stored_object = stored.as_object_mut().unwrap();
        stored_object.remove("counted_replies");
        stored_object.insert("number_of_replies".to_owned(), json!(0));
        let thread_info: ThreadInfo = serde_json::from_value(stored).unwrap();
        assert_eq!(thread_info.number_of_replies, None);

        let counted = ThreadInfo { number_of_replies: Some(3), ..ThreadInfo::new() };
        let thread_info: ThreadInfo =
            serde_json::from_value(serde_json::to_value(counted).unwrap()).unwrap();
        assert_eq!(thread_info.number_of_replies, Some(3));
    }
}
