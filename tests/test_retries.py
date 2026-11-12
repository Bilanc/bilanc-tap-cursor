import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

import requests_mock

import tap_cursor

USAGE_EVENTS_URL = f"{tap_cursor.BASE_URL}/teams/filtered-usage-events"
DAILY_USAGE_URL = f"{tap_cursor.BASE_URL}/teams/daily-usage-data"
AI_COMMITS_URL = f"{tap_cursor.BASE_URL}/analytics/ai-code/commits"

RATE_LIMITED = {
    "status_code": 429,
    "headers": {"Retry-After": "60"},
    "json": {"code": "error", "message": "Rate limit exceeded"},
}
SERVER_ERROR = {"status_code": 500, "json": {"code": "internal"}}


def usage_events_page(timestamps, has_next_page, user_email="dev@example.com"):
    return {
        "status_code": 200,
        "json": {
            "usageEvents": [
                {"timestamp": timestamp, "userEmail": user_email}
                for timestamp in timestamps
            ],
            "pagination": {"hasNextPage": has_next_page},
        },
    }


class RetryTest(unittest.TestCase):
    def setUp(self):
        self.schema = tap_cursor.load_schemas()["usage_events"]
        # One 30-day window, so each test controls every response.
        self.start_date = tap_cursor.singer.utils.strftime(tap_cursor.singer.utils.now())

        patches = [
            mock.patch.object(tap_cursor, "get_request_timeout", return_value=5),
            mock.patch.object(tap_cursor.time, "sleep"),
            mock.patch.object(tap_cursor.singer, "write_record"),
        ]
        self.sleep = None
        for patch in patches:
            mocked = patch.start()
            self.addCleanup(patch.stop)
            if patch.attribute == "sleep":
                self.sleep = mocked
            if patch.attribute == "write_record":
                self.write_record = mocked

    def sync_usage_events(self, state):
        return tap_cursor.get_usage_events(self.schema, state, [], self.start_date)

    def test_events_without_a_user_are_skipped_whatever_the_blank_looks_like(self):
        with requests_mock.Mocker() as api:
            api.post(USAGE_EVENTS_URL, [{
                "status_code": 200,
                "json": {
                    "usageEvents": [
                        {"timestamp": "1", "userEmail": "dev@example.com"},
                        {"timestamp": "2", "userEmail": ""},
                        {"timestamp": "3", "userEmail": "   "},
                        {"timestamp": "4", "userEmail": None},
                        {"timestamp": "5", "model": "grok-bot-default"},
                    ],
                    "pagination": {"hasNextPage": False},
                },
            }])
            self.sync_usage_events({})

        self.assertEqual(self.write_record.call_count, 1)
        self.assertEqual(self.write_record.call_args.args[1]["userEmail"], "dev@example.com")

    def test_rate_limited_page_is_retried_after_the_wait_cursor_asks_for(self):
        with requests_mock.Mocker() as api:
            api.post(USAGE_EVENTS_URL, [
                usage_events_page(["1"], has_next_page=True),
                RATE_LIMITED,
                usage_events_page(["2"], has_next_page=False),
            ])
            state = self.sync_usage_events({})

        self.assertEqual(self.write_record.call_count, 2)
        self.sleep.assert_called_once_with(60.0)
        self.assertIn("since", state["bookmarks"]["usage_events"])
        self.assertEqual([json.loads(r.body)["page"] for r in api.request_history], [1, 2, 2])
        self.assertEqual(json.loads(api.request_history[0].body)["pageSize"], 1000)

    def test_sync_that_retries_past_midnight_leaves_no_gap_for_the_next_run(self):
        clock = [datetime(2026, 11, 10, 23, 58, tzinfo=timezone.utc)]

        class Clock(datetime):
            @classmethod
            def now(cls, tz=None):
                return clock[0].replace(tzinfo=None)

        def sleep(seconds):
            clock[0] += timedelta(seconds=seconds)

        self.sleep.side_effect = sleep
        self.start_date = "2026-11-10T00:00:00Z"
        with mock.patch.object(tap_cursor, "datetime", Clock), \
                mock.patch.object(tap_cursor.singer.utils, "now", lambda: clock[0]), \
                requests_mock.Mocker() as api:
            api.post(USAGE_EVENTS_URL, [RATE_LIMITED] * 5 + [
                usage_events_page(["1"], has_next_page=False),
            ])
            state = self.sync_usage_events({})
            self.assertEqual(clock[0].day, 11)
            first_sync_end = json.loads(api.last_request.body)["endDate"]

            self.sync_usage_events(state)
            next_sync_start = json.loads(api.last_request.body)["startDate"]

        self.assertLessEqual(next_sync_start, first_sync_end)

    def test_server_error_backs_off_then_recovers(self):
        with requests_mock.Mocker() as api:
            api.post(USAGE_EVENTS_URL, [
                SERVER_ERROR,
                SERVER_ERROR,
                usage_events_page(["1"], has_next_page=False),
            ])
            self.sync_usage_events({})

        self.assertEqual([call.args[0] for call in self.sleep.call_args_list], [5, 10])
        self.assertEqual(self.write_record.call_count, 1)

    def test_sync_fails_without_moving_the_bookmark_when_retries_run_out(self):
        state = {}
        with requests_mock.Mocker() as api:
            api.post(USAGE_EVENTS_URL, [RATE_LIMITED])
            with self.assertRaises(tap_cursor.CursorException):
                self.sync_usage_events(state)

        self.assertEqual(api.call_count, tap_cursor.MAX_REQUEST_ATTEMPTS)
        self.assertEqual(state, {})

    def test_invalid_key_fails_the_sync_without_retrying(self):
        with requests_mock.Mocker() as api:
            api.post(USAGE_EVENTS_URL, status_code=401, json={"message": "Invalid Team API Key"})
            with self.assertRaises(tap_cursor.CursorException):
                self.sync_usage_events({})

        self.assertEqual(api.call_count, 1)
        self.sleep.assert_not_called()

    def test_daily_usage_fails_instead_of_skipping_the_window(self):
        schema = tap_cursor.load_schemas()["daily_usage"]
        with requests_mock.Mocker() as api:
            api.post(DAILY_USAGE_URL, status_code=401, json={"message": "Invalid Team API Key"})
            with self.assertRaises(tap_cursor.CursorException):
                tap_cursor.get_daily_usage(schema, {}, [], self.start_date)

    def test_ai_commit_metrics_still_skipped_for_non_enterprise_teams(self):
        schema = tap_cursor.load_schemas()["ai_commit_metrics"]
        with requests_mock.Mocker() as api:
            api.get(AI_COMMITS_URL, status_code=401, json={
                "message": "You must be a member of an enterprise team to access this resource",
            })
            state = tap_cursor.get_ai_commit_metrics(schema, {}, [], self.start_date)

        self.assertEqual(state, {})
        self.assertEqual(api.call_count, 1)


if __name__ == "__main__":
    unittest.main()
