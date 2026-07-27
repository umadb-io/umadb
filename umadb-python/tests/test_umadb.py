import datetime
import os
import signal
import threading
import unittest
from typing import Any
from uuid import uuid4

import time
from umadb import (
    AppendCondition,
    Client,
    Event,
    IntegrityError,
    Query,
    QueryItem,
    SequencedEvent,
    TrackingInfo,
    interrupt_all_stream_responses,
    stop_all_stream_responses,
)


class TestUmaDbClient(unittest.TestCase):
    def _generate_tagged_event(self, tag: str) -> Event:
        return Event(
            uuid=uuid4(),
            event_type="OrderCreated",
            data=b"12345",
            tags=[tag],
        )

    def _generate_tag(self) -> str:
        return "foo" + str(uuid4()) + ":" + "bar"

    def test_interrupt_stream_response_on_main_thread_next(self) -> None:
        client = Client("http://127.0.0.1:50051")
        subscription = client.subscribe(query=Query([QueryItem(tags=[str(uuid4())])]))

        def send_sigint_after_one_second() -> None:
            time.sleep(1)
            os.kill(os.getpid(), signal.SIGINT)

        sigint_thread = threading.Thread(target=send_sigint_after_one_second)
        sigint_thread.start()

        with self.assertRaises(KeyboardInterrupt):
            next(subscription)

    def test_interrupt_stream_response_on_main_thread_next_batch(self) -> None:
        client = Client("http://127.0.0.1:50051")
        subscription = client.subscribe(query=Query([QueryItem(tags=[str(uuid4())])]))

        def send_sigint_after_one_second() -> None:
            time.sleep(1)
            os.kill(os.getpid(), signal.SIGINT)

        sigint_thread = threading.Thread(target=send_sigint_after_one_second)
        sigint_thread.start()

        with self.assertRaises(KeyboardInterrupt):
            subscription.next_batch()

    def test_cancel_all_stream_responses_after_one_second(self) -> None:
        client = Client("http://127.0.0.1:50051")
        subscription = client.subscribe(query=Query([QueryItem(tags=[str(uuid4())])]))

        def cancel_after_one_second() -> None:
            time.sleep(1)
            interrupt_all_stream_responses()

        thread = threading.Thread(target=cancel_after_one_second)
        thread.start()

        with self.assertRaises(KeyboardInterrupt):
            next(subscription)

    def test_stop_all_stream_responses_after_one_second(self) -> None:
        client = Client("http://127.0.0.1:50051")
        subscription = client.subscribe(query=Query([QueryItem(tags=[str(uuid4())])]))

        def stop_after_one_second() -> None:
            time.sleep(1)
            stop_all_stream_responses()

        stopper_thread = threading.Thread(target=stop_after_one_second)
        stopper_thread.start()

        with self.assertRaises(StopIteration):
            next(subscription)

    def test_stop_subscription_thread(self) -> None:
        client = Client("http://127.0.0.1:50051")
        subscription1 = client.subscribe(query=Query([QueryItem(tags=[str(uuid4())])]))

        subscription_thread_errors = []

        def stop_after_one_second() -> None:
            time.sleep(1)
            stop_all_stream_responses()

        def block_on_subscription() -> None:
            subscription2 = client.subscribe(
                query=Query([QueryItem(tags=[str(uuid4())])])
            )
            try:
                next(subscription2)
            except BaseException as e:
                subscription_thread_errors.append(e)

        stopper_thread = threading.Thread(target=stop_after_one_second)
        stopper_thread.start()

        subscription_thread = threading.Thread(target=block_on_subscription)
        subscription_thread.start()

        with self.assertRaises(StopIteration):
            next(subscription1)

        subscription_thread.join(timeout=3)
        self.assertFalse(subscription_thread.is_alive())

        self.assertEqual(len(subscription_thread_errors), 1)
        self.assertIsInstance(subscription_thread_errors[0], StopIteration)

    def test_interrupt_subscription_thread(self) -> None:
        client = Client("http://127.0.0.1:50051")
        subscription1 = client.subscribe(query=Query([QueryItem(tags=[str(uuid4())])]))

        subscription_thread_errors = []

        def stop_after_one_second() -> None:
            time.sleep(1)
            interrupt_all_stream_responses()

        stopper_thread = threading.Thread(target=stop_after_one_second)
        stopper_thread.start()

        def block_on_subscription() -> None:
            subscription2 = client.subscribe(
                query=Query([QueryItem(tags=[str(uuid4())])])
            )
            try:
                next(subscription2)
            except BaseException as e:
                subscription_thread_errors.append(e)

        subscription_thread = threading.Thread(target=block_on_subscription)
        subscription_thread.start()

        with self.assertRaises(KeyboardInterrupt):
            next(subscription1)

        subscription_thread.join(timeout=3)
        self.assertFalse(subscription_thread.is_alive())

        self.assertEqual(len(subscription_thread_errors), 1)
        self.assertIsInstance(subscription_thread_errors[0], KeyboardInterrupt)

    def test_stop_subscription_after_one_second(self) -> None:
        client = Client("http://127.0.0.1:50051")
        subscription = client.subscribe(query=Query([QueryItem(tags=[str(uuid4())])]))

        def stop_after_one_second() -> None:
            time.sleep(1)
            subscription.stop()

        thread = threading.Thread(target=stop_after_one_second)
        thread.start()

        with self.assertRaises(StopIteration):
            next(subscription)

    def test_sigint_handler_cancels_all_stream_responses(self) -> None:
        client = Client("http://127.0.0.1:50051")
        subscription = client.subscribe(query=Query([QueryItem(tags=[str(uuid4())])]))

        def sigint_handler(*args: Any) -> None:
            # print("Handling sigint")
            interrupt_all_stream_responses()

        original_handler = signal.signal(signal.SIGINT, sigint_handler)

        def send_sigint_after_one_second() -> None:
            time.sleep(1)
#             print("Sending sigint")
            os.kill(os.getpid(), signal.SIGINT)

        sigint_thread = threading.Thread(target=send_sigint_after_one_second)
        sigint_thread.start()

        try:
            with self.assertRaises(KeyboardInterrupt):
                next(subscription)
        finally:
            sigint_thread.join()
            signal.signal(signal.SIGINT, original_handler)

    def test_sigint_handler_stops_all_stream_responses(self) -> None:
        client = Client("http://127.0.0.1:50051")
        subscription = client.subscribe(query=Query([QueryItem(tags=[str(uuid4())])]))

        def sigint_handler(*args: Any) -> None:
#             print("Handling sigint")
            stop_all_stream_responses()
            # raise KeyboardInterrupt

        original_handler = signal.signal(signal.SIGINT, sigint_handler)

        def send_sigint_after_one_second() -> None:
            time.sleep(1)
#             print("Sending sigint")
            os.kill(os.getpid(), signal.SIGINT)

        sigint_thread = threading.Thread(target=send_sigint_after_one_second)
        sigint_thread.start()

        try:
            with self.assertRaises(StopIteration):
                next(subscription)
        finally:
            sigint_thread.join()
            signal.signal(signal.SIGINT, original_handler)

    def test_sigint_handler_stops_all_stream_responses_then_raises(self) -> None:
        client = Client("http://127.0.0.1:50051")
        subscription = client.subscribe(query=Query([QueryItem(tags=[str(uuid4())])]))

        def sigint_handler(*args: Any) -> None:
#             print("Handling sigint")
            stop_all_stream_responses()
            raise KeyboardInterrupt

        original_handler = signal.signal(signal.SIGINT, sigint_handler)

        def send_sigint_after_one_second() -> None:
            time.sleep(1)
#             print("Sending sigint")
            os.kill(os.getpid(), signal.SIGINT)

        sigint_thread = threading.Thread(target=send_sigint_after_one_second)
        sigint_thread.start()

        try:
            with self.assertRaises(KeyboardInterrupt):
                next(subscription)
        finally:
            sigint_thread.join()
            signal.signal(signal.SIGINT, original_handler)

    def test_auto_interrupt_threaded_stream_responses(self) -> None:
        client = Client("http://127.0.0.1:50051")

        def kill_after_one_second() -> None:
            time.sleep(1)
            os.kill(os.getpid(), signal.SIGINT)

        subscription_errors = []

        def block_on_subscription() -> None:
            subscription2 = client.subscribe(
                query=Query([QueryItem(tags=[str(uuid4())])])
            )
            try:
                next(subscription2)
            except BaseException as e:
                subscription_errors.append(e)

        subscription_thread = threading.Thread(target=block_on_subscription)
        subscription_thread.start()

        killer_thread = threading.Thread(target=kill_after_one_second)
        killer_thread.start()

        try:
            with self.assertRaises(KeyboardInterrupt):
                try:
                    while True:
                        time.sleep(0.05)
                finally:
                    killer_thread.join()
        finally:
            subscription_thread.join(timeout=1)
            self.assertFalse(subscription_thread.is_alive())
            self.assertTrue(len(subscription_errors), 1)
            self.assertIsInstance(subscription_errors[0], KeyboardInterrupt)

    def test_client_context_manager_auto_stops_threaded_stream_responses_subscription(self) -> None:
        with Client("http://127.0.0.1:50051") as client:

            def block_on_subscription() -> None:
                subscription = client.subscribe(
                    query=Query([QueryItem(tags=[str(uuid4())])])
                )
                print("Calling next(subscription)")
                try:
                    next(subscription)
                except StopIteration:
                    pass
                finally:
                    print("Exited")

            subscription_thread = threading.Thread(target=block_on_subscription)
            subscription_thread.start()

            # All subscription to start, and register etc.
            time.sleep(1)

        try:
            subscription_thread.join(timeout=1)
            self.assertFalse(subscription_thread.is_alive())
        finally:
            stop_all_stream_responses()
            subscription_thread.join(timeout=1)
            self.assertFalse(subscription_thread.is_alive())

    def test_client_context_manager_auto_stops_threaded_stream_responses_read_response_before(self) -> None:
        with Client("http://127.0.0.1:50051") as client:

            tag1 = self._generate_tag()
            client.append(
                events=[
                    self._generate_tagged_event(tag1),
                    self._generate_tagged_event(tag1),
                ],
                condition=AppendCondition(
                    fail_if_events_match=Query(
                        items=[
                            QueryItem(
                                tags=[tag1],
                                types=["OrderCreated"],
                            )
                        ]
                    ),
                    after=0,
                ),
            )

            # Just check we have something to read.
            read_response = client.read(
                query=Query([QueryItem(tags=[tag1])])
            )
            next(read_response)

            del(read_response)

            read_response = client.read(
                query=Query([QueryItem(tags=[tag1])])
            )

        # After exiting, check the read response just stops.
        with self.assertRaises(StopIteration):
            next(read_response)

    def test_client_context_manager_auto_stops_threaded_stream_responses_read_response_after(self) -> None:
        with Client("http://127.0.0.1:50051") as client:

            tag1 = self._generate_tag()
            client.append(
                events=[
                    self._generate_tagged_event(tag1),
                    self._generate_tagged_event(tag1),
                ],
                condition=AppendCondition(
                    fail_if_events_match=Query(
                        items=[
                            QueryItem(
                                tags=[tag1],
                                types=["OrderCreated"],
                            )
                        ]
                    ),
                    after=0,
                ),
            )

            read_response = client.read(
                query=Query([QueryItem(tags=[tag1])])
            )
            # This loads a batch.
            next(read_response)


        # After exiting, check the read response just stops.
        with self.assertRaises(StopIteration):
            next(read_response)

    def test_client_del_auto_stops_threaded_stream_responses(self) -> None:
        client = Client("http://127.0.0.1:50051")

        def block_on_subscription() -> None:
            subscription = client.subscribe(
                query=Query([QueryItem(tags=[str(uuid4())])])
            )
            try:
                next(subscription)
            except StopIteration:
                pass

        subscription_thread = threading.Thread(target=block_on_subscription)
        subscription_thread.start()

        # All subscription to start, and register etc.
        time.sleep(1)

        del client

        try:
            subscription_thread.join(timeout=1)
            self.assertFalse(subscription_thread.is_alive())
        finally:
            stop_all_stream_responses()
            subscription_thread.join(timeout=1)
            self.assertFalse(subscription_thread.is_alive())

    # @skipIf("TEST_BENCHMARK_NUM_ITERS" not in os.environ, "Don't mess up the tags")
    def test_benchmark_dcb_append(self) -> None:
        # Just for comparison with Axon Server.
        client = Client("http://127.0.0.1:50051")

        print()
        num_iters = int(os.environ.get("TEST_BENCHMARK_NUM_ITERS", 3))
        for i in range(num_iters):
            start = datetime.datetime.now()
            num_per_iter = 1000
            for j in range(num_per_iter):
                tag1 = self._generate_tag()
                client.append(
                    events=[self._generate_tagged_event(tag1)],
                    condition=AppendCondition(
                        fail_if_events_match=Query(
                            items=[
                                QueryItem(
                                    tags=[tag1],
                                    types=["OrderCreated"],
                                )
                            ]
                        ),
                        after=0,
                    ),
                )
            duration = datetime.datetime.now() - start
            rate = num_per_iter / duration.total_seconds()
            print(f"After {(i + 1) * num_per_iter:} events, rate: {rate:.0f} events/s")


class TestBasicUsage(unittest.TestCase):
    """Tests mirroring ``umadb-python/examples/basic_usage.py``.

    The UmaDB server is shared and persists across every test (it is started
    once by the ``test-umadb-python-unittests`` Makefile target), so each test
    isolates the events it cares about by tagging them with a value that is
    unique to that test run. Reads are then filtered on that unique tag.
    """

    def setUp(self) -> None:
        self.client = Client("http://127.0.0.1:50051")
        # A tag unique to this test invocation, used to isolate the events this
        # test appends from anything else already present on the shared server.
        self.run_tag = "basic-usage:" + str(uuid4())

    def _query(self) -> Query:
        return Query(items=[QueryItem(tags=[self.run_tag])])

    def _append_sample_events(self) -> int:
        """Appends three tagged events (matching basic_usage.py) and returns
        the last position."""
        events = [
            Event(
                event_type="UserCreated",
                data=b'{"user_id": "123", "name": "Alice"}',
                tags=["user", "user:123", self.run_tag],
                metadata={
                    "source": "basic_usage",
                    "correlation_id": str(uuid4()),
                },
            ),
            Event(
                event_type="UserUpdated",
                data=b'{"user_id": "123", "email": "alice@example.com"}',
                tags=["user", "user:123", self.run_tag],
            ),
            Event(
                event_type="OrderCreated",
                data=b'{"order_id": "456", "user_id": "123"}',
                tags=["order", "user:123", "order:456", self.run_tag],
            ),
        ]
        return self.client.append(events)

    def test_head_returns_optional_int(self) -> None:
        head = self.client.head()
        self.assertTrue(head is None or isinstance(head, int))

    def test_head_advances_after_append(self) -> None:
        position = self._append_sample_events()
        self.assertIsInstance(position, int)
        head = self.client.head()
        self.assertIsNotNone(head)
        assert head is not None
        self.assertGreaterEqual(head, position)

    def test_append_and_read_all_for_run(self) -> None:
        self._append_sample_events()
        response = self.client.read(query=self._query())
        events = list(response)
        self.assertEqual(len(events), 3)
        for seq_event in events:
            self.assertIsInstance(seq_event, SequencedEvent)
            self.assertIn(self.run_tag, seq_event.event.tags)
        self.assertEqual(
            [se.event.event_type for se in events],
            ["UserCreated", "UserUpdated", "OrderCreated"],
        )

    def test_read_response_head(self) -> None:
        self._append_sample_events()
        response = self.client.read(query=self._query())
        head = response.head()
        self.assertTrue(head is None or isinstance(head, int))
        # Consuming the events should not raise and head remains accessible.
        list(response)
        self.assertTrue(response.head() is None or isinstance(response.head(), int))

    def test_event_metadata_round_trip(self) -> None:
        correlation_id = str(uuid4())
        self.client.append(
            [
                Event(
                    event_type="UserCreated",
                    data=b"{}",
                    tags=[self.run_tag],
                    metadata={"source": "basic_usage", "correlation_id": correlation_id},
                )
            ]
        )
        events = list(self.client.read(query=self._query()))
        self.assertEqual(len(events), 1)
        metadata = events[0].event.metadata
        self.assertEqual(metadata["source"], "basic_usage")
        self.assertEqual(metadata["correlation_id"], correlation_id)

    def test_read_with_query_filter_by_tags(self) -> None:
        self._append_sample_events()
        query = Query(items=[QueryItem(tags=["user", self.run_tag])])
        events = list(self.client.read(query=query))
        # Only UserCreated and UserUpdated carry the "user" tag.
        self.assertEqual(len(events), 2)
        self.assertEqual(
            [se.event.event_type for se in events],
            ["UserCreated", "UserUpdated"],
        )

    def test_read_with_multiple_filters_or_logic(self) -> None:
        self._append_sample_events()
        query = Query(
            items=[
                QueryItem(types=["UserCreated"], tags=[self.run_tag]),
                QueryItem(types=["OrderCreated"], tags=[self.run_tag]),
            ]
        )
        events = list(self.client.read(query=query))
        self.assertEqual(
            [se.event.event_type for se in events],
            ["UserCreated", "OrderCreated"],
        )

    def test_read_with_limit_forwards(self) -> None:
        self._append_sample_events()
        events = list(self.client.read(query=self._query(), limit=2))
        self.assertEqual(len(events), 2)
        self.assertEqual(
            [se.event.event_type for se in events],
            ["UserCreated", "UserUpdated"],
        )

    def test_read_with_limit_backwards(self) -> None:
        self._append_sample_events()
        events = list(self.client.read(query=self._query(), limit=2, backwards=True))
        self.assertEqual(len(events), 2)
        # Reading backwards yields the most recent events first.
        self.assertEqual(
            [se.event.event_type for se in events],
            ["OrderCreated", "UserUpdated"],
        )

    def test_read_backwards_without_limit(self) -> None:
        self._append_sample_events()
        events = list(self.client.read(query=self._query(), backwards=True))
        self.assertEqual(
            [se.event.event_type for se in events],
            ["OrderCreated", "UserUpdated", "UserCreated"],
        )
        positions = [se.position for se in events]
        self.assertEqual(positions, sorted(positions, reverse=True))

    def test_read_forwards_positions_ascending(self) -> None:
        self._append_sample_events()
        events = list(self.client.read(query=self._query()))
        positions = [se.position for se in events]
        self.assertEqual(positions, sorted(positions))

    def test_collect_with_head(self) -> None:
        self._append_sample_events()
        response = self.client.read(query=self._query())
        events, head = response.collect_with_head()
        self.assertEqual(len(events), 3)
        self.assertTrue(head is None or isinstance(head, int))

    def test_next_batch(self) -> None:
        self._append_sample_events()
        response = self.client.read(query=self._query())
        collected = []
        while True:
            batch = response.next_batch()
            if not batch:
                break
            collected.extend(batch)
        self.assertEqual(len(collected), 3)

    def test_conditional_append_fails_on_match(self) -> None:
        self._append_sample_events()
        fail_query = Query(
            items=[QueryItem(types=["UserCreated"], tags=["user:123", self.run_tag])]
        )
        condition = AppendCondition(fail_if_events_match=fail_query)
        duplicate_event = Event(
            event_type="UserCreated",
            data=b'{"user_id": "123", "name": "Alice Again"}',
            tags=["user", "user:123", self.run_tag],
        )
        with self.assertRaises(IntegrityError):
            self.client.append([duplicate_event], condition=condition)

    def test_conditional_append_succeeds_for_new_user(self) -> None:
        new_user_tag = str(uuid4())
        new_user_event = Event(
            event_type="UserCreated",
            data=b'{"user_id": "456", "name": "Bob"}',
            tags=["user", new_user_tag, self.run_tag],
        )
        fail_query = Query(
            items=[QueryItem(types=["UserCreated"], tags=[new_user_tag])]
        )
        condition = AppendCondition(fail_if_events_match=fail_query)
        position = self.client.append([new_user_event], condition=condition)
        self.assertIsInstance(position, int)

    def test_get_tracking_info_unknown_source(self) -> None:
        self.assertIsNone(self.client.get_tracking_info("not-a-source:" + str(uuid4())))

    def test_tracking_info_round_trip(self) -> None:
        tracking_source = "example-source:" + str(uuid4())
        pos_before = self.client.get_tracking_info(tracking_source)
        self.assertIsNone(pos_before)

        tracking_info = TrackingInfo(tracking_source, (pos_before or 0) + 1)
        self.assertEqual(tracking_info.source, tracking_source)
        self.assertEqual(tracking_info.position, 1)

        events = [
            Event(
                event_type="UpstreamCommitted",
                data=b"{}",
                tags=["tracking-demo", self.run_tag],
            )
        ]
        self.client.append(events, tracking_info=tracking_info)

        pos_after = self.client.get_tracking_info(tracking_source)
        self.assertEqual(pos_after, tracking_info.position)

    def test_tracking_info_duplicate_position_fails(self) -> None:
        tracking_source = "example-source:" + str(uuid4())
        tracking_info = TrackingInfo(tracking_source, 1)
        events = [
            Event(
                event_type="UpstreamCommitted",
                data=b"{}",
                tags=["tracking-demo", self.run_tag],
            )
        ]
        self.client.append(events, tracking_info=tracking_info)
        self.assertEqual(self.client.get_tracking_info(tracking_source), 1)

        with self.assertRaises(IntegrityError):
            self.client.append(events, tracking_info=tracking_info)

