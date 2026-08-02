"""Tests for bounded camera gRPC calls and feed cancellation."""

import asyncio
from concurrent import futures
import io
from types import SimpleNamespace
import unittest
from unittest import mock

from PIL import Image

from anki_vector import camera


class _CameraGrpcInterface:

    def __init__(self, image_data):
        self.image_data = image_data
        self.capture_timeout = None
        self.status_timeout = None

    async def CaptureSingleImage(self, _request, timeout=None):
        self.capture_timeout = timeout
        return SimpleNamespace(data=self.image_data, image_id=11)

    async def IsImageStreamingEnabled(self, _request, timeout=None):
        self.status_timeout = timeout
        return SimpleNamespace(is_image_streaming_enabled=True)


class _HangingCameraStream:

    def __init__(self):
        self.cancelled = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        await asyncio.Future()

    def cancel(self):
        self.cancelled = True


class _StreamGrpcInterface:

    def __init__(self, stream):
        self.stream = stream

    def CameraFeed(self, _request):
        return self.stream


class _TimedOutFuture:

    def __init__(self):
        self.result_timeout = None
        self.cancelled = False

    def result(self, timeout=None):
        self.result_timeout = timeout
        raise futures.TimeoutError()

    def cancel(self):
        self.cancelled = True


def _jpeg_data():
    output = io.BytesIO()
    Image.new('RGB', (8, 6), color=(20, 40, 60)).save(
        output, format='JPEG')
    return output.getvalue()


class CameraTests(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        self.addCleanup(self.loop.close)

    def _make_component(self, grpc_interface):
        connection = SimpleNamespace(grpc_interface=grpc_interface)
        robot = SimpleNamespace(conn=connection)
        component = object.__new__(camera.CameraComponent)
        component._robot = robot
        component._enabled = False
        component._image_annotator = mock.Mock()
        component._camera_feed_task = None
        component._camera_feed_stream = None
        component.logger = mock.Mock()
        return component

    def _capture(self, component, **kwargs):
        capture = camera.CameraComponent.capture_single_image.__wrapped__
        return self.loop.run_until_complete(capture(component, **kwargs))

    def test_capture_passes_timeout_to_grpc(self):
        grpc_interface = _CameraGrpcInterface(_jpeg_data())
        component = self._make_component(grpc_interface)

        image = self._capture(component, timeout=2.5)

        self.assertEqual(grpc_interface.capture_timeout, 2.5)
        self.assertEqual(image.image_id, 11)
        self.assertEqual(image.raw_image.size, (8, 6))

    def test_capture_rejects_non_positive_timeout(self):
        component = self._make_component(None)

        with self.assertRaises(ValueError):
            self._capture(component, timeout=0)

    def test_streaming_status_passes_timeout_to_grpc(self):
        grpc_interface = _CameraGrpcInterface(_jpeg_data())
        component = self._make_component(grpc_interface)

        enabled = self.loop.run_until_complete(
            component._image_streaming_enabled(1.5))

        self.assertTrue(enabled)
        self.assertEqual(grpc_interface.status_timeout, 1.5)

    def test_cancelled_camera_task_cancels_grpc_iterator(self):
        stream = _HangingCameraStream()
        component = self._make_component(_StreamGrpcInterface(stream))
        component._enabled = True
        task = self.loop.create_task(
            component._request_and_handle_images())
        self.loop.run_until_complete(asyncio.sleep(0))

        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            self.loop.run_until_complete(task)

        self.assertTrue(stream.cancelled)

    def test_close_camera_feed_has_outer_timeout(self):
        timed_out_future = _TimedOutFuture()

        def run_coroutine(coroutine):
            coroutine.close()
            return timed_out_future

        connection = SimpleNamespace(
            run_coroutine=run_coroutine,
            thread=object())
        component = self._make_component(None)
        component._robot.conn = connection
        component._camera_feed_task = object()

        with self.assertRaises(camera.VectorTimeoutException):
            component.close_camera_feed(timeout=0.25)

        self.assertEqual(timed_out_future.result_timeout, 1.25)
        self.assertTrue(timed_out_future.cancelled)


if __name__ == '__main__':
    unittest.main()
