"""Tests for bounded camera gRPC calls and feed cancellation."""

import asyncio
from types import SimpleNamespace
import unittest

from anki_vector import camera


class _CameraGrpcInterface:

    def __init__(self, stream=None):
        self.stream = stream
        self.capture_timeout = None
        self.status_timeout = None

    async def CaptureSingleImage(self, _request, timeout=None):
        self.capture_timeout = timeout
        return SimpleNamespace(data=b'')

    async def IsImageStreamingEnabled(self, _request, timeout=None):
        self.status_timeout = timeout
        return SimpleNamespace(is_image_streaming_enabled=False)

    def CameraFeed(self, _request):
        return self.stream


class _CameraStream:

    def __init__(self):
        self.cancelled = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        await asyncio.Future()

    def cancel(self):
        self.cancelled = True


class CameraTests(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        self.addCleanup(self.loop.close)

    def _make_component(self, grpc_interface):
        component = object.__new__(camera.CameraComponent)
        component._robot = SimpleNamespace(
            conn=SimpleNamespace(grpc_interface=grpc_interface))
        component._enabled = False
        component._camera_feed_stream = None
        component.logger = SimpleNamespace(debug=lambda *_: None, error=lambda *_: None)
        return component

    def test_camera_rpcs_receive_deadlines(self):
        grpc_interface = _CameraGrpcInterface()
        component = self._make_component(grpc_interface)
        capture = camera.CameraComponent.capture_single_image.__wrapped__
        self.loop.run_until_complete(capture(component, timeout=2.5))
        self.loop.run_until_complete(component._image_streaming_enabled(1.5))
        self.assertEqual(
            (grpc_interface.capture_timeout, grpc_interface.status_timeout),
            (2.5, 1.5))

    def test_camera_stream_is_cancelled_with_task(self):
        stream = _CameraStream()
        component = self._make_component(_CameraGrpcInterface(stream))
        component._enabled = True
        task = self.loop.create_task(component._request_and_handle_images())
        self.loop.run_until_complete(asyncio.sleep(0))
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            self.loop.run_until_complete(task)
        self.assertTrue(stream.cancelled)


if __name__ == '__main__':
    unittest.main()
