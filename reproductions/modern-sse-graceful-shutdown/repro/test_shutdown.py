"""Focused expectations and actual chunked-wire controls, without SDK imports."""

import importlib.util
import socketserver
import threading
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location("shutdown_repro", Path(__file__).with_name("run_shutdown.py"))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class Expectations(unittest.TestCase):
    def cases(self, legacy="held", modern="held"):
        def case(state):
            return {
                "ok": True,
                "hung": state == "held",
                "returncode": -9 if state == "held" else -15,
                "asgi_response_error": state == "dirty",
                "stream_end": {"final_chunk_received": state == "clean"},
            }

        return {
            "idle": case("clean"),
            "modern_closed": case("clean"),
            "legacy_get": case(legacy),
            "listen": case(modern),
        }

    def test_default_remains_a_regression_exit(self):
        self.assertEqual(runner._selection(self.cases(), "record", "drains")["exit"], 1)
        self.assertEqual(runner._selection(self.cases(modern="clean"), "record", "drains")["exit"], 0)

    def test_both_generations_are_bound(self):
        for actual in ("held", "dirty", "clean"):
            for expected in ("held", "drains", "clean"):
                with self.subTest(actual=actual, expected=expected):
                    match = (
                        actual == "held"
                        if expected == "held"
                        else actual != "held"
                        if expected == "drains"
                        else actual == "clean"
                    )
                    self.assertEqual(runner._selection(self.cases(actual), expected, "held")["exit"], 0 if match else 1)
        self.assertEqual(runner._selection(self.cases("clean", "clean"), "clean", "held")["exit"], 1)

    def test_bad_setup_is_not_an_sdk_failure(self):
        for key in ("idle", "modern_closed", "listen", "legacy_get"):
            cases = self.cases()
            cases[key] = {"ok": False}
            self.assertEqual(runner._selection(cases, "held", "held")["exit"], 2)

    def test_crashed_process_is_not_a_successful_drain(self):
        cases = self.cases("clean", "clean")
        cases["modern_closed"]["returncode"] = 1
        self.assertEqual(runner._selection(cases, "clean", "clean")["exit"], 2)

    def test_clean_requires_observed_framing_not_only_process_exit(self):
        cases = self.cases("clean")
        for observed in (False, None):
            cases["legacy_get"]["stream_end"]["final_chunk_received"] = observed
            self.assertEqual(runner._selection(cases, "clean", "held")["exit"], 1)
            self.assertEqual(runner._selection(cases, "drains", "held")["exit"], 0)


class Wire(unittest.TestCase):
    def receive(self, final):
        wire = (
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/event-stream\r\n"
            b"Transfer-Encoding: chunked\r\n"
            b"Connection: close\r\n\r\n3\r\nabc\r\n" + (b"0\r\n\r\n" if final else b"")
        )

        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                self.request.recv(4096)
                self.request.sendall(wire)

        with socketserver.TCPServer(("127.0.0.1", 0), Handler) as server:
            thread = threading.Thread(target=server.handle_request)
            thread.start()
            held = runner._Held()
            try:
                opened = held.open(server.server_address[1], "GET", "/", None, {}, read_preview=False)
                self.assertEqual(opened["status"], 200)
                self.assertTrue(opened["chunked"])
                thread.join(2)
                self.assertFalse(thread.is_alive())
                return held.receive_end()
            finally:
                held.close()

    def test_final_zero_chunk_is_received(self):
        self.assertEqual(self.receive(True)["final_chunk_received"], True)

    def test_prompt_connection_close_is_not_a_final_zero_chunk(self):
        result = self.receive(False)
        self.assertFalse(result["body_complete"])
        self.assertFalse(result["final_chunk_received"])
        self.assertEqual(result["error"], "IncompleteRead")


if __name__ == "__main__":
    unittest.main()
