"""Hosted mode basics: the port is validated and real orders are refused."""
import asyncio
import os
import unittest
from unittest import mock

from arbbot.health import DEFAULT_PORT, hosted_port
from arbbot.main import amain, parse_args


class PortTests(unittest.TestCase):
    def test_default_and_env(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("PORT", None)
            self.assertEqual(hosted_port(), DEFAULT_PORT)
        with mock.patch.dict(os.environ, {"PORT": "8080"}):
            self.assertEqual(hosted_port(), 8080)

    def test_bad_values_stop_startup(self):
        for bad in ("abc", "0", "70000", "-1"):
            with mock.patch.dict(os.environ, {"PORT": bad}):
                with self.assertRaises(SystemExit):
                    hosted_port()


class HostedFlagTests(unittest.TestCase):
    def test_flag_parses(self):
        self.assertTrue(parse_args(["--hosted"]).hosted)
        self.assertFalse(parse_args([]).hosted)

    def test_hosted_refuses_live(self):
        with mock.patch("arbbot.main.load_dotenv"), self.assertRaises(SystemExit) as cm:  # never read the real .env
            asyncio.run(amain(parse_args(["--hosted", "--live"])))
        self.assertIn("paper-only", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
