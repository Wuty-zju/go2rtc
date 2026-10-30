"""Publication policy regressions; temporary Git fixtures, no network or pushes."""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

SCRIPT = Path(__file__).with_name("fork-build.py").resolve()
spec = importlib.util.spec_from_file_location("fork_build", SCRIPT)
BUILD = importlib.util.module_from_spec(spec)
spec.loader.exec_module(BUILD)


class PublicationPolicy(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.previous = Path.cwd()
        os.chdir(self.root)
        self.git("init", "-q", "-b", "master")
        self.git("config", "user.name", "CI Fixture")
        self.git("config", "user.email", "ci@example.invalid")
        Path("README").write_text("upstream baseline\n")
        self.git("add", "README")
        self.git("commit", "-qm", "baseline")
        baseline = self.git("rev-parse", "HEAD")
        self.config = {"versions": {"master": "1.9.15.1", "dev": "1.9.15.1_dev"},
                       "upstream": {"master": baseline, "dev": baseline}}
        Path(".github").mkdir()
        self.write_config()
        self.git("add", ".github")
        self.git("commit", "-qm", "fork packaging")
        for branch in ["master", "dev"]:
            self.git("update-ref", "refs/remotes/origin/" + branch, "HEAD")

    def tearDown(self):
        os.chdir(self.previous)
        self.temp.cleanup()

    def git(self, *args):
        return subprocess.check_output(["git", *args], text=True, stderr=subprocess.STDOUT).strip()

    def write_config(self):
        Path(".github/distribution.json").write_text(json.dumps(self.config))

    def plan(self, **changes):
        out = self.root / "outputs"
        out.write_text("")
        env = {"EVENT_NAME": "push", "REF_TYPE": "branch", "REF_NAME": "master",
               "BASE_REF": "", "PUBLISH_INPUT": "false", "GITHUB_OUTPUT": str(out)}
        env.update(changes)
        with mock.patch.dict(os.environ, env), contextlib.redirect_stdout(io.StringIO()):
            BUILD.plan()
        return dict(line.split("=", 1) for line in out.read_text().splitlines())

    def test_branch_push_publishes_only_its_rolling_tag(self):
        for branch in ["master", "dev"]:
            with self.subTest(branch=branch):
                output = self.plan(REF_NAME=branch)
                self.assertEqual(output["publish"], "true")
                self.assertEqual(output["release"], "false")
                self.assertEqual(output["tag"], branch)
                self.assertEqual(output["version"], "")
                self.assertEqual(output["baseline_name"], branch)

    def test_configured_tag_push_has_exact_channel_and_version(self):
        for channel, version in self.config["versions"].items():
            with self.subTest(channel=channel):
                output = self.plan(REF_TYPE="tag", REF_NAME="v" + version)
                self.assertEqual(output["channel"], channel)
                self.assertEqual(output["tag"], "dev" if channel == "dev" else version)
                self.assertEqual(output["version"], version)
                self.assertEqual(output["publish"], "true")
                self.assertEqual(output["release"], "true")

    def test_manual_branch_publication_is_opt_in(self):
        self.assertEqual(self.plan(EVENT_NAME="workflow_dispatch")["publish"], "false")
        self.assertEqual(self.plan(EVENT_NAME="workflow_dispatch", PUBLISH_INPUT="true")["publish"], "true")

    def test_manual_tag_defaults_to_validation_only(self):
        output = self.plan(EVENT_NAME="workflow_dispatch", REF_TYPE="tag", REF_NAME="v1.9.15.1")
        self.assertEqual(output["publish"], "false")
        self.assertEqual(output["release"], "false")
        self.assertEqual(output["version"], "1.9.15.1")

    def test_manual_tag_publication_is_explicit(self):
        output = self.plan(EVENT_NAME="workflow_dispatch", REF_TYPE="tag", REF_NAME="v1.9.15.1", PUBLISH_INPUT="true")
        self.assertEqual(output["publish"], "true")
        self.assertEqual(output["release"], "true")
        self.assertEqual(output["version"], "1.9.15.1")

    def test_pull_request_cannot_publish(self):
        output = self.plan(EVENT_NAME="pull_request", REF_NAME="17/merge", BASE_REF="master", PUBLISH_INPUT="true")
        self.assertEqual(output["publish"], "false")
        self.assertEqual(output["release"], "false")

    def test_unconfigured_tag_is_rejected(self):
        with self.assertRaises(SystemExit):
            self.plan(REF_TYPE="tag", REF_NAME="v1.9.15.99")

    def test_feature_ref_cannot_publish(self):
        with self.assertRaises(SystemExit):
            self.plan(EVENT_NAME="workflow_dispatch", REF_NAME="feat/xiaomi-dual-lens", PUBLISH_INPUT="true")

    def test_wrong_channel_version_is_rejected(self):
        self.config["versions"]["dev"] = "1.9.15.1"
        self.write_config()
        with self.assertRaises(SystemExit):
            self.plan()

    def test_upstream_baseline_must_be_a_real_ancestor(self):
        tree = self.git("rev-parse", "HEAD^{tree}")
        unrelated = self.git("commit-tree", tree, "-m", "unrelated")
        self.config["upstream"]["master"] = unrelated
        self.write_config()
        with self.assertRaises(SystemExit):
            self.plan()

    def test_tag_must_belong_to_its_channel(self):
        self.git("update-ref", "refs/remotes/origin/master", "HEAD~1")
        with self.assertRaises(SystemExit):
            self.plan(REF_TYPE="tag", REF_NAME="v1.9.15.1")


if __name__ == "__main__":
    unittest.main()
