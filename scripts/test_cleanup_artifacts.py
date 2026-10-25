import importlib.util
import json
from pathlib import Path
import stat
import tempfile
import unittest
import zipfile


SPEC = importlib.util.spec_from_file_location(
    "cleanup_artifacts", Path(__file__).with_name("cleanup-artifacts.py")
)
cleanup = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cleanup)


class CleanupArtifactsTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name).resolve()
        self.previous_root = cleanup.ROOT
        cleanup.ROOT = self.root

    def tearDown(self):
        cleanup.ROOT = self.previous_root
        self.directory.cleanup()

    def test_rejects_parent_escape(self):
        with self.assertRaises(ValueError):
            cleanup.checked(self.root / ".." / "outside")

    def test_preserves_existing_evidence_without_overwriting(self):
        source, destination = self.root / "source.json", self.root / "saved/source.json"
        source.write_text('{"passed": true}', encoding="utf-8")
        record = cleanup.copy_verified(source, destination)
        self.assertEqual(record["sha256"], cleanup.digest(destination))
        source.write_text('{"passed": false}', encoding="utf-8")
        with self.assertRaises(RuntimeError):
            cleanup.copy_verified(source, destination)
        self.assertEqual(destination.read_text(encoding="utf-8"), '{"passed": true}')

    def test_bundle_keeps_unique_and_different_evidence(self):
        history = self.root / "results/history/old"
        history.mkdir(parents=True)
        (history / "same.csv").write_bytes(b"same\n")
        (history / "changed.json").write_bytes(b"{}")
        bundle = history / "full-dump.zip"
        with zipfile.ZipFile(bundle, "w") as stream:
            stream.writestr("same.csv", b"same\n")
            stream.writestr("changed.json", b'{"old":true}')
            stream.writestr("unique.log", b"passed\n")
            stream.writestr("binary.exe", b"MZpayload")
        archive = self.root / "results/history/new"
        new, checks = cleanup.extract_evidence(bundle, archive, True)
        self.assertEqual(len(new), 2)
        self.assertEqual(len(checks), 3)
        self.assertEqual((history / "changed.json").read_bytes(), b"{}")
        for record in new:
            self.assertEqual(cleanup.digest(Path(record["preserved_path"])), record["sha256"])

    def test_zip_escape_and_symlink_are_rejected(self):
        for name in ("../escape.json", "/absolute.json", "C:/absolute.json", "a\\escape.json"):
            with self.subTest(name=name):
                bundle = self.root / "bad.zip"
                with zipfile.ZipFile(bundle, "w") as stream:
                    entry = zipfile.ZipInfo("member.json")
                    entry.filename = name
                    stream.writestr(entry, b"{}")
                with self.assertRaises(ValueError):
                    cleanup.extract_evidence(bundle, self.root / "archive", True)
        bundle = self.root / "symlink.zip"
        entry = zipfile.ZipInfo("link.json")
        entry.create_system = 3
        entry.external_attr = (stat.S_IFLNK | 0o777) << 16
        with zipfile.ZipFile(bundle, "w") as stream:
            stream.writestr(entry, "../elsewhere")
        with self.assertRaises(ValueError):
            cleanup.extract_evidence(bundle, self.root / "archive", True)

    def test_only_successful_verification_qualifies_dump(self):
        dump = self.root / "sample.tar.zst"
        report = self.root / "result.json"
        report.write_text(json.dumps({"status": "failed"}), encoding="utf-8")
        self.assertIsNone(cleanup.successful_dump(dump))
        report.write_text(json.dumps({"status": "passed", "archive": {"sha256": "abc"}}), encoding="utf-8")
        self.assertEqual(cleanup.successful_dump(dump), str(report))
        self.assertEqual(cleanup.expected_dump_hash(report), "abc")


if __name__ == "__main__":
    unittest.main()
