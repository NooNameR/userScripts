import unittest
from rewriter import RealRewriter, NoopRewriter

class TestRewriterOne(unittest.TestCase):
    def setUp(self):
        self.real = RealRewriter(
            source="/mnt/cache",
            destination="/mnt/user0",
            _from="/data",
            to="/mnt/cache/data"
        )
        self.noop = NoopRewriter(
            source="/mnt/cache",
            destination="/mnt/user0"
        )

    def test_real_rewrite_source(self):
        result = self.real.on_source("/data/movies/movie.mkv")
        self.assertEqual(result, "/mnt/cache/data/movies/movie.mkv")

    def test_real_rewrite_destination(self):
        result = self.real.on_destination("/data/movies/movie.mkv")
        self.assertEqual(result, "/mnt/user0/data/movies/movie.mkv")

    def test_real_restore_source(self):
        result = self.real.restore("/mnt/cache/data/movies/movie.mkv")
        self.assertEqual(result, "/data/movies/movie.mkv")

    def test_real_restore_destination(self):
        result = self.real.restore("/mnt/user0/data/movies/movie.mkv")
        self.assertEqual(result, "/data/movies/movie.mkv")

    def test_real_restore_non_matching(self):
        result = self.real.restore("/unmatched/path/movie.mkv")
        self.assertEqual(result, "/unmatched/path/movie.mkv")

    def test_noop_rewrite(self):
        result = self.noop.on_source("/mnt/cache/movies/movie.mkv")
        self.assertEqual(result, "/mnt/cache/movies/movie.mkv")

    def test_noop_restore(self):
        result = self.noop.restore("/mnt/user0/movies/movie.mkv")
        self.assertEqual(result, "/mnt/cache/movies/movie.mkv")

class TestRewriterTwo(unittest.TestCase):
    def setUp(self):
        self.real = RealRewriter(
            source="/mnt/cache/data",
            destination="/mnt/user0/data",
            _from="/data",
            to="/mnt/cache/data"
        )
        self.noop = NoopRewriter(
            source="/mnt/cache/data",
            destination="/mnt/user0/data"
        )

    def test_real_rewrite_source(self):
        result = self.real.on_source("/data/movies/movie.mkv")
        self.assertEqual(result, "/mnt/cache/data/movies/movie.mkv")

    def test_real_rewrite_destination(self):
        result = self.real.on_destination("/data/movies/movie.mkv")
        self.assertEqual(result, "/mnt/user0/data/movies/movie.mkv")

    def test_real_restore_source(self):
        result = self.real.restore("/mnt/cache/data/movies/movie.mkv")
        self.assertEqual(result, "/data/movies/movie.mkv")

    def test_real_restore_destination(self):
        result = self.real.restore("/mnt/user0/data/movies/movie.mkv")
        self.assertEqual(result, "/data/movies/movie.mkv")

    def test_real_restore_non_matching(self):
        result = self.real.restore("/unmatched/path/movie.mkv")
        self.assertEqual(result, "/unmatched/path/movie.mkv")

    def test_noop_rewrite(self):
        result = self.noop.on_source("/mnt/cache/data/movies/movie.mkv")
        self.assertEqual(result, "/mnt/cache/data/movies/movie.mkv")

    def test_noop_restore(self):
        result = self.noop.restore("/mnt/user0/data/movies/movie.mkv")
        self.assertEqual(result, "/mnt/cache/data/movies/movie.mkv")


if __name__ == '__main__':
    unittest.main()
