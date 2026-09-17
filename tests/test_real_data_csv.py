import importlib
import io
import unittest


class BoundedCsvTest(unittest.TestCase):
    def setUp(self):
        try:
            self.reader = importlib.import_module("generators.real_data.csv_io").bounded_csv_reader
        except ModuleNotFoundError as exc:
            self.fail(f"bounded CSV reader is missing: {exc}")

    def test_quoted_multiline_and_blank_records_keep_their_identity(self):
        source = io.StringIO('a,"b\nc"\n\nd,e\n', newline="")
        self.assertEqual([["a", "b\nc"], [], ["d", "e"]], list(self.reader(source, max_chars=8)))

    def test_size_bound_is_applied_before_reading_a_wide_physical_line(self):
        class BoundedRead(io.StringIO):
            def readline(self, size=-1):
                if not 0 < size <= 17:
                    raise AssertionError("unbounded physical line read")
                return super().readline(size)

        with self.assertRaisesRegex(ValueError, "record.*limit"):
            list(self.reader(BoundedRead("x," * 100000), max_chars=16))

    def test_multiline_record_has_one_shared_budget(self):
        with self.assertRaisesRegex(ValueError, "record.*limit"):
            list(self.reader(io.StringIO('"a\nb\nc",d\n'), max_chars=8))

    def test_record_budget_resets_and_allows_exact_limit(self):
        self.assertEqual([["ab"], ["cd"]], list(self.reader(io.StringIO("ab\ncd\n"), max_chars=3)))
        self.assertEqual([["abc"]], list(self.reader(io.StringIO("abc"), max_chars=3)))


if __name__ == "__main__":
    unittest.main()
