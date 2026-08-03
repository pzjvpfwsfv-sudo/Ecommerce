from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent.parent


class Chapter10ArtifactsTest(unittest.TestCase):
    def test_readme_closes_chapter9_and_renumbers_ai_evolution(self):
        text = (ROOT / "README.md").read_text(encoding="utf-8")
        for expected in (
            "第 9 章：Java DataStream 数据质量治理",
            "受控切流",
            "安全回滚",
            "第 10 章：受控工具调用与审计",
            "第 11 章：受控 NL2SQL",
            "第 10 章仍不生成或执行 SQL",
        ):
            self.assertIn(expected, text)


if __name__ == "__main__":
    unittest.main()
