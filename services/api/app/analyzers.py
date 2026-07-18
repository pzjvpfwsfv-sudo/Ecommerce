from __future__ import annotations

from typing import Protocol

from app.analysis_models import AnalysisContext, AnalysisNarrative


class MetricAnalyzer(Protocol):
    name: str

    def analyze(self, context: AnalysisContext) -> AnalysisNarrative: ...


class RuleBasedAnalyzer:
    name = "rule_based"

    def analyze(self, context: AnalysisContext) -> AnalysisNarrative:
        realtime = context.evidence.realtime
        historical = context.evidence.historical
        summary = self._summary(realtime.pv, realtime.uv)
        insights: list[str] = []
        risks = ["\u5f53\u524d\u5b9e\u65f6\u6307\u6807\u4e3a\u7d2f\u8ba1\u503c\uff0c\u7f3a\u5c11\u65f6\u95f4\u7a97\u53e3\u5bf9\u7167\u65f6\u4e0d\u80fd\u5224\u65ad\u4e0a\u6da8\u6216\u4e0b\u964d\u3002"]
        actions = ["\u8865\u5145\u5206\u949f\u7ea7\u6216\u5c0f\u65f6\u7ea7\u7a97\u53e3\u6307\u6807\u540e\uff0c\u518d\u5224\u65ad\u53d8\u5316\u8d8b\u52bf\u3002"]

        if realtime.pv is not None and realtime.uv is not None and realtime.uv > 0:
            insights.append(f"\u4eba\u5747\u8bbf\u95ee\u6b21\u6570\u7ea6\u4e3a {realtime.pv / realtime.uv:.1f} \u6b21\u3002")
        elif realtime.uv == 0:
            risks.append("UV \u4e3a 0\uff0c\u5f53\u524d\u8bc1\u636e\u4e0d\u8db3\u4ee5\u8ba1\u7b97\u4eba\u5747\u8bbf\u95ee\u6b21\u6570\u3002")

        if historical and historical.event_count is not None:
            insights.append(f"\u5386\u53f2\u660e\u7ec6\u5171\u5305\u542b {historical.event_count} \u6761\u884c\u4e3a\u4e8b\u4ef6\u3002")
            if historical.event_type_counts:
                top_type, top_count = max(historical.event_type_counts.items(), key=lambda item: item[1])
                share = top_count / historical.event_count if historical.event_count > 0 else 0
                insights.append(f"\u5386\u53f2\u884c\u4e3a\u4ee5 {top_type} \u4e3a\u4e3b\uff0c\u5360\u6bd4\u7ea6 {share:.1%}\u3002")

        return AnalysisNarrative(summary=summary, insights=insights, risks=risks, actions=actions)

    @staticmethod
    def _summary(pv: int | None, uv: int | None) -> str:
        if pv is None or uv is None:
            return "\u5b9e\u65f6 PV/UV \u6307\u6807\u4e0d\u5b8c\u6574\uff0c\u6682\u65f6\u53ea\u80fd\u63d0\u4f9b\u6709\u9650\u5206\u6790\u3002"
        return f"\u5f53\u524d\u7d2f\u8ba1\u8bbf\u95ee {pv} \u6b21\uff0c\u8986\u76d6 {uv} \u540d\u7528\u6237\u3002"
