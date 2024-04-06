import frozendict
from tsum import summarize


class TestTSum:
    def test_it_generates_proper_patterns(self, dataframe):
        expected = {
            frozendict.frozendict({"gender": "M", "age": "adult"}),
            frozendict.frozendict({"age": "child", "blood_pressure": "low"}),
        }
        patterns = summarize(ddf=dataframe)
        assert len(patterns) == 2
        assert {pattern.pattern for pattern in patterns} == expected
