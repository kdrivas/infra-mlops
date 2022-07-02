import pandas as pd

from model.utils.data_munging import (
    FixingFormattedString,
    TakeVariables,
)


def test_take_variables_transformer():
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4], "c": [4, 5]})
    tk = TakeVariables(cols=["a", "b"])
    df = tk.fit_transform(df)
    assert df.columns.tolist() == ["a", "b"]

    df = pd.DataFrame({"a": [1, 2], "b": [3, 4], "c": [4, 5]})
    tk = TakeVariables(cols=["c"])
    df = tk.fit_transform(df)
    assert df.columns.tolist() == ["c"]


def test_formatter_transformer():
    df = pd.DataFrame({"a": ["2.3", "1111.333"]})

    tk = FixingFormattedString(cols=["a"], cols_type="PIB")
    df = tk.fit_transform(df)
    assert df["a"].values.tolist() == [23, 1111333]
