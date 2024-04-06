from pytest import fixture
import pandas as pd
import dask.dataframe as dd


@fixture(scope="session")
def records():
    return [
        {"gender": "M", "age": "adult", "blood_pressure": "normal"},
        {"gender": "M", "age": "adult", "blood_pressure": "low"},
        {"gender": "M", "age": "adult", "blood_pressure": "normal"},
        {"gender": "M", "age": "adult", "blood_pressure": "high"},
        {"gender": "M", "age": "adult", "blood_pressure": "low"},
        {"gender": "F", "age": "child", "blood_pressure": "low"},
        {"gender": "M", "age": "child", "blood_pressure": "low"},
        {"gender": "F", "age": "child", "blood_pressure": "low"},
        {"gender": "M", "age": "teen", "blood_pressure": "high"},
        {"gender": "F", "age": "child", "blood_pressure": "normal"},
    ]


@fixture(scope="session")
def dataframe(records):
    df = pd.DataFrame.from_records(data=records)
    ddf = dd.from_pandas(df, npartitions=4)
    return ddf
