import json
import time

import cattrs
import dask.dataframe as dd
import pandas as pd
import tabulate

from tsum import summarize


def data(n=1):
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
    ] * int(n)


if __name__ == "__main__":
    from dask.distributed import LocalCluster

    cluster = LocalCluster(n_workers=1, nthreads=8, diagnostics_port=8787)
    client = cluster.get_client()
    table = []

    for n in [1]:
        df = pd.DataFrame.from_records(data=data(n=n))
        ddf = dd.from_pandas(df, npartitions=4)
        t0 = time.perf_counter()
        patterns = summarize(ddf=ddf)
        t1 = time.perf_counter()
        dicts = [cattrs.unstructure(_) for _ in patterns]
        print(json.dumps(dicts, indent=4))

        table.append(
            {
                "Rows": len(ddf),
                "Time Taken (seconds)": (t1 - t0),
            }
        )

    print(tabulate.tabulate(table))
