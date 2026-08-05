import bmark


def table_header(benchmark_name, shape):
    rows, cols = shape
    return f"{benchmark_name} (Table size: {rows:,d}, {cols:,d})"


def run_benchmark(benched, header, plot=False, **kwargs):
    bench = bmark.Benchmark(benched)
    kwargs.pop("quiet", None)
    result = bench.run(header=header, quiet=plot, **kwargs)
    if plot:
        result.plot()
    return result
