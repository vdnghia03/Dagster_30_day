# Day 4

The solution today is to use Dagster's `RetryPolicy`. Retry policies make it possible for a data pineline to be robust to transient errors such as flasky APIs, network hiccups, or temporary cpu/memory limitations. This pineline introduces a simple error and retry, but you can use more complex retry polices (such as backoff), or play around with adding retry policies in other places in the pipeline.

```python
from dagster import asset, RetryPolicy
import random

@asset(
    retry_policy = RetryPolicy(max_retries=4)
)
def a():
    if random.randint(1,2) > 1:
        raise Exception()
    return

@asset(
    deps = [a]
)
def b(): ...

@asset(
    deps = [b]
)
def c(): ...

```
