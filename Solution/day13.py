from dagster import asset, AssetExecutionContext

@asset
def a(context: AssetExecutionContext, day):

    return 

@asset(
    deps = [a]
)
def b(context: AssetExecutionContext, day):
    ...

