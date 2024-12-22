#prompt Update asset A so that it fails half the time. Find a way to make the pipeline automatically more robust.

import dagster as dg
import random

@dg.asset(
    automation_condition= dg.AutomationCondition.on_cron("* * * * *")
    , retry_policy=dg.RetryPolicy(max_retries = 2)
)
def asset_one(context: dg.AssetExecutionContext) -> None:
    random_choise = random.random()
    context.log.info(f"Random choice: {random_choise}")
    if random_choise < 0.5:
        raise dg.Failure(
            description=f"Asset Faile Random choice {random_choise}"
            , metadata={
                "filepath": dg.MetadataValue.path("some_path/to_file"),
                "dashboard_url": dg.MetadataValue.url("http://www.google.com")
            }
        )
    context.log.info("Creating asset one")

@dg.asset(
        deps=["asset_one"],
        automation_condition=dg.AutomationCondition.any_downstream_conditions()
)
def asset_two(context: dg.AssetExecutionContext) -> None:
    context.log.info("Creating asset two")

@dg.asset(
        deps=["asset_two"],
         automation_condition=dg.AutomationCondition.on_cron("*/1 * * * *")
)
def asset_three(context: dg.AssetExecutionContext) -> None:
    context.log.info("Creating asset three")

defs = dg.Definitions(
    assets= [asset_one, asset_two, asset_three],
)