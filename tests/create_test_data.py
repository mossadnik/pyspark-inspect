import yaml
import json
from pyspark.sql import SparkSession
from pyspark_inspect.parser.catalyst import get_dataframe_plan


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()

    with open('./sql.yaml') as f:
        test_cases = yaml.load(f, Loader=yaml.FullLoader)

    res = {}
    for tc in test_cases:
        sql = tc['sql']
        tc_id = tc['id']
        res[tc_id] = get_dataframe_plan(spark.sql(sql))

    with open('./sql-plan.json', 'w') as f:
        json.dump(res, f)
