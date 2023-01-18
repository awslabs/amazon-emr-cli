import argparse
from datetime import date
from typing import List

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F

GSOD_S3_BASE = "s3://noaa-gsod-pds"


class ExtremeWeather:
    """
    Usage: extreme-weather [--year xxxx]

    Displays extreme weather stats (highest temp, wind, precipitation) for the given year.
    """

    def __init__(self, year: int) -> None:
        self.year = year
        self.spark = SparkSession.builder.appName("ExtremeWeather").getOrCreate()

    def run(self) -> None:
        df = self._fetch_data()
        for stat in [
            {"description": "Highest temperature", "column_name": "MAX", "units": "°F"},
            {
                "description": "Highest all-day average temperature",
                "column_name": "TEMP",
                "units": "°F",
            },
        ]:
            max_row = self.findLargest(df, stat.get("column_name"))
            print(f"--- {stat['description']}")
            print(
                f"    {max_row[stat['column_name']]}{stat['units']} on {max_row.DATE} at {max_row.NAME} ({max_row.LATITUDE}, {max_row.LONGITUDE})"
            )

            print("--- Top 10 Outliers")
            outliers = self.find_outliers_for_column(df, stat.get("column_name"))
            for i, row in outliers[:10].iterrows():
                print(
                    f"    {row['NAME']} ({row['DATE']}) – {row[stat['column_name']]}{stat['units']}"
                )
            print("\n")

    def find_outliers_for_column(
        self,
        df: DataFrame,
        col: str,
        percent: float = 0.99,
    ) -> DataFrame:
        """
        Converts the provided DataFrame to a Pandas DataFrame and retrieves the top 99% quantile.
        """
        dfp = df.toPandas()
        q = dfp.quantile(0.99)
        return dfp[dfp[col] > q[col]]

    def _gsod_year_uri(self, year: int) -> str:
        """
        Builds the s3 URI for the provided year
        """
        return f"{GSOD_S3_BASE}/{year}/"

    def _fetch_data(self) -> DataFrame:
        """
        Reads GSOD csv data for the specified year.
        """
        df = self.spark.read.csv(
            self._gsod_year_uri(self.year), header=True, inferSchema=True
        )
        return df

    def findLargest(self, df: DataFrame, col_name: str) -> Row:
        """
        Find the largest value in `col_name` column.
        Values of 99.99, 999.9 and 9999.9 are excluded because they indicate "no reading" for that attribute.
        While 99.99 _could_ be a valid value for temperature, for example, we know there are higher readings.
        """
        return (
            df.select(
                "STATION",
                "DATE",
                "LATITUDE",
                "LONGITUDE",
                "ELEVATION",
                "NAME",
                col_name,
            )
            .filter(~F.col(col_name).isin([99.99, 999.9, 9999.9]))
            .orderBy(F.desc(col_name))
            .limit(1)
            .first()
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=False, default=date.today().year)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    weather_data = ExtremeWeather(args.year)
    weather_data.run()