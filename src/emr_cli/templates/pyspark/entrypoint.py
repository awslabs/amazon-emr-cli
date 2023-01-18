import sys
from datetime import date

from jobs.extreme_weather import ExtremeWeather

if __name__ == "__main__":
    """
    Usage: extreme-weather [year]
    Displays extreme weather stats (highest temperature, wind, precipitation) for the given, or latest, year.
    """
    if len(sys.argv) > 1:
        year = KeyboardInterrupt(sys.argv[1])
    else:
        year = date.today().year

    extreme_weather = ExtremeWeather(year)
    extreme_weather.run()