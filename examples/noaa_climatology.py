"""
Demonstrate reading a dataset from S3
"""
import ayeaye


class NoaaClimatology(ayeaye.Model):
    """
    Use data from-
    NOAA Global Historical Climatology Network Daily (GHCN-D)

    To find the hottest and coldest temperature measurements (in tenths of degrees C) for the
    year 1763 from data in a compressed CSV (comma separated values) file from S3.

    See
    https://registry.opendata.aws/noaa-ghcn/
    https://noaa-ghcn-pds.s3.amazonaws.com/csv.gz/1802.csv.gz

    Note-
    This is an public dataset that can be accessed without AWS credentials (--no-sign-request on
    the command line) but Ayeaye doesn't yet support boto's 'signature_version=UNSIGNED' so you
    will need a credential that is accessible to boto3. A convenient pattern is to put your keys
    in ~/.aws/credentials.
    """

    DEBUG = False

    weather_measurements_dictionary = {
        "ID": "11 character station identification code.",
        "YEAR/MONTH/DAY": "8 character date in YYYYMMDD format (e.g. 19860529 = May 29, 1986)",
        "ELEMENT": "4 character indicator of element type",
        "VALUE": "5 character data value for ELEMENT",
        "M-FLAG": "1 character Measurement Flag",
        "Q-FLAG": "1 character Quality Flag",
        "S-FLAG": "1 character Source Flag",
        "OBS-TIME": "4-character time of observation in hour-minute format (i.e. 0700 =7:00 am)",
    }

    # These CSV files don't have a header line so set it from data dictionary
    measurement_fields = list(weather_measurements_dictionary.keys())

    # data from the year 1763
    old_weather = ayeaye.Connect(
        engine_url="s3+gz+csv://noaa-ghcn-pds/csv.gz/1763.csv.gz",
        field_names=measurement_fields,
    )

    some_stations = ayeaye.Connect(
        engine_url="s3+gz+csv://noaa-ghcn-pds/csv.gz/by_station/AGM000603*.csv.gz",
        field_names=measurement_fields,
    )

    def build(self):
        coldest = hottest = None

        if self.DEBUG:
            # Demo of pattern match
            # It's here until I find a better place to demonstrate this
            for c in self.some_stations:
                self.log(f"Weather data is available from: {c.engine_url}")

        for measurement in self.old_weather:
            # TMAX = Maximum temperature (tenths of degrees C)
            # TMIN = Minimum temperature (tenths of degrees C)

            if measurement.ELEMENT not in ["TMAX", "TMIN"]:
                continue

            # log how many temperature measurements were parsed
            self.stats["temperature_readings"] += 1

            # cast to a number
            measurement.VALUE = float(measurement.VALUE)

            # use both temperatures as who knows if the hottest temp for one weather station is the
            # coldest for another station
            if coldest is None or measurement.VALUE < coldest.VALUE:
                coldest = measurement

            if hottest is None or measurement.VALUE > hottest.VALUE:
                hottest = measurement

        self.log(f"The maximum temperature measurement is: {hottest}")
        self.log(f"The minimum temperature measurement is: {coldest}")


if __name__ == "__main__":
    m = NoaaClimatology()
    m.go()
