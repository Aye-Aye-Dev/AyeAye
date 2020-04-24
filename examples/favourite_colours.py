import calendar
from collections import defaultdict
from datetime import datetime

import ayeaye


class FavouriteColours(ayeaye.Model):
    """
    Each person has one favourite colour at a time.

    Aggregate these into a summary of number of colour days in each month. This is the number of
    days, in each month, that each colour in the input dataset was someone's favourite colour.

    So if one person liked the colour Blue from 2020-01-01 until 2020-02-15 I'd expect to see this
    in the the output-

        "Blue": {
            "January": 31,
            "February": 14
        },

    This example model is to demonstrate a data validation within `post_build_check` that reveals
    a coding mistake. I wouldn't write code like this unless I'm demonstrating a coding mistake
    (honest). There is a unit test for this model but it misses the two mistakes as well.

    Data validation tests should compliment rather than substitute unit tests. They are another way
    to spot mistakes that can be more intuitive.
    """
    favourite_colours = ayeaye.Connect(engine_url='csv://data/favourite_colours.csv')

    # output is in readwrite mode because post_buil_check() reads it back in
    favourites_summary = ayeaye.Connect(
        engine_url='json://data/favourite_colours_summary.json;indent=4',
        access=ayeaye.AccessMode.READWRITE
    )

    date_format = "%Y-%m-%d"

    def pre_build_check(self):
        """
        Data validation example on input data.
        """
        error_message = ("This model is only designed to work with data from a single year. "
                         "Both {} and {} have been found in the input dataset."
                         )
        target_year = None
        for survey_record in self.favourite_colours:
            for check_field in ['start', 'end']:
                record_year = datetime.strptime(survey_record[check_field], self.date_format).year

                if target_year is None:
                    target_year = record_year

                if target_year != record_year:
                    self.log(error_message.format(target_year, record_year), "ERROR")
                    return False

        return True

    def build(self):

        by_colour = defaultdict(lambda: defaultdict(int))
        for survey_record in self.favourite_colours:

            start = datetime.strptime(survey_record.start, self.date_format)
            end = datetime.strptime(survey_record.end, self.date_format)
            date_delta = end - start

            unaccounted_days = date_delta.days
            while unaccounted_days > 0:

                for month in range(start.month, end.month + 1):

                    month_name = calendar.month_name[month]
                    days_in_month = calendar.monthrange(start.year, month)[1]

                    if days_in_month < unaccounted_days:
                        unaccounted_days -= days_in_month
                        by_colour[survey_record.colour][month_name] += days_in_month
                    else:
                        by_colour[survey_record.colour][month_name] += unaccounted_days
                        unaccounted_days = 0
                        break

        # write the results to a JSON file
        self.favourites_summary.data = by_colour

        self.log("Done!")

    def post_build_check(self):

        error_message = ("Total days in input doesn't match total days in output. Input has {} "
                         "days and output has {} days."
                         )

        input_days = 0
        for survey_record in self.favourite_colours:

            start = datetime.strptime(survey_record.start, self.date_format)
            end = datetime.strptime(survey_record.end, self.date_format)
            date_delta = end - start
            input_days += abs(date_delta.days)

        output_days = 0
        for month_days in self.favourites_summary.data.values():
            output_days += sum([days for days in month_days.values()])

        if input_days != output_days:
            self.log(error_message.format(input_days, output_days), "ERROR")
            return False

        return True


if __name__ == '__main__':
    m = FavouriteColours()
    m.go()
