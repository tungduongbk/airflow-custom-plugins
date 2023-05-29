
from datetime import datetime

utc_iso_format = '%Y-%m-%dT%H:%M:%S%z'


def parse_hour_date_from_utc_format(date_time: str) -> (str, int):
    converted = convert_to_datetime_from_utc(date_time)
    date_str = converted.date().strftime('%Y-%m-%d')
    return date_str


def parse_year_month_day_hour_from_utc(date_time: str) -> (int, int, int):
    converted = convert_to_datetime_from_utc(date_time)
    return converted.year, converted.month, converted.day, converted.hour


def change_format_date(date_time: str) -> str:
    date, hour = parse_hour_date_from_utc_format(date_time)
    return "{}T{:0>2}-00-00".format(date, hour)


def covert_format_date_from_utc(date_time: datetime, new_format='%Y%m%dT%H%M%S'):
    date_str = date_time.strftime(new_format)
    return date_str


def convert_to_datetime_from_utc(date_time: str, date_format='%Y-%m-%dT%H:%M:%S%z'):
    try:
        converted = datetime.strptime(date_time, date_format)
    except:
        converted = datetime.strptime(date_time, '%Y-%m-%dT%H:%M:%S.%f%z')
    return converted


def create_path_from_date_time(date_time: str):
    year, month, day, hour = parse_year_month_day_hour_from_utc(date_time)
    return "{}/{:0>2}/{:0>2}".format(year, month, day), "{:0>2}-00-00".format(hour)
