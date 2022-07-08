import datetime


def generateLastDaysPaths(date, days):
    date_ = datetime.datetime.strptime(date, '%Y%m%d')
    try:
        if days < 0:
            raise ValueError

        for day in range(days - 1, -1, -1):
            d = date_ - datetime.timedelta(days=day)
            year, month, day = d.year, d.strftime('%m'), d.day
            print(f'https://importantdata@location/{year}/{month}/{day}/')

    except ValueError:
        print("days have to positive")

generateLastDaysPaths("20210410", 10)
