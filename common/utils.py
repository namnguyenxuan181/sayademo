from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from calendar import MONDAY, SUNDAY


class RunDate(str):
    FORMAT = "%Y%m%d"

    def __new__(cls, value):
        if isinstance(value, (datetime, date)):
            return super().__new__(cls, value.strftime(cls.FORMAT))
        elif isinstance(value, (str, int)):
            value = str(value)
            # Validate format
            datetime.strptime(value, cls.FORMAT)
            return super().__new__(cls, value)
        else:
            raise TypeError(
                f"RunDate() got an unexpected type `{type(value)}`. "
                f"Must be datetime, date or str in format {cls.FORMAT}"
            )

    def to_date(self) -> date:
        return self.to_datetime().date()

    def to_datetime(self) -> datetime:
        return datetime.strptime(self, self.FORMAT)

    def begin_of_week(self):
        return self + timedelta(days=MONDAY - self.to_date().weekday())

    def end_of_week(self):
        return self + timedelta(days=SUNDAY - self.to_date().weekday())

    def begin_of_month(self):
        return RunDate(self.to_date().replace(day=1))

    def begin_of_last_month(self):
        return RunDate((self.to_date() - relativedelta(months=1)).replace(day=1))

    def end_of_month(self):
        return self.begin_of_month() + relativedelta(months=1) - relativedelta(days=1)

    def __add__(self, other):
        if isinstance(other, (timedelta, relativedelta)):
            return RunDate(self.to_datetime() + other)
        return super().__add__(other)

    def __sub__(self, other):
        if isinstance(other, (timedelta, relativedelta)):
            return RunDate(self.to_datetime() - other)
        elif isinstance(other, datetime):
            return self.to_datetime() - other
        elif isinstance(other, date):
            return self.to_date() - other
        elif isinstance(other, RunDate):
            return self.to_datetime() - other.to_datetime()
        raise TypeError(f"unsupported operand type(s) for -: '{type(self)}' and '{type(other)}'")

    def __eq__(self, other):
        if isinstance(other, datetime):
            return self.to_datetime() == other
        elif isinstance(other, date):
            return self.to_date() == other
        elif isinstance(other, int):
            other = str(other)
        return super().__eq__(other)

    def __lt__(self, other):
        if isinstance(other, datetime):
            return self.to_datetime() < other
        elif isinstance(other, date):
            return self.to_date() < other
        elif isinstance(other, int):
            other = str(other)
        return super().__lt__(other)

    def __gt__(self, other):
        if isinstance(other, datetime):
            return self.to_datetime() > other
        elif isinstance(other, date):
            return self.to_date() > other
        elif isinstance(other, int):
            other = str(other)
        return super().__gt__(other)

    def __le__(self, other):
        if isinstance(other, datetime):
            return self.to_datetime() <= other
        elif isinstance(other, date):
            return self.to_date() <= other
        elif isinstance(other, int):
            other = str(other)
        return super().__le__(other)

    def __ge__(self, other):
        if isinstance(other, datetime):
            return self.to_datetime() >= other
        elif isinstance(other, date):
            return self.to_date() >= other
        elif isinstance(other, int):
            other = str(other)
        return super().__ge__(other)
