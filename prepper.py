"""Converts tagged CSV files to normalised CSV files for proprietary use.

Raises:
    ValueError: If an unsolvable error pertaining to values occurs 

Returns:
    Various files based on input
"""


import csv
import os
from copy import deepcopy
from datetime import datetime

PATH_FILES_IN = "./useful data/1 tagged data/"
PATH_FILES_OUT = "./useful data/2 processed data/"


def roughen(periodicity, dates, values):
    """Return roughened values for every period.

    Arguments:
        periodicity -- "Y", "M", "d" or "h"
        dates -- list of dates in the current periodicity
        values -- list of values in the current periodicity

    Returns:
        a tuple of matched dates and values for each level of periodicity
    """
    dates = deepcopy(dates)
    row_amount = len(dates)
    empty_column = row_amount*['']

    # determine hourly values
    if periodicity == "h":
        dates_h, values_h = dates, values
    else:
        dates_h = values_h = empty_column

    # determine daily values
    if periodicity == "d":
        dates_d, values_d = dates, values
    elif dates_h == empty_column:
        dates_d = values_d = empty_column
    else:
        dates_d, values_d = roughen_h2d(dates_h, values_h)

    # determine monthly values
    if periodicity == "M":
        dates_M, values_M = dates, values
    elif dates_d == empty_column:
        dates_M = values_M = empty_column
    else:
        dates_M, values_M = roughen_d2M(dates_d, values_d)

    # determine yearly values
    if periodicity == "Y":
        dates_Y, values_Y = dates, values
    elif dates_M == empty_column:
        dates_Y = values_Y = empty_column
    else:
        dates_Y, values_Y = roughen_M2Y(dates_M, values_M)

    # convert datetime objects to strings
    if dates_h is not empty_column:
        dates_h = [h.strftime("%Y-%m-%dT%H") for h in dates_h]
    if dates_d is not empty_column:
        dates_d = [d.strftime("%Y-%m-%d") for d in dates_d]
    if dates_M is not empty_column:
        dates_M = [m.strftime("%Y-%m") for m in dates_M]
    if dates_Y is not empty_column:
        dates_Y = [y.strftime("%Y") for y in dates_Y]

    # pad all values (h is automatically correct)
    dates_d += (row_amount - len(dates_d)) * ['']
    values_d += (row_amount - len(values_d)) * ['']
    dates_M += (row_amount - len(dates_M)) * ['']
    values_M += (row_amount - len(values_M)) * ['']
    dates_Y += (row_amount - len(dates_Y)) * ['']
    values_Y += (row_amount - len(values_Y)) * ['']

    return (dates_h, values_h,
            dates_d, values_d,
            dates_M, values_M,
            dates_Y, values_Y)


def roughen_h2d(dates, values):
    dates = [d.replace(hour=0).isoformat() for d in dates]
    return generic_roughen(dates, values)


def roughen_d2M(dates, values):
    dates = [d.replace(day=1).isoformat() for d in dates]
    return generic_roughen(dates, values)


def roughen_M2Y(dates, values):
    dates = [d.replace(month=1).isoformat() for d in dates]
    return generic_roughen(dates, values)


def generic_roughen(dates, values):
    combined = {d: v for d, v in zip(dates, values)}
    return ([datetime.fromisoformat(k) for k in combined.keys()],
            list(combined.values()))


def normalise(filename):
    """Translate a tagged CSV file into multiple normalised files."""
    raw_data = None
    headers = None
    with open(PATH_FILES_IN+filename, newline='') as file:
        # {"T_x": [], "Z1": [], ...}
        reader = csv.DictReader(file, delimiter=';', dialect="excel")

        for row in reader:
            if headers is None:
                headers = [h for h in row
                           if h[:2] == "T_" or h[:4] == "TAG_"]
                raw_data = {h: list() for h in headers}
                continue

            for header in headers:
                raw_data[header].append(row[header])

    # extract time

    datelabel = [k for k in raw_data.keys() if k[:2] == "T_"][0]

    periodicity = datelabel.split("_")[-1]
    object_dates = []
    for d in raw_data[datelabel]:
        try:
            object_dates.append(datetime.fromisoformat(d).replace(minute=0,
                                                                  second=0,
                                                                  microsecond=0))
        except ValueError:
            if len(d) == 4:
                object_dates.append(datetime(int(d), 1, 1))
            elif len(d) == 7:
                y, m = d.split("-")
                object_dates.append(datetime(int(y), int(m), 1))
            else:
                raise ValueError
    dates = [o.isoformat() for o in object_dates]
    row_amount = len(dates)

    # for each col
    for header, column in raw_data.items():
        if header[:4] != "TAG_":
            continue

        unit = "_"
        # determine unit (if available)
        print(f"\t{header}")
        if len(column[1]) > 0 and not column[1][-1].isnumeric():
            test, unit = column[1].split(" ", 1)
            # check that unit was removed correctly, will crash otherwise
            print(f"\t\t{test}, {unit}")
            float(test)

            # remove the unit
            column = [f"{x.split(' ', 1)[0]}" for x in column]

        # convert to string representation of float with 2 decimal points
        column = ['0' if x == '' else f"{float(x):.2f}" for x in column]

        # calculate all other periodicities of date-value pairs
        new_data_cols = roughen(periodicity, object_dates, column)

        new_data = [["h_date", f"h_{unit}", "d_date", f"d_{unit}",
                     "M_date", f"M_{unit}", "Y_date", f"Y_{unit}"]] \
            + [[hd, hv, dd, dv, Md, Mv, Yd, Yv]
               for hd, hv, dd, dv, Md, Mv, Yd, Yv
               in zip(*new_data_cols)]

        with open(PATH_FILES_OUT + f"{row_amount:06}__{header[4:]}.csv", 'w',
                  newline='') as file:
            writer = csv.writer(file, delimiter=';', dialect="excel")
            for row in new_data:
                writer.writerow(row)


if __name__ == "__main__":
    for file in os.listdir(PATH_FILES_IN):
        if file.startswith("T_") and file.endswith(".csv"):
            print(f"Working on file {file}...")
            try:
                normalise(file)
            except KeyboardInterrupt:
                break
