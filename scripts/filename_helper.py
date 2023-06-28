def get_last_month():
    from datetime import timedelta, date

    today = date.today()
    return (today.replace(day=1) - timedelta(days=1)).strftime('%Y%m')


def get_output_file_name():
    # Get output file name (monthly data)
    return f'PUB_GenOutputCapabilityMonth_{get_last_month()}.csv'


def get_daily_data_file_names():
    from calendar import monthrange

    # Get all load/import/export file names (daily data)
    last_month = get_last_month()
    year = last_month[:4]
    month = last_month[-2:]
    days_in_month = monthrange(int(year), int(month))[1]
    intertie_file_names = []
    load_file_names = []
    for day in range(22, days_in_month+1):
        day = str(day).zfill(2)
        intertie_file_names.append(f'PUB_IntertieScheduleFlow_{year}{month}{day}.xml')
        load_file_names.append(f'PUB_DAConstTotals_{year}{month}{day}.xml')
    return intertie_file_names, load_file_names, year, month
