import pandas as pd
import urllib.request

URL_XLS_FILE = "https://www.eia.gov/dnav/ng/hist_xls/RNGWHHDd.xls"

PRICE_RAW_DATA_FILE = r".\data\raw\price_raw_data_file.xls"

GAS_NATURAL_SPOT_PRICE_DATA_DAILY = r".\data\trusted\gas_natural_spot_price_data_daily.csv"

GAS_NATURAL_SPOT_PRICE_DATA_BY_MONTH = r".\data\trusted\gas_natural_spot_price_data_daily_by_month.csv"

GAS_NATURAL_SPOT_PRICE_DATA_BY_YEAR = r".\data\trusted\gas_natural_spot_price_data_daily_by_year.csv"


def request_data_file():
    '''
    Download the XLS data file.
    :return:
    '''
    urllib.request.urlretrieve(URL_XLS_FILE, PRICE_RAW_DATA_FILE)


def prepare_data():
    '''
    Open the data file and prepares the raw data.
    :return: the prepared data frame
    '''
    excel_file = pd.ExcelFile(PRICE_RAW_DATA_FILE)

    df_prepared_data = pd.DataFrame(excel_file.parse('Data 1')[3:])

    df_prepared_data.columns = ['date', 'price']

    df_prepared_data['price'] = pd.to_numeric(df_prepared_data['price'])

    # Removes the time considering only the day, month and year.
    df_prepared_data['date'] = pd.to_datetime(df_prepared_data['date'], dayfirst=True)

    # Creates the column date floor to group the data by month.
    df_prepared_data['date_floor'] = pd.to_datetime(df_prepared_data['date'].dt.floor('d') - pd.offsets.MonthBegin(1),
                                                    dayfirst=True)

    # Create the column year to group the data by year.
    df_prepared_data['year'] = df_prepared_data['date'].dt.year

    return df_prepared_data


def persist_data(df: pd.DataFrame, columns: list, columns_persistence: list, file_name: str):
    """
    Persists the data in a certain csv file.
    :param df: Data frame
    :param columns: Columns considered from the data frame
    :param columns_persistence: Columns with the names to be persisted
    :param file_name: CSV file name
    :return:
    """
    df_persist = df[columns]
    df_persist.columns = columns_persistence
    df_persist.to_csv(file_name, index=False)


def aggregate_data(df: pd.DataFrame, columns: list, columns_agg: list):
    """
    Aggregates the data according a certain fields.
    :param df: Data frame
    :param columns: columns to be considered from the data frame
    :param columns_agg: aggregation columns
    :return: The aggregated data frame
    """
    return df[columns].groupby(columns_agg).mean().reset_index()[columns]


def process():
    """
    Executes all the data pipeline process.
    :return:
    """
    # Requests and download the data
    request_data_file()

    # Prepares the data
    df_price_data = prepare_data()

    # Saves the spot price by day
    persist_data(df_price_data, ['date', 'price'], ['date', 'price'], GAS_NATURAL_SPOT_PRICE_DATA_DAILY)

    # Saves the spor price prsised by month
    persist_data(
        aggregate_data(df_price_data, ['date_floor', 'price'], ['date_floor']),
        ['date_floor', 'price'],
        ['date', 'price'],
        GAS_NATURAL_SPOT_PRICE_DATA_BY_MONTH
    )

    # Saves the spor price prsised by year
    persist_data(
        aggregate_data(df_price_data, ['year', 'price'], ['year']),
        ['year', 'price'],
        ['date', 'price'],
        GAS_NATURAL_SPOT_PRICE_DATA_BY_YEAR
    )


if __name__ == '__main__':
    process()
