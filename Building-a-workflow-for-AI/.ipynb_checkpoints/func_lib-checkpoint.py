import yfinance as yf
import pandas as pd

def create_hist_prices(start_date, end_date, min_obs_per_ticker=100):
    """
    Fetch historical adjusted close prices for S&P 500 tickers from Yahoo Finance.

    Parameters:
        start_date (str): Start date for historical data in 'YYYY-MM-DD' format.
        end_date (str): End date for historical data in 'YYYY-MM-DD' format.
        min_obs_per_ticker (int): Minimum number of non-missing observations required per ticker.

    Returns:
        pd.DataFrame: DataFrame containing adjusted close prices of valid tickers.
    """
    # Fetch S&P 500 tickers from Wikipedia
    sp500_tickers = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]['Symbol'].to_list()

    # Remove tickers com problemas conhecidos
    sp500_tickers = [ticker for ticker in sp500_tickers if '.B' not in ticker and ticker not in ['AMTM', 'SW']]

    # Divida os tickers em lotes menores
    def split_into_batches(lst, batch_size):
        for i in range(0, len(lst), batch_size):
            yield lst[i:i + batch_size]

    ticker_batches = list(split_into_batches(sp500_tickers, 50))
    
    # Inicializar DataFrame para coletar dados
    all_data = []

    for batch in ticker_batches:
        try:
            print(f"Downloading batch: {batch}")
            batch_data = yf.download(
                batch,
                start=start_date,
                end=end_date,
                auto_adjust=False,
                actions=False,
                progress=False  # Suppress progress bar
            )
            all_data.append(batch_data)
        except Exception as e:
            print(f"Batch failed: {batch}. Error: {e}")
            continue

    # Combine os lotes em um único DataFrame
    if all_data:
        historical_prices = pd.concat(all_data, axis=1)
    else:
        print("No data retrieved. Please check the tickers or date range.")
        return pd.DataFrame()

    # Filtrar para 'Adj Close', se existir
    if 'Adj Close' in historical_prices.columns.get_level_values(0):
        historical_prices = historical_prices.loc[:, historical_prices.columns.get_level_values(0) == 'Adj Close']
        historical_prices.columns = historical_prices.columns.droplevel(0)
    else:
        print("No 'Adj Close' data available. Returning empty DataFrame.")
        return pd.DataFrame()

    # Filtrar tickers com dados suficientes
    valid_tickers = historical_prices.count()[lambda x: x >= min_obs_per_ticker].index
    historical_prices = historical_prices[valid_tickers]

    return historical_prices
