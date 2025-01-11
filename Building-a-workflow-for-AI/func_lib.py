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

# Create a function called 'computingReturns' that takes prices and a list of integers (momentums) as an input
def computingReturns(historical_prices, list_of_momentums):
    '''
    Takes as an input a dataframe of prices and a list of momentums and returns a dataframe with returns over 
    the momentum list and 1 day foward returns
    '''

    # Initialize the forecast horizon
    forecast_horizon = 1
    
    # Compute forward returns by taking percentage change of close prices
    f_returns = historical_prices.pct_change(forecast_horizon, fill_method=None)

    # We then shift the forward returns
    f_returns = f_returns.shift(-forecast_horizon)

    # Pivot the dataframe
    f_returns = pd.DataFrame(f_returns.unstack())
    
    # Name the column based on the forecast horizon
    name = "F_" + str(forecast_horizon) + "_d_returns"
    
    f_returns.rename(columns={0: name}, inplace=True)

    # Initialize total_returns with forward returns
    total_returns = f_returns

    # Iterate over the list of momentum values
    for i in list_of_momentums:
        # Compute returns for each momentum value
        feature = historical_prices.pct_change(i, fill_method=None)
        feature = pd.DataFrame(feature.unstack())
    
        # Name the colum based on the momentum value
        name = str(i) + "_d_returns"
        feature.rename(columns={0:name}, inplace=True)
    
        # Rename columns and reset index
        feature.rename(columns={0: name, 'level_0': 'Ticker'}, inplace=True)
    
        # Merge computed feature returns with total_returns based on Ticker and Date
        total_returns = pd.merge(total_returns, feature, left_index=True, right_index=True, how='outer')

    total_returns.dropna(axis=0, how='any', inplace=True)
    

    # return the total returns DataFrame
    return total_returns

def compute_BM_Perf(total_returns):
    """ computes benchmark performance for investment universe and returns cumulative and calendar returns """

    # Compute the daily mean of all stocks. This will be our equal weighted benchmark
    daily_mean = pd.DataFrame(total_return.loc[:,'F_1_d_returns'].groupby(level='Date').mean())
    daily_mean.rename(columns={'F_1_d_returns':'SP&500'}, inplace=True)
    
    # Convert daily returns to cumulative returns
    cum_returns = pd.DataFrame((daily_mean[['SP&500']]+1).cumprod())
    
    # Ploting the cumulative returns
    cum_returns.plot()
    
    # Customizing the plot
    plt.title('Cumulative Returns Over Time', fontsize=16, fontweight='bold')
    plt.xlabel('Date', fontsize=14)
    plt.ylabel('Cumulative Return', fontsize=14)
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.legend(title_fontsize='13', fontsize='11')
    
    # Display the plot
    plt.show()
    
    # Calculate the number of years in the dataset
    days_of_trading = 252
    number_of_years = len(daily_mean) / days_of_trading # Assuming 252 trading days in a year
    
    ending_value = cum_returns['SP&500'].iloc[-1]
    beginning_value = cum_returns['SP&500'].iloc[1]
    
    # Compute the Compound Annual Growth Rate (CAGR)
    ratio = ending_value/beginning_value
    cagr = round((ratio**(1/number_of_years)-1)*100,2)
    
    print(f'The CAGR is: {cagr}%')
    
    # Compute the Sharpe Ratio by annualizing the daily meaen and the daily std
    average_daily_return = daily_mean[['SP&500']].describe().iloc[1,:] * days_of_trading
    stand_dev_daily_return = daily_mean[['SP&500']].describe().iloc[2,:] * pow(days_of_trading,1/2)
    
    sharpe = average_daily_return/stand_dev_daily_return
    
    print(f'Sharpe Ratio of Strategy: {round(sharpe.iloc[0],2)}')
    
    # df_daily_mean.rename(columns={target: 'Strategy'}, inplace=True)
    ann_returns = (pd.DataFrame((daily_mean[['SP&500']]+1).groupby(daily_mean.index.get_level_values(0).year).cumprod())-1)*100
    calendar_returns = pd.DataFrame(ann_returns['SP&500'].groupby(daily_mean.index.get_level_values(0).year).last())
    
    calendar_returns.plot.bar(rot=30, legend='top_left') #.opts(multi_level=False)

    return cum_returns, calendar_returns
