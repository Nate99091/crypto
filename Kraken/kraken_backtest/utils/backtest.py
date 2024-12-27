def perform_backtest(data):
    results = {}
    for pair, ohlc in data.items():
        total_profit = 0
        for i in range(1, len(ohlc)):
            if ohlc[i][4] > ohlc[i - 1][4]:
                profit = ohlc[i][4] - ohlc[i - 1][4]
                total_profit += profit
        results[pair] = total_profit
    return results
