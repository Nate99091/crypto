import matplotlib.pyplot as plt
import os

def plot_backtest_results(results, output_path):
    os.makedirs(output_path, exist_ok=True)
    for pair, profit in results.items():
        plt.figure()
        plt.bar(pair, profit)
        plt.title(f"Backtest Results for {pair}")
        plt.xlabel("Pair")
        plt.ylabel("Profit")
        plt.savefig(os.path.join(output_path, f"{pair}_results.png"))
        plt.close()
