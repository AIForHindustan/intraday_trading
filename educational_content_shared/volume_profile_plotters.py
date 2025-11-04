
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, AutoMinorLocator

def inr(x, pos=None):
    return f"₹{x:,.0f}"

inr_formatter = FuncFormatter(inr)

def plot_daily_price_with_poc(times, prices, poc, vah=None, val=None, title="Price Action with POC", outpath="daily_price_poc.png"):
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(times, prices, marker='o', linewidth=2, label="Price")
    ax.yaxis.set_major_formatter(inr_formatter)
    ax.grid(True, which='both', alpha=0.3)
    ax.xaxis.set_minor_locator(AutoMinorLocator())
    ax.set_title(title, fontsize=14, fontweight='bold')
    ax.set_xlabel("Time")
    ax.set_ylabel("Price (₹)")
    ax.axhline(poc, linestyle='--', linewidth=2, label=f"POC: ₹{poc:,.2f}")
    if vah is not None and val is not None:
        ymin, ymax = sorted([val, vah])
        ax.axhspan(ymin, ymax, alpha=0.15, label="Value Area (70%)")
        # Handle pandas Series indexing
        times_last = times.iloc[-1] if hasattr(times, 'iloc') else times[-1]
        times_first = times.iloc[0] if hasattr(times, 'iloc') else times[0]
        times_mid = times.iloc[len(times)//2] if hasattr(times, 'iloc') else times[len(times)//2]
        ax.annotate("VA High", xy=(times_last, ymax), xytext=(10, 5), textcoords="offset points")
        ax.annotate("VA Low", xy=(times_first, ymin), xytext=(10, -15), textcoords="offset points")
    times_mid = times.iloc[len(times)//2] if hasattr(times, 'iloc') else times[len(times)//2]
    ax.annotate("POC", xy=(times_mid, poc), xytext=(0, 10), textcoords="offset points",
                ha='center', va='bottom')
    ax.legend(loc="best", frameon=True)
    fig.tight_layout()
    fig.savefig(outpath, dpi=180)
    plt.close(fig)
    return outpath

def plot_volume_profile(price_levels, volumes, poc, vah=None, val=None, title="Volume Profile", outpath="daily_profile.png"):
    fig, ax = plt.subplots(figsize=(8, 8))
    y = np.array(price_levels)
    v = np.array(volumes)
    ax.barh(y, v, height=(y[1]-y[0] if len(y)>1 else 1), alpha=0.8, label="Volume")
    ax.yaxis.set_major_formatter(inr_formatter)
    ax.set_xlabel("Volume")
    ax.set_ylabel("Price (₹)")
    ax.set_title(title, fontsize=14, fontweight='bold')
    ax.grid(True, axis='x', alpha=0.3)
    ax.axhline(poc, linestyle='--', linewidth=2, label=f"POC: ₹{poc:,.2f}")
    if vah is not None and val is not None:
        ymin, ymax = sorted([val, vah])
        ax.axhspan(ymin, ymax, alpha=0.15, label="Value Area (70%)")
    ax.legend(loc="best", frameon=True)
    fig.tight_layout()
    fig.savefig(outpath, dpi=180)
    plt.close(fig)
    return outpath

def plot_multiday_poc(dates, poc_prices, vahs=None, vals=None, title="POC Evolution", outpath="multiday_poc.png"):
    fig, ax = plt.subplots(figsize=(10, 5))
    x = pd.to_datetime(dates)
    y = np.array(poc_prices, dtype=float)
    ax.plot(x, y, marker='o', linewidth=2, label="POC Price")
    ax.yaxis.set_major_formatter(inr_formatter)
    ax.grid(True, which='both', alpha=0.3)
    ax.set_title(title, fontsize=14, fontweight='bold')
    ax.set_xlabel("Date")
    ax.set_ylabel("POC (₹)")
    if vahs is not None and vals is not None:
        for xi, va_h, va_l in zip(x, vahs, vals):
            ymin, ymax = sorted([va_l, va_h])
            ax.axvspan(xi - pd.Timedelta(hours=12), xi + pd.Timedelta(hours=12), ymin=0, ymax=1,
                       alpha=0.05)
            ax.vlines(xi, ymin=ymin, ymax=ymax, linewidth=2, alpha=0.3)
    ax.annotate(f"Last POC: ₹{y[-1]:,.0f}", xy=(x[-1], y[-1]), xytext=(15, 10),
                textcoords="offset points")
    ax.legend(loc="best", frameon=True)
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(outpath, dpi=180)
    plt.close(fig)
    return outpath
