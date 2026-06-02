"""
Time series NDVI plot per zone — saved as PNG and interactive HTML.
"""

from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import plotly.graph_objects as go

PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"

ZONE_COLORS = {
    "Centro Storico":  "#e63946",
    "Villa Borghese":  "#2a9d8f",
    "Castelli Romani": "#e9c46a",
    "Agro Romano":     "#457b9d",
}


def plot_timeseries(csv_path: Path | None = None, out_dir: Path | None = None):
    csv_path = csv_path or PROCESSED_DIR / "zone_stats.csv"
    out_dir = out_dir or PROCESSED_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(csv_path, parse_dates=["date"])
    df = df.sort_values(["zone", "date"])

    # --- Matplotlib static PNG ---
    fig, ax = plt.subplots(figsize=(12, 6))
    for zone, color in ZONE_COLORS.items():
        z = df[df["zone"] == zone]
        ax.plot(z["date"], z["ndvi_mean"], marker="o", label=zone, color=color, linewidth=2)
        ax.fill_between(z["date"], z["ndvi_p25"], z["ndvi_p75"], alpha=0.15, color=color)

    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    plt.xticks(rotation=45, ha="right")
    ax.set_ylabel("NDVI", fontsize=12)
    ax.set_title("NDVI Time Series — Rome 2024 (Sentinel-2 L2A)", fontsize=14)
    ax.legend(loc="upper right")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_dir / "timeseries.png", dpi=150)
    plt.close(fig)
    print(f"Saved timeseries.png")

    # --- Plotly interactive HTML ---
    fig_pl = go.Figure()
    for zone, color in ZONE_COLORS.items():
        z = df[df["zone"] == zone]
        fig_pl.add_trace(go.Scatter(
            x=z["date"], y=z["ndvi_mean"],
            name=zone, mode="lines+markers",
            line=dict(color=color, width=2),
            error_y=dict(
                type="data",
                symmetric=False,
                array=(z["ndvi_p75"] - z["ndvi_mean"]).tolist(),
                arrayminus=(z["ndvi_mean"] - z["ndvi_p25"]).tolist(),
                visible=True,
            ),
        ))

    fig_pl.update_layout(
        title="NDVI Time Series — Rome 2024",
        xaxis_title="Date",
        yaxis_title="NDVI",
        template="plotly_white",
        legend=dict(x=0.01, y=0.99),
    )
    fig_pl.write_html(out_dir / "timeseries.html")
    print(f"Saved timeseries.html")


if __name__ == "__main__":
    plot_timeseries()
