"""
Static NDVI maps (one per acquisition date) saved as PNG.
Laid out in a 2×4 grid, cropped to the Rome land area only
(strips the Tyrrhenian Sea from the western portion of tile T33TTG).
"""

import math
from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"

NDVI_CMAP = mcolors.LinearSegmentedColormap.from_list(
    "ndvi",
    ["#d73027", "#fee08b", "#1a9850"],
    N=256,
)

# Tile T33TTG at 100 m resolution is ~1098×1098 px covering ~110×110 km.
# The western ~25% is the Tyrrhenian Sea.  Crop to land / Rome area:
#   columns 250→1098  (approx 11.95°E eastward — removes the sea)
#   rows    0→850     (removes the southern uninhabited strip)
CROP_ROW_START  = 0
CROP_ROW_END    = 870
CROP_COL_START  = 240
CROP_COL_END    = 1098


def plot_seasonal_maps(cube: np.ndarray, dates: list[str], out_dir: Path | None = None):
    """
    cube : shape (rows, cols, time)
    dates: list of ISO date strings, len == cube.shape[2]
    """
    out_dir = out_dir or PROCESSED_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    # Crop to Rome land area
    cube_crop = cube[CROP_ROW_START:CROP_ROW_END, CROP_COL_START:CROP_COL_END, :]

    n = len(dates)
    ncols = 4
    nrows = math.ceil(n / ncols)

    # Reserve a narrow column on the right exclusively for the colorbar
    fig = plt.figure(figsize=(ncols * 3.8 + 0.8, nrows * 3.2), facecolor="#0d1117")
    # GridSpec: ncols image cols + 1 narrow colorbar col
    import matplotlib.gridspec as gridspec
    gs = gridspec.GridSpec(
        nrows, ncols + 1,
        figure=fig,
        width_ratios=[1] * ncols + [0.06],
        wspace=0.04, hspace=0.25,
    )

    axes = [fig.add_subplot(gs[r, c]) for r in range(nrows) for c in range(ncols)]
    cax  = fig.add_subplot(gs[:, ncols])   # colorbar spans all rows

    im = None
    for i, (date, ax) in enumerate(zip(dates, axes)):
        im = ax.imshow(
            cube_crop[:, :, i],
            cmap=NDVI_CMAP,
            vmin=-0.2,
            vmax=0.8,
            interpolation="bilinear",
        )
        ax.set_title(date, fontsize=9, color="white", pad=4)
        ax.axis("off")

    for j in range(len(dates), len(axes)):
        axes[j].set_visible(False)

    cbar = fig.colorbar(im, cax=cax)
    cbar.set_label("NDVI", color="white", labelpad=8)
    cbar.ax.yaxis.set_tick_params(color="white")
    plt.setp(cbar.ax.yaxis.get_ticklabels(), color="white")

    fig.suptitle("Sentinel-2 NDVI — Rome 2024 (T33TTG)", fontsize=13,
                 color="white", y=1.01)
    fig.savefig(out_dir / "seasonal_maps.png", dpi=150, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"Saved seasonal_maps.png")


if __name__ == "__main__":
    cube_path = PROCESSED_DIR / "ndvi_cube.npy"
    meta_path = PROCESSED_DIR / "cube_dates.txt"
    cube = np.load(cube_path)
    dates = meta_path.read_text().splitlines()
    plot_seasonal_maps(cube, dates)
