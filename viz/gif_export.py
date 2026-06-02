"""
Export an animated GIF of the NDVI seasonal cycle.
"""

from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import imageio.v3 as iio
from io import BytesIO

PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"

NDVI_CMAP = mcolors.LinearSegmentedColormap.from_list(
    "ndvi",
    ["#d73027", "#fee08b", "#1a9850"],
    N=256,
)


def export_gif(
    cube: np.ndarray,
    dates: list[str],
    out_path: Path | None = None,
    fps: int = 2,
):
    """cube: (rows, cols, time). Writes an animated GIF."""
    out_path = out_path or PROCESSED_DIR / "ndvi_cycle.gif"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    frames = []
    for t, date in enumerate(dates):
        fig, ax = plt.subplots(figsize=(6, 5))
        ax.imshow(
            cube[:, :, t],
            cmap=NDVI_CMAP,
            vmin=-0.2,
            vmax=0.8,
            interpolation="nearest",
        )
        ax.set_title(f"NDVI — Rome {date}", fontsize=11)
        ax.axis("off")
        fig.tight_layout()

        buf = BytesIO()
        fig.savefig(buf, format="png", dpi=120)
        plt.close(fig)
        buf.seek(0)
        frames.append(iio.imread(buf))

    iio.imwrite(out_path, frames, duration=1000 // fps, loop=0)
    print(f"Saved animated GIF: {out_path}")
    return out_path


if __name__ == "__main__":
    cube_path = PROCESSED_DIR / "ndvi_cube.npy"
    meta_path = PROCESSED_DIR / "cube_dates.txt"
    cube = np.load(cube_path)
    dates = meta_path.read_text().splitlines()
    export_gif(cube, dates)
