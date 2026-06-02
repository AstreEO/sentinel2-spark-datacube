"""
Interactive 3-D visualisation of the NDVI data cube using Plotly.

Each acquisition date is a coloured surface stacked at its time index
along the Z axis.  Saved as interactive HTML + a rotating GIF for GitHub.
"""

from pathlib import Path

import numpy as np
import plotly.graph_objects as go

PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"

# Same land crop as seasonal_maps.py
CROP_ROW_START, CROP_ROW_END   = 0,   870
CROP_COL_START, CROP_COL_END   = 240, 1098


def plot_3d_cube(
    cube: np.ndarray,
    dates: list[str],
    out_dir: Path | None = None,
    downsample: int = 6,
):
    """
    cube: (rows, cols, time)
    Each time slice → coloured surface at z = t.
    """
    out_dir = out_dir or PROCESSED_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    # Crop to land area, then spatial downsample for HTML size
    cube_crop = cube[CROP_ROW_START:CROP_ROW_END, CROP_COL_START:CROP_COL_END, :]
    sliced = cube_crop[::downsample, ::downsample, :]
    rows, cols, n_dates = sliced.shape

    x = np.arange(cols)
    y = np.arange(rows)
    X, Y = np.meshgrid(x, y)

    NDVI_COLORSCALE = [
        [0.0,  "#d73027"],
        [0.33, "#fee08b"],
        [0.66, "#91cf60"],
        [1.0,  "#1a9850"],
    ]

    traces = []
    for t, date in enumerate(dates):
        z_offset = np.full_like(X, fill_value=t, dtype=float)
        ndvi_slice = np.where(np.isnan(sliced[:, :, t]), 0.0, sliced[:, :, t])
        traces.append(
            go.Surface(
                x=X, y=Y, z=z_offset,
                surfacecolor=ndvi_slice,
                colorscale=NDVI_COLORSCALE,
                cmin=-0.2, cmax=0.8,
                showscale=(t == 0),
                colorbar=dict(title="NDVI", x=1.02, thickness=15) if t == 0 else None,
                name=date,
                opacity=0.88,
                hovertemplate=f"Date: {date}<br>NDVI: %{{surfacecolor:.3f}}<extra></extra>",
            )
        )

    fig = go.Figure(data=traces)
    fig.update_layout(
        title=dict(text="NDVI Data Cube — Rome 2024 (Sentinel-2 T33TTG)",
                   font=dict(size=15)),
        scene=dict(
            xaxis_title="W → E  (px)",
            yaxis_title="N → S  (px)",
            zaxis=dict(
                title="Acquisition",
                tickvals=list(range(n_dates)),
                ticktext=[d[5:] for d in dates],  # show MM-DD only
            ),
            aspectratio=dict(x=1.2, y=1, z=0.45),
            camera=dict(eye=dict(x=1.6, y=-1.6, z=1.0)),
        ),
        template="plotly_dark",
        width=1100, height=720,
        margin=dict(l=0, r=80, t=50, b=0),
    )

    html_path = out_dir / "datacube_3d.html"
    fig.write_html(html_path)
    print(f"Saved interactive 3-D cube: {html_path}")

    # ── Rotating 360° GIF (for GitHub README) ────────────────────────
    try:
        import imageio.v2 as iio
        import io as _io
        import plotly.io as pio

        frames = []
        for angle in range(0, 360, 8):
            rad = np.deg2rad(angle)
            fig.update_layout(scene_camera=dict(
                eye=dict(x=1.8 * np.cos(rad), y=1.8 * np.sin(rad), z=0.9)
            ))
            img_bytes = pio.to_image(fig, format="png", width=900, height=560, scale=1)
            frames.append(iio.imread(_io.BytesIO(img_bytes)))

        gif_path = out_dir / "datacube_3d_rotating.gif"
        iio.mimwrite(gif_path, frames, fps=12, loop=0)
        print(f"Saved rotating GIF: {gif_path}")
    except Exception as e:
        print(f"Rotating GIF skipped ({e})")

    # ── Static PNG fallback ──────────────────────────────────────────
    fig.update_layout(scene_camera=dict(eye=dict(x=1.6, y=-1.6, z=1.0)))
    png_path = out_dir / "datacube_3d.png"
    try:
        fig.write_image(png_path, width=1200, height=700)
        print(f"Saved static screenshot: {png_path}")
    except Exception:
        print("kaleido not available — skipping static PNG.")

    return fig


if __name__ == "__main__":
    cube_path = PROCESSED_DIR / "ndvi_cube.npy"
    meta_path = PROCESSED_DIR / "cube_dates.txt"
    cube = np.load(cube_path)
    dates = meta_path.read_text().splitlines()
    plot_3d_cube(cube, dates)
