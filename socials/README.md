# Social media videos and stills

This folder makes videos and still images for social media from the archive (`realtimedata.parquet`), in the same style as the [docs site](https://aeturrell.github.io/real-time-data/). It makes two kinds of output:

- **Videos**: MP4 animations, 1920×1080 by default, which X, LinkedIn and Bluesky all accept.
- **Stills**: PNG images of each animation's final frame, for link previews, video cover images or anywhere a picture is needed.

Both come from the same animation scripts, so a still always matches its video. Every number, date and label comes from the archive when you run the commands, so rerunning them after the archive is updated gives up-to-date outputs. Running them twice on the same archive gives byte-identical files.

## Commands

Run these from the repository root.

```bash
uv sync --group socials                                   # install the Python packages
uv run --group socials playwright install chromium        # install the browser (once)

uv run --group socials python -m socials mp4              # every video
uv run --group socials python -m socials png              # every still
uv run --group socials python -m socials mp4 tape_stack   # only the named ones
uv run --group socials python -m socials png tape_stack estimate_path
uv run --group socials python -m socials --help
```

Two options work with either format:

- `--settings PATH` reads a different settings file. The default is `socials/socials.toml`.
- `--archive PATH` draws from a different archive file. The default is `realtimedata.parquet` in the repository root.

Files are written to `socials/output/`, named after the animation, such as `socials/output/tape_stack.mp4` and `socials/output/tape_stack.png`. That folder is ignored by git.

If something can't be made, for example because a date range holds no data, the command logs the reason, carries on with the rest, and exits with status 1.

## What each animation shows

| Name | What it shows |
|---|---|
| `tape_stack` | The landing page animation. Every vintage (published version) of GDP growth stacks up as a strip of tape. Amber marks quarters revised in each release. Bars beyond ±2.5% are cut off; amber stripes on a cut-off bar show how much is cut off (one stripe per further 2.5 points), and the true value is printed beside it. |
| `estimate_path` | Every published estimate of growth in one quarter, in the order they were published. |
| `revision_heatmap` | The revision triangle, coloured by how far each figure has moved from its first estimate. |
| `vintage_overlay` | Every vintage drawn faintly on top of the others, with the latest in white. |
| `two_clocks` | Lines from the quarter a figure describes to each date it was published or revised. |
| `archive_stats` | The archive's four headline numbers: series, rows, earliest and latest publication. |
| `release_calendar` | Every publication date in the archive, with line height showing how many values were published that day. |
| `weekday_rhythm` | How many values are published on each day of the week, drawn as an oscilloscope trace. |
| `provenance` | Fingerprints (SHA-256 hashes) of the source files, with a few archive rows and the file each came from. |
| `title_card` | The site title and subtitle. |

## Settings

All settings live in `socials.toml`, which is commented throughout.

- **`[output]`**: the output folder, and the footer text on every video and still.
- **`[video]`**: video size, frames per second, how long the last frame is held, and quality. The defaults (1920×1080 at 30 frames per second) suit X, LinkedIn and Bluesky. `quality` is ffmpeg's x264 constant rate factor: lower numbers are sharper and larger, and 18 looks lossless.
- **`[still]`**: still image size. Both videos and stills must keep the 16:9 shape every animation is designed for, such as 1920×1080, 1280×720 or 1200×675; any other shape stops with an error.
- **One section per animation**: which series and quarter to show, the date range, and the headline. Each section sets both the video and the still of that name.

Dates are written `YYYY-MM-DD` and quarters `YYYYQn`. Settings beginning `first_` or `last_` set the start and end of a range; leave one out to use the earliest or latest the archive holds. Headlines can include placeholders such as `{quarter}`, `{count}`, `{first_year}` and `{last_year}`, which are filled in from the data. The comment above each section lists the ones it accepts.

An unknown or missing setting stops that animation with a message naming the section, so typos don't pass silently.

## How it works

Making a video or still takes four steps:

1. **Data.** `video_data.py` has one function per animation. It reads the archive and the animation's settings, and returns a *scene*: the data the animation draws, plus the headline, tag and footer. The calculations it shares, such as revision triangles, first estimates and date ranges, are in `archive_data.py`.
2. **Page.** `video.py` builds a self-contained web page holding the site's fonts and colours, the shared script `animations/common.js`, and the animation's own script. Nothing is fetched from the internet.
3. **Frames.** A headless browser loads the page. For a video, Python asks for frame 0, then frame 1, and so on. The page draws each frame at an exact time (frame *n* is drawn at *n* ÷ frames per second) and hands it back as a PNG image. Nothing depends on how fast the computer is, so every run draws the same frames. For a still, the page draws a single frame at the moment the animation ends.
4. **Encoding.** For a video, the frames are fed straight into ffmpeg, which writes an H.264 MP4. Flags leave out timestamps and version strings, so repeated runs give identical files. A still is saved as the PNG the page returns.

Each animation script registers itself under its name with `Social.register(...)` and provides:

- `duration`: how long the animation runs, in seconds. A video adds the end hold from `[video]` on top.
- `reset(ctx, width, height, pixelRatio)`: called once before the first frame.
- `draw(ctx, width, height, time)`: draws one frame at `time` seconds. Drawing straight at the end time must give the finished picture, because that is how stills are made.

`common.js` draws everything around the animation: headline, tag, footer, border and scan lines. It also exposes the helpers the scripts share: colours, fonts, text with offset cyan and magenta copies, and year ticks.

### What comes from where

- **Colours** come from `docs/_chart_theme.py`, the same source as the docs site's charts. Python passes them into the recording page.
- **Fonts** come from `docs/assets/fonts/` and are embedded in the recording page.
- **The tape stack** uses the landing page's own script, `docs/assets/tape_hero.js`, and its data-building code, `docs/_hero.py`. A change to the landing page animation shows up in the video and still too. On the live site, the same script takes its colours from the site's theme.

### Why the output is reproducible

- The browser is the Chromium build pinned by the Playwright version in `uv.lock`, not whatever browser is installed locally.
- ffmpeg is the copy bundled with the pinned `imageio-ffmpeg` package.
- Fonts are embedded from `docs/assets/fonts/`, so no system fonts are used.
- Animations use no randomness: anything that looks random comes from a fixed formula of the frame time. `provenance` picks its rows with a fixed `seed` from the settings.

Different operating systems can draw text very slightly differently, so outputs are byte-identical on the same machine but may differ slightly between, say, macOS and Linux.

## Files

| File | What it does |
|---|---|
| `__main__.py` | The command line: reads the settings and archive, starts the browser, then makes the requested videos or stills. |
| `socials.toml` | All settings. |
| `settings.py` | Reads `socials.toml` into typed settings, one class per section. |
| `archive_data.py` | Shared calculations on the archive: revision triangles, first estimates, revisions, date and quarter ranges, headline years. |
| `video_data.py` | One function per animation, building its scene, and the `VIDEOS` list of animations. |
| `video.py` | Builds the recording page, draws the frames in the browser, and writes MP4s and PNGs. |
| `animations/common.js` | The recorder, the frame around each animation, and helpers shared by the animation scripts. |
| `animations/<name>.js` | One animation script per name. |

## Adding a new animation

1. Add a settings class to `settings.py` and a section with the same name to `socials.toml`.
2. Write a function in `video_data.py` that returns a `Scene`, and add it to `VIDEOS`.
3. Add `animations/<name>.js`, which calls `Social.register("<name>", ...)`.
4. Make the still first (`python -m socials png <name>`), since it is quick, then the video. Check a middle frame of the video too: `ffmpeg -ss 5 -i socials/output/<name>.mp4 -frames:v 1 frame.png` saves the frame at 5 seconds.

## Troubleshooting

- **"Executable doesn't exist" or a similar browser error**: run `uv run --group socials playwright install chromium`.
- **"Fonts did not load"**: a file in `docs/assets/fonts/` is missing or has been renamed. `FONT_FACES` in `video.py` lists the files used.
- **"... is not the 16:9 shape every animation is designed for"**: change `width_px` and `height_px` in `[video]` or `[still]` to a 16:9 pair.
- **"No data is left inside the chosen quarters and vintage dates"**: the `first_` and `last_` settings for that section select nothing. Widen the range.
- **A video looks wrong partway through**: save that frame with the ffmpeg command above. Frames are drawn at exact times, so the same frame always shows the same problem.
