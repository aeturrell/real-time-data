"""Records an animation script to MP4, one exactly timed frame at a time, or saves its final frame as PNG.

A headless browser draws each frame at a fixed timestamp, so the same archive,
settings, browser and encoder always give the same video. Frames are passed
straight to ffmpeg and encoded as H.264, which X, LinkedIn and Bluesky all accept.
"""

import base64
import json
import subprocess
from pathlib import Path

import imageio_ffmpeg
from loguru import logger
from playwright.sync_api import Browser
from playwright.sync_api import Error as BrowserError

from docs import _chart_theme as site
from socials.settings import StillSettings, VideoSettings
from socials.video_data import Scene

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
ANIMATION_DIRECTORY = Path(__file__).parent / "animations"
COMMON_SCRIPT = ANIMATION_DIRECTORY / "common.js"
TAPE_SCRIPT = REPOSITORY_ROOT / "docs" / "assets" / "tape_hero.js"
FONT_DIRECTORY = REPOSITORY_ROOT / "docs" / "assets" / "fonts"
FONT_FACES = [
    ("Space Mono", 400, "SpaceMono-Regular.ttf"),
    ("Space Mono", 700, "SpaceMono-Bold.ttf"),
    ("Saira Semi Condensed", 600, "SairaSemiCondensed-SemiBold.ttf"),
    ("Silkscreen", 400, "Silkscreen-Regular.ttf"),
    ("JetBrains Mono", 700, "JetBrainsMono-Bold.ttf"),
]
SCRIPTS_NEEDING_TAPE = {"tape_stack"}
DATA_URL_SEPARATOR = ","
PROGRESS_EVERY_FRAMES = 150
ENCODER_PRESET = "slow"
STILL_FRAMES_PER_SECOND = 1
STILL_HOLD_SECONDS = 0
PALETTE = {
    "grey": site.STATIC_GREY,
    "deep": site.STATIC_GREY_DEEP,
    "violet": site.CORRUPTED_VIOLET,
    "cyan": site.SCAN_LINE_CYAN,
    "magenta": site.DEGAUSS_MAGENTA,
    "amber": site.PHOSPHOR_AMBER,
    "white": site.ELECTRON_WHITE,
    "slate": site.OXIDE_SLATE,
    "secondary": site.SECONDARY_TEXT,
}


def font_face_css() -> str:
    """Embeds the site's fonts in the recording page, so nothing is fetched.

    Returns:
        CSS @font-face rules with the fonts as data URLs.

    Raises:
        FileNotFoundError: If a font file is missing.
    """
    rules = []
    for family, weight, file_name in FONT_FACES:
        encoded = base64.b64encode((FONT_DIRECTORY / file_name).read_bytes()).decode(
            "ascii"
        )
        rules.append(
            f'@font-face {{ font-family: "{family}"; font-weight: {weight}; '
            f'src: url(data:font/ttf;base64,{encoded}) format("truetype"); }}'
        )
    return "\n".join(rules)


def animation_scripts(name: str) -> list[Path]:
    """Lists the scripts a video needs, in loading order.

    Args:
        name: The video's name.

    Returns:
        The script files.
    """
    tape = [TAPE_SCRIPT] if name in SCRIPTS_NEEDING_TAPE else []
    return [COMMON_SCRIPT, *tape, ANIMATION_DIRECTORY / f"{name}.js"]


def recording_page(name: str) -> str:
    """Builds a self-contained page holding the fonts and scripts for one video.

    Args:
        name: The video's name.

    Returns:
        The page's HTML.

    Raises:
        FileNotFoundError: If a script or font file is missing.
    """
    palette = f"<script>window.SOCIAL_PALETTE = {json.dumps(PALETTE)};</script>"
    scripts = "\n".join(
        [
            palette,
            *(
                f"<script>{path.read_text()}</script>"
                for path in animation_scripts(name)
            ),
        ]
    )
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><style>{font_face_css()} body {{ margin: 0; }}</style></head>
<body>{scripts}</body></html>"""


def encoder_command(path: Path, video: VideoSettings) -> list[str]:
    """Builds the ffmpeg command that turns a stream of PNG frames into an MP4.

    The bit-exact flags leave out timestamps and version strings, so repeated
    runs write identical files.

    Args:
        path: The MP4 file to write.
        video: The video settings.

    Returns:
        The command, ready for subprocess.
    """
    rate = str(video.frames_per_second)
    return [
        imageio_ffmpeg.get_ffmpeg_exe(),
        "-y",
        "-loglevel", "error",
        "-f", "image2pipe",
        "-framerate", rate,
        "-c:v", "png",
        "-i", "-",
        "-c:v", "libx264",
        "-profile:v", "high",
        "-pix_fmt", "yuv420p",
        "-preset", ENCODER_PRESET,
        "-crf", str(video.quality),
        "-r", rate,
        "-movflags", "+faststart",
        "-map_metadata", "-1",
        "-fflags", "+bitexact",
        "-flags:v", "+bitexact",
        str(path),
    ]  # fmt: skip


def png_bytes(data_url: str) -> bytes:
    """Decodes a PNG data URL returned by the page.

    Args:
        data_url: A data URL from canvas.toDataURL.

    Returns:
        The PNG file's bytes.
    """
    return base64.b64decode(data_url.split(DATA_URL_SEPARATOR, 1)[1])


def recorder_config(
    name: str,
    scene: Scene,
    size: tuple[int, int],
    frames_per_second: int,
    hold_seconds: float,
) -> dict:
    """Packs what the page's recorder needs to start.

    Args:
        name: The animation's name.
        scene: The animation's data and text.
        size: Width and height in pixels.
        frames_per_second: Frames drawn for each second of animation.
        hold_seconds: How long the final frame stays on screen.

    Returns:
        The configuration passed to recorder.start in the page.
    """
    width_px, height_px = size
    return {
        "name": name,
        "payload": scene.payload,
        "card": scene.card,
        "widthPx": width_px,
        "heightPx": height_px,
        "fps": frames_per_second,
        "holdSeconds": hold_seconds,
    }


def record_video(
    browser: Browser, name: str, scene: Scene, video: VideoSettings, path: Path
) -> Path:
    """Draws every frame of one video in the browser and encodes them to MP4.

    Args:
        browser: A running headless browser.
        name: The video's name, which picks its animation script.
        scene: The video's data and text.
        video: The video settings.
        path: The MP4 file to write.

    Returns:
        The path written.

    Raises:
        RuntimeError: If the page fails to draw or ffmpeg fails to encode.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    page = browser.new_page()
    encoder = None
    try:
        page.set_content(recording_page(name))
        config = recorder_config(
            name,
            scene,
            (video.width_px, video.height_px),
            video.frames_per_second,
            video.end_hold_seconds,
        )
        frame_count = page.evaluate("config => window.recorder.start(config)", config)
        encoder = subprocess.Popen(
            encoder_command(path, video), stdin=subprocess.PIPE, stderr=subprocess.PIPE
        )
        for index in range(frame_count):
            encoder.stdin.write(
                png_bytes(page.evaluate("index => window.recorder.frame(index)", index))
            )
            if index % PROGRESS_EVERY_FRAMES == 0:
                logger.debug(f"{name}: frame {index} of {frame_count}")
        encoder.stdin.close()
        error_output = encoder.stderr.read().decode()
        if encoder.wait() != 0:
            raise RuntimeError(f"ffmpeg could not encode {name}: {error_output}")
    except BrowserError as error:
        raise RuntimeError(f"The browser could not draw {name}: {error}") from error
    finally:
        page.close()
        if encoder is not None and encoder.poll() is None:
            encoder.kill()
    return path


def record_still(
    browser: Browser, name: str, scene: Scene, still: StillSettings, path: Path
) -> Path:
    """Draws the final frame of one animation and saves it as a PNG.

    Args:
        browser: A running headless browser.
        name: The animation's name, which picks its script.
        scene: The animation's data and text.
        still: The still image settings.
        path: The PNG file to write.

    Returns:
        The path written.

    Raises:
        RuntimeError: If the page fails to draw.
        OSError: If the file cannot be written.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    page = browser.new_page()
    try:
        page.set_content(recording_page(name))
        config = recorder_config(
            name,
            scene,
            (still.width_px, still.height_px),
            STILL_FRAMES_PER_SECOND,
            STILL_HOLD_SECONDS,
        )
        page.evaluate("config => window.recorder.start(config)", config)
        path.write_bytes(png_bytes(page.evaluate("window.recorder.finalFrame()")))
    except BrowserError as error:
        raise RuntimeError(f"The browser could not draw {name}: {error}") from error
    finally:
        page.close()
    return path
