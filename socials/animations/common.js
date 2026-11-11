(() => {
  const COLOURS = window.SOCIAL_PALETTE;
  const FONTS = {
    mono: '"Space Mono", monospace',
    head: '"Saira Semi Condensed", sans-serif',
    pixel: '"Silkscreen", "Space Mono", monospace',
    number: '"JetBrains Mono", "Space Mono", monospace',
  };
  const MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"];
  const DESIGN = { width: 1200, height: 675 };
  const LEFT_MARGIN = 60;
  const CHART_AREA = { x: LEFT_MARGIN, y: 140, width: DESIGN.width - 2 * LEFT_MARGIN, height: 455 };
  const FULL_AREA = { x: LEFT_MARGIN, y: 40, width: DESIGN.width - 2 * LEFT_MARGIN, height: 555 };
  const TAG_Y = 62;
  const HEADLINE_Y = 104;
  const FOOTER_Y = 646;
  const HEADLINE_SIZE = 35;
  const HEADLINE_SPLIT = 1.5;
  const SCAN_LINE_SPACING = 4;
  const SCAN_LINE_ALPHA = 0.18;
  const FONT_WAIT_MS = 5000;

  const clamp = (x, lo, hi) => Math.max(lo, Math.min(hi, x));
  const lerp = (a, b, t) => a + (b - a) * t;
  const ease = (t) => 1 - Math.pow(1 - clamp(t, 0, 1), 3);
  const noise = (a, b = 0) => {
    const x = Math.sin(a * 127.1 + b * 311.7) * 43758.5453;
    return x - Math.floor(x);
  };
  const quarterYear = (label) => Number(label.slice(0, 4)) + (Number(label.slice(5)) - 1) / 4;
  const quarterText = (label) => `${label.slice(0, 4)} Q${label.slice(5)}`;
  const dateYear = (isoDate) => {
    const [year, month, day] = isoDate.split("-").map(Number);
    return year + (month - 1) / 12 + (day - 1) / 365;
  };
  const stamp = (isoDate) => {
    const [year, month, day] = isoDate.split("-");
    return `${day} ${MONTHS[Number(month) - 1]} ${year}`;
  };
  const thousands = (n) => Math.round(n).toLocaleString("en-GB");
  const percent = (v) => `${v > 0 ? "+" : v < 0 ? "−" : ""}${Math.abs(v).toFixed(1)}%`;

  function makeLayer(width, height, pixelRatio) {
    const canvas = document.createElement("canvas");
    canvas.width = Math.max(1, Math.round(width * pixelRatio));
    canvas.height = Math.max(1, Math.round(height * pixelRatio));
    const context = canvas.getContext("2d");
    context.scale(pixelRatio, pixelRatio);
    return { canvas, context };
  }

  function clear(ctx, width, height, colour = COLOURS.deep) {
    ctx.globalCompositeOperation = "source-over";
    ctx.globalAlpha = 1;
    ctx.shadowBlur = 0;
    ctx.fillStyle = colour;
    ctx.fillRect(0, 0, width, height);
  }

  function label(ctx, text, x, y, { colour = COLOURS.amber, size = 10, font = FONTS.pixel, align = "left", baseline = "alphabetic", alpha = 1 } = {}) {
    ctx.globalAlpha = alpha;
    ctx.font = `${size}px ${font}`;
    ctx.fillStyle = colour;
    ctx.textAlign = align;
    ctx.textBaseline = baseline;
    ctx.fillText(text, x, y);
    ctx.globalAlpha = 1;
  }

  function yearTicks(ctx, { x0, x1, y, from, to, every = 10, above = false }) {
    ctx.fillStyle = COLOURS.slate;
    ctx.font = `10px ${FONTS.mono}`;
    ctx.textAlign = "center";
    ctx.textBaseline = above ? "bottom" : "top";
    for (let year = Math.ceil(from / every) * every; year <= to; year += every) {
      const x = x0 + ((year - from) / (to - from)) * (x1 - x0);
      ctx.fillRect(x, above ? y - 4 : y, 1, 4);
      ctx.fillText(String(year), x, above ? y - 6 : y + 6);
    }
  }

  function glitchText(ctx, text, x, y, { size, family = FONTS.head, weight = 600, amount = 0, seed = 0, colour = COLOURS.white, align = "center" }) {
    ctx.save();
    ctx.font = `${weight} ${size}px ${family}`;
    ctx.textAlign = align;
    ctx.textBaseline = "middle";
    const glitching = amount > 0.01;
    const sliceCount = glitching ? 9 : 1;
    const top = y - size * 0.62;
    const sliceHeight = (size * 1.24) / sliceCount;
    const layers = glitching
      ? [
          [COLOURS.cyan, -1],
          [COLOURS.magenta, 1],
          [colour, 0],
        ]
      : [[colour, 0]];
    layers.forEach(([fill, direction]) => {
      ctx.globalCompositeOperation = direction ? "lighter" : "source-over";
      ctx.globalAlpha = direction ? Math.min(1, amount * 2.5) : 1;
      ctx.fillStyle = fill;
      for (let i = 0; i < sliceCount; i++) {
        const tear = noise(seed, i) > 0.62 ? (noise(seed, i + 40) - 0.5) * amount * size * 1.4 : 0;
        ctx.save();
        ctx.beginPath();
        ctx.rect(-1e4, top + i * sliceHeight, 2e4, sliceHeight + 0.6);
        ctx.clip();
        ctx.fillText(text, x + tear + direction * amount * size * 0.1, y);
        ctx.restore();
      }
    });
    ctx.restore();
  }

  function splitText(ctx, text, x, y, { size, family, weight = 400, colour = COLOURS.white, split, align = "left", baseline = "alphabetic" }) {
    ctx.font = `${weight} ${size}px ${family}`;
    ctx.textAlign = align;
    ctx.textBaseline = baseline;
    ctx.globalAlpha = 0.75;
    ctx.fillStyle = COLOURS.cyan;
    ctx.fillText(text, x - split, y);
    ctx.fillStyle = COLOURS.magenta;
    ctx.fillText(text, x + split, y);
    ctx.globalAlpha = 1;
    ctx.fillStyle = colour;
    ctx.fillText(text, x, y);
  }

  function drawCardFrame(ctx, card) {
    clear(ctx, DESIGN.width, DESIGN.height, COLOURS.grey);
    if (card.tag) label(ctx, card.tag.toUpperCase(), LEFT_MARGIN, TAG_Y, { size: 13 });
    if (card.headline) {
      splitText(ctx, card.headline, LEFT_MARGIN, HEADLINE_Y, { size: HEADLINE_SIZE, family: FONTS.head, weight: 600, split: HEADLINE_SPLIT });
    }
    label(ctx, card.footer, LEFT_MARGIN, FOOTER_Y, { colour: COLOURS.slate, size: 12, font: FONTS.mono });
    if (card.note) label(ctx, card.note, DESIGN.width - LEFT_MARGIN, FOOTER_Y, { colour: COLOURS.secondary, size: 12, font: FONTS.mono, align: "right" });
  }

  function drawFinish(ctx) {
    ctx.globalCompositeOperation = "source-over";
    ctx.globalAlpha = SCAN_LINE_ALPHA;
    ctx.fillStyle = "#000000";
    for (let y = 0; y < DESIGN.height; y += SCAN_LINE_SPACING) ctx.fillRect(0, y, DESIGN.width, 1);
    ctx.globalAlpha = 1;
    ctx.strokeStyle = COLOURS.violet;
    ctx.lineWidth = 2;
    ctx.strokeRect(1, 1, DESIGN.width - 2, DESIGN.height - 2);
  }

  const animations = {};

  window.Social = {
    COLOURS,
    FONTS,
    MONTHS,
    clamp,
    lerp,
    ease,
    noise,
    quarterYear,
    quarterText,
    dateYear,
    stamp,
    thousands,
    percent,
    makeLayer,
    clear,
    label,
    yearTicks,
    glitchText,
    register(name, factory) {
      animations[name] = factory;
    },
  };

  let session = null;

  window.recorder = {
    async start(config) {
      const fontChecks = ["13px Silkscreen", "12px 'Space Mono'", "700 12px 'Space Mono'", "600 20px 'Saira Semi Condensed'", "700 20px 'JetBrains Mono'"];
      await Promise.race([Promise.all(fontChecks.map((font) => document.fonts.load(font))), new Promise((resolve) => setTimeout(resolve, FONT_WAIT_MS))]);
      const missing = fontChecks.filter((font) => !document.fonts.check(font));
      if (missing.length) throw new Error(`Fonts did not load: ${missing.join(", ")}`);
      const factory = animations[config.name];
      if (!factory) throw new Error(`No animation is registered as ${config.name}`);
      if (Math.abs(config.widthPx / config.heightPx - DESIGN.width / DESIGN.height) > 0.01) {
        throw new Error(`${config.widthPx}x${config.heightPx} is not the 16:9 shape every animation is designed for`);
      }

      const canvas = document.createElement("canvas");
      canvas.width = config.widthPx;
      canvas.height = config.heightPx;
      document.body.appendChild(canvas);
      const ctx = canvas.getContext("2d");
      const scale = config.widthPx / DESIGN.width;
      const area = config.card.fullArea ? FULL_AREA : CHART_AREA;
      const animation = factory(config.payload);
      animation.reset(ctx, area.width, area.height, scale);
      session = { canvas, ctx, scale, area, animation, card: config.card, fps: config.fps };
      return Math.ceil((animation.duration + config.holdSeconds) * config.fps);
    },

    frame(index) {
      return drawAt(index / session.fps);
    },

    finalFrame() {
      return drawAt(session.animation.duration);
    },
  };

  function drawAt(time) {
    const { canvas, ctx, scale, area, animation, card } = session;
    ctx.setTransform(scale, 0, 0, scale, 0, 0);
    drawCardFrame(ctx, card);
    ctx.save();
    ctx.translate(area.x, area.y);
    ctx.beginPath();
    ctx.rect(0, 0, area.width, area.height);
    ctx.clip();
    animation.draw(ctx, area.width, area.height, time);
    ctx.restore();
    ctx.setTransform(scale, 0, 0, scale, 0, 0);
    drawFinish(ctx);
    return canvas.toDataURL("image/png");
  }
})();
