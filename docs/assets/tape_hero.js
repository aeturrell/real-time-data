(() => {
  const COLOUR_NAMES = ["deep", "violet", "cyan", "magenta", "amber", "white", "slate"];
  const MONO_FONT = '"Space Mono", ui-monospace, monospace';
  const PIXEL_FONT = '"Silkscreen", "Space Mono", monospace';
  const MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"];
  const VINTAGES_PER_SECOND = 24;
  const FONT_TIMEOUT_MS = 2500;
  const MAX_PIXEL_RATIO = 2;
  const LABEL_EVERY_YEARS = 10;
  const STATUS = { empty: 0, first: 1, revised: 2, unchanged: 3 };
  const STRIPE_PIXELS = 3;
  const STRIPE_GAP_PIXELS = 2;
  const CAP_GLOW = 8;
  const ARROW_PIXELS = 5;
  const CAP_LABEL_LINE = 11;
  const CAP_LABEL_GAP = 3;
  const CAP_LABEL_ROWS = 3;

  const clamp = (x, lo, hi) => Math.max(lo, Math.min(hi, x));
  const lerp = (a, b, t) => a + (b - a) * t;
  const easeOut = (t) => 1 - Math.pow(1 - clamp(t, 0, 1), 3);
  const noise = (a, b = 0) => {
    const x = Math.sin(a * 127.1 + b * 311.7) * 43758.5453;
    return x - Math.floor(x);
  };
  const stamp = (isoDate) => {
    const [year, month, day] = isoDate.split("-");
    return `${day} ${MONTHS[Number(month) - 1]} ${year}`;
  };

  function createTapeStack(payload, COLOURS) {
    const GROWTH_LIMIT = payload.growthLimit;
    const quarters = payload.quarters;
    const vintages = payload.vintages;
    const quarterCount = quarters.length;
    const vintageCount = vintages.length;
    const valueAt = (k, j) => vintages[k].values[j] ?? null;
    const vintageYear = (k) => Number(vintages[k].date.slice(0, 4));
    const firstQuarterYear = Number(quarters[0].slice(0, 4)) + (Number(quarters[0].slice(5)) - 1) / 4;

    const status = vintages.map((_, k) => {
      const row = new Uint8Array(quarterCount);
      for (let j = 0; j < quarterCount; j++) {
        const now = valueAt(k, j);
        const before = k ? valueAt(k - 1, j) : null;
        if (now === null) row[j] = STATUS.empty;
        else if (before === null) row[j] = STATUS.first;
        else row[j] = now !== before ? STATUS.revised : STATUS.unchanged;
      }
      return row;
    });

    let context = null;
    let width = 0;
    let height = 0;
    let pixelRatio = 1;
    let geometry = null;
    let tape = null;
    let landed = 0;

    function buildGeometry() {
      const left = 44;
      const right = width - 12;
      const liveTop = 34;
      const liveBottom = Math.round(height * 0.36);
      const stackTop = Math.round(height * 0.43);
      const stackBottom = height - 24;
      return {
        left,
        right,
        cellWidth: (right - left) / quarterCount,
        zero: (liveTop + liveBottom) / 2,
        half: (liveBottom - liveTop) / 2 - 3,
        liveBottom,
        stackTop,
        stackBottom,
        rowHeight: (stackBottom - stackTop) / vintageCount,
      };
    }

    function buildTape() {
      const layer = document.createElement("canvas");
      layer.width = Math.max(1, Math.round(width * pixelRatio));
      layer.height = Math.max(1, Math.round(height * pixelRatio));
      const layerContext = layer.getContext("2d");
      layerContext.scale(pixelRatio, pixelRatio);
      return { canvas: layer, context: layerContext };
    }

    const rowTop = (k) => geometry.stackBottom - (k + 1) * geometry.rowHeight;
    const quarterLeft = (j) => geometry.left + j * geometry.cellWidth;

    function cellStyle(k, j) {
      const value = valueAt(k, j);
      switch (status[k][j]) {
        case STATUS.revised:
          return [COLOURS.amber, 0.95];
        case STATUS.first:
          return [COLOURS.white, 0.9];
        default:
          return value >= 0
            ? [COLOURS.cyan, clamp(value / 1.5, 0.12, 0.85)]
            : [COLOURS.magenta, clamp(-value / 1.5, 0.3, 1)];
      }
    }

    function writeStrip(k) {
      const ctx = tape.context;
      const top = rowTop(k);
      for (let j = 0; j < quarterCount; j++) {
        if (status[k][j] === STATUS.empty) continue;
        const [colour, alpha] = cellStyle(k, j);
        ctx.globalAlpha = alpha;
        ctx.fillStyle = colour;
        ctx.fillRect(quarterLeft(j), top, Math.max(1, geometry.cellWidth - 0.6), Math.max(0.8, geometry.rowHeight + 0.25));
      }
      ctx.globalAlpha = 1;
    }

    function label(text, x, y, { colour = COLOURS.amber, size = 10, font = PIXEL_FONT, align = "left" } = {}) {
      context.font = `${size}px ${font}`;
      context.fillStyle = colour;
      context.textAlign = align;
      context.textBaseline = "alphabetic";
      context.fillText(text, x, y);
    }

    function drawOverflowCap(x, barWidth, end, direction, value) {
      const excess = Math.abs(value) - GROWTH_LIMIT;
      const stripeStep = STRIPE_PIXELS + STRIPE_GAP_PIXELS;
      const capLength = Math.max(STRIPE_PIXELS, Math.min(geometry.half * 0.85, (excess / GROWTH_LIMIT) * stripeStep));
      context.save();
      context.globalAlpha = 1;
      context.fillStyle = COLOURS.amber;
      context.shadowColor = COLOURS.amber;
      context.shadowBlur = CAP_GLOW;
      for (let offset = 0; offset < capLength; offset += stripeStep) {
        const stripe = Math.min(STRIPE_PIXELS, capLength - offset);
        const top = direction > 0 ? end + offset : end - offset - stripe;
        context.fillRect(x - 1, top, barWidth + 2, stripe);
      }
      const middle = x + barWidth / 2;
      const tip = direction > 0 ? end - ARROW_PIXELS : end + ARROW_PIXELS;
      context.beginPath();
      context.moveTo(middle - ARROW_PIXELS, end);
      context.lineTo(middle + ARROW_PIXELS, end);
      context.lineTo(middle, tip);
      context.closePath();
      context.fill();
      context.restore();
    }

    function capLabelPosition(cap, textWidth, blocked, placed) {
      const overlaps = (left, right, [otherLeft, otherRight]) => left < otherRight && otherLeft < right;
      const candidates = [];
      for (let row = 0; row < CAP_LABEL_ROWS; row++) {
        candidates.push({ row, left: cap.x + cap.barWidth + CAP_LABEL_GAP });
        candidates.push({ row, left: cap.x - CAP_LABEL_GAP - textWidth });
      }
      const isClear = ({ row, left }) => {
        const span = [left - 2, left + textWidth + 2];
        return !blocked.some((bar) => overlaps(...span, bar)) && !placed.some((label) => label.row === row && overlaps(...span, label.span));
      };
      return candidates.find(isClear) ?? candidates[0];
    }

    function drawCapLabels(caps) {
      context.font = `9px ${MONO_FONT}`;
      [1, -1].forEach((direction) => {
        const sameSide = caps.filter((cap) => cap.direction === direction);
        const blocked = sameSide.map((cap) => [cap.x - 1, cap.x + cap.barWidth + 1]);
        const placed = [];
        sameSide.forEach((cap) => {
          const text = `${cap.value > 0 ? "+" : "−"}${Math.abs(cap.value).toFixed(1)}`;
          const textWidth = context.measureText(text).width;
          const { row, left } = capLabelPosition(cap, textWidth, blocked, placed);
          placed.push({ row, span: [left - 2, left + textWidth + 2] });
          const y = direction > 0 ? cap.end + 8 + row * CAP_LABEL_LINE : cap.end - 2 - row * CAP_LABEL_LINE;
          context.fillStyle = COLOURS.deep;
          context.globalAlpha = 0.9;
          context.fillRect(left - 2, y - 9, textWidth + 4, CAP_LABEL_LINE);
          context.globalAlpha = 1;
          label(text, left, y, { colour: COLOURS.amber, size: 9, font: MONO_FONT });
          context.font = `9px ${MONO_FONT}`;
        });
      });
    }

    function drawLiveChart(k, progress, finished) {
      const g = geometry;
      context.fillStyle = COLOURS.violet;
      context.fillRect(g.left, g.zero, g.right - g.left, 1);
      const split = finished ? 0 : 2 + (1 - progress) * 5;
      const barWidth = Math.max(1, g.cellWidth - 1);
      const caps = [];
      for (let j = 0; j < quarterCount; j++) {
        const value = valueAt(k, j);
        if (value === null) continue;
        const barHeight = (clamp(value, -GROWTH_LIMIT, GROWTH_LIMIT) / GROWTH_LIMIT) * g.half;
        const x = quarterLeft(j);
        const y = barHeight > 0 ? g.zero - barHeight : g.zero;
        const size = Math.abs(barHeight);
        const cell = status[k][j];
        if (cell === STATUS.revised && split) {
          context.globalCompositeOperation = "lighter";
          context.globalAlpha = 0.85;
          context.fillStyle = COLOURS.cyan;
          context.fillRect(x - split, y + (noise(k, j) - 0.5) * 3, barWidth, size);
          context.fillStyle = COLOURS.magenta;
          context.fillRect(x + split, y, barWidth, size);
          context.globalCompositeOperation = "source-over";
        }
        const highlighted = cell === STATUS.revised || cell === STATUS.first;
        context.globalAlpha = highlighted ? 1 : 0.55;
        context.fillStyle = highlighted ? COLOURS.white : COLOURS.cyan;
        context.fillRect(x, y, barWidth, size);
        if (Math.abs(value) > GROWTH_LIMIT) {
          const cap = { x, barWidth, end: barHeight > 0 ? y : y + size, direction: barHeight > 0 ? 1 : -1, value };
          drawOverflowCap(cap.x, cap.barWidth, cap.end, cap.direction, cap.value);
          caps.push(cap);
        }
      }
      drawCapLabels(caps);
      context.globalAlpha = 1;
    }

    function drawAxes() {
      const g = geometry;
      const axisLabel = { colour: COLOURS.slate, size: 9, font: MONO_FONT, align: "right" };
      label(`+${GROWTH_LIMIT}`, g.left - 6, g.zero - g.half + 4, axisLabel);
      label("0", g.left - 6, g.zero + 3, axisLabel);
      label(`−${GROWTH_LIMIT}`, g.left - 6, g.zero + g.half + 2, axisLabel);

      const lastQuarterYear = firstQuarterYear + quarterCount / 4;
      const firstTick = Math.ceil(firstQuarterYear / LABEL_EVERY_YEARS) * LABEL_EVERY_YEARS;
      for (let year = firstTick; year < lastQuarterYear; year += LABEL_EVERY_YEARS) {
        const x = g.left + ((year - firstQuarterYear) / (lastQuarterYear - firstQuarterYear)) * (g.right - g.left);
        context.fillStyle = COLOURS.slate;
        context.fillRect(x, g.stackBottom + 2, 1, 4);
        label(String(year), x, g.stackBottom + 17, { ...axisLabel, align: "center" });
      }

      const firstVintageTick = Math.ceil(vintageYear(0) / LABEL_EVERY_YEARS) * LABEL_EVERY_YEARS;
      for (let year = firstVintageTick; year <= vintageYear(vintageCount - 1); year += LABEL_EVERY_YEARS) {
        const k = vintages.findIndex((vintage) => Number(vintage.date.slice(0, 4)) >= year);
        if (k >= 0 && k < landed) label(String(year), g.left - 6, rowTop(k) + 3, axisLabel);
      }
    }

    function drawSummary(time) {
      const message = `${vintageCount} VINTAGES · ${vintageYear(0)}–${vintageYear(vintageCount - 1)}`;
      const size = clamp(width / 34, 12, 22);
      const middle = (geometry.stackTop + geometry.stackBottom) / 2;
      context.font = `${size}px ${PIXEL_FONT}`;
      const boxWidth = context.measureText(message).width + 28;
      const boxLeft = (width - boxWidth) / 2;
      context.globalAlpha = 0.88;
      context.fillStyle = COLOURS.deep;
      context.fillRect(boxLeft, middle - size, boxWidth, size * 2);
      context.globalAlpha = 1;
      context.strokeStyle = COLOURS.violet;
      context.strokeRect(boxLeft, middle - size, boxWidth, size * 2);
      context.textAlign = "center";
      context.textBaseline = "middle";
      if (noise(Math.floor(time * 6)) > 0.85) {
        context.globalCompositeOperation = "lighter";
        context.fillStyle = COLOURS.cyan;
        context.fillText(message, width / 2 - 2, middle);
        context.fillStyle = COLOURS.magenta;
        context.fillText(message, width / 2 + 2, middle);
        context.globalCompositeOperation = "source-over";
      }
      context.fillStyle = COLOURS.white;
      context.fillText(message, width / 2, middle);
    }

    function draw(elapsed) {
      const time = Math.max(0, elapsed);
      const target = Math.min(vintageCount, Math.floor(time * VINTAGES_PER_SECOND) + 1);
      while (landed < target) writeStrip(landed++);
      const k = target - 1;
      const progress = clamp(time * VINTAGES_PER_SECOND - k, 0, 1);
      const finished = time * VINTAGES_PER_SECOND >= vintageCount;
      const g = geometry;

      context.globalCompositeOperation = "source-over";
      context.globalAlpha = 1;
      context.fillStyle = COLOURS.deep;
      context.fillRect(0, 0, width, height);
      context.drawImage(tape.canvas, 0, 0, width, height);

      if (!finished) {
        const bandTop = Math.max(g.stackTop, rowTop(k) - 6);
        const bandHeight = 14;
        const shift = (noise(k, 1) - 0.5) * 26 * (1 - progress);
        context.globalAlpha = 0.9;
        context.drawImage(tape.canvas, 0, bandTop * pixelRatio, width * pixelRatio, bandHeight * pixelRatio, shift, bandTop, width, bandHeight);
        context.globalAlpha = 1 - progress;
        context.fillStyle = COLOURS.cyan;
        context.fillRect(g.left, lerp(g.liveBottom, rowTop(k), easeOut(progress)), g.right - g.left, 1.5);
        context.globalAlpha = 1;
      }

      drawLiveChart(k, progress, finished);
      drawAxes();
      label(`VINTAGE ${stamp(vintages[k].date)}`, g.left, 20, { size: 11 });
      label(`${payload.seriesId} · ${String(k + 1).padStart(String(vintageCount).length, "0")} / ${vintageCount}`, g.right, 20, {
        colour: COLOURS.white,
        align: "right",
      });
      if (finished) drawSummary(time);
      return finished;
    }

    function setup(targetContext, targetWidth, targetHeight, targetPixelRatio) {
      context = targetContext;
      width = targetWidth;
      height = targetHeight;
      pixelRatio = targetPixelRatio;
      geometry = buildGeometry();
      tape = buildTape();
      landed = 0;
    }

    return { duration: vintageCount / VINTAGES_PER_SECOND, setup, draw };
  }

  window.TapeStack = { create: createTapeStack };

  const figure = document.querySelector(".tape-hero");
  const dataElement = document.getElementById("tape-hero-data");
  if (!figure || !dataElement) return;

  const styles = getComputedStyle(figure);
  const siteColours = Object.fromEntries(COLOUR_NAMES.map((name) => [name, styles.getPropertyValue(`--tape-${name}`).trim()]));
  const animation = createTapeStack(JSON.parse(dataElement.textContent), siteColours);
  const canvas = figure.querySelector("canvas");
  const context = canvas.getContext("2d");
  const replayButton = figure.querySelector(".tape-hero-replay");
  const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  const pixelRatio = Math.min(MAX_PIXEL_RATIO, window.devicePixelRatio || 1);
  let ready = false;
  let playing = false;
  let startTime = 0;

  function resize() {
    const rect = canvas.getBoundingClientRect();
    if (!rect.width || !rect.height) return;
    canvas.width = Math.round(rect.width * pixelRatio);
    canvas.height = Math.round(rect.height * pixelRatio);
    context.setTransform(pixelRatio, 0, 0, pixelRatio, 0, 0);
    animation.setup(context, rect.width, rect.height, pixelRatio);
    ready = true;
    if (!playing) animation.draw(Number.MAX_SAFE_INTEGER);
  }

  function tick(now) {
    if (!playing) return;
    const finished = animation.draw((now - startTime) / 1000);
    if (finished) playing = false;
    else requestAnimationFrame(tick);
  }

  function play() {
    if (!ready) return;
    animation.setup(context, canvas.width / pixelRatio, canvas.height / pixelRatio, pixelRatio);
    startTime = performance.now();
    if (!playing) {
      playing = true;
      requestAnimationFrame(tick);
    }
  }

  function start() {
    new ResizeObserver(resize).observe(canvas);
    resize();
    replayButton?.addEventListener("click", play);
    if (!reduceMotion) play();
  }

  Promise.race([
    Promise.all([`10px ${PIXEL_FONT}`, `10px ${MONO_FONT}`].map((font) => document.fonts.load(font))),
    new Promise((resolve) => setTimeout(resolve, FONT_TIMEOUT_MS)),
  ]).then(start, start);
})();
