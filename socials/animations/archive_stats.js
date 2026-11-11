Social.register("archive_stats", (payload) => {
  const S = Social;
  const C = S.COLOURS;
  const ROLL_SECONDS = 2.6;
  const STAGGER_SECONDS = 0.35;
  const GAP = 24;
  const LABEL_WIDTH = 20;
  const tiles = payload.tiles;

  function wheelPositions(value, digits) {
    return Array.from({ length: digits }, (_, i) => {
      const unit = 10 ** i;
      const lower = value % unit;
      return (Math.floor(value / unit) % 10) + Math.max(0, lower - (unit - 1));
    });
  }

  function counter(ctx, value, digits, centreX, centreY, size, commas) {
    const cellWidth = size * 0.64;
    const commaWidth = size * 0.32;
    const commaCount = commas ? Math.floor((digits - 1) / 3) : 0;
    let x = centreX - (digits * cellWidth + commaCount * commaWidth) / 2;
    const wheels = wheelPositions(value, digits);
    ctx.font = `700 ${size}px ${S.FONTS.number}`;
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";
    for (let i = digits - 1; i >= 0; i--) {
      const digit = Math.floor(wheels[i]) % 10;
      const fraction = wheels[i] - Math.floor(wheels[i]);
      ctx.save();
      ctx.beginPath();
      ctx.rect(x, centreY - size * 0.62, cellWidth, size * 1.24);
      ctx.clip();
      ctx.fillStyle = i > 0 && value < 10 ** i ? C.slate : C.cyan;
      ctx.fillText(String(digit), x + cellWidth / 2, centreY - fraction * size * 1.15);
      ctx.fillText(String((digit + 1) % 10), x + cellWidth / 2, centreY + (1 - fraction) * size * 1.15);
      ctx.restore();
      x += cellWidth;
      if (commas && i > 0 && i % 3 === 0) {
        ctx.fillStyle = C.slate;
        ctx.fillText(",", x + commaWidth / 2, centreY);
        x += commaWidth;
      }
    }
  }

  function wrap(text) {
    const lines = [""];
    text.split(" ").forEach((word) => {
      const line = lines[lines.length - 1];
      if (line && (line + " " + word).length > LABEL_WIDTH) lines.push(word);
      else lines[lines.length - 1] = line ? `${line} ${word}` : word;
    });
    return lines;
  }

  function drawValue(ctx, tile, progress, centreX, centreY, size) {
    if (tile.kind === "month") {
      const start = (tile.year - 1) * 12;
      const months = S.lerp(0, tile.year * 12 + tile.month - start, progress) + start;
      const year = Math.floor(months / 12) + Math.max(0, (months % 12) - 11);
      ctx.font = `700 ${size}px ${S.FONTS.number}`;
      ctx.fillStyle = C.cyan;
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillText(S.MONTHS[Math.floor(months) % 12], centreX - size * 1.45, centreY);
      counter(ctx, year, 4, centreX + size * 1.1, centreY, size, false);
      return;
    }
    const digits = String(tile.value).length;
    counter(ctx, progress * tile.value, digits, centreX, centreY, size, tile.commas);
  }

  return {
    duration: ROLL_SECONDS + STAGGER_SECONDS * (tiles.length - 1),
    reset() {},
    draw(ctx, width, height, time) {
      S.clear(ctx, width, height, C.grey);
      const tileWidth = (width - GAP * (tiles.length - 1)) / tiles.length;
      const tileHeight = height * 0.62;
      const top = (height - tileHeight) / 2;
      tiles.forEach((tile, index) => {
        const left = index * (tileWidth + GAP);
        ctx.fillStyle = C.deep;
        ctx.fillRect(left, top, tileWidth, tileHeight);
        ctx.strokeStyle = C.violet;
        ctx.lineWidth = 1;
        ctx.strokeRect(left + 0.5, top + 0.5, tileWidth - 1, tileHeight - 1);
        const progress = S.ease((time - index * STAGGER_SECONDS) / ROLL_SECONDS);
        const size = Math.min(tileHeight * 0.24, tileWidth / 5.6);
        drawValue(ctx, tile, progress, left + tileWidth / 2, top + tileHeight * 0.42, size);
        wrap(tile.label).forEach((line, row) => {
          S.label(ctx, line, left + tileWidth / 2, top + tileHeight * 0.68 + row * 20, { size: 14, font: S.FONTS.mono, colour: C.secondary, align: "center" });
        });
      });
    },
  };
});
