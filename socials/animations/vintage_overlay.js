Social.register("vintage_overlay", (payload) => {
  const S = Social;
  const C = S.COLOURS;
  const VINTAGES_PER_SECOND = 20;
  const LAYER_ALPHA = 0.07;
  const quarters = payload.quarters;
  const vintages = payload.vintages;
  const quarterCount = quarters.length;
  const vintageCount = vintages.length;
  const limit = payload.valueLimit;
  const fromYear = S.quarterYear(quarters[0]);
  const toYear = fromYear + quarterCount / 4;
  let layer, drawn, geometry;

  const x = (j) => geometry.left + (j + 0.5) * geometry.cellWidth;
  const y = (value) => geometry.zero - (S.clamp(value, -limit, limit) / limit) * geometry.half;

  function tracePath(ctx, vintage, shift = 0) {
    ctx.beginPath();
    let started = false;
    vintage.values.forEach((value, j) => {
      if (value === null) return;
      started ? ctx.lineTo(x(j) + shift, y(value)) : ctx.moveTo(x(j) + shift, y(value));
      started = true;
    });
  }

  function drawGrid(ctx) {
    const g = geometry;
    [-limit, -limit / 2, limit / 2, limit].forEach((value) => {
      ctx.fillStyle = C.violet;
      ctx.globalAlpha = 0.5;
      for (let px = g.left; px < g.right; px += 6) ctx.fillRect(px, y(value), 2, 1);
      ctx.globalAlpha = 1;
      S.label(ctx, `${value > 0 ? "+" : "−"}${Math.abs(value)}`, g.left - 6, y(value) + 4, { colour: C.slate, size: 10, font: S.FONTS.mono, align: "right" });
    });
    ctx.fillStyle = C.slate;
    ctx.fillRect(g.left, g.zero, g.right - g.left, 1);
  }

  return {
    duration: vintageCount / VINTAGES_PER_SECOND,
    reset(ctx, width, height, pixelRatio) {
      const top = 34;
      const bottom = height - 30;
      geometry = { left: 44, right: width - 12, top, bottom, cellWidth: (width - 56) / quarterCount, zero: (top + bottom) / 2, half: (bottom - top) / 2 };
      layer = S.makeLayer(width, height, pixelRatio);
      layer.context.globalCompositeOperation = "lighter";
      layer.context.strokeStyle = C.cyan;
      layer.context.lineWidth = 1;
      layer.context.globalAlpha = LAYER_ALPHA;
      drawn = 0;
    },
    draw(ctx, width, height, time) {
      const target = Math.min(vintageCount, Math.floor(time * VINTAGES_PER_SECOND) + 1);
      while (drawn < target) {
        tracePath(layer.context, vintages[drawn++]);
        layer.context.stroke();
      }
      const k = target - 1;
      const finished = time * VINTAGES_PER_SECOND >= vintageCount;
      S.clear(ctx, width, height);
      drawGrid(ctx);
      ctx.drawImage(layer.canvas, 0, 0, width, height);
      ctx.lineWidth = finished ? 1.8 : 1.3;
      if (!finished) {
        ctx.globalCompositeOperation = "lighter";
        [[C.cyan, -1.5], [C.magenta, 1.5]].forEach(([colour, shift]) => {
          ctx.strokeStyle = colour;
          ctx.globalAlpha = 0.6;
          tracePath(ctx, vintages[k], shift);
          ctx.stroke();
        });
        ctx.globalCompositeOperation = "source-over";
      }
      ctx.globalAlpha = 0.95;
      ctx.strokeStyle = C.white;
      tracePath(ctx, vintages[k]);
      ctx.stroke();
      ctx.globalAlpha = 1;
      S.yearTicks(ctx, { x0: geometry.left, x1: geometry.left + quarterCount * geometry.cellWidth, y: geometry.bottom + 4, from: fromYear, to: toYear });
      S.label(ctx, `${finished ? "LATEST" : "VINTAGE"} · ${S.stamp(vintages[k].date)}`, geometry.left, 22, { size: 12 });
      S.label(ctx, `${k + 1} / ${vintageCount}`, geometry.right, 22, { size: 12, colour: C.white, align: "right" });
    },
  };
});
