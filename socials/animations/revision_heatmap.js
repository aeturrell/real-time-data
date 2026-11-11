Social.register("revision_heatmap", (payload) => {
  const S = Social;
  const C = S.COLOURS;
  const VINTAGES_PER_SECOND = 22;
  const UNCHANGED = 0.05;
  const quarters = payload.quarters;
  const vintages = payload.vintages;
  const quarterCount = quarters.length;
  const vintageCount = vintages.length;
  const firstCells = vintages.map((vintage) => new Set(vintage.first));
  const fromYear = S.quarterYear(quarters[0]);
  const toYear = fromYear + quarterCount / 4;
  let layer, geometry, ratio;

  function cellColour(k, j) {
    const revision = vintages[k].revisions[j] ?? null;
    if (revision === null) return null;
    if (firstCells[k].has(j)) return [C.cyan, 1];
    if (Math.abs(revision) < UNCHANGED) return [C.slate, 0.16];
    const strength = S.clamp(Math.abs(revision) / payload.colourLimit, 0.2, 1);
    return [revision > 0 ? C.amber : C.magenta, strength];
  }

  function drawLegend(ctx, height) {
    let x = geometry.left;
    [[C.cyan, "FIRST ESTIMATE"], [C.amber, "REVISED UP"], [C.magenta, "REVISED DOWN"]].forEach(([colour, text]) => {
      ctx.fillStyle = colour;
      ctx.fillRect(x, height - 15, 9, 9);
      S.label(ctx, text, x + 14, height - 6, { size: 10, colour: C.white });
      ctx.font = `10px ${S.FONTS.pixel}`;
      x += ctx.measureText(text).width + 30;
    });
  }

  return {
    duration: vintageCount / VINTAGES_PER_SECOND,
    reset(ctx, width, height, pixelRatio) {
      const left = 48;
      const right = width - 12;
      const top = 36;
      const bottom = height - 44;
      geometry = { left, right, top, plotWidth: right - left, plotHeight: bottom - top, cellWidth: (right - left) / quarterCount, rowHeight: (bottom - top) / vintageCount };
      layer = S.makeLayer(geometry.plotWidth, geometry.plotHeight, pixelRatio);
      for (let k = 0; k < vintageCount; k++) {
        const y = (vintageCount - 1 - k) * geometry.rowHeight;
        for (let j = 0; j < quarterCount; j++) {
          const cell = cellColour(k, j);
          if (!cell) continue;
          layer.context.globalAlpha = cell[1];
          layer.context.fillStyle = cell[0];
          layer.context.fillRect(j * geometry.cellWidth, y, Math.max(1, geometry.cellWidth - 0.4), Math.max(0.8, geometry.rowHeight + 0.25));
        }
      }
      layer.context.globalAlpha = 1;
      ratio = pixelRatio;
    },
    draw(ctx, width, height, time) {
      const g = geometry;
      const shown = Math.min(vintageCount, Math.floor(time * VINTAGES_PER_SECOND) + 1);
      const finished = time * VINTAGES_PER_SECOND >= vintageCount;
      S.clear(ctx, width, height);
      const sourceTop = (vintageCount - shown) * g.rowHeight;
      const shownHeight = shown * g.rowHeight;
      ctx.drawImage(layer.canvas, 0, sourceTop * ratio, g.plotWidth * ratio, Math.max(1, shownHeight * ratio), g.left, g.top, g.plotWidth, shownHeight);
      if (!finished) {
        ctx.fillStyle = C.cyan;
        ctx.shadowColor = C.cyan;
        ctx.shadowBlur = 10;
        ctx.fillRect(g.left, g.top, g.plotWidth, 1.5);
        ctx.shadowBlur = 0;
      }
      const firstYear = Number(vintages[0].date.slice(0, 4));
      const lastYear = Number(vintages[vintageCount - 1].date.slice(0, 4));
      for (let year = Math.ceil(firstYear / 10) * 10; year <= lastYear; year += 10) {
        const k = vintages.findIndex((vintage) => Number(vintage.date.slice(0, 4)) >= year);
        if (k >= 0 && k < shown) S.label(ctx, String(year), g.left - 6, g.top + (shown - 1 - k) * g.rowHeight + 3, { colour: C.slate, size: 10, font: S.FONTS.mono, align: "right" });
      }
      S.yearTicks(ctx, { x0: g.left, x1: g.right, y: g.top + shownHeight + 2, from: fromYear, to: toYear });
      S.label(ctx, `${finished ? "COMPLETE" : "WRITING"} ${S.stamp(vintages[shown - 1].date)}`, g.left, 22, { size: 12 });
      S.label(ctx, "NEWEST RELEASE AT TOP", g.right, 22, { size: 11, colour: C.slate, align: "right" });
      drawLegend(ctx, height);
    },
  };
});
