Social.register("two_clocks", (payload) => {
  const S = Social;
  const C = S.COLOURS;
  const VINTAGES_PER_SECOND = 3;
  const TRACE_ALPHA = { first: 0.35, revised: 0.12 };
  const FADE_SECONDS = 1.6;
  const SNAP_SECONDS = 0.12;
  const DAYS_PER_YEAR = 365.25;
  const quarters = payload.quarters;
  const vintages = payload.vintages;
  const vintageCount = vintages.length;
  const firstSets = vintages.map((vintage) => new Set(vintage.first));
  const firstVintageOf = quarters.map((_, j) => firstSets.findIndex((set) => set.has(j)));
  const newestQuarter = [];
  vintages.forEach((vintage, k) => newestQuarter.push(Math.max(k ? newestQuarter[k - 1] : -1, ...vintage.first)));
  const positions = [...quarters.map(S.quarterYear), ...vintages.map((vintage) => S.dateYear(vintage.date))];
  const fromYear = Math.floor(Math.min(...positions));
  const toYear = Math.ceil(Math.max(...positions) + 0.25);
  let geometry, trace, traced;

  const xAt = (year) => geometry.left + ((year - fromYear) / (toYear - fromYear)) * (geometry.right - geometry.left);

  function drawLinks(ctx, time, last) {
    const g = geometry;
    const reach = Math.ceil(FADE_SECONDS * VINTAGES_PER_SECOND);
    ctx.lineWidth = 1.2;
    for (let k = Math.max(0, last - reach); k <= last; k++) {
      const age = time - k / VINTAGES_PER_SECOND;
      if (age > FADE_SECONDS || age < 0) continue;
      const fade = Math.exp(-age * 2.2);
      const bottomX = xAt(S.dateYear(vintages[k].date));
      [[vintages[k].first, C.cyan, 1], [vintages[k].revised, C.magenta, 0.55]].forEach(([indices, colour, strength]) => {
        ctx.strokeStyle = colour;
        ctx.globalAlpha = fade * strength;
        indices.forEach((j) => {
          const topX = xAt(S.quarterYear(quarters[j]) + 0.125);
          ctx.beginPath();
          ctx.moveTo(topX, g.topY);
          if (age < SNAP_SECONDS) ctx.lineTo(S.lerp(topX, bottomX, 0.5) + (S.noise(j, k) - 0.5) * 30, S.lerp(g.topY, g.bottomY, 0.3 + S.noise(k, j) * 0.4));
          ctx.lineTo(bottomX, g.bottomY);
          ctx.stroke();
        });
      });
    }
    ctx.globalAlpha = 1;
  }

  function addToTrace(k) {
    const ctx = trace.context;
    const bottomX = xAt(S.dateYear(vintages[k].date));
    ctx.lineWidth = 1;
    [[vintages[k].first, C.cyan, TRACE_ALPHA.first], [vintages[k].revised, C.magenta, TRACE_ALPHA.revised]].forEach(([indices, colour, alpha]) => {
      ctx.strokeStyle = colour;
      ctx.globalAlpha = alpha;
      indices.forEach((j) => {
        ctx.beginPath();
        ctx.moveTo(xAt(S.quarterYear(quarters[j]) + 0.125), geometry.topY);
        ctx.lineTo(bottomX, geometry.bottomY);
        ctx.stroke();
      });
    });
    ctx.globalAlpha = 1;
  }

  function drawTicks(ctx, last) {
    const g = geometry;
    ctx.fillStyle = C.white;
    ctx.globalAlpha = 0.35;
    for (let j = 0; j <= newestQuarter[last]; j++) if (firstVintageOf[j] >= 0 && firstVintageOf[j] <= last) ctx.fillRect(xAt(S.quarterYear(quarters[j]) + 0.125), g.topY + 2, 1, 6);
    ctx.fillStyle = C.slate;
    ctx.globalAlpha = 0.7;
    for (let k = 0; k <= last; k++) ctx.fillRect(xAt(S.dateYear(vintages[k].date)), g.bottomY - 8, 1, 6);
    ctx.globalAlpha = 1;
  }

  return {
    duration: vintageCount / VINTAGES_PER_SECOND,
    reset(ctx, width, height, pixelRatio) {
      geometry = { left: 24, right: width - 24, topY: height * 0.36, bottomY: height * 0.72 };
      trace = S.makeLayer(width, height, pixelRatio);
      traced = 0;
    },
    draw(ctx, width, height, time) {
      const g = geometry;
      const last = Math.min(vintageCount - 1, Math.floor(time * VINTAGES_PER_SECOND));
      S.clear(ctx, width, height);
      ctx.fillStyle = C.violet;
      ctx.fillRect(g.left, g.topY, g.right - g.left, 1.5);
      ctx.fillRect(g.left, g.bottomY, g.right - g.left, 1.5);
      S.yearTicks(ctx, { x0: g.left, x1: g.right, y: g.topY - 2, from: fromYear, to: toYear, every: 1, above: true });
      S.yearTicks(ctx, { x0: g.left, x1: g.right, y: g.bottomY + 2, from: fromYear, to: toYear, every: 1 });
      while (traced <= last) addToTrace(traced++);
      ctx.drawImage(trace.canvas, 0, 0, width, height);
      drawTicks(ctx, last);
      drawLinks(ctx, time, last);

      const newest = newestQuarter[last];
      const fresh = time - last / VINTAGES_PER_SECOND < SNAP_SECONDS;
      const size = S.clamp(height * 0.1, 18, 40);
      S.label(ctx, "QUARTER THE FIGURE DESCRIBES", g.left, height * 0.07, { size: 11, colour: C.slate });
      S.glitchText(ctx, S.quarterText(quarters[newest]), g.left, height * 0.15, { size, align: "left", amount: fresh && vintages[last].first.length ? 0.3 : 0, seed: last, colour: C.cyan });
      S.label(ctx, "DATE IT WAS PUBLISHED", g.right, height * 0.07, { size: 11, colour: C.slate, align: "right" });
      S.glitchText(ctx, S.stamp(vintages[last].date), g.right, height * 0.15, { size, align: "right", amount: fresh ? 0.25 : 0, seed: last + 3 });

      const firstDate = vintages[firstVintageOf[newest]].date;
      const quarterEnd = S.quarterYear(quarters[newest]) + 0.25;
      const lagDays = Math.round((S.dateYear(firstDate) - quarterEnd) * DAYS_PER_YEAR);
      S.label(ctx, `FIRST ESTIMATE OF ${S.quarterText(quarters[newest])} PUBLISHED ABOUT ${lagDays} DAYS AFTER THE QUARTER ENDED`, width / 2, height - 12, { size: 11, align: "center" });
      [[C.cyan, "FIRST ESTIMATE"], [C.magenta, "REVISION"]].forEach(([colour, text], index) => {
        ctx.fillStyle = colour;
        ctx.fillRect(g.right - 150, g.topY + 26 + index * 20, 18, 3);
        S.label(ctx, text, g.right - 126, g.topY + 31 + index * 20, { size: 10, colour: C.white });
      });
    },
  };
});
