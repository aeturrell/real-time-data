Social.register("release_calendar", (payload) => {
  const S = Social;
  const C = S.COLOURS;
  const SWEEP_SECONDS = 8;
  const FRESH_SHARE = 0.025;
  const days = payload.days.map(([date, count]) => [S.dateYear(date), count]);
  const fromYear = Math.floor(days[0][0]);
  const toYear = Math.floor(days[days.length - 1][0]) + 1;
  const largestLog = Math.log10(Math.max(...days.map((day) => day[1])) + 1);

  return {
    duration: SWEEP_SECONDS,
    reset() {},
    draw(ctx, width, height, time) {
      const left = 20;
      const right = width - 20;
      const base = height - 36;
      const top = 44;
      const xAt = (year) => left + ((year - fromYear) / (toYear - fromYear)) * (right - left);
      const progress = S.clamp(time / SWEEP_SECONDS, 0, 1);
      const headX = left + progress * (right - left);
      S.clear(ctx, width, height);
      let shown = 0;
      days.forEach(([year, count]) => {
        const x = xAt(year);
        if (x > headX) return;
        shown++;
        const lineHeight = (Math.log10(count + 1) / largestLog) * (base - top);
        const fresh = (headX - x) / (right - left) < FRESH_SHARE && progress < 1;
        const busy = count > payload.highlightAbove;
        ctx.fillStyle = fresh ? C.cyan : busy ? C.amber : C.white;
        ctx.globalAlpha = fresh ? 1 : busy ? 0.85 : 0.45;
        ctx.fillRect(x, base - lineHeight, 1, lineHeight);
      });
      ctx.globalAlpha = 1;
      ctx.fillStyle = C.violet;
      ctx.fillRect(left, base, right - left, 1);
      S.yearTicks(ctx, { x0: left, x1: right, y: base + 2, from: fromYear, to: toYear });
      if (progress < 1) {
        ctx.fillStyle = C.cyan;
        ctx.shadowColor = C.cyan;
        ctx.shadowBlur = 14;
        ctx.fillRect(headX - 1, top - 10, 2, base - top + 10);
        ctx.shadowBlur = 0;
      }
      const year = Math.min(toYear - 1, Math.floor(fromYear + progress * (toYear - fromYear)));
      S.label(ctx, progress < 1 ? `SCANNING ${year}` : "SCAN COMPLETE", left, 24, { size: 13 });
      S.label(ctx, `${S.thousands(shown)} PUBLICATION DATES`, right, 24, { size: 12, colour: C.white, align: "right" });
    },
  };
});
