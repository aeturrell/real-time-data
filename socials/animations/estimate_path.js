Social.register("estimate_path", (payload) => {
  const S = Social;
  const C = S.COLOURS;
  const GLITCH_TIME = 0.22;
  const TARGET_SECONDS = 16;
  const estimates = payload.estimates;
  const count = estimates.length;
  const values = estimates.map((estimate) => estimate[1]);
  const step = S.clamp(TARGET_SECONDS / count, 0.5, 1.2);
  const lowest = Math.min(...values);
  const highest = Math.max(...values);
  const padding = (highest - lowest) * 0.2 || 1;

  function drawPath(ctx, width, height, index) {
    const x = (k) => 24 + (k / Math.max(1, count - 1)) * (width - 48);
    const y = (v) => height - 30 - ((v - (lowest - padding)) / (highest - lowest + 2 * padding)) * (height - 60);
    ctx.strokeStyle = C.violet;
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    for (let k = 0; k <= index; k++) k ? ctx.lineTo(x(k), y(values[k])) : ctx.moveTo(x(k), y(values[k]));
    ctx.stroke();
    for (let k = 0; k <= index; k++) {
      ctx.fillStyle = k === index ? C.amber : C.slate;
      ctx.fillRect(x(k) - 2, y(values[k]) - 2, 4, 4);
    }
  }

  function drawTrackingBars(ctx, width, height, index, strength) {
    for (let band = 0; band < 3; band++) {
      const top = S.noise(index, band) * height;
      const bandHeight = 4 + S.noise(index, band + 9) * 18;
      ctx.fillStyle = C.white;
      ctx.globalAlpha = 0.07 * strength + 0.03;
      ctx.fillRect(0, top, width, bandHeight);
      for (let speck = 0; speck < 30; speck++) {
        ctx.globalAlpha = 0.5 * strength;
        ctx.fillRect(S.noise(index * 31 + speck, band) * width, top + S.noise(speck, index) * bandHeight, 4 + S.noise(speck, band) * 30, 1);
      }
    }
    ctx.globalAlpha = 1;
  }

  return {
    duration: count * step,
    reset() {},
    draw(ctx, width, height, time) {
      const index = Math.min(count - 1, Math.floor(time / step));
      const local = time - index * step;
      const finished = time >= count * step;
      const glitch = finished ? 0 : Math.max(0, 1 - local / GLITCH_TIME) * (index > 0 ? 1 : 0.6);
      S.clear(ctx, width, height);
      drawPath(ctx, width, height, index);
      if (glitch > 0) drawTrackingBars(ctx, width, height, index, glitch);
      S.glitchText(ctx, S.percent(values[index]), width / 2, height * 0.47, {
        size: Math.min(height * 0.36, width * 0.22),
        amount: glitch * 0.45,
        seed: Math.floor(time * 14),
      });
      const transport = glitch > 0.3 ? "◂◂ REW" : finished ? "■ STOP" : "PLAY ▸";
      S.label(ctx, `${transport}  ${S.stamp(estimates[index][0])}`, 16, 26, { size: 14 });
      S.label(ctx, `${S.quarterText(payload.quarter)} · ESTIMATE ${index + 1} OF ${count}`, 16, height - 14, { size: 12, colour: C.white });
      S.label(ctx, `RANGE ${S.percent(lowest)} TO ${S.percent(highest)}`, width - 16, 26, { size: 12, colour: C.slate, align: "right" });
    },
  };
});
