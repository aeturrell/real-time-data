Social.register("title_card", (payload) => {
  const S = Social;
  const C = S.COLOURS;
  const CYCLE_SECONDS = 6;
  const CYCLES = 2;
  const PEAK = 0.9;
  const SNAP_SECONDS = 0.6;
  const RING_SECONDS = 1.2;
  const DRIFT_PEAK = 0.55;
  const REFERENCE_SIZE = 100;

  function corruption(local) {
    if (local < SNAP_SECONDS) return S.lerp(PEAK, 0, S.ease(local / SNAP_SECONDS));
    return Math.pow((local - SNAP_SECONDS) / (CYCLE_SECONDS - SNAP_SECONDS), 2) * DRIFT_PEAK;
  }

  function drawRing(ctx, width, height, local) {
    const radius = S.ease(local / RING_SECONDS) * Math.hypot(width, height) * 0.6;
    const strength = 1 - local / RING_SECONDS;
    ctx.lineWidth = 3;
    ctx.globalCompositeOperation = "lighter";
    [[C.cyan, 0], [C.magenta, 10]].forEach(([colour, offset]) => {
      ctx.strokeStyle = colour;
      ctx.globalAlpha = strength * 0.7;
      ctx.beginPath();
      ctx.arc(width / 2, height / 2, Math.max(0, radius - offset), 0, Math.PI * 2);
      ctx.stroke();
    });
    ctx.globalCompositeOperation = "source-over";
    ctx.globalAlpha = 1;
  }

  function fittedSize(ctx, text, weight, family, width, cap) {
    ctx.font = `${weight} ${REFERENCE_SIZE}px ${family}`;
    return Math.min(cap, (REFERENCE_SIZE * width) / ctx.measureText(text).width);
  }

  return {
    duration: CYCLE_SECONDS * CYCLES,
    reset() {},
    draw(ctx, width, height, time) {
      const finished = time >= CYCLE_SECONDS * CYCLES;
      const local = finished ? SNAP_SECONDS : time % CYCLE_SECONDS;
      const amount = corruption(local);
      S.clear(ctx, width, height, C.grey);
      if (local < RING_SECONDS && !finished) drawRing(ctx, width, height, local);
      const seed = Math.floor(time * 8);
      for (let band = 0; band < 4; band++) {
        if (S.noise(seed, band) > amount * 1.4) continue;
        ctx.fillStyle = C.white;
        ctx.globalAlpha = 0.05;
        ctx.fillRect(0, S.noise(seed, band + 5) * height, width, 3 + S.noise(seed, band + 9) * 14);
      }
      ctx.globalAlpha = 1;
      const titleSize = fittedSize(ctx, payload.title, 600, S.FONTS.head, width * 0.84, height * 0.3);
      const wobble = Math.sin(time * 23) * amount * 6;
      S.glitchText(ctx, payload.title, width / 2, height * 0.44 + wobble, { size: titleSize, amount, seed: Math.floor(time * 16) });
      const subtitleSize = Math.min(titleSize * 0.2, fittedSize(ctx, payload.subtitle, 400, S.FONTS.mono, width * 0.84, height));
      S.glitchText(ctx, payload.subtitle, width / 2, height * 0.44 + titleSize * 0.68, {
        size: subtitleSize,
        family: S.FONTS.mono,
        weight: 400,
        amount: amount * 0.6,
        seed: Math.floor(time * 16) + 7,
        colour: C.secondary,
      });
    },
  };
});
