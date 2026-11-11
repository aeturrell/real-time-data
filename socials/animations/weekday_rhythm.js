Social.register("weekday_rhythm", (payload) => {
  const S = Social;
  const C = S.COLOURS;
  const SWEEP_SECONDS = 3.5;
  const SWEEPS = 3;
  const CYCLES_PER_DAY = 7;
  const FADE_ALPHA = 0.05;
  const DAYS = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"];
  const counts = payload.counts;
  const peak = Math.max(...counts);
  const busiest = counts.indexOf(peak);
  let layer, lastX, geometry;

  function traceY(x, time) {
    const g = geometry;
    const position = S.clamp(((x - g.left) / (g.right - g.left)) * 7, 0, 6.9999);
    const day = Math.floor(position);
    const within = position - day;
    const amplitude = (counts[day] / peak) * g.half * 0.88;
    const jitter = (S.noise(Math.floor(x), Math.floor(time * 30)) - 0.5) * 2;
    return g.middle - amplitude * Math.sin(within * Math.PI) * Math.sin(within * 2 * Math.PI * CYCLES_PER_DAY) + jitter;
  }

  function segment(ctx, fromX, toX, time) {
    ctx.beginPath();
    ctx.moveTo(fromX, traceY(fromX, time));
    for (let x = fromX + 1; x <= toX; x += 1) ctx.lineTo(x, traceY(x, time));
    ctx.stroke();
  }

  function drawGraticule(ctx) {
    const g = geometry;
    ctx.fillStyle = C.violet;
    ctx.globalAlpha = 0.45;
    for (let i = 0; i <= 7; i++) ctx.fillRect(g.left + (i / 7) * (g.right - g.left), g.top, 1, g.bottom - g.top);
    for (let i = 0; i <= 4; i++) ctx.fillRect(g.left, g.top + (i / 4) * (g.bottom - g.top), g.right - g.left, 1);
    ctx.globalAlpha = 1;
  }

  function drawLabels(ctx) {
    const g = geometry;
    DAYS.forEach((day, i) => {
      const centre = g.left + ((i + 0.5) / 7) * (g.right - g.left);
      const colour = i === busiest ? C.amber : C.white;
      S.label(ctx, day, centre, g.bottom + 20, { size: 12, colour, align: "center" });
      S.label(ctx, S.thousands(counts[i]), centre, g.bottom + 38, { size: 12, font: S.FONTS.mono, colour: i === busiest ? C.amber : C.slate, align: "center" });
    });
  }

  return {
    duration: SWEEP_SECONDS * SWEEPS,
    reset(ctx, width, height, pixelRatio) {
      geometry = { left: 20, right: width - 20, top: 10, bottom: height - 50 };
      geometry.middle = (geometry.top + geometry.bottom) / 2;
      geometry.half = (geometry.bottom - geometry.top) / 2;
      layer = S.makeLayer(width, height, pixelRatio);
      lastX = geometry.left;
    },
    draw(ctx, width, height, time) {
      const g = geometry;
      if (time >= SWEEP_SECONDS * SWEEPS) {
        S.clear(ctx, width, height);
        drawGraticule(ctx);
        ctx.strokeStyle = C.cyan;
        ctx.lineWidth = 1.8;
        ctx.shadowColor = C.cyan;
        ctx.shadowBlur = 6;
        segment(ctx, g.left, g.right, SWEEP_SECONDS * SWEEPS);
        ctx.shadowBlur = 0;
        drawLabels(ctx);
        return;
      }
      const beamX = g.left + ((time % SWEEP_SECONDS) / SWEEP_SECONDS) * (g.right - g.left);
      const trace = layer.context;
      trace.shadowBlur = 0;
      trace.globalCompositeOperation = "destination-out";
      trace.fillStyle = `rgba(0, 0, 0, ${FADE_ALPHA})`;
      trace.fillRect(0, 0, width, height);
      trace.globalCompositeOperation = "source-over";
      trace.strokeStyle = C.cyan;
      trace.lineWidth = 1.8;
      trace.shadowColor = C.cyan;
      trace.shadowBlur = 6;
      if (beamX >= lastX) segment(trace, lastX, beamX, time);
      else {
        segment(trace, lastX, g.right, time);
        segment(trace, g.left, beamX, time);
      }
      lastX = beamX;
      S.clear(ctx, width, height);
      drawGraticule(ctx);
      ctx.drawImage(layer.canvas, 0, 0, width, height);
      ctx.fillStyle = C.white;
      ctx.shadowColor = C.cyan;
      ctx.shadowBlur = 12;
      ctx.beginPath();
      ctx.arc(beamX, traceY(beamX, time), 3, 0, Math.PI * 2);
      ctx.fill();
      ctx.shadowBlur = 0;
      drawLabels(ctx);
    },
  };
});
