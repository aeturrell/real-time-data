Social.register("provenance", (payload) => {
  const S = Social;
  const C = S.COLOURS;
  const EVENT_SECONDS = 2.6;
  const FADE_SECONDS = 0.35;
  const FONT_SIZE = 14;
  const HEX = "0123456789abcdef";
  const records = payload.records;
  let tracks, characterWidth, rowHeight;

  return {
    duration: records.length * EVENT_SECONDS,
    reset(ctx, width, height) {
      ctx.font = `${FONT_SIZE}px ${S.FONTS.mono}`;
      characterWidth = ctx.measureText("0").width;
      rowHeight = height / payload.tracks.length;
      tracks = payload.tracks.map((text, row) => ({
        text: `${text}  `,
        width: (text.length + 2) * characterWidth,
        speed: 16 + S.noise(row, 2) * 48,
        direction: row % 2 ? 1 : -1,
        alpha: 0.18 + S.noise(row, 9) * 0.35,
      }));
    },
    draw(ctx, width, height, time) {
      S.clear(ctx, width, height);
      const event = Math.min(records.length - 1, Math.floor(time / EVENT_SECONDS));
      const local = time - event * EVENT_SECONDS;
      const eventRow = Math.floor(S.noise(event, 3) * tracks.length);
      const message = `${records[event][0]}  ${records[event][1]}`;
      ctx.font = `${FONT_SIZE}px ${S.FONTS.mono}`;
      ctx.textBaseline = "middle";
      ctx.textAlign = "left";
      tracks.forEach((track, row) => {
        const y = row * rowHeight + rowHeight / 2;
        const shift = (time * track.speed) % track.width;
        const start = track.direction < 0 ? -shift : shift - track.width;
        ctx.globalAlpha = track.alpha;
        ctx.fillStyle = C.amber;
        for (let x = start; x < width; x += track.width) ctx.fillText(track.text, x, y);
        ctx.globalAlpha = 0.25;
        ctx.fillStyle = C.violet;
        ctx.fillRect(0, y + rowHeight / 2 - 1, width, 1);
      });

      const lastEvent = event === records.length - 1;
      const fade = !lastEvent && local > EVENT_SECONDS - FADE_SECONDS ? (EVENT_SECONDS - local) / FADE_SECONDS : 1;
      const boxWidth = Math.min(width - 24, message.length * characterWidth + 24);
      const shown = message.slice(0, Math.floor((boxWidth - 24) / characterWidth));
      const boxLeft = (width - boxWidth) / 2;
      const boxTop = eventRow * rowHeight + 2;
      ctx.globalAlpha = fade;
      ctx.fillStyle = C.deep;
      ctx.fillRect(boxLeft, boxTop, boxWidth, rowHeight - 4);
      ctx.strokeStyle = C.cyan;
      ctx.lineWidth = 1;
      ctx.strokeRect(boxLeft + 0.5, boxTop + 0.5, boxWidth - 1, rowHeight - 5);
      const scramble = Math.floor(time * 24);
      const hashLength = records[event][0].length;
      for (let i = 0; i < shown.length; i++) {
        const revealed = local > 0.12 + i * 0.016;
        ctx.fillStyle = revealed ? (i < hashLength ? C.cyan : C.white) : C.magenta;
        ctx.fillText(revealed ? shown[i] : HEX[Math.floor(S.noise(scramble, i) * 16)], boxLeft + 12 + i * characterWidth, boxTop + (rowHeight - 4) / 2);
      }
      ctx.globalAlpha = 1;
    },
  };
});
