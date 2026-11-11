Social.register("tape_stack", (payload) => {
  const tape = TapeStack.create(payload, Social.COLOURS);
  return {
    duration: tape.duration,
    reset(ctx, width, height, pixelRatio) {
      tape.setup(ctx, width, height, pixelRatio);
    },
    draw(ctx, width, height, time) {
      tape.draw(time);
    },
  };
});
