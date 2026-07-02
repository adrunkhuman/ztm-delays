(() => {
  const MIN_DELAY = -600;
  const MAX_DELAY = 1200;
  const HEIGHT = 150;
  const PADDING = 18;
  const ON_TIME_START = -60;
  const ON_TIME_END = 180;

  function clamp(value, min, max) {
    return Math.max(min, Math.min(max, value));
  }

  function xForDelay(delay, width) {
    const clamped = clamp(delay, MIN_DELAY, MAX_DELAY);
    return PADDING + ((clamped - MIN_DELAY) / (MAX_DELAY - MIN_DELAY)) * (width - PADDING * 2);
  }

  function yForIndex(index) {
    const band = HEIGHT - PADDING * 2;
    return PADDING + ((index * 37) % band);
  }

  function drawGuide(ctx, width, delay, label, color) {
    const x = xForDelay(delay, width);
    ctx.strokeStyle = color;
    ctx.beginPath();
    ctx.moveTo(x, PADDING / 2);
    ctx.lineTo(x, HEIGHT - PADDING / 2);
    ctx.stroke();
    ctx.fillStyle = color;
    ctx.fillText(label, x + 4, HEIGHT - 4);
  }

  function draw(canvas) {
    const source = document.getElementById(canvas.dataset.pointsId);
    if (!source) return;

    const points = JSON.parse(source.textContent || "[]");
    const ratio = window.devicePixelRatio || 1;
    const width = Math.max(280, canvas.clientWidth);
    canvas.width = width * ratio;
    canvas.height = HEIGHT * ratio;
    const ctx = canvas.getContext("2d");
    ctx.scale(ratio, ratio);
    ctx.clearRect(0, 0, width, HEIGHT);
    ctx.font = "12px system-ui, sans-serif";
    ctx.lineWidth = 1;

    const onTimeStart = xForDelay(ON_TIME_START, width);
    const onTimeEnd = xForDelay(ON_TIME_END, width);
    ctx.fillStyle = "rgba(255, 255, 255, 0.06)";
    ctx.fillRect(onTimeStart, PADDING / 2, onTimeEnd - onTimeStart, HEIGHT - PADDING);

    drawGuide(ctx, width, MIN_DELAY, "-10m", "#555");
    drawGuide(ctx, width, 0, "0", "#ddd");
    drawGuide(ctx, width, ON_TIME_END, "+3m", "#777");
    drawGuide(ctx, width, MAX_DELAY, "+20m", "#555");

    ctx.fillStyle = "rgba(245, 245, 245, 0.62)";
    points.forEach((delay, index) => {
      ctx.beginPath();
      ctx.arc(xForDelay(delay, width), yForIndex(index), 2.4, 0, Math.PI * 2);
      ctx.fill();
    });

    if (points.length === 0) {
      ctx.fillStyle = "#8e8e8e";
      ctx.fillText("No points", PADDING, HEIGHT / 2);
    }
  }

  function drawAll() {
    document.querySelectorAll("canvas.delay-distribution").forEach(draw);
  }

  window.addEventListener("resize", drawAll);
  document.addEventListener("DOMContentLoaded", drawAll);
})();
