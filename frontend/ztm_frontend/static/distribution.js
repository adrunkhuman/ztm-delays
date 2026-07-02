(() => {
  const MIN_DELAY = -300;
  const MAX_DELAY = 900;
  const ROW_HEIGHT = 42;
  const PADDING = { top: 14, right: 120, bottom: 22, left: 92 };
  const ON_TIME_START = -60;
  const ON_TIME_END = 180;

  function clamp(value, min, max) {
    return Math.max(min, Math.min(max, value));
  }

  function formatDelay(value) {
    if (value === null || value === undefined) return "n/a";
    const rounded = Math.round(value);
    return rounded > 0 ? `+${rounded}s` : `${rounded}s`;
  }

  function formatPercent(value) {
    if (value === null || value === undefined) return "n/a";
    return `${Math.round(value * 100)}%`;
  }

  function plotWidth(width) {
    return width - PADDING.left - PADDING.right;
  }

  function xForDelay(delay, width) {
    const clamped = clamp(delay, MIN_DELAY, MAX_DELAY);
    return PADDING.left + ((clamped - MIN_DELAY) / (MAX_DELAY - MIN_DELAY)) * plotWidth(width);
  }

  function drawVerticalGuide(ctx, width, delay, label, color, height) {
    const x = xForDelay(delay, width);
    ctx.strokeStyle = color;
    ctx.beginPath();
    ctx.moveTo(x, PADDING.top);
    ctx.lineTo(x, height - PADDING.bottom);
    ctx.stroke();
    ctx.fillStyle = color;
    ctx.fillText(label, x - 10, height - 5);
  }

  function draw(canvas) {
    const source = document.getElementById(canvas.dataset.pointsId);
    if (!source) return;

    const periods = JSON.parse(source.textContent || "[]");
    const height = PADDING.top + PADDING.bottom + periods.length * ROW_HEIGHT;
    const ratio = window.devicePixelRatio || 1;
    const width = Math.max(360, canvas.clientWidth);
    canvas.width = width * ratio;
    canvas.height = height * ratio;
    canvas.style.height = `${height}px`;

    const ctx = canvas.getContext("2d");
    ctx.scale(ratio, ratio);
    ctx.clearRect(0, 0, width, height);
    ctx.font = "12px system-ui, sans-serif";
    ctx.lineWidth = 1;

    const onTimeStart = xForDelay(ON_TIME_START, width);
    const onTimeEnd = xForDelay(ON_TIME_END, width);
    ctx.fillStyle = "rgba(255, 255, 255, 0.055)";
    ctx.fillRect(onTimeStart, PADDING.top, onTimeEnd - onTimeStart, height - PADDING.top - PADDING.bottom);

    drawVerticalGuide(ctx, width, MIN_DELAY, "-5m", "#444", height);
    drawVerticalGuide(ctx, width, 0, "0", "#ddd", height);
    drawVerticalGuide(ctx, width, ON_TIME_END, "+3m", "#666", height);
    drawVerticalGuide(ctx, width, MAX_DELAY, "+15m", "#444", height);

    periods.forEach((period, periodIndex) => {
      const centerY = PADDING.top + periodIndex * ROW_HEIGHT + ROW_HEIGHT / 2;
      ctx.fillStyle = "#f5f5f5";
      ctx.fillText(period.label, 8, centerY - 4);
      ctx.fillStyle = "#8e8e8e";
      ctx.fillText(period.range, 8, centerY + 12);

      ctx.strokeStyle = "rgba(255, 255, 255, 0.12)";
      ctx.beginPath();
      ctx.moveTo(PADDING.left, centerY);
      ctx.lineTo(width - PADDING.right, centerY);
      ctx.stroke();

      ctx.fillStyle = "rgba(245, 245, 245, 0.68)";
      period.points.forEach((delay, index) => {
        const jitter = ((index * 17) % 19) - 9;
        ctx.beginPath();
        ctx.arc(xForDelay(delay, width), centerY + jitter, 2.2, 0, Math.PI * 2);
        ctx.fill();
      });

      ctx.fillStyle = "#f5f5f5";
      ctx.fillText(formatPercent(period.on_time_rate), width - 104, centerY - 8);
      ctx.fillStyle = "#8e8e8e";
      ctx.fillText(`med ${formatDelay(period.median_delay_seconds)}`, width - 104, centerY + 7);
      ctx.fillText(`p90 ${formatDelay(period.p90_delay_seconds)}`, width - 104, centerY + 22);
    });

    if (periods.every((period) => period.points.length === 0)) {
      ctx.fillStyle = "#8e8e8e";
      ctx.fillText("No points", PADDING.left, height / 2);
    }
  }

  function drawAll() {
    document.querySelectorAll("canvas.delay-distribution").forEach(draw);
  }

  window.addEventListener("resize", drawAll);
  document.addEventListener("DOMContentLoaded", drawAll);
})();
