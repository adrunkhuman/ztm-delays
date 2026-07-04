(() => {
  const ROW_HEIGHT = 42;
  const PADDING = { top: 12, right: 118, bottom: 24, left: 92 };
  const BUCKETS = [
    { label: "early", min: Number.NEGATIVE_INFINITY, max: -61 },
    { label: "on time", min: -60, max: 180 },
    { label: "3-5m", min: 181, max: 300 },
    { label: "5-10m", min: 301, max: 600 },
    { label: ">10m", min: 601, max: Number.POSITIVE_INFINITY },
  ];

  function formatDelay(value) {
    if (value === null || value === undefined) return "n/a";
    const rounded = Math.round(value);
    return rounded > 0 ? `+${rounded}s` : `${rounded}s`;
  }

  function formatPercent(value) {
    if (value === null || value === undefined) return "n/a";
    return `${Math.round(value * 100)}%`;
  }

  function bucketCounts(points) {
    const counts = BUCKETS.map(() => 0);
    points.forEach((delay) => {
      const index = BUCKETS.findIndex((bucket) => delay >= bucket.min && delay <= bucket.max);
      if (index >= 0) counts[index] += 1;
    });
    return counts;
  }

  function drawBucket(ctx, x, y, width, height, share, isOnTime) {
    const alpha = 0.08 + share * 0.62;
    ctx.fillStyle = isOnTime ? `rgba(245, 245, 245, ${alpha})` : `rgba(145, 145, 145, ${alpha})`;
    ctx.fillRect(x, y, width, height);
    ctx.strokeStyle = "rgba(255, 255, 255, 0.12)";
    ctx.strokeRect(x, y, width, height);
  }

  function draw(canvas) {
    const source = document.getElementById(canvas.dataset.pointsId);
    if (!source) return;

    const periods = JSON.parse(source.textContent || "[]");
    const height = PADDING.top + PADDING.bottom + periods.length * ROW_HEIGHT;
    const ratio = window.devicePixelRatio || 1;
    const width = Math.max(420, canvas.clientWidth);
    canvas.width = width * ratio;
    canvas.height = height * ratio;
    canvas.style.height = `${height}px`;

    const ctx = canvas.getContext("2d");
    ctx.scale(ratio, ratio);
    ctx.clearRect(0, 0, width, height);
    ctx.font = "12px system-ui, sans-serif";
    ctx.lineWidth = 1;

    const bucketAreaWidth = width - PADDING.left - PADDING.right;

    periods.forEach((period, periodIndex) => {
      const rowTop = PADDING.top + periodIndex * ROW_HEIGHT;
      const centerY = rowTop + ROW_HEIGHT / 2;
      const counts = bucketCounts(period.points);
      const total = Math.max(1, period.points.length);

      ctx.fillStyle = "#f5f5f5";
      ctx.fillText(period.label, 8, centerY - 4);
      ctx.fillStyle = "#8e8e8e";
      ctx.fillText(period.range, 8, centerY + 12);

      let x = PADDING.left;
      BUCKETS.forEach((bucket, index) => {
        const share = counts[index] / total;
        const segmentWidth = bucketAreaWidth * share;
        if (segmentWidth <= 0) return;

        drawBucket(ctx, x, centerY - 11, Math.max(2, segmentWidth - 2), 22, share, bucket.label === "on time");

        if (segmentWidth >= 34) {
          ctx.fillStyle = share > 0.34 ? "#000" : "#d8d8d8";
          ctx.fillText(`${Math.round(share * 100)}%`, x + 8, centerY + 4);
        }
        x += segmentWidth;
      });

      ctx.fillStyle = "#f5f5f5";
      ctx.fillText(formatPercent(period.on_time_rate), width - 104, centerY - 8);
      ctx.fillStyle = "#8e8e8e";
      ctx.fillText(`med ${formatDelay(period.median_delay_seconds)}`, width - 104, centerY + 7);
      ctx.fillText(`p90 ${formatDelay(period.p90_delay_seconds)}`, width - 104, centerY + 22);
    });

    ctx.fillStyle = "#777";
    ctx.fillText("early", PADDING.left, height - 6);
    ctx.fillText("on time", PADDING.left + bucketAreaWidth * 0.28, height - 6);
    ctx.fillText("late", PADDING.left + bucketAreaWidth * 0.72, height - 6);

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
