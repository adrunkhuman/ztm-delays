// Status page: refreshes the live panel and adds a crosshair readout to the server-drawn history charts.
(() => {
  const REFRESH_MS = 30000;

  // Boosted navigation can run this script again; keep a single timer.
  clearInterval(window.ztmStatusTimer);
  window.ztmStatusTimer = setInterval(async () => {
    const panel = document.getElementById("status-live");
    if (!panel) return;
    try {
      const response = await fetch("/status/live", { headers: { Accept: "text/html" } });
      if (!response.ok) return;
      panel.outerHTML = await response.text();
      const fresh = document.getElementById("status-live");
      const pill = document.getElementById("status-pill");
      if (!fresh || !pill) return;
      const overall = fresh.dataset.overall;
      pill.hidden = !overall;
      if (overall) {
        pill.className = "status-pill " + overall;
        pill.querySelector(".status-pill-label").textContent = fresh.dataset.overallLabel;
      }
    } catch {
      // Keep the last rendered state; the next tick tries again.
    }
  }, REFRESH_MS);

  const format = (n) => (n == null ? "n/a" : Math.round(n).toLocaleString("en-US"));

  function row(color, value, label) {
    const line = document.createElement("div");
    const key = document.createElement("i");
    const strong = document.createElement("b");
    key.style.background = color;
    strong.textContent = value;
    line.append(key, strong, document.createTextNode(" " + label));
    return line;
  }

  const observers = new Map();
  const cleanup = (event) => {
    for (const [figure, observer] of observers) {
      if (event.target === figure || event.target.contains(figure)) {
        observer.disconnect();
        observers.delete(figure);
      }
    }
    if (!observers.size) document.removeEventListener("htmx:before:cleanup", cleanup);
  };
  document.addEventListener("htmx:before:cleanup", cleanup);

  for (const figure of document.querySelectorAll(".status-chart")) {
    const svg = figure.querySelector("svg");
    const cross = figure.querySelector(".cross");
    const tip = figure.querySelector(".status-tip");
    const points = JSON.parse(figure.dataset.points);
    const start = new Date(figure.dataset.start);
    const left = +figure.dataset.left;
    const right = +figure.dataset.right;
    const width = +figure.dataset.w;
    const height = svg.viewBox.baseVal.height;
    const plot = figure.querySelector(".status-plot");
    let scale = 1;
    const resize = () => {
      const available = svg.getBoundingClientRect().width;
      if (available <= left + right) return;
      // Compress the plot, not its text: phone-sized charts retain readable axes and a useful height.
      scale = (available - left - right) / (width - left - right);
      svg.setAttribute("viewBox", `0 0 ${available} ${height}`);
      plot.setAttribute("transform", `translate(${left} 0) scale(${scale} 1) translate(${-left} 0)`);
      let previousEnd = -Infinity;
      for (const label of figure.querySelectorAll(".time-axis")) {
        const x = left + (+label.dataset.x - left) * scale;
        label.setAttribute("x", x);
        label.style.display = "";
        const half = label.getComputedTextLength() / 2;
        const fits = x - half >= previousEnd + 12 && x - half >= 0 && x + half <= available;
        label.style.display = fits ? "" : "none";
        if (fits) previousEnd = x + half;
      }
    };
    resize();
    document.fonts.ready.then(() => { if (figure.isConnected) resize(); });
    const observer = new ResizeObserver(resize);
    observer.observe(figure);
    observers.set(figure, observer);
    const hide = () => {
      cross.style.visibility = tip.style.visibility = "hidden";
    };
    svg.addEventListener("pointerleave", hide);
    svg.addEventListener("pointermove", (event) => {
      const box = svg.getBoundingClientRect();
      const vx = left + (event.clientX - box.left - left) / scale;
      const i = Math.max(0, Math.min(points.length - 1, Math.round(((vx - left) / (width - left - right)) * (points.length - 1))));
      const x = left + ((width - left - right) * i) / (points.length - 1);
      cross.setAttribute("x1", x);
      cross.setAttribute("x2", x);
      cross.style.visibility = "visible";
      const [actual, usual] = points[i];
      const when = new Date(start.getTime() + i * 60000).toLocaleTimeString("en-GB", {
        timeZone: "Europe/Warsaw",
        weekday: "short",
        hour: "2-digit",
        minute: "2-digit",
      });
      const head = document.createElement("div");
      head.textContent = when;
      tip.replaceChildren(head, row("var(--blue)", actual == null ? "no data" : format(actual), "fresh"), row("#8a8a8a", format(usual), "usual"));
      tip.style.visibility = "visible";
      tip.style.left = Math.max(0, Math.min(left + (x - left) * scale + 10, box.width - tip.offsetWidth)) + "px";
    });
  }
})();
