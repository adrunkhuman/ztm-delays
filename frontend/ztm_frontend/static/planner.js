// Live planner results: htmx refreshes them while a live card is on screen; this keeps open cards open across a
// refresh and draws each live ride's minimap when its card opens. Cards are server-rendered.
(() => {
  const MAPLIBRE = "https://cdn.jsdelivr.net/npm/maplibre-gl@5.24.0/dist/maplibre-gl";
  const VEHICLE = "#ffb02e";
  const STOP = "#f4f4f4";
  const PATH = "#9a9a9a";

  // Poll only while the page is visible and a live card is in view. A trigger filter ([...]) is evaluated by
  // htmx 4.0.0-beta5 but does not stop the request, so the request is cancelled instead.
  const shouldRefresh = () =>
    document.visibilityState === "visible" &&
    [...document.querySelectorAll(".pl-card.live")].some((card) => {
      const box = card.getBoundingClientRect();
      return box.bottom > 0 && box.top < window.innerHeight;
    });

  document.addEventListener("htmx:before:request", (event) => {
    if (event.target?.id === "pl-results" && !shouldRefresh()) event.preventDefault();
  });

  let opened = [];
  document.addEventListener("htmx:before:swap", (event) => {
    if (event.detail?.ctx?.target?.id !== "pl-results") return;
    opened = [...document.querySelectorAll("#pl-results details[open][data-key]")].map((d) => d.dataset.key);
  });
  document.addEventListener("htmx:after:swap", () => {
    for (const key of opened) {
      document.querySelector(`#pl-results details[data-key="${CSS.escape(key)}"]`)?.setAttribute("open", "");
    }
    opened = [];
    for (const card of document.querySelectorAll("#pl-results .pl-card[open]")) draw(card);
  });
  // toggle does not bubble.
  document.addEventListener(
    "toggle",
    (event) => {
      if (event.target.matches?.(".pl-card") && event.target.open) draw(event.target);
    },
    true,
  );

  let loading;
  function loadMapLibre() {
    if (window.maplibregl) return Promise.resolve();
    loading ??= new Promise((resolve, reject) => {
      const css = document.createElement("link");
      css.rel = "stylesheet";
      css.href = `${MAPLIBRE}.css`;
      const tag = document.createElement("script");
      tag.src = `${MAPLIBRE}.js`;
      tag.onload = resolve;
      tag.onerror = () => reject(new Error("MapLibre failed to load"));
      document.head.append(css, tag);
    });
    return loading;
  }

  async function draw(card) {
    const boxes = card.querySelectorAll(".pl-minimap:not([data-drawn])");
    if (!boxes.length) return;
    try {
      await loadMapLibre();
    } catch {
      for (const box of boxes) box.hidden = true;
      return;
    }
    for (const box of boxes) {
      box.dataset.drawn = "1";
      const { vehicle, stop, path } = JSON.parse(box.dataset.map);
      const points = [vehicle, stop, ...path];
      const bounds = points.reduce((b, p) => b.extend(p), new maplibregl.LngLatBounds(vehicle, vehicle));
      const map = new maplibregl.Map({
        container: box,
        style: new URL(box.dataset.style, window.location.href).href,
        bounds,
        fitBoundsOptions: { padding: 28, maxZoom: 16 },
        interactive: false,
        attributionControl: { compact: true },
      });
      map.on("load", () => {
        // Compact attribution opens expanded on wide screens; on a minimap it would cover the route.
        box.querySelector(".maplibregl-ctrl-attrib")?.classList.remove("maplibregl-compact-show");
        const feature = (geometry) => ({ type: "Feature", geometry, properties: {} });
        map.addSource("path", { type: "geojson", data: feature({ type: "LineString", coordinates: path }) });
        map.addSource("stop", { type: "geojson", data: feature({ type: "Point", coordinates: stop }) });
        map.addSource("vehicle", { type: "geojson", data: feature({ type: "Point", coordinates: vehicle }) });
        map.addLayer({ id: "path", type: "line", source: "path", paint: { "line-color": PATH, "line-width": 3, "line-dasharray": [1.5, 1.5] } });
        map.addLayer({ id: "stop", type: "circle", source: "stop", paint: { "circle-radius": 6, "circle-color": "#111111", "circle-stroke-color": STOP, "circle-stroke-width": 3 } });
        map.addLayer({ id: "vehicle", type: "circle", source: "vehicle", paint: { "circle-radius": 7, "circle-color": VEHICLE, "circle-stroke-color": "#111111", "circle-stroke-width": 2 } });
      });
    }
  }
})();
