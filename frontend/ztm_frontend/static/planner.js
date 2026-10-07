// Live planner results: htmx refreshes them while a live card is on screen; this keeps open cards open across a
// refresh and draws each live ride's minimap when its card opens. Cards are server-rendered.
(() => {
  const MAPLIBRE = "https://cdn.jsdelivr.net/npm/maplibre-gl@5.24.0/dist/maplibre-gl";

  // The results' polling filter (htmx 4 reads it only attached to the event: every[...] 60s): poll while the page
  // is visible and a live card is in view.
  window.plannerShouldRefresh = () =>
    document.visibilityState === "visible" &&
    [...document.querySelectorAll(".pl-card.live")].some((card) => {
      const box = card.getBoundingClientRect();
      return box.bottom > 0 && box.top < window.innerHeight;
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

  // Both files: without its stylesheet, MapLibre's markers are not positioned yet and land in the wrong place.
  let loading;
  function loadMapLibre() {
    loading ??= Promise.all([
      load(Object.assign(document.createElement("link"), { rel: "stylesheet", href: `${MAPLIBRE}.css` })),
      load(Object.assign(document.createElement("script"), { src: `${MAPLIBRE}.js` })),
    ]);
    return loading;
  }

  function load(element) {
    return new Promise((resolve, reject) => {
      element.onload = resolve;
      element.onerror = () => reject(new Error(`${element.src || element.href} failed to load`));
      document.head.append(element);
    });
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
        fitBoundsOptions: { padding: 36, maxZoom: 16 },
        interactive: false,
        attributionControl: { compact: true },
      });
      // The page's own symbols: the timeline's stop dot, the line pill for the vehicle, the ride's mode colour.
      const stopDot = document.createElement("span");
      stopDot.className = "pl-map-stop";
      const pill = document.createElement("span");
      pill.className = `landing-line-pill mono pl-pill mode-${box.dataset.mode} pl-map-vehicle`;
      pill.textContent = box.dataset.line;
      new maplibregl.Marker({ element: stopDot }).setLngLat(stop).addTo(map);
      new maplibregl.Marker({ element: pill }).setLngLat(vehicle).addTo(map);
      const colour = getComputedStyle(box).getPropertyValue("--rail").trim() || "#888888";
      map.on("load", () => {
        // Compact attribution opens expanded on wide screens; on a minimap it would cover the route.
        box.querySelector(".maplibregl-ctrl-attrib")?.classList.remove("maplibregl-compact-show");
        const line = { type: "Feature", geometry: { type: "LineString", coordinates: path }, properties: {} };
        map.addSource("path", { type: "geojson", data: line });
        map.addLayer({
          id: "path",
          type: "line",
          source: "path",
          layout: { "line-cap": "round", "line-join": "round" },
          paint: { "line-color": colour, "line-width": 4 },
        });
      });
    }
  }
})();
