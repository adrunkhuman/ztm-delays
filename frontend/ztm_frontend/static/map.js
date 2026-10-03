// Draws the route map. Page state, totals and popup contents are server-rendered; this only owns the canvas.
(() => {
  const MAPLIBRE_JS = "https://cdn.jsdelivr.net/npm/maplibre-gl@5.24.0/dist/maplibre-gl.js";
  const WARSAW = { center: [21.0122, 52.2297], zoom: 10.6 };
  // Diverging blue (recovers) / red (gains) around the site's early/late hues; neutral recedes into the dark base.
  const STEPS = [-60, -30, -10, 10, 30, 60];
  // Site early/late hues. Neutral sits near the background; red cannot brighten past pink, so width carries the top bin.
  const PALETTE = ["#4a9eff", "#3f80d0", "#33608f", "#4a4a4a", "#a03d3d", "#d84a45", "#ff5c5c"];
  // Width multiplier by |change|; doubles magnitude encoding so hotspots survive dense overlap.
  const WIDTH_STEPS = [0.55, 10, 0.85, 30, 1.2, 60, 1.75];
  const BACKGROUND = "#111111";
  const SELECTED = "#f4f4f4";

  const container = document.getElementById("route-map");
  const stats = document.getElementById("map-stats");
  const errorBox = document.getElementById("map-error");
  const view = { mode: container.dataset.mode, period: container.dataset.period };
  const tramBounds = JSON.parse(container.dataset.tramBounds);
  const key = () => `${view.period}_${view.mode}_d`;

  function showView() {
    for (const button of document.querySelectorAll(".map-toggle button")) {
      const active = button.dataset.value === view[button.parentElement.dataset.view];
      button.classList.toggle("active", active);
      button.setAttribute("aria-pressed", String(active));
    }
    for (const span of stats.children) span.hidden = span.dataset.view !== `${view.period}_${view.mode}`;
    // Month links and reloads keep the chosen view.
    for (const link of [window.location, ...document.querySelectorAll(".date a")]) {
      const url = new URL(link.href);
      url.searchParams.set("mode", view.mode);
      url.searchParams.set("period", view.period);
      if (link === window.location) window.history.replaceState(window.history.state, "", url);
      else link.href = url;
    }
  }

  function loadMapLibre() {
    if (window.maplibregl) return Promise.resolve();
    return new Promise((resolve, reject) => {
      const tag = document.createElement("script");
      tag.src = MAPLIBRE_JS;
      tag.onload = resolve;
      tag.onerror = () => reject(new Error("MapLibre failed to load"));
      document.head.append(tag);
    });
  }

  // Paint and filter read view-prefixed properties, so switching period or mode never re-uploads geometry.
  const colorExpression = () => ["step", ["get", key()], ...PALETTE.flatMap((color, i) => (i ? [STEPS[i - 1], color] : [color]))];
  const magnitude = () => ["abs", ["get", key()]];
  const widthExpression = (extra) => {
    const scale = ["step", magnitude(), ...WIDTH_STEPS];
    const stops = [[9, 1.4], [11, 2.6], [14, 4.5], [18, 8]];
    return ["interpolate", ["linear"], ["zoom"], ...stops.flatMap(([zoom, width]) => [zoom, ["+", ["*", width, scale], extra]])];
  };

  const ROUTE_LAYERS = { "route-halo": 1.6, "route-selected": 3, "route-lines": 0 };
  const isSelected = ["boolean", ["feature-state", "selected"], false];
  const motion = () => (window.matchMedia("(prefers-reduced-motion: reduce)").matches ? 0 : 700);
  let selected = [];

  // Selection: white casing under the chosen paths, everything else dimmed.
  function select(map, ids) {
    if (ids.length === selected.length && ids.every((id, i) => id === selected[i])) return;
    for (const id of selected) map.setFeatureState({ source: "routes", id }, { selected: false });
    selected = ids;
    for (const id of ids) map.setFeatureState({ source: "routes", id }, { selected: true });
    map.setPaintProperty("route-lines", "line-opacity", ids.length ? ["case", isSelected, 1, 0.25] : 1);
  }

  function applyView(map) {
    for (const [id, extra] of Object.entries(ROUTE_LAYERS)) {
      map.setFilter(id, ["has", key()]);
      map.setLayoutProperty(id, "line-sort-key", magnitude());
      map.setPaintProperty(id, "line-width", widthExpression(extra));
    }
    map.setPaintProperty("route-lines", "line-color", colorExpression());
  }

  // Popup placement: nearest spot around the click whose box stays clear of the selected paths.
  const PLACE_DISTANCES = [14, 40, 80, 140, 220];
  const PLACE_DIRECTIONS = [[0, -1], [0, 1], [1, 0], [-1, 0], [1, -1], [-1, -1], [1, 1], [-1, 1]];
  const PLACE_CLEARANCE = 8;
  const PLACE_EDGE = 6;

  function samplePath(map, coords) {
    const points = [];
    let previous = null;
    for (const coord of coords) {
      const point = map.project(coord);
      const steps = previous ? Math.max(1, Math.ceil(Math.hypot(point.x - previous.x, point.y - previous.y) / 4)) : 1;
      for (let i = 1; i <= steps; i++) {
        points.push(previous ? [previous.x + ((point.x - previous.x) * i) / steps, previous.y + ((point.y - previous.y) * i) / steps] : [point.x, point.y]);
      }
      previous = point;
    }
    return points;
  }

  function placePopup(map, popup, click, paths) {
    const { width, height } = popup.getElement().getBoundingClientRect();
    const maxX = container.clientWidth - PLACE_EDGE - width;
    const maxY = container.clientHeight - PLACE_EDGE - height;
    const points = paths.flatMap((coords) => samplePath(map, coords));
    const hits = (x, y) =>
      points.filter(
        ([px, py]) => px > x - PLACE_CLEARANCE && px < x + width + PLACE_CLEARANCE && py > y - PLACE_CLEARANCE && py < y + height + PLACE_CLEARANCE,
      ).length;
    let best = null;
    for (const distance of PLACE_DISTANCES) {
      for (const [dx, dy] of PLACE_DIRECTIONS) {
        const x = dx < 0 ? click.x - distance - width : dx > 0 ? click.x + distance : click.x - width / 2;
        const y = dy < 0 ? click.y - distance - height : dy > 0 ? click.y + distance : click.y - height / 2;
        if (x < PLACE_EDGE || y < PLACE_EDGE || x > maxX || y > maxY) continue;
        const count = hits(x, y);
        if (!best || count < best.count) best = { x, y, count };
        if (count === 0) break;
      }
      if (best?.count === 0) break;
    }
    // Tiny maps may leave no in-bounds candidate; fall back to clamping beside the click.
    const x = best ? best.x : Math.min(Math.max(click.x + PLACE_DISTANCES[0], PLACE_EDGE), Math.max(maxX, PLACE_EDGE));
    const y = best ? best.y : Math.min(Math.max(click.y + PLACE_DISTANCES[0], PLACE_EDGE), Math.max(maxY, PLACE_EDGE));
    popup.setLngLat(map.unproject([x, y]));
  }

  // The popup body is an htmx element: the server renders its rows and pager, and pager buttons swap it in place.
  function openPopup(map, event, ids, geometryById) {
    const url = new URL(container.dataset.segments, window.location.href);
    url.searchParams.set("mode", view.mode);
    url.searchParams.set("period", view.period);
    url.searchParams.set("ids", ids.join(","));
    const body = document.createElement("div");
    body.className = "map-pop-body";
    body.setAttribute("hx-get", url.pathname + url.search);
    body.setAttribute("hx-trigger", "load");
    const popup = new maplibregl.Popup({ maxWidth: "320px", className: "map-pop", closeButton: false, anchor: "top-left" })
      .setLngLat(event.lngLat)
      .setDOMContent(body)
      .addTo(map);
    const element = popup.getElement();
    // Hidden until the first page arrives, so it can be measured and placed before anyone sees it.
    element.style.visibility = "hidden";
    const pageIds = () => [...body.querySelectorAll(".map-pop-item")].map((row) => Number(row.dataset.id));
    let placed = false;
    let step = null;
    // after:settle fires on the swap target; after:swap goes to the pager button, which the swap just removed.
    body.addEventListener("htmx:after:settle", () => {
      if (!placed) {
        placed = true;
        // Clear of every overlapping path, so paging never reveals one hidden under the popup.
        placePopup(map, popup, event.point, ids.map((id) => geometryById.get(id)));
        // Hold the first page's height so a shorter last page doesn't move the pager under the cursor.
        const content = element.querySelector(".maplibregl-popup-content");
        content.style.minHeight = `${content.getBoundingClientRect().height}px`;
        element.style.visibility = "";
      }
      select(map, pageIds());
      if (step) body.querySelector(`.map-pop-pager button[data-step="${step}"]:not(:disabled)`)?.focus();
    });
    popup.on("close", () => select(map, []));
    element.addEventListener("click", (click) => {
      step = click.target.closest(".map-pop-pager button")?.dataset.step ?? null;
    });
    // Only entering a row or the pager changes the selection: the gaps between
    // rows belong to neither, and resetting there flashes the whole page on.
    element.addEventListener("mouseover", (hover) => {
      const row = hover.target.closest(".map-pop-item");
      if (row) select(map, [Number(row.dataset.id)]);
      else if (hover.target.closest(".map-pop-pager")) select(map, pageIds());
    });
    element.addEventListener("mouseleave", () => select(map, pageIds()));
    htmx.process(body);
    return popup;
  }

  async function init() {
    try {
      const [, routesResponse] = await Promise.all([loadMapLibre(), fetch(container.dataset.routes)]);
      if (!routesResponse.ok) throw new Error(`route data HTTP ${routesResponse.status}`);
      const routes = await routesResponse.json();
      // Full geometry for popup placement; rendered features are clipped to tiles.
      const geometryById = new Map(routes.features.map((feature) => [feature.id, feature.geometry.coordinates]));

      const map = new maplibregl.Map({
        container,
        style: new URL(container.dataset.style, window.location.href).href,
        ...(view.mode === "tram" ? { bounds: tramBounds, fitBoundsOptions: { padding: 40 } } : WARSAW),
        minZoom: 8,
        maxZoom: 18,
        dragRotate: false,
        pitchWithRotate: false,
        touchPitch: false,
        maxPitch: 0,
        renderWorldCopies: false,
        attributionControl: { compact: true },
      });
      map.touchZoomRotate.disableRotation();
      map.keyboard.disableRotation();
      map.addControl(new maplibregl.NavigationControl({ showCompass: false }));
      map.addControl(new maplibregl.ScaleControl({ unit: "metric" }), "bottom-left");
      // The frame is sized in dvh, which changes without a window resize event on mobile.
      new ResizeObserver(() => map.resize()).observe(container);

      map.on("load", () => {
        map.addSource("routes", { type: "geojson", data: routes });
        const before = map.getStyle().layers.find((layer) => layer.type === "symbol")?.id;
        const offset = ["interpolate", ["linear"], ["zoom"], 9, 0.8, 11, 1.8, 14, 3.3, 18, 5.5];
        const line = (id, paint) => map.addLayer({ id, type: "line", source: "routes", layout: { "line-join": "round" }, paint: { "line-offset": offset, ...paint } }, before);
        line("route-halo", { "line-color": BACKGROUND });
        line("route-selected", { "line-color": SELECTED, "line-opacity": ["case", isSelected, 1, 0] });
        line("route-lines", {});
        applyView(map);
      });

      let popup = null;
      map.on("click", (event) => {
        const { x, y } = event.point;
        const hits = map.queryRenderedFeatures([[x - 4, y - 4], [x + 4, y + 4]], { layers: ["route-lines"] });
        popup?.remove();
        popup = hits.length ? openPopup(map, event, [...new Set(hits.map((feature) => feature.id))], geometryById) : null;
      });
      map.on("mousemove", (event) => {
        const hit = map.queryRenderedFeatures(event.point, { layers: ["route-lines"] }).length;
        map.getCanvas().style.cursor = hit ? "pointer" : "";
      });

      for (const button of document.querySelectorAll(".map-toggle button")) {
        button.addEventListener("click", () => {
          const name = button.parentElement.dataset.view;
          view[name] = button.dataset.value;
          showView();
          popup?.remove();
          if (map.getLayer("route-lines")) applyView(map);
          if (name === "mode") {
            if (view.mode === "tram") map.fitBounds(tramBounds, { padding: 40, duration: motion() });
            else map.easeTo({ ...WARSAW, duration: motion() });
          }
        });
      }
    } catch (error) {
      stats.hidden = true;
      errorBox.textContent = `Map could not load: ${error.message}. It needs internet access and WebGL.`;
      errorBox.hidden = false;
    }
  }

  init();
})();
