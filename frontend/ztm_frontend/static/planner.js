// Stop selection edits the form; only submitting it searches. Live cards refresh with htmx and draw minimaps.
(() => {
  // The script also arrives inside boosted page responses. Install delegated handlers just once.
  if (window.plannerInitialized) return;
  window.plannerInitialized = true;
  const MAPLIBRE = "https://cdn.jsdelivr.net/npm/maplibre-gl@5.24.0/dist/maplibre-gl";
  let picker = null;
  const suggestionQueries = new WeakMap();
  const minimaps = new WeakMap();

  function cancelSuggestions(input) {
    suggestionQueries.delete(input);
    input.dispatchEvent(new Event("htmx:abort", { bubbles: true }));
  }

  function currentSuggestion(ctx) {
    const input = ctx?.sourceElement;
    return input?.matches(".pl-input") && input.isConnected && !ctx.request.signal.aborted &&
      input.value.trim().length >= 2 && suggestionQueries.get(input) === input.value &&
      new URL(ctx.request.action, location.href).searchParams.get(input.name) === input.value;
  }

  // htmx owns the debounce and HTML swap. These guards also invalidate delayed requests after a selection.
  document.addEventListener("htmx:before:request", (event) => {
    const ctx = event.detail.ctx;
    if (ctx.sourceElement.matches(".pl-input") && !currentSuggestion(ctx)) event.preventDefault();
  });
  for (const type of ["htmx:response:error", "htmx:error"]) {
    document.addEventListener(type, (event) => {
      const ctx = event.detail.ctx;
      if (!currentSuggestion(ctx)) return;
      const note = document.createElement("p");
      note.className = "pl-picker-note";
      note.setAttribute("role", "status");
      note.textContent = ctx.sourceElement.dataset.lookupError;
      ctx.target.replaceChildren(note);
    });
  }

  function coordinates(form, field, point) {
    for (const axis of ["lat", "lon"]) {
      const input = form.elements.namedItem(`${field}_${axis}`);
      if (!input) continue;
      input.value = point ? point[axis].toFixed(6) : "";
      input.disabled = !point;
    }
  }

  function choose(form, field, name, stopId, point, focus = true) {
    form.elements.namedItem(field).value = stopId || "";
    coordinates(form, field, point);
    const input = form.elements.namedItem(`q_${field}`);
    cancelSuggestions(input);
    input.value = name;
    form.querySelector(`#pl-suggest-${field}`).replaceChildren();
    if (focus) input.focus();
  }

  // Boosted navigation replaces the form, so delegate from the document to the current one.
  document.addEventListener("click", (event) => {
    const form = event.target.closest(".pl-form");
    const row = event.target.closest(".pl-field");
    const endpointRow = row && !event.target.closest(".pl-suggestions, .pl-swap");
    if (picker && !event.target.closest(".pl-location-panel") && !endpointRow) closePicker();
    if (!form) return;
    const suggestion = event.target.closest(".pl-suggestion");
    if (suggestion) {
      const field = suggestion.dataset.field;
      const point = suggestion.dataset.lat ? { lat: Number(suggestion.dataset.lat), lon: Number(suggestion.dataset.lon) } : null;
      choose(form, field, suggestion.querySelector(".pl-suggestion-name").textContent, suggestion.dataset.stopId, point);
    } else if (event.target.closest(".pl-swap")) {
      for (const input of form.querySelectorAll(".pl-input")) cancelSuggestions(input);
      for (const [from, to] of [["from", "to"], ["q_from", "q_to"], ["from_lat", "to_lat"], ["from_lon", "to_lon"]]) {
        const origin = form.elements.namedItem(from);
        const destination = form.elements.namedItem(to);
        if (!origin || !destination) continue;
        [origin.value, destination.value] = [destination.value, origin.value];
        [origin.disabled, destination.disabled] = [destination.disabled, origin.disabled];
      }
      for (const suggestions of form.querySelectorAll(".pl-suggestions")) suggestions.replaceChildren();
    } else if (endpointRow) {
      const input = row.querySelector(".pl-input");
      input.focus();
      openPicker(form, input.name.slice(2));
    }
  });
  document.addEventListener("input", (event) => {
    const form = event.target.closest(".pl-form");
    if (!form) return;
    if (event.target.matches(".pl-input")) {
      const field = event.target.name.slice(2);
      form.elements.namedItem(field).value = "";
      coordinates(form, field, null);
      closePicker();
      cancelSuggestions(event.target);
      form.querySelector(`#pl-suggest-${field}`).replaceChildren();
      suggestionQueries.set(event.target, event.target.value);
    }
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && picker) {
      event.preventDefault();
      closePicker(true);
    }
  });

  function validPoint(point) {
    return Number.isFinite(point.lat) && Number.isFinite(point.lon) && Math.abs(point.lat) <= 90 && Math.abs(point.lon) <= 180;
  }

  function closePicker(focus = false) {
    if (!picker) return;
    const state = picker;
    picker = null;
    for (const record of Object.values(state.points)) if (record) cancelPointLookup(record);
    state.map?.remove();
    state.panel.hidden = true;
    state.form.classList.remove("pl-picking");
    for (const row of state.form.querySelectorAll(".pl-field")) row.classList.remove("is-map-active");
    if (focus) state.form.elements.namedItem(`q_${state.field}`).focus();
  }

  function cancelPointLookup(record) {
    clearTimeout(record.timer);
    record.controller?.abort();
  }

  function samePoint(form, field, point) {
    return ["lat", "lon"].every((axis) => {
      const input = form.elements.namedItem(`${field}_${axis}`);
      return input && !input.disabled && Number(input.value) === Number(point[axis].toFixed(6));
    });
  }

  async function resolvePoint(state, field, record) {
    if (!state.panel.dataset.reverseUrl) return;
    const controller = new AbortController();
    record.controller = controller;
    const url = new URL(state.panel.dataset.reverseUrl, location.href);
    url.searchParams.set("lat", record.point.lat.toFixed(6));
    url.searchParams.set("lon", record.point.lon.toFixed(6));
    try {
      const response = await fetch(url, { signal: controller.signal });
      if (!response.ok) return;
      const result = await response.json();
      if (controller.signal.aborted || state.points[field] !== record || !state.form.isConnected ||
          !samePoint(state.form, field, record.point) || !result.name) return;
      record.name = result.name;
      choose(state.form, field, record.name, null, record.point, false);
    } catch { /* A failed reverse lookup leaves the selected coordinates in place. */ }
  }

  function addMarker(state, field) {
    const record = state.points[field];
    if (!record || !state.map) return;
    if (!record.marker) {
      const dot = document.createElement("span");
      dot.className = `pl-map-point ${field}`;
      dot.setAttribute("role", "button");
      dot.setAttribute("aria-label", state.form.elements.namedItem(`q_${field}`).getAttribute("aria-label"));
      dot.tabIndex = 0;
      dot.addEventListener("click", (event) => {
        event.stopPropagation();
        selectPickerField(state, field);
      });
      dot.addEventListener("keydown", (event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          selectPickerField(state, field);
        }
      });
      record.marker = new maplibregl.Marker({ element: dot, draggable: true })
        .setLngLat([record.point.lon, record.point.lat]).addTo(state.map);
      record.marker.on("dragstart", () => selectPickerField(state, field));
      record.marker.on("dragend", () => {
        const at = state.points[field].marker.getLngLat();
        setPoint(state, field, { lat: at.lat, lon: at.lng });
      });
    }
    record.marker.setLngLat([record.point.lon, record.point.lat]);
  }

  function setPoint(state, field, point, advance = false) {
    if (!validPoint(point)) return;
    const previous = state.points[field];
    if (previous) cancelPointLookup(previous);
    const record = { point, name: `${point.lat.toFixed(6)}, ${point.lon.toFixed(6)}`, marker: previous?.marker || null };
    state.points[field] = record;
    // Commit immediately; geocoding changes only the label and never delays the next map click.
    choose(state.form, field, record.name, null, point, false);
    addMarker(state, field);
    record.timer = setTimeout(() => resolvePoint(state, field, record), 450);
    if (advance && field === "from") selectPickerField(state, "to");
  }

  function selectPickerField(state, field) {
    state.field = field;
    for (const row of state.form.querySelectorAll(".pl-field")) {
      row.classList.toggle("is-map-active", row.contains(state.form.elements.namedItem(`q_${field}`)));
    }
    // Selecting the other endpoint must not move the map or remove either marker.
  }

  async function pickerMapStyle(url, cityName) {
    const response = await fetch(url);
    if (!response.ok) throw new Error("map style unavailable");
    const style = await response.json();
    // Roads and labels come from the shared basemap; only the city name follows the planner language.
    style.layers.find(layer => layer.id === "warsaw-name").layout["text-field"] = cityName;
    return style;
  }

  async function openPicker(form, field) {
    if (picker?.form === form) {
      selectPickerField(picker, field);
      return;
    }
    closePicker();
    const panel = form.querySelector("#pl-picker");
    const state = { form, field, panel, points: { from: null, to: null }, map: null };
    picker = state;
    form.classList.add("pl-picking");
    panel.hidden = false;
    panel.querySelector(".pl-picker-error").hidden = true;
    for (const input of form.querySelectorAll(".pl-input")) cancelSuggestions(input);
    for (const suggestions of form.querySelectorAll(".pl-suggestions")) suggestions.replaceChildren();
    for (const field of ["from", "to"]) {
      const lat = form.elements.namedItem(`${field}_lat`);
      const lon = form.elements.namedItem(`${field}_lon`);
      if (lat && lon && !lat.disabled && !lon.disabled) {
        const point = { lat: Number(lat.value), lon: Number(lon.value) };
        if (validPoint(point)) state.points[field] = { point, name: form.elements.namedItem(`q_${field}`).value, marker: null };
      }
    }
    selectPickerField(state, field);
    try {
      const [, style] = await Promise.all([
        loadMapLibre(), pickerMapStyle(panel.dataset.style, panel.dataset.city),
      ]);
      if (picker !== state || !panel.isConnected) return;
      const points = Object.values(state.points).filter(Boolean).map(record => record.point);
      const options = points.length === 2 ? {
        bounds: new maplibregl.LngLatBounds([points[0].lon, points[0].lat], [points[0].lon, points[0].lat])
          .extend([points[1].lon, points[1].lat]), fitBoundsOptions: { padding: 50, maxZoom: 14 },
      } : { center: points.length ? [points[0].lon, points[0].lat] : [21.0122, 52.2297], zoom: points.length ? 14 : 9.5 };
      state.map = new maplibregl.Map({
        container: panel.querySelector(".pl-location-map"), style, ...options, attributionControl: { compact: true },
      });
      state.map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");
      state.map.on("click", (event) => {
        if (event.originalEvent?.target?.closest(".pl-map-point, .maplibregl-ctrl")) return;
        setPoint(state, state.field, { lat: event.lngLat.lat, lon: event.lngLat.lng }, true);
      });
      state.map.on("error", () => { panel.querySelector(".pl-picker-error").hidden = false; });
      for (const field of ["from", "to"]) addMarker(state, field);
    } catch {
      if (picker === state) panel.querySelector(".pl-picker-error").hidden = false;
    }
  }

  // The results' polling filter (htmx 4 reads it only attached to the event: every[...] 60s): poll while the page
  // is visible and a live card is in view.
  window.plannerShouldRefresh = () =>
    document.visibilityState === "visible" &&
    [...document.querySelectorAll(".pl-card.live")].some((card) => {
      const box = card.getBoundingClientRect();
      return box.bottom > 0 && box.top < window.innerHeight;
    });

  const openedResults = new WeakMap();
  document.addEventListener("htmx:before:swap", (event) => {
    const ctx = event.detail.ctx;
    if (ctx.sourceElement.matches(".pl-input") && !currentSuggestion(ctx)) {
      event.preventDefault();
      return;
    }
    if (ctx.target.id !== "pl-results") return;
    openedResults.set(ctx, [...ctx.target.querySelectorAll("details[open][data-key]")].map((d) => d.dataset.key));
  });
  document.addEventListener("htmx:after:swap", (event) => {
    const ctx = event.detail.ctx;
    const opened = openedResults.get(ctx);
    if (!opened) return;
    const results = document.querySelector("#pl-results");
    for (const key of opened) {
      results?.querySelector(`details[data-key="${CSS.escape(key)}"]`)?.setAttribute("open", "");
    }
    openedResults.delete(ctx);
    for (const card of results?.querySelectorAll(".pl-card[open]") || []) draw(card);
  });
  document.addEventListener("htmx:before:cleanup", (event) => {
    const root = event.target;
    if (picker && (root === picker.panel || root.contains(picker.panel))) closePicker();
    if (root.matches(".pl-input")) cancelSuggestions(root);
    for (const box of root.querySelectorAll(".pl-minimap")) {
      minimaps.get(box)?.remove();
      minimaps.delete(box);
    }
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
  let mapStyles, mapScript;
  function loadMapLibre() {
    mapStyles ??= load(Object.assign(document.createElement("link"), { rel: "stylesheet", href: `${MAPLIBRE}.css` }))
      .catch((error) => { mapStyles = null; throw error; });
    if (!window.maplibregl) {
      mapScript ??= load(Object.assign(document.createElement("script"), { src: `${MAPLIBRE}.js` }))
        .catch((error) => { mapScript = null; throw error; });
    }
    return Promise.all([mapStyles, mapScript]);
  }

  function load(element) {
    return new Promise((resolve, reject) => {
      element.onload = resolve;
      element.onerror = () => {
        element.remove();
        reject(new Error(`${element.src || element.href} failed to load`));
      };
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
      // A second toggle or an htmx replacement may happen while MapLibre is loading.
      if (!box.isConnected || !card.open || minimaps.has(box)) continue;
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
      minimaps.set(box, map);
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
        if (!box.isConnected || minimaps.get(box) !== map) return;
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
