// Stop selection edits the form; only submitting it searches. Live cards refresh with htmx; open cards draw a journey map.
(() => {
  // The script also arrives inside boosted page responses. Install delegated handlers just once.
  if (window.plannerInitialized) return;
  window.plannerInitialized = true;
  const MAPLIBRE = "https://cdn.jsdelivr.net/npm/maplibre-gl@5.24.0/dist/maplibre-gl";
  let picker = null;
  const suggestionQueries = new WeakMap();
  // Each card's minimap by its box: {map, json}, the data it shows.
  const minimaps = new Map();

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
    const endpointRow = row && !event.target.closest(".pl-suggestions, .pl-swap, .pl-locate");
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
    } else if (event.target.closest(".pl-locate")) {
      locate(form, event.target.closest(".pl-locate"));
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

  // The start becomes the device's position, labelled like a map click once the reverse lookup answers.
  function locate(form, button) {
    closePicker();
    const notice = (text) => {
      const note = document.createElement("p");
      note.className = "pl-picker-note";
      note.setAttribute("role", "status");
      note.textContent = text;
      form.querySelector("#pl-suggest-from").replaceChildren(note);
    };
    if (!navigator.geolocation) {
      notice(button.dataset.error);
      return;
    }
    button.classList.add("busy");
    navigator.geolocation.getCurrentPosition(
      async ({ coords }) => {
        button.classList.remove("busy");
        const point = { lat: coords.latitude, lon: coords.longitude };
        if (!form.isConnected || !validPoint(point)) return;
        choose(form, "from", button.dataset.label, null, point, false);
        const reverse = form.querySelector(".pl-location-panel")?.dataset.reverseUrl;
        if (!reverse) return;
        const url = new URL(reverse, location.href);
        url.searchParams.set("lat", point.lat.toFixed(6));
        url.searchParams.set("lon", point.lon.toFixed(6));
        try {
          const { name } = await (await fetch(url)).json();
          // The reader may have typed or picked something else meanwhile.
          if (name && form.isConnected && samePoint(form, "from", point)) choose(form, "from", name, null, point, false);
        } catch { /* The "my location" label stays. */ }
      },
      () => {
        button.classList.remove("busy");
        if (form.isConnected) notice(button.dataset.error);
      },
      { enableHighAccuracy: true, timeout: 10_000, maximumAge: 60_000 },
    );
  }

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

  // The results' polling filter: poll while the page is visible and a live card is in view.
  window.plannerShouldRefresh = () =>
    document.visibilityState === "visible" &&
    [...document.querySelectorAll(".pl-card.live")].some((card) => {
      const box = card.getBoundingClientRect();
      return box.bottom > 0 && box.top < window.innerHeight;
    });

  document.addEventListener("htmx:before:swap", (event) => {
    const ctx = event.detail.ctx;
    if (ctx.sourceElement.matches(".pl-input") && !currentSuggestion(ctx)) event.preventDefault();
  });
  // The server never renders a card or stop list open, so the refresh's morph would close the reader's. htmx offers
  // its morph hooks to extensions only; returning false keeps the attribute.
  htmx.registerExtension("planner-open", {
    htmx_before_morph_attr: (element, { attrName }) => attrName !== "open" || !element.closest("#pl-results"),
  });
  document.addEventListener("htmx:after:swap", (event) => {
    const results = event.detail.ctx.target;
    if (results?.id !== "pl-results") return;
    // htmx cleans up only the elements it processed, so a card the morph removed leaves its map behind.
    for (const [box, minimap] of minimaps) {
      if (box.isConnected) continue;
      minimap.map.remove();
      minimaps.delete(box);
    }
    for (const card of results.querySelectorAll(".pl-card[open]")) draw(card);
    // Not bubbling: the refresh is for the stop list alone.
    for (const stops of results.querySelectorAll(".pl-stops-toggle[open]")) htmx.trigger(stops, "refresh", {}, false);
  });
  document.addEventListener("htmx:before:cleanup", (event) => {
    const root = event.target;
    if (picker && (root === picker.panel || root.contains(picker.panel))) closePicker();
    if (root.matches(".pl-input")) cancelSuggestions(root);
    for (const box of root.querySelectorAll(".pl-journey-map")) {
      minimaps.get(box)?.map.remove();
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

  // A vehicle further than this from the boarding stop (often on its previous trip across the city) would zoom the
  // map out past the journey, so it is not drawn.
  const APPROACH_SHOWN_M = 2500;

  function metres([lon1, lat1], [lon2, lat2]) {
    const x = (lon2 - lon1) * Math.cos(((lat1 + lat2) / 2) * Math.PI / 180);
    return Math.hypot(x, lat2 - lat1) * 111_320;
  }

  function marker(map, element, at) {
    return new maplibregl.Marker({ element }).setLngLat(at).addTo(map);
  }

  function legLabel(mode, line) {
    const pill = document.createElement("span");
    pill.className = `landing-line-pill mono pl-pill mode-${mode} pl-map-leg`;
    pill.textContent = line;
    return pill;
  }

  async function draw(card) {
    // Wide screens show the side map instead; a hidden box would get a zero-sized map.
    if (wide.matches) return;
    const boxes = card.querySelectorAll(".pl-journey-map");
    if (!boxes.length) return;
    try {
      await loadMapLibre();
    } catch {
      for (const box of boxes) box.hidden = true;
      return;
    }
    for (const box of boxes) {
      // A second toggle or an htmx replacement may happen while MapLibre is loading.
      if (!box.isConnected || !card.open) continue;
      const minimap = minimaps.get(box);
      if (minimap) {
        // The refresh brought new data for the drawn map; the reader's pan and zoom stay.
        if (minimap.json === box.dataset.map) continue;
        minimap.json = box.dataset.map;
        update(minimap.map, minimap.map.getContainer(), JSON.parse(box.dataset.map));
        continue;
      }
      // MapLibre's elements get a container of their own, which the refresh's morph leaves alone.
      const container = document.createElement("div");
      container.className = "pl-journey-canvas";
      box.replaceChildren(container);
      // Inline in the page: a scroll passing over the map still scrolls the page, two fingers or ctrl move the map.
      const map = journeyMap(container, JSON.parse(box.dataset.map), { cooperativeGestures: true });
      minimaps.set(box, { map, json: box.dataset.map });
    }
  }

  // Map data: legs in order ({kind: "walk"|"ride", path, mode?, line?}) and, while the next ride is live and not yet
  // at the stop, approach {vehicle, path, mode, line} ending at that ride's boarding stop.
  function journeyMap(box, data, options) {
    const map = new maplibregl.Map({
      container: box,
      // A minimap draws into a container inside its box, which carries the style.
      style: new URL(box.closest("[data-style]").dataset.style, window.location.href).href,
      bounds: bounds(data),
      fitBoundsOptions: { padding: 28, maxZoom: 16 },
      dragRotate: false,
      pitchWithRotate: false,
      touchPitch: false,
      attributionControl: { compact: true },
      ...options,
    });
    journeys.set(map, data);
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");
    map.on("load", () => {
      if (!box.isConnected) return;
      // Compact attribution opens expanded on wide screens; on a minimap it would cover the route.
      box.querySelector(".maplibregl-ctrl-attrib")?.classList.remove("maplibregl-compact-show");
      // The large city name lands mid-route on any cross-city journey.
      if (map.getLayer("warsaw-name")) map.setLayoutProperty("warsaw-name", "visibility", "none");
      const empty = { type: "geojson", data: { type: "FeatureCollection", features: [] } };
      for (const id of ["approach", "walks", "rides"]) map.addSource(id, empty);
      const round = { "line-cap": "round", "line-join": "round" };
      // The approach is context, not the trip: thin, dashed and faded under everything else.
      map.addLayer({ id: "approach", type: "line", source: "approach", layout: { "line-join": "round" },
        paint: { "line-color": ["get", "colour"], "line-width": 2.5, "line-opacity": 0.7, "line-dasharray": [2, 2] } });
      map.addLayer({ id: "walks", type: "line", source: "walks", layout: round,
        paint: { "line-color": "#b4b4b4", "line-width": 3, "line-dasharray": [0, 2] } });
      map.addLayer({ id: "ride-casing", type: "line", source: "rides", layout: round,
        paint: { "line-color": "#111111", "line-width": 8 } });
      map.addLayer({ id: "rides", type: "line", source: "rides", layout: round,
        paint: { "line-color": ["get", "colour"], "line-width": 5 } });
      paintLines(map, box, journeys.get(map));
    });
    placeMarkers(map, data);
    return map;
  }

  // The newest data of each map: a refresh can arrive before the map's style has loaded.
  const journeys = new WeakMap();
  function update(map, box, data) {
    journeys.set(map, data);
    placeMarkers(map, data);
    // Before the style loads there are no sources yet, and the load handler paints the newest data.
    paintLines(map, box, data);
  }

  function shownApproach({ approach }) {
    return approach && metres(approach.vehicle, approach.path.at(-1)) <= APPROACH_SHOWN_M ? approach : null;
  }

  function bounds(data) {
    const first = data.legs[0].path[0];
    const shown = shownApproach(data);
    return data.legs.flatMap(leg => leg.path).concat(shown ? shown.path : [])
      .reduce((b, p) => b.extend(p), new maplibregl.LngLatBounds(first, first));
  }

  // Markers are DOM elements, so a redraw of the same map removes the previous set first.
  const markers = new WeakMap();
  function placeMarkers(map, { legs, ...data }) {
    for (const old of markers.get(map) || []) old.remove();
    const placed = [];
    // Leg ends become the timeline's dots; a change at the same post is one dot, not two.
    const nodes = [legs[0].path[0], ...legs.map(leg => leg.path.at(-1))];
    nodes.forEach((at, i) => {
      if (i > 0 && i < nodes.length - 1 && metres(at, nodes[i - 1]) < 15) return;
      const dot = document.createElement("span");
      dot.className = `pl-map-node${i === 0 || i === nodes.length - 1 ? " end" : ""}`;
      placed.push(marker(map, dot, at));
    });
    for (const leg of legs) {
      if (leg.kind === "ride") placed.push(marker(map, legLabel(leg.mode, leg.line), leg.path[Math.floor(leg.path.length / 2)]));
    }
    const shown = shownApproach({ legs, ...data });
    if (shown) {
      const vehicle = document.createElement("span");
      vehicle.className = "pl-live-dot pl-map-vehicle";
      placed.push(marker(map, vehicle, shown.vehicle));
    }
    markers.set(map, placed);
  }

  function paintLines(map, box, data) {
    const styles = getComputedStyle(box);
    const colour = (mode) => styles.getPropertyValue(`--rail-${mode}`).trim() || "#888888";
    const feature = (path, properties) => ({ type: "Feature", geometry: { type: "LineString", coordinates: path }, properties });
    const collection = (features) => ({ type: "FeatureCollection", features });
    const shown = shownApproach(data);
    map.getSource("approach")?.setData(collection(shown ? [feature(shown.path, { colour: colour(shown.mode) })] : []));
    map.getSource("walks")?.setData(collection(data.legs.filter(leg => leg.kind === "walk").map(leg => feature(leg.path, {}))));
    map.getSource("rides")?.setData(collection(
      data.legs.filter(leg => leg.kind === "ride").map(leg => feature(leg.path, { colour: colour(leg.mode) }))));
  }

  // Wide screens: one large map beside the list. It follows the opened card, or the first one; a refresh of the
  // same card updates its lines in place, so a reader's pan and zoom survive the minute's live update.
  const wide = window.matchMedia("(min-width: 1100px)");
  let side = null;
  async function showSide() {
    const box = document.querySelector("#pl-side-map");
    if (!box || !wide.matches) return;
    const card = document.querySelector(".pl-card[open]") || document.querySelector(".pl-card");
    const source = card?.querySelector(".pl-journey-map");
    if (!source) return;
    const data = JSON.parse(source.dataset.map);
    for (const other of document.querySelectorAll(".pl-card.mapped")) other.classList.remove("mapped");
    card.classList.add("mapped");
    if (side?.box === box && side.key === card.dataset.key) {
      if (side.json === source.dataset.map) return;
      side.json = source.dataset.map;
      update(side.map, box, data);
      return;
    }
    try {
      await loadMapLibre();
    } catch {
      box.hidden = true;
      return;
    }
    if (!box.isConnected || !wide.matches) return;
    side?.map.remove();
    side = { box, key: card.dataset.key, json: source.dataset.map, map: journeyMap(box, data, {}) };
  }

  function dropSide() {
    side?.map.remove();
    side = null;
  }

  wide.addEventListener("change", () => {
    if (wide.matches) {
      showSide();
    } else {
      dropSide();
      for (const card of document.querySelectorAll(".pl-card[open]")) draw(card);
    }
  });
  document.addEventListener("htmx:after:swap", () => showSide());
  document.addEventListener("htmx:before:cleanup", (event) => {
    if (side && event.target.contains(side.box)) dropSide();
  });
  document.addEventListener(
    "toggle",
    (event) => {
      if (event.target.matches?.(".pl-card") && event.target.open) showSide();
    },
    true,
  );
  showSide();
})();
