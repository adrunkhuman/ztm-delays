/* Offline boundary mocks only: production htmx handles requests, swaps and history. */
(() => {
  const {case: scenario, lang, snapshots} = window.browserFixture;
  const requests = [], maps = [], swaps = [];
  const check = (ok, message) => { if (!ok) throw new Error(message); };
  const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
  const until = async (predicate, message) => {
    for (let i = 0; i < 80; i++) { if (predicate()) return; await sleep(10); }
    throw new Error(message);
  };
  const form = () => document.querySelector('.pl-form');
  const input = field => form().elements.namedItem('q_' + field);
  const target = field => document.querySelector('#pl-suggest-' + field);
  const type = (field, value) => {
    input(field).value = value;
    input(field).dispatchEvent(new Event('input', {bubbles: true}));
  };
  const suggestions = () => requests.filter(r => r.url.pathname.includes('/suggest/'));
  const journeys = () => requests.filter(r => r.url.pathname === '/planner');
  const reload = () => {
    const script = document.createElement('script');
    script.textContent = document.querySelector('#page script').textContent;
    document.body.append(script);
    script.remove();
  };
  const fresh = r => r.resolve(snapshots['suggest_' + r.url.pathname.split('/').at(-1)]);

  class MapMock {
    constructor(options) {
      this.box = options.container; this.options = options; this.handlers = {};
      this.removals = 0; this.sources = 0; this.layers = 0; maps.push(this);
    }
    on(event, callback) { this.handlers[event] = callback; }
    remove() { this.removals++; }
    addControl() {}
    addSource() { this.sources++; }
    addLayer() { this.layers++; }
  }
  const mapLibrary = {
    Map: MapMock,
    Marker: class {
      setLngLat(point) { this.point = point; return this; }
      addTo() { return this; }
      on() { return this; }
      remove() {}
      getLngLat() { return {lng: this.point[0], lat: this.point[1]}; }
    },
    LngLatBounds: class { extend() { return this; } },
    NavigationControl: class {},
  };
  window.maplibregl = mapLibrary;
  const cdnAssets = [], headAppend = document.head.append.bind(document.head);
  document.head.append = (...nodes) => {
    for (const node of nodes) {
      if (/maplibre-gl/.test(node.src || node.href || '')) {
        cdnAssets.push(node);
        if (!scenario.startsWith('assets')) queueMicrotask(() => node.onload());
      } else headAppend(node);
    }
  };
  window.fetch = (url, options = {}) => {
    // Deliberately ignore abort when resolving: stale-response guards must still protect the DOM.
    const record = {url: new URL(String(url), location.href), signal: options.signal,
      headers: new Headers(options.headers), at: performance.now()};
    requests.push(record);
    return new Promise((resolve, reject) => {
      record.resolve = (html, status = 200) => resolve(new Response(html, {
        status, headers: {'Content-Type': 'text/html; charset=utf-8'},
      }));
      record.reject = () => reject(new TypeError('offline network failure'));
      if (record.url.pathname.endsWith('map-style.json')) {
        record.resolve(JSON.stringify({sources: {}, layers: []}));
      } else if (record.url.pathname.includes('/reverse')) {
        record.resolve(JSON.stringify({name: 'Map point'}));
      } else if (record.url.pathname.includes('/trip/')) {
        record.resolve(snapshots.trip);
      } else if (record.url.pathname === '/planner') {
        record.resolve(snapshots[record.url.searchParams.get('lang') || lang]);
      } else if (record.url.pathname !== '/planner/results' && !record.url.pathname.includes('/suggest/')) {
        reject(new Error('Unexpected boundary request: ' + record.url));
      }
    });
  };
  document.addEventListener('htmx:before:swap', event => swaps.push(event.detail.ctx));

  async function assets() {
    delete window.maplibregl;
    input('from').click();
    await until(() => cdnAssets.length === 2, 'initial CSS/JS requests missing');
    const css = cdnAssets.find(node => node.tagName === 'LINK');
    const js = cdnAssets.find(node => node.tagName === 'SCRIPT');
    const failed = scenario === 'assets-css' ? css : js;
    const succeeded = failed === css ? js : css;
    if (succeeded === js) window.maplibregl = mapLibrary;
    succeeded.onload(); failed.onerror();
    await until(() => !document.querySelector('.pl-picker-error').hidden, 'asset failure not reported');
    check(maps.length === 0, 'map created without both assets');
    document.dispatchEvent(new KeyboardEvent('keydown', {key: 'Escape'}));
    input('from').click();
    await until(() => cdnAssets.length === 3, 'failed asset not retried');
    check(cdnAssets[2].tagName === failed.tagName, 'successful asset unnecessarily reloaded');
    await sleep(20);
    check(maps.length === 0, 'map created while retry still pending');
    document.dispatchEvent(new KeyboardEvent('keydown', {key: 'Escape'}));
    input('to').click(); await sleep(20);
    check(cdnAssets.length === 3 && maps.length === 0, 'pending retry duplicated or bypassed');
    if (failed === js) window.maplibregl = mapLibrary;
    cdnAssets[2].onload();
    await until(() => maps.length === 1, 'map did not recover after retry');
    check(!document.querySelector('#pl-picker').hidden && document.querySelector('.pl-picker-error').hidden, 'retry retained error state');
    check(input('to').closest('.pl-field').classList.contains('is-map-active'), 'retry activated stale picker');
    check(submissions === 0 && journeys().length === 0, 'asset retry submitted journey');
  }

  async function autocomplete() {
    check(document.querySelector('.pl-main').lang === lang, 'wrong initial language');
    for (const field of ['from', 'to']) {
      const el = input(field);
      check(el.getAttribute('hx-trigger') === 'input delay:450ms', 'production debounce changed');
      check(el.getAttribute('hx-get') === '/planner/suggest/' + field, 'production suggest URL changed');
      check(el.getAttribute('hx-target') === '#pl-suggest-' + field, 'production target changed');
      check(el.getAttribute('hx-sync') === 'this:replace', 'production sync changed');
    }
    reload(); reload();
    const started = performance.now();
    type('from', 'lo'); await sleep(150); type('from', 'lom');
    await sleep(250);
    check(suggestions().length === 0, 'debounce sent a request before 450ms');
    await until(() => suggestions().length === 1, 'debounced request missing');
    const first = suggestions()[0];
    check(first.at - started >= 590, 'debounce not reset on subsequent input');
    check(first.url.searchParams.get('q_from') === 'lom', 'request has stale query');
    check(first.headers.get('HX-Request') === 'true' && first.signal instanceof AbortSignal, 'not a real htmx fetch');
    check(!first.signal.aborted, 'new request already aborted');
    // A same-value, non-bubbling trigger bypasses planner's immediate-abort delegate,
    // isolating the production hx-sync=this:replace behavior in pinned htmx.
    input('from').dispatchEvent(new Event('input'));
    await until(() => suggestions().length === 2, 'sync replacement request missing');
    const second = suggestions()[1];
    check(first.signal.aborted && !second.signal.aborted, 'hx-sync did not replace the pending request');
    type('from', 'lomi');
    check(second.signal.aborted, 'input did not abort immediately, before debounce');
    fresh(first); fresh(second); await sleep(20);
    check(!target('from').textContent.trim(), 'old response appeared during new debounce');
    await until(() => suggestions().length === 3, 'new query request missing');
    const third = suggestions()[2];
    type('from', 'x');
    check(third.signal.aborted, 'short input did not abort in-flight request');
    fresh(third); await sleep(470);
    check(suggestions().length === 3 && !target('from').textContent.trim(), 'short query requested or accepted stale HTML');
    // Exercise the before-swap value/query guard without relying on the abort signal.
    type('to', 'cent');
    await until(() => suggestions().length === 4, 'destination request missing');
    const fourth = suggestions()[3];
    input('to').value = 'changed without an input event';
    check(!fourth.signal.aborted, 'guard test accidentally aborted fetch');
    fresh(fourth); await sleep(10);
    check(!target('to').textContent.trim(), 'query-mismatched response swapped');
    check(swaps.some(ctx => ctx.sourceElement === input('to')), 'mismatched response never reached before-swap guard');

    for (const choice of ['swap', 'suggestion', 'map']) {
      const count = suggestions().length;
      type('to', 'cent');
      if (choice === 'swap') form().querySelector('.pl-swap').click();
      if (choice === 'suggestion') {
        target('to').innerHTML = snapshots.suggest_to;
        target('to').querySelector('.pl-suggestion-name').click();
        check(input('to').value === 'Centrum' && form().elements.to.value === '4004', 'suggestion did not select stop');
      }
      if (choice === 'map') {
        input('to').click();
        await until(() => maps.some(map => map.box.classList.contains('pl-location-map') && !map.removals), 'picker map missing');
        maps.findLast(map => !map.removals).handlers.click({lngLat: {lat: 52.23, lng: 21.01}});
        check(form().elements.to_lat.value === '52.230000' && !form().elements.to_lat.disabled, 'map selection missing');
        document.dispatchEvent(new KeyboardEvent('keydown', {key: 'Escape'}));
      }
      await sleep(470);
      check(suggestions().length === count, choice + ' did not suppress delayed suggestion request');
    }
    for (const failure of ['http', 'network']) {
      type('to', 'cent');
      const count = suggestions().length;
      await until(() => suggestions().length === count + 1, failure + ' request missing');
      const request = suggestions().at(-1);
      if (failure === 'http') request.resolve('<p>server-only diagnostic</p>', 503);
      else request.reject();
      await until(() => target('to').querySelector('[role="status"]'), failure + ' error UI missing');
      check(target('to').textContent === input('to').dataset.lookupError, failure + ' error not localized');
      check(!target('from').querySelector('[role="status"]'), failure + ' error leaked to other input');
      check(!target('to').textContent.includes('server-only'), 'HTTP error body was swapped');
    }
    check(journeys().length === 0 && submissions === 0, 'autocomplete/control interaction submitted journey');
  }

  async function cancellation() {
    type('from', 'lom'); type('to', 'cent');
    await until(() => suggestions().length === 2, 'independent endpoint requests missing');
    const origin = suggestions().find(r => r.url.pathname.endsWith('/from'));
    const destination = suggestions().find(r => r.url.pathname.endsWith('/to'));
    check(!origin.signal.aborted && !destination.signal.aborted, 'per-input sync cancelled other endpoint');
    target('to').innerHTML = snapshots.suggest_to;
    target('to').querySelector('.pl-suggestion-name').click();
    check(destination.signal.aborted && !origin.signal.aborted, 'selection did not immediately cancel only its request');
    destination.resolve('<p>stale destination</p>', 503); await sleep(10);
    check(!target('to').textContent.trim() && input('to').value === 'Centrum', 'stale HTTP error overwrote selection');
    form().querySelector('.pl-swap').click();
    check(origin.signal.aborted, 'swap did not immediately abort pending origin');
    fresh(origin); await sleep(10);
    check(!target('from').textContent.trim(), 'stale response survived swap');
    type('to', 'cent');
    await until(() => suggestions().length === 3, 'pending map-selection request missing');
    const pending = suggestions().at(-1);
    input('to').click();
    check(pending.signal.aborted, 'picker open did not immediately abort pending request');
    await until(() => maps.length === 1, 'picker map missing');
    maps[0].handlers.click({lngLat: {lat: 52.23, lng: 21.01}});
    fresh(pending); await sleep(10);
    check(input('to').value === '52.230000, 21.010000' && !target('to').textContent.trim(), 'stale response overwrote map selection');
    check(journeys().length === 0 && submissions === 0, 'selection/swap/map submitted journey');
  }

  async function navigation() {
    const initialUrl = location.href;
    const initialForm = form();
    type('from', 'lom');
    await until(() => suggestions().length === 1, 'pre-navigation request missing');
    const detachedRequest = suggestions()[0];
    initialForm.elements.time.value = '08:30';
    initialForm.requestSubmit();
    await until(() => form() !== initialForm && location.href !== initialUrl, 'boosted submit/history push missing');
    check(detachedRequest.signal.aborted, 'page cleanup did not abort disconnected input');
    fresh(detachedRequest); await sleep(10);
    check(!target('from').textContent.trim(), 'disconnected input response reached new form');
    check(journeys().length === 1, 'submit did not issue exactly one journey request');
    check(journeys()[0].url.searchParams.get('time') === '08:30', 'submit lost form values');
    check(journeys()[0].headers.get('HX-Boosted') === 'true', 'submit was not boosted');
    // Real link navigation with the actual language link and #page outerSync swap.
    const submittedUrl = location.href, submittedForm = form();
    form().closest('#page').querySelector('.pl-lang a[lang="pl"]').click();
    await until(() => form() !== submittedForm && document.querySelector('.pl-main').lang === 'pl', 'boosted language navigation missing');
    check(journeys().length === 2 && journeys()[1].headers.get('HX-Boosted') === 'true', 'link request was not boosted');
    reload(); reload();
    let current = form(), from = input('from').value, to = input('to').value;
    current.querySelector('.pl-swap').click();
    check(input('from').value === to && input('to').value === from, 'script reload duplicated swap handlers');
    target('to').innerHTML = snapshots.suggest_to;
    target('to').querySelector('.pl-suggestion-name').click();
    check(form().elements.to.value === '4004', 'reloaded handlers did not select stop');
    input('from').click(); await until(() => maps.length === 1, 'picker did not open');
    const pickerMap = maps[0];
    // Force a history cache miss: pinned htmx must fetch with its restore header.
    sessionStorage.clear(); localStorage.clear();
    history.back();
    await until(() => location.href === submittedUrl && form() !== current, 'history back did not restore submitted planner');
    check(journeys().some(r => r.headers.get('HX-History-Restore-Request') === 'true'), 'history cache miss lacked restore header');
    check(pickerMap.removals === 1, 'navigation cleanup did not destroy picker exactly once');
    current = form(); from = input('from').value; to = input('to').value;
    current.querySelector('.pl-swap').click();
    check(input('from').value === to && input('to').value === from, 'history restore duplicated or lost handlers');
    type('to', 'cent');
    await until(() => suggestions().length === 2, 'restored input not processed by htmx');
    fresh(suggestions()[1]);
    await until(() => target('to').querySelector('.pl-suggestion'), 'restored target not swapped');
    check(form() === current, 'suggestion response replaced form');
    target('to').querySelector('.pl-suggestion-name').click();
    check(form().elements.to.value === '4004', 'restored suggestion not selectable');
  }

  async function results() {
    let section = document.querySelector('#pl-results');
    check(section.getAttribute('hx-trigger') === 'refresh, every[plannerShouldRefresh()] 60s', 'fixture polling trigger changed');
    const draft = form();
    type('to', 'Draft destination'); draft.elements.time.value = '09:12';
    const draftValues = [...draft.elements].map(el => [el.name, el.value, el.disabled]);
    const card = section.querySelector('.pl-card'); card.open = true;
    const trip = card.querySelector('.pl-stops-toggle'); trip.open = true;
    await until(() => maps.length === 1 && trip.querySelector('.pl-trip').textContent.includes('Łomianki'), 'initial minimap/lazy trip request missing');
    const oldMap = maps[0], oldBox = oldMap.box;
    check(requests.some(r => r.url.pathname === '/planner/trip/1' && r.headers.get('HX-Request') === 'true'), 'trip fragment did not use htmx');
    check(oldBox.dataset.drawn === '1', 'initial minimap not drawn');
    oldMap.handlers.load(); check(oldMap.sources === 1 && oldMap.layers === 1, 'initial map load missing path');
    const openKeys = [card.dataset.key, trip.dataset.key];
    for (let refresh = 0; refresh < 2; refresh++) {
      const previous = section;
      htmx.trigger(section, 'refresh');
      await until(() => requests.filter(r => r.url.pathname === '/planner/results').length === refresh + 1, 'results refresh request missing');
      const request = requests.filter(r => r.url.pathname === '/planner/results').at(-1);
      check(request.headers.get('HX-Request') === 'true' && request.url.searchParams.get('to') === '2002', 'refresh submitted draft instead of displayed journey');
      request.resolve(snapshots['results_' + lang]);
      await until(() => document.querySelector('#pl-results') !== previous && maps.length === refresh + 2, 'refresh did not replace results/draw fresh minimap');
      section = document.querySelector('#pl-results');
      check(form() === draft, 'results refresh replaced draft form');
      check(JSON.stringify([...draft.elements].map(el => [el.name, el.value, el.disabled])) === JSON.stringify(draftValues), 'results refresh changed draft values');
      for (const key of openKeys) check(section.querySelector('details[data-key="' + key + '"]').open, 'refresh lost open card/trip: ' + key);
      const liveMaps = maps.filter(map => !map.removals && map.box.classList.contains('pl-minimap'));
      check(liveMaps.length === 1 && liveMaps[0].box.isConnected, 'refresh leaked or duplicated minimap');
      check(maps[refresh].removals === 1, 'old minimap not removed exactly once');
      const before = oldMap.sources; oldMap.handlers.load();
      check(oldMap.sources === before && !oldBox.isConnected, 'detached map load still added path');
    }
    // Suspend MapLibre loading at the browser boundary; concurrent toggles must not
    // create duplicate maps, and a replacement must invalidate suspended old draws.
    delete window.maplibregl;
    const assets = [], append = document.head.append.bind(document.head);
    document.head.append = (...nodes) => {
      for (const node of nodes) {
        if (/maplibre-gl/.test(node.src || node.href || '')) assets.push(node);
        else append(node);
      }
    };
    const closedCard = section.querySelectorAll('.pl-card')[1];
    const box = section.querySelector('.pl-minimap').cloneNode(true); box.removeAttribute('data-drawn');
    closedCard.append(box); closedCard.open = true;
    closedCard.dispatchEvent(new Event('toggle'));
    await until(() => assets.length === 1, 'MapLibre script load not suspended');
    htmx.trigger(section, 'refresh');
    await until(() => requests.filter(r => r.url.pathname === '/planner/results').length === 3, 'suspended refresh missing');
    requests.filter(r => r.url.pathname === '/planner/results').at(-1).resolve(snapshots['results_' + lang]);
    await until(() => !section.isConnected, 'suspended result boxes not disconnected');
    const restoredCard = document.querySelector('.pl-card');
    restoredCard.dispatchEvent(new Event('toggle')); restoredCard.dispatchEvent(new Event('toggle'));
    const closingCard = document.querySelectorAll('.pl-card')[1];
    const closedBox = box.cloneNode(true); closingCard.append(closedBox);
    closingCard.dispatchEvent(new Event('toggle')); closingCard.open = false;
    const before = maps.length;
    window.maplibregl = mapLibrary;
    for (const asset of assets) asset.onload();
    document.head.append = append;
    await until(() => maps.length === before + 1, 'pending current draw did not complete');
    await sleep(20);
    check(maps.length === before + 1 && maps.at(-1).box.isConnected, 'duplicate/disconnected async draws created maps');
    check(!maps.some(map => map.box === box), 'suspended disconnected box got a map');
    check(!maps.some(map => map.box === closedBox), 'suspended closed card got a map');
    check(form() === draft && journeys().length === 0 && submissions === 0, 'refresh/typing submitted or replaced form');
  }

  let submissions = 0;
  document.addEventListener('submit', () => submissions++);
  document.addEventListener('DOMContentLoaded', () => {
    // Let htmx finish its initial processing before exercising the production controls.
    setTimeout(async () => {
      const result = document.querySelector('#browser-result');
      try {
        await (scenario.startsWith('assets') ? assets : ({autocomplete, cancellation, navigation, results})[scenario])();
        result.textContent = 'PASS: ' + scenario + '/' + lang;
      } catch (error) {
        result.textContent = 'FAIL: ' + error.stack + '\nrequests: ' + requests.map(r => r.url.pathname + r.url.search + ' aborted=' + r.signal?.aborted).join('\n');
      }
    }, 0);
  });
})();
