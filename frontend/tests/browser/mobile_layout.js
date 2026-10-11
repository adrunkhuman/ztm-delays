/* Render real templates/CSS in phone-sized frames without a CDN or production data. */
(async () => {
  const check = (ok, message) => { if (!ok) throw new Error(message); };
  const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
  const until = async (predicate, message) => {
    for (let i = 0; i < 80; i++) {
      if (predicate()) return;
      await sleep(20);
    }
    throw new Error(message);
  };
  try {
    for (const width of [320, 375, 430, 600, 768, 1280]) {
      const frames = {};
      for (const [name, html] of Object.entries(window.layoutFixture)) {
        const frame = document.createElement('iframe');
        frame.style.cssText = `display:block;border:0;width:${width}px;height:900px`;
        frame.srcdoc = html;
        document.body.append(frame);
        frames[name] = frame;
        await until(() => frame.contentDocument?.readyState === 'complete' && frame.contentDocument?.querySelector('.main'), name + ' did not load');
      }
      const planner = frames.planner.contentDocument;
      const controls = [...planner.querySelectorAll('.pl-when > *')].map(e => e.getBoundingClientRect());
      const row = planner.querySelector('.pl-when').getBoundingClientRect();
      check(controls.every(r => Math.abs(r.top - controls[0].top) < 1), 'planner controls wrapped at ' + width);
      check(controls.every(r => r.left >= row.left - 1 && r.right <= row.right + 1), 'planner controls overflow at ' + width);
      check(controls.every(r => r.height >= 39 && r.width >= 55), 'planner control too small at ' + width);
      check(planner.documentElement.scrollWidth <= width + 1, 'planner page overflows at ' + width);
      const status = frames.status.contentDocument;
      await until(() => [...status.querySelectorAll('.status-chart svg')].every(svg =>
        Math.abs(svg.viewBox.baseVal.width - svg.getBoundingClientRect().width) < 1), 'charts did not resize at ' + width);
      check(status.documentElement.scrollWidth <= width + 1, 'status page overflows at ' + width);
      for (const figure of status.querySelectorAll('.status-chart')) {
        const svg = figure.querySelector('svg');
        const box = svg.getBoundingClientRect();
        check(box.width <= width && box.height >= 200, 'chart squashed or too wide at ' + width);
        const labels = [...figure.querySelectorAll('.time-axis')].filter(e => e.style.display !== 'none');
        check(labels.length >= 2, 'too few time labels at ' + width);
        let previousEnd = -Infinity;
        for (const label of labels) {
          const text = label.getBoundingClientRect();
          check(text.height >= 9, 'chart text shrunk at ' + width);
          check(text.left >= previousEnd && text.left >= box.left && text.right <= box.right + 1,
            'time labels overlap or overflow at ' + width);
          previousEnd = text.right;
        }
        const at = box.left + box.width / 2;
        svg.dispatchEvent(new frames.status.contentWindow.PointerEvent('pointermove', {clientX: at, clientY: box.top + 50}));
        const cross = figure.querySelector('.cross').getBoundingClientRect();
        check(Math.abs(cross.left - at) < 1, 'crosshair drifted after resize at ' + width);
        const tip = figure.querySelector('.status-tip').getBoundingClientRect();
        check(tip.left >= box.left - 1 && tip.right <= box.right + 1, 'tooltip outside chart at ' + width);
        svg.dispatchEvent(new frames.status.contentWindow.PointerEvent('pointerleave'));
        check(figure.querySelector('.status-tip').style.visibility === 'hidden', 'tooltip did not hide');
      }
      // Exercise the observer cleanup used when boosted navigation discards the status page.
      frames.status.contentDocument.querySelector('.status-page').dispatchEvent(
        new frames.status.contentWindow.CustomEvent('htmx:before:cleanup', {bubbles: true}));
      for (const frame of Object.values(frames)) frame.remove();
    }
    document.querySelector('#layout-result').textContent = 'PASS';
  } catch (error) {
    document.querySelector('#layout-result').textContent = 'FAIL: ' + error.message;
  }
})();
