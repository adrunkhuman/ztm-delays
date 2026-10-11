# Browser test dependency

`htmx-4.0.0-beta5.min.js` is the unmodified build loaded by `templates/base.html`, copied from <https://cdn.jsdelivr.net/npm/htmx.org@4.0.0-beta5/dist/htmx.min.js>. Its license is in `LICENSE.htmx`.

Browser tests use this local copy so they exercise the actual htmx runtime without network access. The integrity check in `test_planner_browser.py` must pass when updating the site's htmx version and this fixture together.
