/**
 * Shared Chart.js theme + init for the report and dashboard pages.
 *
 * Usage from a template:
 *   <canvas id="chart-foo"></canvas>
 *   <script src="/static/js/chart-theme.js"></script>
 *   <script>
 *     window.OpsBiCharts.init(
 *       [{id: "chart-foo", kind: "line", title: "…", x_categories: [...],
 *         series: [{label: "…", data: [...]}], x_label: "", y_label: ""}],
 *       [{id: "sparkline-instances_total", data: [1,2,3,4]}]
 *     );
 *   </script>
 *
 * Both full charts and tile sparklines are theme-aware: the IIFE
 * installs a MutationObserver on <html data-theme> so flipping the
 * theme mid-session repaints every chart in place.
 */
(function () {
  var PALETTES = {
    light: {
      color: '#1f2937',
      grid: 'rgba(0,0,0,0.08)',
      border: 'rgba(0,0,0,0.15)',
      series: ['#1E3A5F', '#4A90A4', '#0e7490', '#b45309',
               '#7c3aed', '#15803d', '#be123c', '#0369a1']
    },
    dark: {
      color: '#e5e7eb',
      grid: 'rgba(255,255,255,0.08)',
      border: 'rgba(255,255,255,0.15)',
      series: ['#22d3ee', '#a78bfa', '#34d399', '#fbbf24',
               '#f87171', '#60a5fa', '#f472b6', '#2dd4bf']
    }
  };

  function currentTheme() {
    return document.documentElement.getAttribute('data-theme') === 'light'
      ? 'light' : 'dark';
  }

  function applyDefaults(theme) {
    if (typeof Chart === 'undefined') return;
    var p = PALETTES[theme];
    Chart.defaults.color = p.color;
    Chart.defaults.borderColor = p.border;
  }

  function paintInstance(chart, theme) {
    var p = PALETTES[theme];
    (chart.data.datasets || []).forEach(function (ds, i) {
      var c = p.series[i % p.series.length];
      ds.backgroundColor = c;
      ds.borderColor = c;
    });
    if (chart.options) {
      chart.options.scales = chart.options.scales || {};
      ['x', 'y'].forEach(function (axis) {
        chart.options.scales[axis] = chart.options.scales[axis] || {};
        chart.options.scales[axis].ticks = Object.assign(
          {}, chart.options.scales[axis].ticks || {}, {color: p.color});
        chart.options.scales[axis].grid = Object.assign(
          {}, chart.options.scales[axis].grid || {}, {color: p.grid});
        chart.options.scales[axis].title = Object.assign(
          {}, chart.options.scales[axis].title || {}, {color: p.color});
      });
      chart.options.plugins = chart.options.plugins || {};
      chart.options.plugins.title = Object.assign(
        {}, chart.options.plugins.title || {}, {color: p.color});
      chart.options.plugins.legend = Object.assign(
        {}, chart.options.plugins.legend || {}, {});
      chart.options.plugins.legend.labels = Object.assign(
        {}, chart.options.plugins.legend.labels || {}, {color: p.color});
    }
  }

  // Paint a single sparkline — Chart.js line chart with no axes, no
  // legend, no tooltips, just the line. Used by the dashboard tiles.
  function paintSparkline(chart, theme) {
    var p = PALETTES[theme];
    (chart.data.datasets || []).forEach(function (ds) {
      ds.borderColor = p.series[0];
      ds.backgroundColor = 'transparent';
      ds.pointRadius = 0;
      ds.borderWidth = 1.5;
      ds.tension = 0.2;
    });
  }

  function makeChart(spec) {
    var el = document.getElementById(spec.id);
    if (!el) return null;
    var type = spec.kind === 'line' ? 'line' : 'bar';
    var datasets = (spec.series || []).map(function (s) {
      return { label: s.label, data: s.data };
    });
    var cfg = {
      type: type,
      data: { labels: spec.x_categories || [], datasets: datasets },
      options: {
        plugins: { title: { display: !!spec.title, text: spec.title || '' } },
        scales: {
          x: { stacked: spec.kind === 'stacked_bar',
               title: { display: !!spec.x_label, text: spec.x_label || '' } },
          y: { stacked: spec.kind === 'stacked_bar', beginAtZero: true,
               title: { display: !!spec.y_label, text: spec.y_label || '' } }
        }
      }
    };
    return new Chart(el, cfg);
  }

  function makeSparkline(spec) {
    var el = document.getElementById(spec.id);
    if (!el) return null;
    if (!spec.data || !spec.data.length) {
      // Still render a flat baseline so the tile has visual continuity.
      spec.data = [0];
    }
    var cfg = {
      type: 'line',
      data: {
        labels: spec.data.map(function (_, i) { return i; }),
        datasets: [{ data: spec.data }]
      },
      options: {
        plugins: { legend: { display: false }, title: { display: false },
                   tooltip: { enabled: false } },
        scales: { x: { display: false }, y: { display: false } },
        elements: { point: { radius: 0 } },
        maintainAspectRatio: false,
        responsive: true
      }
    };
    return new Chart(el, cfg);
  }

  var charts = [];        // full charts
  var sparklines = [];    // tile sparklines

  function init(chartSpecs, sparklineSpecs) {
    var theme = currentTheme();
    applyDefaults(theme);

    (chartSpecs || []).forEach(function (spec) {
      var c = makeChart(spec);
      if (c) {
        paintInstance(c, theme);
        c.update('none');
        charts.push(c);
      }
    });
    (sparklineSpecs || []).forEach(function (spec) {
      var c = makeSparkline(spec);
      if (c) {
        paintSparkline(c, theme);
        c.update('none');
        sparklines.push(c);
      }
    });

    // Re-paint everything when the theme changes mid-session.
    var observer = new MutationObserver(function () {
      var t = currentTheme();
      applyDefaults(t);
      charts.forEach(function (c) { paintInstance(c, t); c.update('none'); });
      sparklines.forEach(function (c) { paintSparkline(c, t); c.update('none'); });
    });
    observer.observe(document.documentElement,
      { attributes: true, attributeFilter: ['data-theme'] });
  }

  // Replace data in already-rendered charts without rebuilding them —
  // used by the dashboard's ⟳ partial-refresh handler.
  function update(chartSpecs, sparklineSpecs) {
    var theme = currentTheme();
    var byId = {};
    charts.forEach(function (c) {
      var el = c.canvas;
      if (el && el.id) byId[el.id] = c;
    });
    var sparkById = {};
    sparklines.forEach(function (c) {
      var el = c.canvas;
      if (el && el.id) sparkById[el.id] = c;
    });
    (chartSpecs || []).forEach(function (spec) {
      var c = byId[spec.id];
      if (!c) return;
      c.data.labels = spec.x_categories || [];
      c.data.datasets = (spec.series || []).map(function (s) {
        return { label: s.label, data: s.data };
      });
      paintInstance(c, theme);
      c.update('none');
    });
    (sparklineSpecs || []).forEach(function (spec) {
      var c = sparkById[spec.id];
      if (!c) return;
      var data = (spec.data && spec.data.length) ? spec.data : [0];
      c.data.labels = data.map(function (_, i) { return i; });
      c.data.datasets = [{ data: data }];
      paintSparkline(c, theme);
      c.update('none');
    });
  }

  window.OpsBiCharts = { init: init, update: update };
})();
