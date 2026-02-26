function escapeLabelValue(value) {
  return String(value)
    .replace(/\\/g, "\\\\")
    .replace(/\n/g, "\\n")
    .replace(/"/g, '\\"');
}

function labelsKey(labels = {}) {
  return Object.entries(labels)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([k, v]) => `${k}=${v}`)
    .join(",");
}

function labelsText(labels = {}) {
  const parts = Object.entries(labels)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([k, v]) => `${k}="${escapeLabelValue(v)}"`);
  return parts.length > 0 ? `{${parts.join(",")}}` : "";
}

class MetricsCollector {
  constructor() {
    this.counters = new Map();
    this.gauges = new Map();
    this.histograms = new Map();
  }

  inc(name, labels = {}, value = 1) {
    const key = `${name}|${labelsKey(labels)}`;
    const current = this.counters.get(key) || { name, labels, value: 0 };
    current.value += Number(value) || 0;
    this.counters.set(key, current);
  }

  set(name, labels = {}, value = 0) {
    const key = `${name}|${labelsKey(labels)}`;
    this.gauges.set(key, {
      name,
      labels,
      value: Number(value) || 0,
    });
  }

  observe(name, labels = {}, value = 0) {
    const key = `${name}|${labelsKey(labels)}`;
    const current = this.histograms.get(key) || {
      name,
      labels,
      count: 0,
      sum: 0,
      min: Number.POSITIVE_INFINITY,
      max: Number.NEGATIVE_INFINITY,
    };

    const numeric = Number(value) || 0;
    current.count += 1;
    current.sum += numeric;
    current.min = Math.min(current.min, numeric);
    current.max = Math.max(current.max, numeric);
    this.histograms.set(key, current);
  }

  renderPrometheus() {
    const lines = [];

    for (const item of this.counters.values()) {
      lines.push(`${item.name}${labelsText(item.labels)} ${item.value}`);
    }

    for (const item of this.gauges.values()) {
      lines.push(`${item.name}${labelsText(item.labels)} ${item.value}`);
    }

    for (const item of this.histograms.values()) {
      lines.push(`${item.name}_count${labelsText(item.labels)} ${item.count}`);
      lines.push(`${item.name}_sum${labelsText(item.labels)} ${item.sum}`);
      if (Number.isFinite(item.min)) {
        lines.push(`${item.name}_min${labelsText(item.labels)} ${item.min}`);
      }
      if (Number.isFinite(item.max)) {
        lines.push(`${item.name}_max${labelsText(item.labels)} ${item.max}`);
      }
    }

    return lines.join("\n") + (lines.length > 0 ? "\n" : "");
  }
}

module.exports = {
  MetricsCollector,
  labelsKey,
  labelsText,
};
