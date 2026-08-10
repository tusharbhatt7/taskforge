/* Taskforge dashboard — dependency-free vanilla JS. */

const token = localStorage.getItem('tf_token');
if (!token) location.href = '/';

const $ = (id) => document.getElementById(id);
const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) =>
  ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

const JOB_TYPES = ['sleep', 'flaky', 'http_fetch', 'thumbnail', 'email_sim',
  'llm_summarize', 'llm_classify', 'llm_extract'];
let jobsOffset = 0;
const JOBS_LIMIT = 25;

/* ---------------- api helper ---------------- */
async function api(path, options = {}) {
  const res = await fetch(`/api/v1${path}`, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${token}`,
      ...(options.headers || {}),
    },
  });
  if (res.status === 401) {
    localStorage.removeItem('tf_token');
    location.href = '/';
    return;
  }
  const body = res.status === 204 ? null : await res.json();
  if (!res.ok) throw new Error(body?.detail || `HTTP ${res.status}`);
  return body;
}

function toast(msg, kind = '') {
  const el = document.createElement('div');
  el.className = `toast ${kind}`;
  el.textContent = msg;
  document.body.appendChild(el);
  setTimeout(() => el.remove(), 4200);
}

/* ---------------- formatting ---------------- */
const badge = (state) => `<span class="badge ${esc(state)}">${esc(state)}</span>`;

function ago(iso) {
  if (!iso) return '–';
  const secs = Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000);
  if (secs < 60) return `${Math.floor(secs)}s ago`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m ago`;
  if (secs < 86400) return `${Math.floor(secs / 3600)}h ago`;
  return `${Math.floor(secs / 86400)}d ago`;
}

function until(iso) {
  if (!iso) return '–';
  const secs = (new Date(iso).getTime() - Date.now()) / 1000;
  if (secs < 0) return 'due now';
  if (secs < 60) return `in ${Math.floor(secs)}s`;
  if (secs < 3600) return `in ${Math.floor(secs / 60)}m`;
  return `in ${Math.floor(secs / 3600)}h`;
}

function duration(job) {
  if (!job.started_at || !job.finished_at) return '–';
  const ms = new Date(job.finished_at) - new Date(job.started_at);
  return ms < 1000 ? `${ms}ms` : `${(ms / 1000).toFixed(1)}s`;
}

const shortId = (id) => (id ? String(id).slice(0, 8) : '–');

/* ---------------- tab routing ---------------- */
const LOADERS = {
  overview: loadOverview,
  jobs: loadJobs,
  workers: loadWorkers,
  dlq: loadDlq,
  schedules: loadSchedules,
  submit: () => {},
  apikeys: loadApiKeys,
};
let currentView = 'overview';

document.querySelectorAll('.tabs button').forEach((btn) => {
  btn.onclick = () => {
    currentView = btn.dataset.view;
    document.querySelectorAll('.tabs button').forEach((b) => b.classList.toggle('active', b === btn));
    document.querySelectorAll('.view').forEach((v) =>
      v.classList.toggle('active', v.id === `view-${currentView}`));
    LOADERS[currentView]();
  };
});

$('logout').onclick = () => {
  localStorage.removeItem('tf_token');
  location.href = '/';
};

/* ---------------- overview ---------------- */
async function loadOverview() {
  const [metrics, queues] = await Promise.all([api('/metrics/overview'), api('/queues')]);
  const states = metrics.job_states || {};

  $('s-queued').textContent = states.queued ?? 0;
  $('s-pending').textContent = `waiting on deps: ${states.pending ?? 0}`;
  $('s-running').textContent = states.running ?? 0;
  $('s-workers').textContent = `${metrics.workers?.online ?? 0} workers online`;
  $('s-dead').textContent = states.dead ?? 0;

  const rate = metrics.success_rate_1h;
  $('s-success').textContent = rate === null || rate === undefined ? '–' : `${(rate * 100).toFixed(1)}%`;
  const p95 = metrics.exec_duration_ms?.p95;
  $('s-p95').textContent = `p95 exec: ${p95 ? `${Math.round(p95)}ms` : '–'}`;

  drawThroughput(metrics.throughput_per_minute || []);

  $('queues-body').innerHTML = queues.length
    ? queues.map((q) => `
        <tr>
          <td><code>${esc(q.name)}</code></td>
          <td>${q.paused ? '<span class="badge pending">paused</span>'
                         : '<span class="badge online">active</span>'}</td>
          <td>${q.counts.queued ?? 0}</td>
          <td>${q.counts.running ?? 0}</td>
          <td>${q.counts.succeeded ?? 0}</td>
          <td>${q.counts.dead ?? 0}</td>
          <td class="nowrap"><button class="ghost sm" data-queue="${esc(q.name)}"
              data-action="${q.paused ? 'resume' : 'pause'}">
              ${q.paused ? 'Resume' : 'Pause'}</button></td>
        </tr>`).join('')
    : '<tr><td colspan="7" class="empty">No queues yet — submit a job to create one.</td></tr>';

  $('queues-body').querySelectorAll('button[data-queue]').forEach((btn) => {
    btn.onclick = async () => {
      await api(`/queues/${encodeURIComponent(btn.dataset.queue)}/${btn.dataset.action}`, { method: 'POST' });
      toast(`Queue ${btn.dataset.queue} ${btn.dataset.action}d`, 'ok');
      loadOverview();
    };
  });
}

/* Minimal grouped bar chart on a canvas — no charting library needed. */
function drawThroughput(points) {
  const canvas = $('chart-throughput');
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth || 400;
  const h = 150;
  canvas.width = w * dpr;
  canvas.height = h * dpr;
  const ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, w, h);

  const style = getComputedStyle(document.documentElement);
  const cOk = style.getPropertyValue('--ok').trim();
  const cErr = style.getPropertyValue('--err').trim();
  const cMuted = style.getPropertyValue('--muted').trim();
  const cBorder = style.getPropertyValue('--border').trim();

  if (!points.length) {
    ctx.fillStyle = cMuted;
    ctx.font = '12px system-ui';
    ctx.textAlign = 'center';
    ctx.fillText('No completed jobs in the last 30 minutes', w / 2, h / 2);
    return;
  }

  const pad = { top: 12, right: 6, bottom: 20, left: 28 };
  const plotW = w - pad.left - pad.right;
  const plotH = h - pad.top - pad.bottom;
  const max = Math.max(...points.map((p) => p.count), 4);

  // y gridlines
  ctx.strokeStyle = cBorder;
  ctx.fillStyle = cMuted;
  ctx.font = '10px system-ui';
  ctx.lineWidth = 1;
  ctx.textAlign = 'right';
  for (let i = 0; i <= 2; i++) {
    const val = Math.round((max / 2) * i);
    const y = pad.top + plotH - (val / max) * plotH;
    ctx.beginPath();
    ctx.moveTo(pad.left, y);
    ctx.lineTo(w - pad.right, y);
    ctx.stroke();
    ctx.fillText(val, pad.left - 6, y + 3);
  }

  const slot = plotW / points.length;
  const barW = Math.max(2, Math.min(18, slot * 0.7));
  points.forEach((p, i) => {
    const x = pad.left + slot * i + (slot - barW) / 2;
    const failed = p.count - p.succeeded;
    const okH = (p.succeeded / max) * plotH;
    const failH = (failed / max) * plotH;
    ctx.fillStyle = cOk;
    ctx.fillRect(x, pad.top + plotH - okH, barW, okH);
    if (failed > 0) {
      ctx.fillStyle = cErr;
      ctx.fillRect(x, pad.top + plotH - okH - failH, barW, failH);
    }
  });

  // x labels: first and last minute
  ctx.fillStyle = cMuted;
  ctx.textAlign = 'left';
  const label = (iso) => new Date(iso).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  ctx.fillText(label(points[0].minute), pad.left, h - 6);
  if (points.length > 1) {
    ctx.textAlign = 'right';
    ctx.fillText(label(points[points.length - 1].minute), w - pad.right, h - 6);
  }
}

/* ---------------- jobs ---------------- */
async function loadJobs() {
  const state = $('filter-state').value;
  const qs = new URLSearchParams({ limit: JOBS_LIMIT, offset: jobsOffset });
  if (state) qs.set('state', state);
  const data = await api(`/jobs?${qs}`);

  $('jobs-body').innerHTML = data.items.length
    ? data.items.map((j) => `
        <tr class="clickable" data-job="${j.id}">
          <td><code>${esc(j.type)}</code></td>
          <td>${esc(j.queue)}</td>
          <td>${badge(j.state)}</td>
          <td>${j.attempts}/${j.max_attempts}</td>
          <td class="nowrap muted">${ago(j.created_at)}</td>
          <td class="nowrap muted">${duration(j)}</td>
          <td class="nowrap">${actionButtons(j)}</td>
        </tr>`).join('')
    : '<tr><td colspan="7" class="empty">No jobs match this filter.</td></tr>';

  const shown = data.items.length ? `${jobsOffset + 1}–${jobsOffset + data.items.length}` : '0';
  $('jobs-count').textContent = `Showing ${shown} of ${data.total}`;
  $('jobs-prev').disabled = jobsOffset === 0;
  $('jobs-next').disabled = jobsOffset + JOBS_LIMIT >= data.total;
  wireJobRows($('jobs-body'));
}

function triagePanel(t) {
  if (!t) return '';
  const pct = Math.round((t.confidence || 0) * 100);
  const reused = t.reused_from_id
    ? '<span class="muted"> · reused from an earlier job with the same error signature ' +
      '(no additional API call)</span>'
    : '';
  return `
    <div class="field"><label>AI triage</label>
      <div class="triage-card">
        <div class="row-head">
          <span class="badge ai">${esc(CATEGORY_LABEL[t.category] || t.category)}</span>
          <span class="badge ${t.is_transient ? 'transient' : 'permanent'}">
            ${t.is_transient ? 'likely transient' : 'likely permanent'}</span>
          <span class="muted" style="font-size:12px">confidence
            <span class="conf-bar"><i style="width:${pct}%"></i></span> ${pct}%</span>
        </div>
        <dl style="margin:0">
          <dt>Root cause</dt><dd>${esc(t.root_cause)}</dd>
          <dt>Suggested action</dt><dd>${esc(t.suggested_action)}</dd>
        </dl>
        <div class="meta">
          <code>${esc(t.model)}</code> ·
          ${(t.input_tokens + t.output_tokens).toLocaleString()} tokens ·
          $${t.cost_usd.toFixed(5)} ·
          fingerprint <code>${esc(t.fingerprint)}</code>${reused}
        </div>
      </div>
    </div>`;
}

function actionButtons(j) {
  if (j.state === 'queued' || j.state === 'pending')
    return `<button class="ghost sm" data-cancel="${j.id}">Cancel</button>`;
  if (j.state === 'dead' || j.state === 'canceled')
    return `<button class="ghost sm" data-retry="${j.id}">Retry</button>`;
  return '';
}

function wireJobRows(tbody) {
  tbody.querySelectorAll('tr[data-job]').forEach((tr) => {
    tr.onclick = (e) => {
      if (e.target.tagName === 'BUTTON') return;
      showJob(tr.dataset.job);
    };
  });
  tbody.querySelectorAll('button[data-cancel]').forEach((b) => {
    b.onclick = async () => {
      try {
        await api(`/jobs/${b.dataset.cancel}/cancel`, { method: 'POST' });
        toast('Job canceled', 'ok');
        LOADERS[currentView]();
      } catch (err) { toast(err.message, 'err'); }
    };
  });
  tbody.querySelectorAll('button[data-retry]').forEach((b) => {
    b.onclick = async () => {
      try {
        await api(`/jobs/${b.dataset.retry}/retry`, { method: 'POST' });
        toast('Job requeued with a fresh retry budget', 'ok');
        LOADERS[currentView]();
      } catch (err) { toast(err.message, 'err'); }
    };
  });
}

$('filter-state').onchange = () => { jobsOffset = 0; loadJobs(); };
$('refresh-jobs').onclick = loadJobs;
$('jobs-prev').onclick = () => { jobsOffset = Math.max(0, jobsOffset - JOBS_LIMIT); loadJobs(); };
$('jobs-next').onclick = () => { jobsOffset += JOBS_LIMIT; loadJobs(); };

async function showJob(id) {
  const j = await api(`/jobs/${id}`);
  const attempts = (j.job_attempts || []).map((a) => `
    <div class="attempt-row">
      <span class="attempt-no">#${a.attempt_no}</span>
      ${badge(a.outcome)}
      <span class="muted mono" style="font-size:12px">worker ${shortId(a.worker_id)}</span>
      <span class="muted">${a.duration_ms ? `${Math.round(a.duration_ms)}ms` : '–'}</span>
      <span class="spacer" style="flex:1"></span>
      <span class="muted" style="font-size:12px">${ago(a.started_at)}</span>
    </div>
    ${a.error ? `<div class="muted mono" style="font-size:11.5px;padding:0 0 8px 40px">${esc(a.error)}</div>` : ''}
  `).join('');

  const modal = document.createElement('div');
  modal.className = 'modal-backdrop';
  modal.innerHTML = `
    <div class="modal">
      <div class="row" style="align-items:center">
        <h2 style="font-size:17px"><code>${esc(j.type)}</code> ${badge(j.state)}</h2>
        <div class="spacer" style="flex:1"></div>
        <button class="ghost sm shrink" data-close>Close</button>
      </div>
      <p class="muted mono" style="font-size:12px">${esc(j.id)}</p>
      <div class="grid cols-4" style="margin:14px 0">
        <div><label>Queue</label><div>${esc(j.queue)}</div></div>
        <div><label>Priority</label><div>${j.priority}</div></div>
        <div><label>Attempts</label><div>${j.attempts} / ${j.max_attempts}</div></div>
        <div><label>Duration</label><div>${duration(j)}</div></div>
      </div>
      ${j.depends_on?.length ? `<div class="field"><label>Depends on</label>
        <div class="mono" style="font-size:12px">${j.depends_on.map(shortId).join(', ')}</div></div>` : ''}
      <div class="field"><label>Payload</label><pre class="json">${esc(JSON.stringify(j.payload, null, 2))}</pre></div>
      ${j.result ? `<div class="field"><label>Result</label>
        <pre class="json">${esc(JSON.stringify(j.result, null, 2))}</pre></div>` : ''}
      ${j.error ? `<div class="field"><label>Last error</label>
        <pre class="json" style="color:var(--err)">${esc(j.error)}</pre></div>` : ''}
      ${triagePanel(j.triage)}
      <div class="field"><label>Attempt timeline</label>${attempts || '<div class="muted">No attempts yet.</div>'}</div>
    </div>`;
  modal.onclick = (e) => { if (e.target === modal || e.target.dataset.close !== undefined) modal.remove(); };
  $('modal-root').appendChild(modal);
}

/* ---------------- workers ---------------- */
async function loadWorkers() {
  const workers = await api('/workers');
  $('workers-body').innerHTML = workers.length
    ? workers.map((w) => {
        const stale = (Date.now() - new Date(w.last_heartbeat_at)) / 1000;
        return `
        <tr>
          <td class="mono">${shortId(w.id)}</td>
          <td class="muted">${esc(w.hostname)} · ${w.pid}</td>
          <td>${badge(w.state === 'dead' ? 'dead' : w.state)}</td>
          <td>${w.concurrency}</td>
          <td class="muted">${w.queues.length ? w.queues.map(esc).join(', ') : 'all'}</td>
          <td class="${stale > 20 && w.state === 'online' ? '' : 'muted'}">${ago(w.last_heartbeat_at)}</td>
          <td>${w.state === 'online'
              ? `<button class="danger sm" data-kill="${w.id}">💥 Kill</button>` : ''}</td>
        </tr>`;
      }).join('')
    : '<tr><td colspan="7" class="empty">No workers have registered yet.</td></tr>';

  $('workers-body').querySelectorAll('button[data-kill]').forEach((b) => {
    b.onclick = () => killWorker(b.dataset.kill);
  });
}

async function killWorker(id) {
  try {
    const w = await api(`/workers/chaos/kill${id ? `?worker_id=${id}` : ''}`, { method: 'POST' });
    toast(`Kill signal sent to worker ${shortId(w.id)} (pid ${w.pid}). Watch its jobs get reclaimed.`, 'err');
    setTimeout(loadWorkers, 6000);
  } catch (err) { toast(err.message, 'err'); }
}

$('chaos-random').onclick = () => killWorker(null);

/* ---------------- dlq + webhooks ---------------- */
const CATEGORY_LABEL = {
  network: 'network', timeout: 'timeout', rate_limit: 'rate limit', auth: 'auth',
  bad_input: 'bad input', dependency: 'dependency', resource: 'resource',
  bug: 'bug', unknown: 'unknown',
};

function triageCell(t) {
  if (!t) return '<span class="muted" style="font-size:12px">—</span>';
  return `<span class="badge ai">${esc(CATEGORY_LABEL[t.category] || t.category)}</span> ` +
    `<span class="badge ${t.is_transient ? 'transient' : 'permanent'}">` +
    `${t.is_transient ? 'transient' : 'permanent'}</span>`;
}

async function loadDlq() {
  const [dead, hooks, triage, ai] = await Promise.all([
    api('/jobs?state=dead&limit=50'),
    api('/webhook-deliveries?limit=25'),
    api('/triage?limit=200'),
    api('/metrics/ai'),
  ]);
  const byJob = Object.fromEntries(triage.map((t) => [t.job_id, t]));
  renderAiBanner(ai);

  $('dlq-body').innerHTML = dead.items.length
    ? dead.items.map((j) => `
        <tr class="clickable" data-job="${j.id}">
          <td><code>${esc(j.type)}</code></td>
          <td>${esc(j.queue)}</td>
          <td>${j.attempts}/${j.max_attempts}</td>
          <td class="nowrap">${triageCell(byJob[j.id])}</td>
          <td class="muted mono" style="font-size:11.5px;max-width:280px;overflow:hidden;text-overflow:ellipsis">
            ${esc(j.error)}</td>
          <td class="nowrap muted">${ago(j.finished_at)}</td>
          <td class="nowrap">${byJob[j.id] || !ai.enabled ? '' :
              `<button class="ghost sm" data-triage="${j.id}">Triage</button> `}` +
            `<button class="ghost sm" data-retry="${j.id}">Requeue</button></td>
        </tr>`).join('')
    : '<tr><td colspan="7" class="empty">Dead-letter queue is empty. Submit a <code>flaky</code> job with <code>fail_times</code> ≥ max attempts to populate it.</td></tr>';
  wireJobRows($('dlq-body'));

  $('dlq-body').querySelectorAll('button[data-triage]').forEach((b) => {
    b.onclick = async () => {
      b.disabled = true;
      try {
        await api(`/jobs/${b.dataset.triage}/triage`, { method: 'POST' });
        toast('Triage queued — it runs as a job on the triage queue', 'ok');
      } catch (err) { toast(err.message, 'err'); b.disabled = false; }
    };
  });

  renderWebhooks(hooks);
}

function renderAiBanner(ai) {
  const el = $('ai-banner');
  if (!ai.enabled) {
    // Say so plainly rather than showing an empty panel that looks broken.
    el.className = 'ai-banner off';
    el.innerHTML = '<strong>AI triage is off.</strong> Set <code>ANTHROPIC_API_KEY</code> ' +
      'to have dead-lettered jobs automatically classified and explained. ' +
      'Everything else on this platform works without it.';
    return;
  }
  const t = ai.triage;
  el.className = 'ai-banner';
  el.innerHTML = `<strong>AI triage is on</strong> — when a job exhausts its retries, ` +
    `<code>${esc(ai.model)}</code> classifies the failure, explains the root cause, and ` +
    `recommends an action. Triage runs as a job on the <code>triage</code> queue, so it ` +
    `inherits the same retries and dead-lettering as everything else.` +
    `<div class="cost" style="margin-top:7px">` +
    `${t.analyzed} analyzed · ${t.reused_from_fingerprint} reused from an identical error ` +
    `signature (no API call) · ${t.tokens.toLocaleString()} tokens · $${t.cost_usd.toFixed(4)}` +
    `</div>`;
}

function renderWebhooks(hooks) {
  $('webhooks-body').innerHTML = hooks.length
    ? hooks.map((d) => `
        <tr>
          <td><code>${esc(d.event)}</code></td>
          <td class="muted" style="max-width:280px;overflow:hidden;text-overflow:ellipsis">${esc(d.url)}</td>
          <td>${badge(d.state === 'delivered' ? 'succeeded' : d.state === 'failed' ? 'dead' : 'queued')}</td>
          <td>${d.attempts}</td>
          <td class="muted">${d.response_status ?? esc(d.last_error ?? '–')}</td>
          <td class="nowrap muted">${ago(d.created_at)}</td>
        </tr>`).join('')
    : '<tr><td colspan="6" class="empty">No webhooks sent yet — submit a job with a <code>callback_url</code>.</td></tr>';
}

/* ---------------- schedules ---------------- */
async function loadSchedules() {
  const rows = await api('/schedules');
  $('schedules-body').innerHTML = rows.length
    ? rows.map((s) => `
        <tr>
          <td>${esc(s.name)}</td>
          <td><code>${esc(s.cron)}</code></td>
          <td><code>${esc(s.job_type)}</code></td>
          <td>${esc(s.queue)}</td>
          <td class="nowrap ${s.enabled ? '' : 'muted'}">${s.enabled ? until(s.next_run_at) : 'disabled'}</td>
          <td class="nowrap muted">${ago(s.last_run_at)}</td>
          <td><button class="ghost sm" data-del="${s.id}">Delete</button></td>
        </tr>`).join('')
    : '<tr><td colspan="7" class="empty">No schedules yet.</td></tr>';

  $('schedules-body').querySelectorAll('button[data-del]').forEach((b) => {
    b.onclick = async () => {
      await api(`/schedules/${b.dataset.del}`, { method: 'DELETE' });
      toast('Schedule deleted', 'ok');
      loadSchedules();
    };
  });
}

$('sc-create').onclick = async () => {
  try {
    await api('/schedules', {
      method: 'POST',
      body: JSON.stringify({
        name: $('sc-name').value || 'schedule',
        cron: $('sc-cron').value,
        job_type: $('sc-type').value,
        queue: $('sc-queue').value || 'default',
        payload: JSON.parse($('sc-payload').value || '{}'),
      }),
    });
    toast('Schedule created', 'ok');
    loadSchedules();
  } catch (err) { toast(err.message, 'err'); }
};

/* ---------------- submit ---------------- */
const DEFAULT_PAYLOADS = {
  sleep: { seconds: 3 },
  flaky: { fail_times: 2 },
  http_fetch: { url: 'https://example.com' },
  thumbnail: { width: 128, height: 128 },
  email_sim: { to: 'someone@example.com', subject: 'Welcome!' },
  llm_summarize: {
    text: 'Paste the text to summarize here. LLM calls are slow and rate-limited, '
      + 'which is exactly why they belong on a queue rather than in a request handler.',
    max_words: 40,
  },
  llm_classify: {
    text: 'The checkout page returns a 500 whenever I apply a discount code.',
    labels: ['bug_report', 'feature_request', 'billing_question', 'other'],
  },
  llm_extract: {
    text: 'Invoice INV-2043 dated 14 March 2026, total $1,299.00, billed to Acme Corp.',
    fields: ['invoice_number', 'date', 'total', 'customer'],
  },
};

function fillTypeSelects() {
  const options = JOB_TYPES.map((t) => `<option value="${t}">${t}</option>`).join('');
  $('sb-type').innerHTML = options;
  $('sc-type').innerHTML = options;
  $('sb-type').onchange = () => {
    $('sb-payload').value = JSON.stringify(DEFAULT_PAYLOADS[$('sb-type').value] ?? {}, null, 2);
  };
}

$('sb-submit').onclick = async () => {
  try {
    const body = {
      type: $('sb-type').value,
      queue: $('sb-queue').value || 'default',
      payload: JSON.parse($('sb-payload').value || '{}'),
      priority: Number($('sb-priority').value) || 0,
      max_attempts: Number($('sb-max').value) || 3,
    };
    const delay = Number($('sb-delay').value);
    if (delay > 0) body.delay_seconds = delay;
    if ($('sb-idem').value) body.idempotency_key = $('sb-idem').value;
    if ($('sb-callback').value) body.callback_url = $('sb-callback').value;

    const job = await api('/jobs', { method: 'POST', body: JSON.stringify(body) });
    toast(`Job ${shortId(job.id)} submitted (${job.state})`, 'ok');
  } catch (err) { toast(err.message, 'err'); }
};

$('sb-burst').onclick = async () => {
  const burst = [
    { type: 'sleep', payload: { seconds: 2 } },
    { type: 'sleep', payload: { seconds: 5 } },
    { type: 'email_sim', payload: { to: 'a@example.com', subject: 'Receipt' } },
    { type: 'email_sim', payload: { to: 'b@example.com', subject: 'Digest' } },
    { type: 'thumbnail', payload: { width: 128, height: 128 } },
    { type: 'thumbnail', payload: { width: 64, height: 64 } },
    { type: 'http_fetch', payload: { url: 'https://example.com' } },
    { type: 'http_fetch', payload: { url: 'https://www.wikipedia.org' } },
    { type: 'flaky', payload: { fail_times: 1 }, max_attempts: 3 },
    { type: 'flaky', payload: { fail_times: 2 }, max_attempts: 3 },
    { type: 'flaky', payload: { fail_times: 9 }, max_attempts: 2 },   // -> dead letter
    { type: 'sleep', payload: { seconds: 1 }, priority: 10 },          // -> jumps the queue
  ];
  try {
    await Promise.all(burst.map((b) => api('/jobs', { method: 'POST', body: JSON.stringify(b) })));
    toast('12 demo jobs submitted — watch the Overview stream', 'ok');
  } catch (err) { toast(err.message, 'err'); }
};

/* ---------------- api keys ---------------- */
async function loadApiKeys() {
  const keys = await api('/api-keys');
  $('apikeys-body').innerHTML = keys.length
    ? keys.map((k) => `
        <tr>
          <td>${esc(k.name)}</td>
          <td class="mono muted">${esc(k.prefix)}…</td>
          <td class="muted">${ago(k.created_at)}</td>
          <td class="muted">${k.last_used_at ? ago(k.last_used_at) : 'never'}</td>
          <td><button class="ghost sm" data-revoke="${k.id}">Revoke</button></td>
        </tr>`).join('')
    : '<tr><td colspan="5" class="empty">No API keys yet.</td></tr>';

  $('apikeys-body').querySelectorAll('button[data-revoke]').forEach((b) => {
    b.onclick = async () => {
      await api(`/api-keys/${b.dataset.revoke}`, { method: 'DELETE' });
      toast('API key revoked', 'ok');
      loadApiKeys();
    };
  });

  $('curl-example').textContent =
`curl -X POST ${location.origin}/api/v1/jobs \\
  -H "X-API-Key: tf_live_..." \\
  -H "Content-Type: application/json" \\
  -d '{
    "type": "http_fetch",
    "payload": {"url": "https://example.com"},
    "max_attempts": 5,
    "idempotency_key": "fetch-example-once",
    "callback_url": "https://your-app.com/hooks/taskforge"
  }'`;
}

$('ak-create').onclick = async () => {
  try {
    const key = await api('/api-keys', {
      method: 'POST',
      body: JSON.stringify({ name: $('ak-name').value || 'default' }),
    });
    $('ak-new').innerHTML = `
      <div class="chaos-banner" style="background:linear-gradient(90deg,rgba(46,204,143,.12),transparent);
           border-color:rgba(46,204,143,.3)">
        <strong>Copy this key now — it is shown only once.</strong>
        <pre class="json" style="margin-top:8px">${esc(key.key)}</pre>
      </div>`;
    $('ak-name').value = '';
    loadApiKeys();
  } catch (err) { toast(err.message, 'err'); }
};

/* ---------------- live SSE ---------------- */
const EVENT_LABEL = {
  'job.created': 'created', 'job.queued': 'queued', 'job.started': 'started',
  'job.succeeded': 'succeeded', 'job.retrying': 'retrying', 'job.dead': 'dead-lettered',
  'job.canceled': 'canceled', 'worker.online': 'worker online', 'worker.dead': 'WORKER DIED',
  'worker.stopped': 'worker stopped', 'worker.kill_requested': 'kill requested',
  'queue.paused': 'queue paused', 'queue.resumed': 'queue resumed',
};

function connectStream() {
  const source = new EventSource(`/api/v1/stream?token=${encodeURIComponent(token)}`);
  const log = $('event-log');

  source.onopen = () => {
    $('live').classList.add('on');
    $('live').querySelector('span').textContent = 'live';
  };
  source.onerror = () => {
    $('live').classList.remove('on');
    $('live').querySelector('span').textContent = 'reconnecting…';
  };
  source.onmessage = (e) => {
    const ev = JSON.parse(e.data);
    log.querySelector('#event-placeholder')?.remove();

    const cls = ev.type.startsWith('worker') ? 'ev-worker'
      : ev.type === 'job.succeeded' ? 'ev-succeeded'
      : ev.type === 'job.dead' ? 'ev-dead'
      : ev.type === 'job.retrying' ? 'ev-retrying' : '';

    let detail = '';
    if (ev.job_type) detail = `<code>${esc(ev.job_type)}</code> ${shortId(ev.job_id)}`;
    else if (ev.hostname) detail = `${esc(ev.hostname)} pid ${esc(ev.pid)}`;
    if (ev.next_run_in_s !== undefined) detail += ` <span class="muted">retry in ${ev.next_run_in_s}s</span>`;
    if (ev.attempt) detail += ` <span class="muted">attempt ${ev.attempt}</span>`;

    const row = document.createElement('div');
    row.innerHTML = `<span class="t">${new Date().toLocaleTimeString()}</span> ` +
      `<span class="${cls}">${esc(EVENT_LABEL[ev.type] || ev.type)}</span> ${detail}`;
    log.prepend(row);
    while (log.children.length > 120) log.lastChild.remove();

    // Keep the visible view fresh, throttled so a burst doesn't hammer the API.
    scheduleRefresh();
  };
}

let refreshTimer = null;
function scheduleRefresh() {
  if (refreshTimer) return;
  refreshTimer = setTimeout(() => {
    refreshTimer = null;
    if (['overview', 'jobs', 'workers', 'dlq'].includes(currentView)) LOADERS[currentView]();
  }, 1500);
}

/* ---------------- boot ---------------- */
fillTypeSelects();
loadOverview();
connectStream();
setInterval(() => { if (currentView === 'overview') loadOverview(); }, 15000);
