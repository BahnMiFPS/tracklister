const $ = (s) => document.querySelector(s);
let current = null;      // job currently shown
let stream = null;       // EventSource for the running job
let renderedTracks = ""; // JSON of the track list last drawn, to skip needless rebuilds

const fmt = (sec) => {
  sec = Math.max(0, Math.floor(sec || 0));
  const h = Math.floor(sec / 3600), m = Math.floor((sec % 3600) / 60), s = sec % 60;
  const mm = String(m).padStart(2, "0"), ss = String(s).padStart(2, "0");
  return h ? `${h}:${mm}:${ss}` : `${m}:${ss}`;
};
const esc = (s) => String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
const youtubeId = (url) => (url || "").match(/(?:v=|youtu\.be\/|shorts\/|live\/|embed\/)([\w-]{11})/)?.[1] || null;

async function api(path, opts) {
  const r = await fetch(path, opts);
  if (!r.ok) throw new Error((await r.json().catch(() => ({}))).detail || r.statusText);
  return r.json();
}

// ---------------------------------------------------------------- player
// One interface over the YouTube IFrame API and a local <audio> element, so
// the rest of the UI only ever calls playFrom / toggle / time.

let ytApi = null;
const loadYouTubeApi = () => ytApi ||= new Promise((resolve) => {
  window.onYouTubeIframeAPIReady = () => resolve(window.YT);
  const s = document.createElement("script");
  s.src = "https://www.youtube.com/iframe_api";
  document.head.appendChild(s);
});

const Player = {
  kind: null, jobId: null, videoId: null, yt: null, ytReady: false, pending: null, target: 0,
  audio: new Audio(),

  async playFrom(sec) {
    if (!current) return;
    const vid = youtubeId(current.webpage_url);
    $("#mini").classList.remove("hidden");
    if (vid) return this._youtube(vid, sec);
    if (current.has_audio) return this._local(sec);
    if (current.webpage_url) window.open(current.webpage_url, "_blank");
  },

  async _youtube(vid, sec) {
    this.target = sec;
    this.audio.pause();
    $("#mini").classList.remove("audio-only");
    $("#mini-audio").classList.add("hidden");
    if (this.yt && this.videoId === vid) {
      this.kind = "yt";
      if (this.ytReady) { this.yt.seekTo(sec, true); this.yt.playVideo(); } else this.pending = sec;
      return;
    }
    this.close(false);
    this.kind = "yt";
    this.videoId = vid;
    this.pending = sec;
    const YT = await loadYouTubeApi();
    this.yt = new YT.Player("yt-frame", {
      host: "https://www.youtube-nocookie.com",
      videoId: vid,
      playerVars: { autoplay: 1, playsinline: 1, rel: 0, start: Math.floor(sec), origin: location.origin },
      events: {
        onReady: (e) => {
          this.ytReady = true;
          const at = this.pending ?? sec;
          this.pending = null;
          e.target.seekTo(at, true);
          e.target.playVideo();
          // Embedding can be refused without firing onError (age/region
          // limits, missing referrer); fall back if it never starts.
          setTimeout(() => {
            if (this.kind === "yt" && this.yt === e.target && e.target.getPlayerState() === -1) this._fallback(at);
          }, 6000);
        },
        // Before playback starts the player reports 0s, so keep the asked-for spot.
        onError: () => this._fallback(Math.max(this.time() || 0, this.target)),
      },
    });
  },

  _fallback(sec) {
    if (!current?.has_audio) return;
    this.close(false);
    $("#mini-track").textContent = "YouTube refused to embed this video, playing the local copy";
    this._local(sec);
  },

  _local(sec) {
    this.kind = "audio";
    $("#mini").classList.add("audio-only");
    $("#mini-audio").classList.remove("hidden");
    const seek = () => { this.audio.currentTime = sec; this.audio.play(); };
    if (this.jobId !== current.id) {
      // A seek before metadata loads is ignored, so wait for it on a new source.
      this.audio.src = `/api/jobs/${current.id}/audio`;
      this.jobId = current.id;
      this.audio.addEventListener("loadedmetadata", seek, { once: true });
    } else seek();
  },

  toggle() {
    if (this.kind === "yt" && this.ytReady) {
      this.playing() ? this.yt.pauseVideo() : this.yt.playVideo();
    } else if (this.kind === "audio") {
      this.audio.paused ? this.audio.play() : this.audio.pause();
    }
  },

  time() {
    if (this.kind === "yt" && this.ytReady) return this.yt.getCurrentTime();
    if (this.kind === "audio") return this.audio.currentTime;
    return null;
  },

  playing() {
    if (this.kind === "yt" && this.ytReady) return this.yt.getPlayerState() === 1;
    if (this.kind === "audio") return !this.audio.paused;
    return false;
  },

  close(hide = true) {
    if (this.yt) {
      this.yt.destroy();
      $(".mini-video").innerHTML = '<div id="yt-frame"></div>';
    }
    this.yt = null; this.ytReady = false; this.videoId = null; this.pending = null;
    this.audio.pause();
    this.kind = null;
    if (hide) $("#mini").classList.add("hidden");
  },
};
$("#mini-close").onclick = () => Player.close();
$("#audio-toggle").onclick = () => Player.toggle();

// ---------------------------------------------------------------- waveform
// Server sends 3 bytes per column (low, mid, high band energy), CPS columns/s.

let wave = null; // { jobId, data: Uint8Array, cps, count }

async function loadWave(job) {
  if (wave?.jobId === job.id || wave?.loading === job.id) return;
  wave = { loading: job.id };
  try {
    const r = await fetch(`/api/jobs/${job.id}/waveform`);
    if (!r.ok) throw new Error();
    const data = new Uint8Array(await r.arrayBuffer());
    wave = { jobId: job.id, data, cps: +r.headers.get("X-Columns-Per-Second") || 10, count: data.length / 3 };
  } catch {
    wave = null; // not built yet (job still decoding); retried on the next render
  }
  drawAll();
}

const cssVar = (n) => getComputedStyle(document.documentElement).getPropertyValue(n).trim();

function drawWave(canvas, from, to, opts = {}) {
  const dpr = Math.min(window.devicePixelRatio || 1, 2);
  const w = canvas.clientWidth, h = canvas.clientHeight;
  if (!w || !h || to <= from) return;
  if (canvas.width !== Math.round(w * dpr)) { canvas.width = Math.round(w * dpr); canvas.height = Math.round(h * dpr); }
  const ctx = canvas.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, w, h);
  const mid = h / 2, span = to - from;
  const xOf = (sec) => ((sec - from) / span) * w;

  if (opts.highlight) {
    ctx.fillStyle = "rgba(255,255,255,.06)";
    ctx.fillRect(xOf(opts.highlight[0]), 0, xOf(opts.highlight[1]) - xOf(opts.highlight[0]), h);
  }

  const colors = [cssVar("--wf-low"), cssVar("--wf-mid"), cssVar("--wf-high")];
  const step = 3, bars = Math.floor(w / step);
  if (wave?.data) {
    const c0 = from * wave.cps, c1 = Math.min(wave.count, to * wave.cps);
    for (let i = 0; i < bars; i++) {
      const a = Math.floor(c0 + (i * (c1 - c0)) / bars);
      const b = Math.max(a + 1, Math.floor(c0 + ((i + 1) * (c1 - c0)) / bars));
      const peak = [0, 0, 0];
      for (let c = a; c < b && c < wave.count; c++) {
        for (let k = 0; k < 3; k++) peak[k] = Math.max(peak[k], wave.data[c * 3 + k]);
      }
      const sec = from + ((i + 0.5) / bars) * span;
      ctx.globalAlpha = opts.playhead == null ? 0.85 : sec <= opts.playhead ? 1 : 0.32;
      // Bass widest underneath, mids then highs layered on top (DJ-software style).
      [1, 0.75, 0.45].forEach((scale, k) => {
        const hh = (peak[k] / 255) * (mid - 2) * scale + 0.5;
        ctx.fillStyle = colors[k];
        ctx.fillRect(i * step, mid - hh, 2, hh * 2);
      });
    }
    ctx.globalAlpha = 1;
  } else {
    ctx.fillStyle = cssVar("--line");
    ctx.fillRect(0, mid - 1, w, 2);
  }

  ctx.fillStyle = "rgba(255,255,255,.35)";
  for (const m of opts.marks || []) ctx.fillRect(Math.round(xOf(m)), 0, 1, h);
  if (opts.playhead != null && opts.playhead >= from && opts.playhead <= to) {
    ctx.fillStyle = "#fff";
    ctx.fillRect(Math.round(xOf(opts.playhead)) - 1, 0, 2, h);
  }
}

function activeIndex(t) {
  if (t == null || !current) return -1;
  return current.tracks.findIndex((tr) => t >= tr.start && t < tr.end);
}

function drawAll() {
  if (!current) return;
  const t = Player.jobId === current.id || Player.kind === "yt" ? Player.time() : null;
  const idx = activeIndex(t);
  const total = current.duration || 1;
  const tr = current.tracks[idx];
  drawWave($("#overview-canvas"), 0, total, {
    playhead: t, marks: current.tracks.slice(1).map((x) => x.start), highlight: tr && [tr.start, tr.end],
  });
  const li = document.querySelector(`.track[data-i="${idx}"]`);
  if (li && tr) drawWave(li.querySelector(".track-wave"), tr.start, tr.end, { playhead: t });
}

// Playback clock: highlight the playing track and move the playheads.
let lastActive = -2;
setInterval(() => {
  if (!current || !Player.kind) return;
  const t = Player.time();
  const idx = activeIndex(t);
  if (idx !== lastActive) {
    document.querySelectorAll(".track").forEach((li) => li.classList.toggle("active", +li.dataset.i === idx));
    const tr = current.tracks[idx];
    $("#mini-track").textContent = tr ? `${tr.artist} – ${tr.title}` : current.title || "";
    lastActive = idx;
  }
  $("#audio-time").textContent = t == null ? "" : `${fmt(t)} / ${fmt(current.duration)}`;
  $("#audio-toggle").textContent = Player.playing() ? "❚❚" : "▶";
  document.querySelectorAll(".track .playicon").forEach((el) => {
    el.textContent = +el.closest(".track").dataset.i === idx && Player.playing() ? "❚❚" : "▶";
  });
  drawAll();
}, 200);
window.addEventListener("resize", drawAll);

const overview = $("#overview-canvas");
overview.onclick = (e) => {
  const r = overview.getBoundingClientRect();
  Player.playFrom(((e.clientX - r.left) / r.width) * (current.duration || 0));
};
overview.onmousemove = (e) => {
  const r = overview.getBoundingClientRect();
  const sec = ((e.clientX - r.left) / r.width) * (current?.duration || 0);
  const tr = current?.tracks[activeIndex(sec)];
  const tip = $("#overview-tip");
  tip.classList.remove("hidden");
  tip.style.left = `${e.clientX - r.left}px`;
  tip.textContent = tr ? `${fmt(sec)} · ${tr.artist} – ${tr.title}` : fmt(sec);
};
overview.onmouseleave = () => $("#overview-tip").classList.add("hidden");

// ---------------------------------------------------------------- navigation

function showView(view) {
  document.querySelectorAll(".tab").forEach((t) => t.classList.toggle("active", t.dataset.view === view));
  $("#view-identify").classList.toggle("hidden", view !== "identify");
  $("#view-list").classList.toggle("hidden", view === "identify");
  if (view !== "identify") loadList(view === "saved");
}
document.querySelectorAll(".tab").forEach((t) => (t.onclick = () => showView(t.dataset.view)));

// ---------------------------------------------------------------- submit

$("#submit-form").onsubmit = async (e) => {
  e.preventDefault();
  const url = $("#url").value.trim();
  if (!url) return;
  try {
    const { job_id } = await api("/api/jobs", {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ url }),
    });
    $("#url").value = "";
    openJob(job_id);
  } catch (err) { alert(err.message); }
};

async function upload(file) {
  const fd = new FormData();
  fd.append("file", file);
  try {
    const { job_id } = await api("/api/jobs/upload", { method: "POST", body: fd });
    openJob(job_id);
  } catch (err) { alert(err.message); }
}
$("#file").onchange = (e) => e.target.files[0] && upload(e.target.files[0]);
const drop = $("#drop");
["dragenter", "dragover"].forEach((ev) => drop.addEventListener(ev, (e) => { e.preventDefault(); drop.classList.add("over"); }));
["dragleave", "drop"].forEach((ev) => drop.addEventListener(ev, (e) => { e.preventDefault(); drop.classList.remove("over"); }));
drop.addEventListener("drop", (e) => e.dataTransfer.files[0] && upload(e.dataTransfer.files[0]));

// ---------------------------------------------------------------- job view

async function openJob(id) {
  showView("identify");
  if (stream) stream.close();
  if (current?.id !== id) { Player.close(); renderedTracks = ""; lastActive = -2; $("#filter").value = ""; }
  history.replaceState(null, "", `#${id}`);
  render(await api(`/api/jobs/${id}`));
  if (!["completed", "failed"].includes(current.status)) {
    stream = new EventSource(`/api/jobs/${id}/stream`);
    stream.onmessage = (e) => render(JSON.parse(e.data));
    stream.onerror = () => { stream.close(); setTimeout(() => current && openJob(current.id), 3000); };
  }
}

function render(job) {
  const done = job.status === "completed";
  current = job;
  $("#job").classList.remove("hidden");
  $("#job").classList.toggle("failed", job.status === "failed");
  $("#job-thumb").src = job.thumbnail || "";
  $("#job-title").textContent = job.title || job.source;
  $("#job-sub").textContent = [job.uploader, job.duration && fmt(job.duration), job.tracks.length && `${job.tracks.length} tracks`]
    .filter(Boolean).join(" · ");
  $("#job-bar").style.width = `${job.progress || 0}%`;
  $("#job-msg").textContent = job.status === "failed" ? `Error: ${job.error}` : job.message || "";
  $("#job-actions").classList.toggle("hidden", !done && job.status !== "failed");
  $("#act-save").textContent = job.saved ? "★ Saved" : "☆ Save";
  $("#act-csv").href = `/api/jobs/${job.id}/export.csv`;
  $("#filter").classList.toggle("hidden", !job.tracks.length);
  if (job.duration && wave?.jobId !== job.id) loadWave(job);

  const key = JSON.stringify(job.tracks);
  if (key !== renderedTracks) { renderTracks(job); renderedTracks = key; lastActive = -2; }
  drawAll();
}

const ICONS = {
  youtube: '<svg viewBox="0 0 24 24"><path d="M21.6 7.2a2.5 2.5 0 0 0-1.8-1.8C18.2 5 12 5 12 5s-6.2 0-7.8.4A2.5 2.5 0 0 0 2.4 7.2 26 26 0 0 0 2 12a26 26 0 0 0 .4 4.8 2.5 2.5 0 0 0 1.8 1.8C5.8 19 12 19 12 19s6.2 0 7.8-.4a2.5 2.5 0 0 0 1.8-1.8A26 26 0 0 0 22 12a26 26 0 0 0-.4-4.8ZM10 15V9l5.2 3Z"/></svg>',
  spotify: '<svg viewBox="0 0 24 24"><path d="M12 2a10 10 0 1 0 0 20 10 10 0 0 0 0-20Zm4.6 14.4a.6.6 0 0 1-.9.2c-2.4-1.5-5.4-1.8-9-1a.6.6 0 1 1-.3-1.2c3.9-.9 7.2-.5 9.9 1.1.3.2.4.6.3.9Zm1.2-2.7a.8.8 0 0 1-1 .3c-2.7-1.7-6.9-2.2-10.2-1.2a.8.8 0 1 1-.4-1.5c3.7-1.1 8.3-.6 11.4 1.3.3.2.4.7.2 1.1Zm.1-2.8C14.6 9 9.2 8.8 6.1 9.8a.9.9 0 1 1-.5-1.8c3.6-1.1 9.5-.9 13.2 1.3a.9.9 0 0 1-.9 1.6Z"/></svg>',
};

// Camelot wheel colours: neighbouring numbers mix harmonically, so give them
// neighbouring hues; B (major) slightly lighter than A (minor).
const camelotColor = (c) => {
  const n = parseInt(c, 10) || 1;
  return `hsl(${((n - 1) * 30 + 150) % 360} 60% ${c?.endsWith("B") ? 42 : 34}%)`;
};

function renderTracks(job) {
  const ol = $("#tracks");
  ol.innerHTML = "";
  if (!job.tracks.length && job.status === "completed") {
    ol.innerHTML = `<li class="empty">No tracks identified in this mix.</li>`;
    return;
  }
  const tpl = $("#track-tpl");
  job.tracks.forEach((t, i) => {
    const li = tpl.content.firstElementChild.cloneNode(true);
    li.dataset.i = i;
    li.dataset.search = [t.title, t.artist, t.label, t.album].join(" ").toLowerCase();
    const art = li.querySelector(".art");
    if (t.artwork_url) art.src = t.artwork_url; else art.style.visibility = "hidden";
    li.querySelector(".num").textContent = i + 1;
    li.querySelector(".range").textContent = `${fmt(t.start)}-${fmt(t.end)}`;
    li.querySelector(".bpm").textContent = t.bpm ? `${Math.round(t.bpm)} BPM` : "";
    if (t.key) {
      const key = li.querySelector(".key");
      key.innerHTML = `${esc(t.key)} <b style="background:${camelotColor(t.camelot)}">${esc(t.camelot)}</b>`;
    }
    li.querySelector(".when").title = t.bpm_source
      ? `Original release: ${t.bpm_source}` + (t.played_bpm ? `\nPlayed in this mix: ${t.played_bpm} BPM · ${t.played_key ?? "?"} (${t.played_camelot ?? "?"})` : "")
      : "Measured from the mix audio (what the DJ played)";
    li.querySelector(".t-title").textContent = t.title;
    li.querySelector(".t-artist").textContent = [t.artist, t.release_date?.slice(0, 4)].filter(Boolean).join(" · ");
    li.querySelector(".t-label").textContent = t.label || "";
    li.querySelector(".t-label").title = `${t.confidence} confidence · ${t.hits} matching sample(s)`;

    const q = encodeURIComponent(`${t.artist} ${t.title}`);
    li.querySelector(".icons").innerHTML = `
      <a href="https://www.youtube.com/results?search_query=${q}" target="_blank" rel="noopener" title="Find on YouTube">${ICONS.youtube}</a>
      <a href="${esc(t.spotify_url)}" target="_blank" rel="noopener" class="${t.spotify_exact ? "spotify-exact" : ""}"
         title="${t.spotify_exact ? "Open on Spotify" : "Search Spotify"}">${ICONS.spotify}</a>`;

    li.querySelector(".cover").onclick = () => {
      if (+li.dataset.i === lastActive && Player.kind) Player.toggle(); else Player.playFrom(t.start);
    };
    const wv = li.querySelector(".track-wave");
    wv.onclick = (e) => {
      const r = wv.getBoundingClientRect();
      Player.playFrom(t.start + ((e.clientX - r.left) / r.width) * (t.end - t.start));
    };
    ol.appendChild(li);
  });
  // GetSongBPM's terms require visible attribution wherever their data shows.
  $("#credit").classList.toggle("hidden", !job.tracks.some((t) => t.bpm_source === "GetSongBPM"));
  applyFilter();
}

function applyFilter() {
  const q = $("#filter").value.trim().toLowerCase();
  document.querySelectorAll(".track").forEach((li) => li.classList.toggle("hidden", !!q && !li.dataset.search.includes(q)));
}
$("#filter").oninput = applyFilter;

$("#act-play").onclick = () => (Player.kind ? Player.toggle() : Player.playFrom(0));
$("#act-save").onclick = async () => {
  const { saved } = await api(`/api/jobs/${current.id}/save`, { method: "POST" });
  current.saved = saved;
  $("#act-save").textContent = saved ? "★ Saved" : "☆ Save";
};
$("#act-retry").onclick = async () => {
  try {
    await api(`/api/jobs/${current.id}/retry`, { method: "POST" });
    openJob(current.id);
  } catch (err) { alert(err.message); }
};
$("#act-copy").onclick = async () => {
  const text = await (await fetch(`/api/jobs/${current.id}/export.txt`)).text();
  await navigator.clipboard.writeText(text);
  $("#act-copy").textContent = "Copied!";
  setTimeout(() => ($("#act-copy").textContent = "Copy tracklist"), 1500);
};
$("#act-delete").onclick = async () => {
  if (!confirm("Delete this set and its stored audio?")) return;
  await api(`/api/jobs/${current.id}`, { method: "DELETE" });
  Player.close();
  $("#job").classList.add("hidden");
  history.replaceState(null, "", "#");
  current = null;
};

// ---------------------------------------------------------------- history / saved

let searchTimer;
$("#search").oninput = () => {
  clearTimeout(searchTimer);
  searchTimer = setTimeout(() => loadList($(".tab.active").dataset.view === "saved"), 250);
};

async function loadList(saved) {
  const q = encodeURIComponent($("#search").value.trim());
  const items = await api(`/api/jobs?q=${q}&saved=${saved}`);
  const ul = $("#list");
  if (!items.length) {
    ul.innerHTML = `<li class="empty">${saved ? "No saved sets yet." : "Nothing identified yet."}</li>`;
    return;
  }
  ul.innerHTML = items.map((j) => `
    <li data-id="${j.id}">
      <img src="${esc(j.thumbnail || "")}" alt="">
      <div class="meta">
        <div class="t-title">${j.saved ? "★ " : ""}${esc(j.title || j.source)}</div>
        <div class="t-artist">${esc(j.uploader || "")}${j.duration ? " · " + fmt(j.duration) : ""}</div>
        <div class="status">${j.status === "completed" ? `${j.track_count} tracks` : esc(j.status)} · ${new Date(j.created_at * 1000).toLocaleString()}</div>
      </div>
    </li>`).join("");
  ul.querySelectorAll("li[data-id]").forEach((li) => (li.onclick = () => openJob(li.dataset.id)));
}

if (location.hash.length > 1) openJob(location.hash.slice(1)).catch(() => history.replaceState(null, "", "#"));
