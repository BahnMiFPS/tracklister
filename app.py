"""Tracklister - local DJ mix track identifier.

Pipeline per job: yt-dlp download -> ffmpeg to 16 kHz mono WAV -> fingerprint a
sample every STEP seconds with Shazam -> merge consecutive hits into tracks ->
enrich with Deezer metadata and streaming search links.
"""
import asyncio
import csv
import io
import json
import os
import re
import sqlite3
import subprocess
import threading
import time
import uuid
import wave
from difflib import SequenceMatcher
from pathlib import Path
from urllib.parse import quote_plus

import httpx
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, PlainTextResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from aiohttp_retry import ExponentialRetry
from shazamio import Shazam
from shazamio.client import HTTPClient
from shazamio.exceptions import RateLimited

ROOT = Path(__file__).parent
DATA = ROOT / "data"
WORK = DATA / "audio"
DB_PATH = DATA / "tracklister.db"
WORK.mkdir(parents=True, exist_ok=True)

# Minimal .env support (KEY=value lines) for Spotify credentials and tuning.
if (ROOT / ".env").exists():
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"'))

WINDOW = int(os.getenv("TL_WINDOW", "12"))  # seconds of audio per fingerprint
STEP = int(os.getenv("TL_STEP", "30"))  # seconds between fingerprints
AUDD_TOKEN = os.getenv("AUDD_API_TOKEN", "").strip()  # set -> AudD replaces Shazam
AUDD_CONCURRENCY = int(os.getenv("TL_AUDD_CONCURRENCY", "4"))
# Hybrid mode: Shazam re-checks AudD tracks backed by this many samples or fewer.
VERIFY_MAX_HITS = int(os.getenv("TL_VERIFY_MAX_HITS", "2"))
VERIFY_BUDGET = int(os.getenv("TL_VERIFY_BUDGET", "30"))  # max Shazam calls; 0 disables
MIN_INTERVAL = float(os.getenv("TL_MIN_INTERVAL", "9"))  # seconds between Shazam calls
SAMPLE_RATE = 16000

# ---------------------------------------------------------------- storage

_db_lock = threading.Lock()
_db = sqlite3.connect(DB_PATH, check_same_thread=False)
_db.row_factory = sqlite3.Row
_db.execute(
    """CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY, source TEXT, title TEXT, uploader TEXT,
        duration REAL, thumbnail TEXT, webpage_url TEXT, audio_file TEXT,
        status TEXT, progress REAL, message TEXT, error TEXT,
        tracks TEXT DEFAULT '[]', saved INTEGER DEFAULT 0,
        created_at REAL, updated_at REAL)"""
)
_db.commit()


def job_dict(row) -> dict:
    d = dict(row)
    d["tracks"] = json.loads(d["tracks"] or "[]")
    d["saved"] = bool(d["saved"])
    d["has_audio"] = bool(d["audio_file"]) and Path(d["audio_file"]).exists()
    d.pop("audio_file")
    return d


def get_job(job_id: str) -> dict | None:
    with _db_lock:
        row = _db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    return job_dict(row) if row else None


def update_job(job_id: str, **fields):
    if "tracks" in fields:
        fields["tracks"] = json.dumps(fields["tracks"])
    fields["updated_at"] = time.time()
    cols = ", ".join(f"{k}=?" for k in fields)
    with _db_lock:
        _db.execute(f"UPDATE jobs SET {cols} WHERE id=?", (*fields.values(), job_id))
        _db.commit()


def create_job(source: str) -> str:
    job_id = uuid.uuid4().hex[:12]
    now = time.time()
    with _db_lock:
        _db.execute(
            "INSERT INTO jobs (id, source, status, progress, message, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (job_id, source, "queued", 0, "Waiting in queue", now, now),
        )
        _db.commit()
    return job_id


# ---------------------------------------------------------------- download / decode

def fetch_metadata(url: str) -> dict:
    r = subprocess.run(
        ["yt-dlp", "--no-playlist", "-j", url],
        capture_output=True, text=True, encoding="utf-8", errors="ignore",
    )
    if r.returncode:
        raise RuntimeError(r.stderr.strip().splitlines()[-1] if r.stderr.strip() else "yt-dlp failed")
    return json.loads(r.stdout.splitlines()[0])


def download_audio(job_id: str, url: str) -> Path:
    proc = subprocess.Popen(
        ["yt-dlp", "--no-playlist", "-f", "bestaudio/best", "--newline",
         "-o", str(WORK / f"{job_id}.%(ext)s"), url],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, encoding="utf-8", errors="ignore",
    )
    last = 0.0
    for line in proc.stdout:
        m = re.search(r"\[download\]\s+([\d.]+)%", line)
        if m and time.time() - last > 1:
            pct = float(m.group(1))
            update_job(job_id, progress=round(pct * 0.25, 1), message=f"Downloading audio ({pct:.0f}%)")
            last = time.time()
    if proc.wait():
        raise RuntimeError("Download failed")
    files = [p for p in WORK.glob(f"{job_id}.*") if p.suffix not in (".wav", ".part", ".json", ".wave")]
    if not files:
        raise RuntimeError("Download produced no audio file")
    return files[0]


def to_wav(src: Path) -> Path:
    dst = src.with_name(src.stem + ".work.wav")
    r = subprocess.run(
        ["ffmpeg", "-y", "-v", "error", "-i", str(src), "-vn", "-ac", "1",
         "-ar", str(SAMPLE_RATE), "-sample_fmt", "s16", str(dst)],
        capture_output=True, text=True, errors="ignore",
    )
    if r.returncode:
        raise RuntimeError(f"ffmpeg failed: {r.stderr.strip()[-300:]}")
    return dst


def wav_slice(wav_path: Path, start: int, length: int) -> bytes:
    with wave.open(str(wav_path), "rb") as w:
        w.setpos(min(start * SAMPLE_RATE, w.getnframes()))
        frames = w.readframes(length * SAMPLE_RATE)
    buf = io.BytesIO()
    with wave.open(buf, "wb") as out:
        out.setnchannels(1)
        out.setsampwidth(2)
        out.setframerate(SAMPLE_RATE)
        out.writeframes(frames)
    return buf.getvalue()


WAVE_CPS = 10  # waveform columns per second
WAVE_BANDS = ((0, 250), (250, 4000), (4000, SAMPLE_RATE // 2))  # low / mid / high Hz


def build_waveform(wav_path: Path, out: Path) -> None:
    """3-band (low/mid/high) energy waveform like DJ software shows.

    Output is raw bytes: 3 per column (low, mid, high), 0-255, WAVE_CPS columns
    per second of audio.
    """
    import numpy as np

    hop = SAMPLE_RATE // WAVE_CPS
    samples = np.memmap(wav_path, dtype="<i2", mode="r", offset=44)
    cols = len(samples) // hop
    freqs = np.fft.rfftfreq(hop, 1 / SAMPLE_RATE)
    masks = [(freqs >= lo) & (freqs < hi) for lo, hi in WAVE_BANDS]
    window = np.hanning(hop).astype(np.float32)
    energy = np.zeros((cols, 3), dtype=np.float32)
    for start in range(0, cols, 2000):  # chunked to bound memory
        stop = min(cols, start + 2000)
        block = np.asarray(samples[start * hop:stop * hop], dtype=np.float32).reshape(-1, hop)
        spec = np.abs(np.fft.rfft(block * window, axis=1))
        for b, mask in enumerate(masks):
            energy[start:stop, b] = np.sqrt((spec[:, mask] ** 2).mean(axis=1))
    # Scale each band by its loud end so quiet bands still show shape.
    ref = np.percentile(energy, 99.5, axis=0)
    ref[ref <= 0] = 1
    scaled = np.clip(energy / ref * 230, 0, 255).astype(np.uint8)
    out.write_bytes(scaled.tobytes())


KEY_NAMES = ["C", "Db", "D", "Eb", "E", "F", "Gb", "G", "Ab", "A", "Bb", "B"]
# Camelot wheel numbers indexed by pitch class (A = minor, B = major).
CAMELOT_MAJOR = [8, 3, 10, 5, 12, 7, 2, 9, 4, 11, 6, 1]
CAMELOT_MINOR = [5, 12, 7, 2, 9, 4, 11, 6, 1, 8, 3, 10]
# EDMA key profiles (Faraldo et al.); on DJ-mix audio they beat
# Krumhansl-Kessler and Temperley in testing (11/23 vs 7/23 and 3/23).
EDMA_MAJOR = [0.1652, 0.0475, 0.0829, 0.0669, 0.0999, 0.0927, 0.0529, 0.1316, 0.0522, 0.0744, 0.0518, 0.0819]
EDMA_MINOR = [0.1724, 0.0400, 0.0761, 0.1216, 0.0548, 0.0876, 0.0501, 0.1334, 0.0783, 0.0544, 0.0699, 0.0635]


def analyze_segment(wav_path: Path, start: float, end: float) -> dict:
    """Tempo and musical key of one track as played in the mix.

    This is what the DJ played: tempo changes (and pitch, without key lock)
    can differ from the original release's metadata. Edges are trimmed so
    transitions from neighbouring tracks don't blur the measurement.
    """
    import librosa
    import numpy as np

    pad = min(10.0, (end - start) * 0.15)
    a, b = start + pad, end - pad
    if b - a > 90:
        mid = (a + b) / 2
        a, b = mid - 45, mid + 45
    if b - a < 8:
        return {}
    with wave.open(str(wav_path), "rb") as w:
        w.setpos(int(a * SAMPLE_RATE))
        raw = w.readframes(int((b - a) * SAMPLE_RATE))
    y = np.frombuffer(raw, dtype="<i2").astype(np.float32) / 32768

    onset = librosa.onset.onset_strength(y=y, sr=SAMPLE_RATE)
    tempo = float(np.atleast_1d(librosa.feature.tempo(onset_envelope=onset, sr=SAMPLE_RATE))[0])
    while tempo and tempo < 70:
        tempo *= 2
    while tempo > 180:
        tempo /= 2

    # Harmonic part only: drums smear the pitch-class profile.
    chroma = librosa.feature.chroma_cqt(y=librosa.effects.harmonic(y), sr=SAMPLE_RATE).mean(axis=1)
    best = (-2.0, 0, "major")
    for mode, profile in (("major", EDMA_MAJOR), ("minor", EDMA_MINOR)):
        for tonic in range(12):
            r = float(np.corrcoef(chroma, np.roll(profile, tonic))[0, 1])
            if r > best[0]:
                best = (r, tonic, mode)
    _, tonic, mode = best
    camelot = (CAMELOT_MAJOR if mode == "major" else CAMELOT_MINOR)[tonic]
    return {
        "bpm": round(tempo),
        "key": f"{KEY_NAMES[tonic]} {mode.title()}",
        "camelot": f"{camelot}{'B' if mode == 'major' else 'A'}",
    }


def wav_duration(wav_path: Path) -> float:
    with wave.open(str(wav_path), "rb") as w:
        return w.getnframes() / w.getframerate()


# ---------------------------------------------------------------- recognition

def _find_url(obj, needle: str):
    """First string inside a nested Shazam response that contains needle."""
    if isinstance(obj, str):
        return obj if needle in obj and obj.startswith("http") else None
    if isinstance(obj, dict):
        obj = obj.values()
    if isinstance(obj, (list, tuple, type({}.values()))):
        for v in obj:
            found = _find_url(v, needle)
            if found:
                return found
    return None


def parse_shazam(resp: dict) -> dict | None:
    track = resp.get("track")
    if not track:
        return None
    meta = {}
    for section in track.get("sections", []):
        for item in section.get("metadata", []) or []:
            meta[item.get("title", "").lower()] = item.get("text")
    matches = resp.get("matches") or [{}]
    return {
        "key": track.get("key"),
        "title": track.get("title"),
        "artist": track.get("subtitle"),
        "artwork_url": (track.get("images") or {}).get("coverart"),
        "shazam_url": track.get("url"),
        "isrc": track.get("isrc"),
        "album": meta.get("album"),
        "label": meta.get("label"),
        "release_date": meta.get("released"),
        "genre": (track.get("genres") or {}).get("primary"),
        "apple_music_url": _find_url(track.get("hub"), "music.apple.com"),
        "song_offset": matches[0].get("offset"),
    }


class Pacer:
    """Global request pacing for Shazam's unofficial endpoint.

    Shazam answers bursts with HTTP 429 and keeps an IP throttled for a while,
    so requests are spaced out and every worker pauses together on a 429.
    """

    def __init__(self):
        self.lock = asyncio.Lock()
        self.next_at = 0.0
        self.cooldown = 0

    async def wait(self):
        async with self.lock:
            delay = self.next_at - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            self.next_at = time.monotonic() + MIN_INTERVAL

    def ok(self):
        self.cooldown = 0

    def throttled(self) -> int:
        self.cooldown = min(max(self.cooldown * 2, 30), 300)
        self.next_at = time.monotonic() + self.cooldown
        return self.cooldown


def new_shazam() -> Shazam:
    # shazamio retries 429s 20 times with backoff by default, which looks like a
    # hang; surface them immediately and let Pacer handle backoff.
    return Shazam(http_client=HTTPClient(retry_options=ExponentialRetry(attempts=1, statuses=set())))


async def recognize_window(shazam: Shazam, pacer: Pacer, wav: Path, offset: int, on_wait):
    data = await asyncio.to_thread(wav_slice, wav, offset, WINDOW)
    for _ in range(8):
        await pacer.wait()
        try:
            resp = await asyncio.wait_for(shazam.recognize(data), 30)
            pacer.ok()
            return offset, parse_shazam(resp)
        except RateLimited:
            on_wait(pacer.throttled())
        except Exception:
            await asyncio.sleep(3)
    return offset, None


class AudDError(RuntimeError):
    pass


def _timecode(tc: str | None) -> float | None:
    try:
        parts = [int(p) for p in (tc or "").split(":")]
    except ValueError:
        return None
    return sum(v * 60 ** i for i, v in enumerate(reversed(parts))) if parts else None


def parse_audd(res: dict | None) -> dict | None:
    if not res:
        return None
    sp = res.get("spotify") or {}
    album = sp.get("album") or {}
    images = album.get("images") or []
    spotify_url = (sp.get("external_urls") or {}).get("spotify")
    return {
        "key": f"{res.get('artist')}|{res.get('title')}",
        "title": res.get("title"),
        "artist": res.get("artist"),
        "album": album.get("name") or res.get("album"),
        "album_type": album.get("album_type"),
        "label": res.get("label"),
        "release_date": album.get("release_date") or res.get("release_date"),
        "isrc": (sp.get("external_ids") or {}).get("isrc"),
        "artwork_url": images[1]["url"] if len(images) > 1 else images[0]["url"] if images else None,
        "spotify_url": spotify_url,
        "spotify_exact": bool(spotify_url),
        "song_offset": _timecode(res.get("timecode")),
    }


async def recognize_audd(client: httpx.AsyncClient, sem: asyncio.Semaphore, wav: Path, offset: int):
    async with sem:
        data = await asyncio.to_thread(wav_slice, wav, offset, WINDOW)
        for attempt in range(3):
            try:
                r = await client.post(
                    "https://api.audd.io/",
                    data={"api_token": AUDD_TOKEN, "return": "spotify"},
                    files={"file": ("sample.wav", data, "audio/wav")},
                )
                body = r.json()
            except Exception:
                await asyncio.sleep(2 * (attempt + 1))
                continue
            if body.get("status") == "error":
                err = body.get("error") or {}
                # 900/901/902: bad token or no requests left, fail the whole job.
                if err.get("error_code") in (900, 901, 902):
                    raise AudDError(f"AudD: {err.get('error_message')}")
                await asyncio.sleep(2 * (attempt + 1))
                continue
            return offset, parse_audd(body.get("result"))
        return offset, None


def group_runs(hits: dict[int, dict | None]) -> list[dict]:
    """Group consecutive per-window matches of the same song into runs."""
    def add(run, info, offset):
        run["hits"] += 1
        run["offsets"].append(offset)
        run["variants"].setdefault(info["key"], [0, info])[0] += 1

    runs: list[dict] = []
    for offset in sorted(hits):
        info = hits[offset]
        if not info:
            continue
        # Shazam often flips between releases of one song (remix, sped-up,
        # "bass boosted"); group by base title + primary artist instead of id.
        key = (_norm(info["title"]), _norm(re.split(r",|&|/| x ", info["artist"] or "")[0]))
        last = runs[-1] if runs else None
        if last and last["key"] == key and offset - last["last"] <= STEP * 4:
            last["last"] = offset
            add(last, info, offset)
            continue
        # A single stray hit between two runs of the same song is a misread:
        # drop it and keep extending the earlier run.
        if (len(runs) >= 2 and last["hits"] == 1 and runs[-2]["key"] == key
                and offset - runs[-2]["last"] <= STEP * 4):
            runs.pop()
            runs[-1]["last"] = offset
            add(runs[-1], info, offset)
            continue
        runs.append({"key": key, "first": offset, "last": offset, "hits": 0,
                     "offsets": [], "variants": {}})
        add(runs[-1], info, offset)
    for run in runs:
        # Show the release heard most often; on a tie prefer one with an exact
        # Spotify link, then the plainest title.
        run["info"] = max(run["variants"].values(),
                          key=lambda v: (v[0], bool(v[1].get("spotify_exact")),
                                         -len(v[1]["title"] or "")))[1]
    return runs


def same_song(a: dict, b: dict) -> bool:
    """Loose identity check between two engines' answers for one sample."""
    if _norm(a["title"]) != _norm(b["title"]):
        return False
    if ("remix" in (a["title"] or "").lower()) != ("remix" in (b["title"] or "").lower()):
        return False
    return bool(set(_norm(a["artist"]).split()) & set(_norm(b["artist"]).split()))


async def verify_with_shazam(hits: dict[int, dict | None], wav: Path, on_update) -> None:
    """Re-check weakly supported AudD runs with Shazam, which is slower (rate
    limited) but more accurate on DJ mixes. Shazam's answer wins on conflict."""
    runs = group_runs(hits)
    weak = sorted((r for r in runs if r["hits"] <= VERIFY_MAX_HITS), key=lambda r: r["hits"])
    queue = [(r["offsets"][len(r["offsets"]) // 2], r) for r in weak]
    if not queue:
        return
    shazam, pacer = new_shazam(), Pacer()
    done = 0
    try:
        while queue and done < VERIFY_BUDGET:
            offset, run = queue.pop(0)
            on_update(f"Double-checking with Shazam ({done + 1}/{min(len(queue) + 1 + done, VERIFY_BUDGET)})")
            _, info = await recognize_window(
                shazam, pacer, wav, offset,
                lambda s: on_update(f"Double-checking with Shazam (rate limited, pausing {s}s)"))
            done += 1
            current = hits.get(offset)
            if info is None:
                if run["hits"] == 1:
                    hits[offset] = None  # neither engine is sure: drop it
            elif current is None or not same_song(current, info):
                hits[offset] = info
                # The rest of this run is probably the same misread; check it next.
                queue[:0] = [(o, run) for o in run["offsets"] if o != offset]
            on_update(None)
    finally:
        await shazam.close()


def merge_hits(hits: dict[int, dict | None], duration: float | None = None) -> list[dict]:
    """Collapse per-window matches into a tracklist with start/end times."""
    runs = group_runs(hits)

    starts = []
    prev_heard = 0  # last moment the previous track was definitely playing
    for run in runs:
        start = run["first"]
        # Shazam reports where in the song our sample sits; use it to pull the
        # start back toward the real mix-in point, never into the previous track.
        song_off = run["info"].get("song_offset")
        if isinstance(song_off, (int, float)) and 0 < song_off <= STEP * 2:
            start = max(run["first"] - song_off, prev_heard, 0)
        starts.append(start)
        prev_heard = run["last"] + WINDOW

    tracks = []
    for i, run in enumerate(runs):
        info = run["info"]
        # Ranges are contiguous: a track ends where the next one is mixed in.
        if i + 1 < len(runs):
            end = starts[i + 1]
        else:
            end = duration or run["last"] + WINDOW
        track = {k: v for k, v in info.items() if k != "song_offset"}
        track.update(
            start=round(starts[i]),
            end=round(end),
            hits=run["hits"],
            confidence="high" if run["hits"] >= 3 else "medium" if run["hits"] == 2 else "low",
        )
        tracks.append(track)
    return tracks


# ---------------------------------------------------------------- enrichment

def _norm(s: str) -> str:
    s = re.sub(r"\(.*?\)|\[.*?\]|feat\..*|ft\..*", "", (s or "").lower())
    return re.sub(r"[^a-z0-9]+", " ", s).strip()


class Spotify:
    """Client-credentials Spotify Web API lookup (needs a free developer app)."""

    def __init__(self):
        self.client_id = os.getenv("SPOTIFY_CLIENT_ID", "").strip()
        self.secret = os.getenv("SPOTIFY_CLIENT_SECRET", "").strip()
        self.token = None
        self.expires = 0.0

    @property
    def enabled(self) -> bool:
        return bool(self.client_id and self.secret)

    async def _auth(self, client: httpx.AsyncClient):
        if self.token and time.time() < self.expires - 60:
            return
        r = await client.post("https://accounts.spotify.com/api/token",
                              data={"grant_type": "client_credentials"},
                              auth=(self.client_id, self.secret))
        r.raise_for_status()
        body = r.json()
        self.token, self.expires = body["access_token"], time.time() + body["expires_in"]

    async def _search(self, client: httpx.AsyncClient, q: str) -> list[dict]:
        await self._auth(client)
        for _ in range(3):
            r = await client.get("https://api.spotify.com/v1/search",
                                 params={"q": q, "type": "track", "limit": 5},
                                 headers={"Authorization": f"Bearer {self.token}"})
            if r.status_code == 429:
                await asyncio.sleep(int(r.headers.get("Retry-After", "2")) + 1)
                continue
            r.raise_for_status()
            return r.json()["tracks"]["items"]
        return []

    ALT_VERSION = re.compile(r"instrumental|karaoke|sped up|slowed|acapella|a cappella|"
                             r"\b8d\b|reverb|\blive\b|clean|remix|beat edition", re.I)

    async def find(self, client: httpx.AsyncClient, artist: str, title: str, isrc: str | None):
        """Canonical Spotify track: the best-ranked search hit that matches the
        song, skipping alternate versions (instrumental, sped up, remix...)
        unless the identified title itself is one. Spotify's ranking already
        favours the popular original release."""
        first_artist = re.split(r",|&|/| x | feat\.", artist or "", flags=re.I)[0].strip()
        wanted_alt = set(m.lower() for m in self.ALT_VERSION.findall(title or ""))
        items = await self._search(client, f"track:{_norm(title)} artist:{first_artist}")
        if not items:
            items = await self._search(client, f"{first_artist} {_norm(title)}")
        for it in items:
            if SequenceMatcher(None, _norm(title), _norm(it["name"])).ratio() < 0.8:
                continue
            if first_artist.lower() not in " ".join(a["name"] for a in it["artists"]).lower():
                continue
            alt = set(m.lower() for m in self.ALT_VERSION.findall(f"{it['name']} {it['album']['name']}"))
            if alt - wanted_alt or wanted_alt - alt:
                continue
            return it
        # Nothing clean by name: fall back to the exact recording if known.
        if isrc:
            items = await self._search(client, f"isrc:{isrc}")
            if items:
                return items[0]
        return None


SPOTIFY = Spotify()
GETSONGBPM_KEY = os.getenv("GETSONGBPM_API_KEY", "").strip()
_SHARP_TO_FLAT = {"C#": "Db", "D#": "Eb", "F#": "Gb", "G#": "Ab", "A#": "Bb"}


def parse_key(text: str | None) -> tuple[str, str] | None:
    """'E♭m' / 'Ebm' / 'F#' / 'Eb minor' -> ('Eb Minor', '2A')."""
    m = re.match(r"\s*([A-Ga-g])\s*([#♯b♭]?)\s*(maj(?:or)?|m(?:in(?:or)?)?)?", (text or ""), re.I)
    if not m:
        return None
    tonic = m.group(1).upper() + {"♯": "#", "♭": "b"}.get(m.group(2), m.group(2))
    tonic = _SHARP_TO_FLAT.get(tonic, tonic)
    tonic = {"Cb": "B", "Fb": "E", "E#": "F", "B#": "C"}.get(tonic, tonic)
    if tonic not in KEY_NAMES:
        return None
    minor = (m.group(3) or "").lower().startswith("m") and not (m.group(3) or "").lower().startswith("maj")
    idx = KEY_NAMES.index(tonic)
    camelot = (CAMELOT_MINOR if minor else CAMELOT_MAJOR)[idx]
    return f"{tonic} {'Minor' if minor else 'Major'}", f"{camelot}{'A' if minor else 'B'}"


async def songbpm_lookup(client: httpx.AsyncClient, artist: str, title: str) -> dict | None:
    """Original-release tempo and key from the GetSongBPM database."""
    first_artist = re.split(r",|&|/| x | feat\.", artist or "", flags=re.I)[0].strip()
    r = await client.get("https://api.getsong.co/search/", params={
        "api_key": GETSONGBPM_KEY, "type": "both", "lookup": f"song:{_norm(title)} artist:{first_artist}"})
    items = r.json().get("search")
    if not isinstance(items, list):  # {"search": {"error": "no result"}}
        return None
    for song in items:
        names = ((song.get("artist") or {}).get("name") or "").lower()
        if first_artist.lower() not in names and names not in first_artist.lower():
            continue
        if SequenceMatcher(None, _norm(title), _norm(song.get("title"))).ratio() < 0.8:
            continue
        if "tempo" not in song and song.get("id"):
            detail = await client.get("https://api.getsong.co/song/", params={
                "api_key": GETSONGBPM_KEY, "id": song["id"]})
            song = detail.json().get("song") or song
        key = parse_key(song.get("key_of"))
        try:
            bpm = round(float(song.get("tempo")))
        except (TypeError, ValueError):
            bpm = None
        if not (bpm or key):
            return None
        out = {"bpm": bpm, "bpm_source": "GetSongBPM"}
        if key:
            out["key"], out["camelot"] = key
        return out
    return None
ALT_RELEASE = re.compile(r"edition|\bhits\b|\bvol\.|compilation|collection|best of|playlist", re.I)


async def enrich(client: httpx.AsyncClient, track: dict) -> dict:
    if not track.get("spotify_exact"):
        q = quote_plus(f"{track['artist']} {track['title']}")
        track["spotify_url"] = f"https://open.spotify.com/search/{q}"
        track["spotify_exact"] = False
    track.setdefault("apple_music_url", None)
    try:
        q = f'artist:"{track["artist"]}" track:"{track["title"]}"'
        r = await client.get("https://api.deezer.com/search", params={"q": q, "limit": 1})
        items = r.json().get("data") or []
        if not items:
            r = await client.get("https://api.deezer.com/search",
                                 params={"q": f"{track['artist']} {track['title']}", "limit": 1})
            items = r.json().get("data") or []
        if items:
            hit = items[0]
            track["deezer_url"] = hit.get("link")
            track["preview_url"] = hit.get("preview")
            track["album"] = track.get("album") or (hit.get("album") or {}).get("title")
            track["artwork_url"] = track.get("artwork_url") or (hit.get("album") or {}).get("cover_medium")
            detail = (await client.get(f"https://api.deezer.com/track/{hit['id']}")).json()
            track["isrc"] = track.get("isrc") or detail.get("isrc")
            track["release_date"] = track.get("release_date") or detail.get("release_date")
            track["label"] = track.get("label") or (detail.get("album") or {}).get("label")
    except Exception:
        pass
    if GETSONGBPM_KEY:
        try:
            db = await songbpm_lookup(client, track["artist"], track["title"])
            if db:
                # Keep the in-mix measurement alongside the release values.
                for k in ("bpm", "key", "camelot"):
                    if track.get(k) is not None:
                        track[f"played_{k}"] = track[k]
                track.update({k: v for k, v in db.items() if v is not None})
        except Exception:
            pass
    # AudD's own Spotify link is usually the right release, but it sometimes
    # points at a deluxe "Edition" or a compilation; look those up again.
    off_release = track.get("album_type") == "compilation" or bool(ALT_RELEASE.search(track.get("album") or ""))
    if SPOTIFY.enabled and (not track["spotify_exact"] or off_release):
        try:
            hit = await SPOTIFY.find(client, track["artist"], track["title"], track.get("isrc"))
            if hit:
                track["spotify_url"] = hit["external_urls"]["spotify"]
                track["spotify_exact"] = True
                if off_release or not track.get("album"):
                    track["album"] = hit["album"]["name"]
                    track["album_type"] = hit["album"]["album_type"]
                    track["release_date"] = hit["album"].get("release_date")
        except Exception:
            pass
    return track


# ---------------------------------------------------------------- job runner

_job_slot = asyncio.Semaphore(1)  # one mix at a time keeps Shazam rate limits sane


async def process_job(job_id: str, url: str | None, uploaded: Path | None):
    async with _job_slot:
        wav = None
        try:
            update_job(job_id, status="downloading", message="Reading link")
            if url:
                meta = await asyncio.to_thread(fetch_metadata, url)
                update_job(
                    job_id,
                    title=meta.get("title"),
                    uploader=meta.get("uploader") or meta.get("channel"),
                    duration=meta.get("duration"),
                    thumbnail=meta.get("thumbnail"),
                    webpage_url=meta.get("webpage_url") or url,
                )
                src = await asyncio.to_thread(download_audio, job_id, url)
            else:
                src = uploaded
            update_job(job_id, audio_file=str(src), status="decoding", progress=25,
                       message="Decoding audio")
            wav = await asyncio.to_thread(to_wav, src)
            duration = wav_duration(wav)
            update_job(job_id, duration=duration)
            wave_file = WORK / f"{job_id}.wave"
            if not wave_file.exists():
                await asyncio.to_thread(build_waveform, wav, wave_file)

            offsets = list(range(0, max(int(duration) - 5, 1), STEP))
            update_job(job_id, status="identifying", progress=30,
                       message=f"Identifying tracks (0/{len(offsets)} samples)")
            hits: dict[int, dict | None] = {}

            def record(offset, info):
                hits[offset] = info
                update_job(
                    job_id,
                    progress=round(30 + 60 * len(hits) / len(offsets), 1),
                    message=f"Identifying tracks ({len(hits)}/{len(offsets)} samples)",
                    tracks=merge_hits(hits, duration),
                )

            audd_cache = WORK / f"{job_id}.audd.json"
            if AUDD_TOKEN and audd_cache.exists():
                # Re-runs reuse paid AudD results and only redo the Shazam check.
                cached = json.loads(audd_cache.read_text(encoding="utf-8"))
                for offset in offsets:
                    record(offset, cached.get(str(offset)))
            if AUDD_TOKEN:
                missing = [o for o in offsets if o not in hits]
                async with httpx.AsyncClient(timeout=60) as client:
                    sem = asyncio.Semaphore(AUDD_CONCURRENCY)
                    tasks = [asyncio.create_task(recognize_audd(client, sem, wav, o)) for o in missing]
                    try:
                        for fut in asyncio.as_completed(tasks):
                            record(*await fut)
                    except Exception:
                        for t in tasks:
                            t.cancel()
                        raise
                audd_cache.write_text(json.dumps({str(k): v for k, v in hits.items()}), encoding="utf-8")

                if VERIFY_BUDGET:
                    def on_update(message):
                        fields = {"tracks": merge_hits(hits, duration)}
                        if message:
                            fields["message"] = message
                        update_job(job_id, **fields)

                    update_job(job_id, status="verifying", progress=90)
                    await verify_with_shazam(hits, wav, on_update)
            else:
                shazam = new_shazam()
                pacer = Pacer()

                def on_wait(seconds):
                    update_job(job_id, message=f"Shazam rate limit hit, pausing {seconds}s "
                                               f"({len(hits)}/{len(offsets)} samples done)")

                try:
                    for offset in offsets:
                        record(*await recognize_window(shazam, pacer, wav, offset, on_wait))
                finally:
                    await shazam.close()

            tracks = merge_hits(hits, duration)
            update_job(job_id, status="analyzing", progress=91, message="Measuring BPM and key")
            for t in tracks:
                t.update(await asyncio.to_thread(analyze_segment, wav, t["start"], t["end"]))
            update_job(job_id, status="enriching", progress=95, message="Finding streaming links")
            async with httpx.AsyncClient(timeout=10) as client:
                sem_e = asyncio.Semaphore(4)

                async def guarded(t):
                    async with sem_e:
                        return await enrich(client, t)

                tracks = await asyncio.gather(*(guarded(t) for t in tracks))
            update_job(job_id, status="completed", progress=100, tracks=list(tracks),
                       message=f"Found {len(tracks)} tracks")
        except Exception as e:  # surfaced to the UI
            update_job(job_id, status="failed", error=str(e)[:500], message="Failed")
        finally:
            if wav and wav.exists():
                wav.unlink()


# ---------------------------------------------------------------- API

app = FastAPI(title="Tracklister")


@app.middleware("http")
async def no_stale_frontend(request, call_next):
    # The UI changes often while developing locally; always revalidate it.
    response = await call_next(request)
    if not request.url.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-cache"
    return response


@app.on_event("startup")
def fail_interrupted_jobs():
    # Jobs cut off by a restart can never finish.
    with _db_lock:
        _db.execute(
            "UPDATE jobs SET status='failed', error='Interrupted by server restart' "
            "WHERE status NOT IN ('completed','failed')"
        )
        _db.commit()


class JobIn(BaseModel):
    url: str


@app.post("/api/jobs")
async def submit(body: JobIn):
    url = body.url.strip()
    if not re.match(r"^https?://", url):
        raise HTTPException(400, "Paste a full http(s) link to a mix")
    job_id = create_job(url)
    asyncio.create_task(process_job(job_id, url, None))
    return {"job_id": job_id}


@app.post("/api/jobs/upload")
async def submit_upload(file: UploadFile = File(...)):
    job_id = create_job(file.filename or "upload")
    ext = Path(file.filename or "").suffix or ".audio"
    dst = WORK / f"{job_id}{ext}"
    with dst.open("wb") as f:
        while chunk := await file.read(1 << 20):
            f.write(chunk)
    update_job(job_id, title=Path(file.filename or "Upload").stem, uploader="Local file")
    asyncio.create_task(process_job(job_id, None, dst))
    return {"job_id": job_id}


@app.get("/api/jobs")
def history(q: str = "", saved: bool = False, page: int = 1, per_page: int = 30):
    sql = "SELECT * FROM jobs WHERE 1=1"
    args: list = []
    if q:
        sql += " AND (title LIKE ? OR uploader LIKE ? OR source LIKE ? OR tracks LIKE ?)"
        args += [f"%{q}%"] * 4
    if saved:
        sql += " AND saved=1"
    sql += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
    args += [per_page, (page - 1) * per_page]
    with _db_lock:
        rows = _db.execute(sql, args).fetchall()
    out = []
    for r in rows:
        d = job_dict(r)
        d["track_count"] = len(d.pop("tracks"))
        out.append(d)
    return out


@app.get("/api/jobs/{job_id}")
def read_job(job_id: str):
    job = get_job(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    return job


@app.get("/api/jobs/{job_id}/stream")
async def stream(job_id: str):
    async def events():
        last = None
        while True:
            job = get_job(job_id)
            if not job:
                yield "event: gone\ndata: {}\n\n"
                return
            payload = json.dumps(job)
            if payload != last:
                yield f"data: {payload}\n\n"
                last = payload
            if job["status"] in ("completed", "failed"):
                return
            await asyncio.sleep(1)

    return StreamingResponse(events(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache"})


@app.post("/api/jobs/{job_id}/save")
def toggle_save(job_id: str):
    job = get_job(job_id) or {}
    if not job:
        raise HTTPException(404, "Job not found")
    update_job(job_id, saved=0 if job["saved"] else 1)
    return {"saved": not job["saved"]}


@app.post("/api/jobs/{job_id}/retry")
async def retry(job_id: str):
    with _db_lock:
        row = _db.execute("SELECT source, audio_file, status FROM jobs WHERE id=?", (job_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Job not found")
    if row["status"] not in ("completed", "failed"):
        raise HTTPException(409, "Job is still running")
    stored = Path(row["audio_file"]) if row["audio_file"] else None
    # Reuse audio already on disk so a retry doesn't download the mix again.
    if not stored or not stored.exists():
        stored = next((p for p in WORK.glob(f"{job_id}.*")
                       if p.suffix not in (".wav", ".part", ".json", ".wave")), None)
    url = None if stored else row["source"]
    if not stored and not re.match(r"^https?://", url or ""):
        raise HTTPException(400, "Original audio is gone; upload the file again")
    update_job(job_id, status="queued", progress=0, error=None, tracks=[], message="Waiting in queue")
    asyncio.create_task(process_job(job_id, url, stored))
    return {"job_id": job_id}


@app.delete("/api/jobs/{job_id}")
def delete_job(job_id: str):
    for p in WORK.glob(f"{job_id}*"):
        p.unlink(missing_ok=True)
    with _db_lock:
        _db.execute("DELETE FROM jobs WHERE id=?", (job_id,))
        _db.commit()
    return {"ok": True}


@app.get("/api/jobs/{job_id}/audio")
def audio(job_id: str):
    with _db_lock:
        row = _db.execute("SELECT audio_file FROM jobs WHERE id=?", (job_id,)).fetchone()
    if not row or not row[0] or not Path(row[0]).exists():
        raise HTTPException(404, "No audio stored")
    return FileResponse(row[0])


_wave_locks: dict[str, asyncio.Lock] = {}


@app.get("/api/jobs/{job_id}/waveform")
async def waveform(job_id: str):
    out = WORK / f"{job_id}.wave"
    if not out.exists():
        # Jobs from before waveforms existed: build from the stored audio once.
        async with _wave_locks.setdefault(job_id, asyncio.Lock()):
            if not out.exists():
                with _db_lock:
                    row = _db.execute("SELECT audio_file FROM jobs WHERE id=?", (job_id,)).fetchone()
                if not row or not row[0] or not Path(row[0]).exists():
                    raise HTTPException(404, "No audio stored for this job")
                wav = await asyncio.to_thread(to_wav, Path(row[0]))
                try:
                    await asyncio.to_thread(build_waveform, wav, out)
                finally:
                    wav.unlink(missing_ok=True)
    return FileResponse(out, media_type="application/octet-stream",
                        headers={"X-Columns-Per-Second": str(WAVE_CPS), "Cache-Control": "max-age=86400"})


def fmt_ts(sec: float) -> str:
    sec = int(sec)
    h, m, s = sec // 3600, sec % 3600 // 60, sec % 60
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def track_text(t: dict) -> str:
    lines = [f"{fmt_ts(t['start'])}-{fmt_ts(t['end'])} - {t['artist']} - {t['title']}"]
    year = (t.get("release_date") or "")[:4]
    if t.get("album"):
        lines.append(f"  Album: {t['album']}" + (f" ({year})" if year else ""))
    if t.get("label"):
        lines.append(f"  Label: {t['label']}")
    music = [f"{round(t['bpm'])} BPM" if t.get("bpm") else None, t.get("key"),
             f"[{t['camelot']}]" if t.get("camelot") else None]
    if any(music):
        lines.append("  " + " / ".join(m for m in music if m))
    if t.get("spotify_url"):
        lines.append(f"  Spotify: {t['spotify_url']}" + ("" if t.get("spotify_exact") else "  (search)"))
    return "\n".join(lines)


@app.get("/api/jobs/{job_id}/export.{fmt}")
def export(job_id: str, fmt: str):
    job = get_job(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    if fmt == "txt":
        return PlainTextResponse("\n".join(track_text(t) for t in job["tracks"]))
    if fmt == "csv":
        buf = io.StringIO()
        cols = ["start", "end", "artist", "title", "album", "label", "release_date", "bpm",
                "key", "camelot", "isrc", "confidence", "spotify_url", "spotify_exact"]
        w = csv.DictWriter(buf, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for t in job["tracks"]:
            w.writerow({**t, "start": fmt_ts(t["start"]), "end": fmt_ts(t["end"])})
        return PlainTextResponse(buf.getvalue(), media_type="text/csv", headers={
            "Content-Disposition": f'attachment; filename="tracklist-{job_id}.csv"'})
    raise HTTPException(400, "Format must be txt or csv")


app.mount("/", StaticFiles(directory=ROOT / "static", html=True), name="static")
