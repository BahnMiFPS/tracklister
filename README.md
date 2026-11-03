# Tracklister

Identify every track in a DJ mix, locally. Paste a YouTube / SoundCloud / Mixcloud link
(or drop an audio file) and get a timestamped tracklist with Spotify links, BPM and key,
a clickable 3-band waveform, and a YouTube mini-player that jumps to each track.

## How it works

1. **Download & decode**: `yt-dlp` fetches the audio, `ffmpeg` converts it to 16 kHz mono.
2. **Identify**: a 12 s sample every 30 s goes to AudD; samples backed by weak evidence are
   double-checked with Shazam, whose answer wins on conflict.
3. **Merge**: consecutive matches become tracks with contiguous start/end times.
4. **Enrich**: Spotify Web API (canonical release), Deezer (album/label),
   GetSongBPM (original BPM + key), plus BPM/key measured from the mix itself.
5. **Waveform**: low/mid/high band energy per 0.1 s, drawn on a canvas.

## Run

```
start.bat
```

Then open http://127.0.0.1:8765. Copy `.env.example` to `.env` and add your API keys first.

## Credits

BPM & key data by [GetSongBPM](https://getsongbpm.com).
Track recognition by [AudD](https://audd.io) and Shazam. Links via the Spotify Web API and Deezer.
