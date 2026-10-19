"""Generate the hosted READ-ONLY dashboard page from the local one (web/index.html), so there is one source of truth.

    .venv/bin/python scripts/build_hosted_dashboard.py

Changes made: control buttons and the control token are removed; the WebSocket is replaced by polling /api/snapshot
(every 5 s while the tab is visible); a READ-ONLY badge, a "last update" indicator and an offline/stale banner are added.
Every transformation is asserted, so a future edit to web/index.html that breaks one fails loudly instead of silently.
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "web" / "index.html"
DST = ROOT / "vercel-dashboard" / "views" / "dashboard.html"

POLLING_JS = r"""
let lastOk = 0, offlineMsg = "";
const STALE_AFTER_S = 45;

function showBanner(text, error) {
  const b = $("banner");
  b.className = "banner show" + (error ? " err" : "");
  b.textContent = text;
}

function applyAge(age) {
  const upd = $("updated");
  upd.textContent = "updated " + dur(age) + " ago";
  if (age > STALE_AFTER_S) {
    $("dot").className = "dot bad";
    showBanner(`Last update ${dur(age)} ago. The bot may be stopped, offline or the computer asleep, so the figures below are old.`, true);
  }
}

async function poll() {
  try {
    // Built from the origin only: a page opened as https://user:pass@host/ would otherwise make fetch() throw.
    const r = await fetch(location.origin + "/api/snapshot", { cache: "no-store", credentials: "same-origin" });
    if (r.status === 401) { location.reload(); return; }
    const data = await r.json();
    if (!r.ok) throw new Error(data.error || ("HTTP " + r.status));
    if (data.offline || !data.snapshot) {
      offlineMsg = "The bot is not publishing (stopped, offline or asleep).";
      $("dot").className = "dot bad"; $("feedtxt").textContent = "bot offline"; $("updated").textContent = "no data";
      showBanner(offlineMsg + (snap ? " Showing the last data received." : ""), true);
      return;
    }
    snap = data.snapshot;
    lastOk = Date.now();
    renderAll();
    applyAge(Math.max(0, data.server_time - snap.published_at));
  } catch (e) {
    $("dot").className = "dot bad";
    showBanner("Cannot reach the dashboard service: " + e.message, true);
  }
}

renderHead();
poll();
setInterval(() => { if (!document.hidden) poll(); }, 5000);
document.addEventListener("visibilitychange", () => { if (!document.hidden) poll(); });
"""


def must_sub(pattern: str, repl: str, text: str, flags: int = re.S) -> str:
    new, n = re.subn(pattern, lambda _m: repl, text, count=1, flags=flags)
    if n != 1:
        sys.exit(f"build failed: could not find {pattern[:60]!r} in {SRC.name} (did the page change?)")
    return new


def main() -> None:
    html = SRC.read_text(encoding="utf-8")
    # 1. header: control buttons -> read-only badges
    html = must_sub(
        r'<div class="hdr-right">.*?</div>\s*</header>',
        '<div class="hdr-right"><span class="badge" title="This page has no controls">READ-ONLY</span>'
        '<span class="badge" id="updated">connecting…</span></div>\n  </header>', html)
    # 2. no control token on the hosted page
    html = must_sub(r'const TOKEN = "__TOKEN__";\n', "", html)
    # 3. renderAll: drop the button state lines
    html = must_sub(r'  const pause = \$\("btn-pause"\);.*?\$\("btn-flat"\)\.disabled = s\.open === 0;\n', "", html)
    # 4. transport: control actions + WebSocket -> polling
    html = must_sub(r"async function act\(name\).*?</script>", POLLING_JS + "</script>", html)
    # 5. hosted page should never be indexed
    html = must_sub(r"<title>", '<meta name="robots" content="noindex, nofollow">\n<title>', html)
    for forbidden in ("btn-pause", "btn-flat", "WebSocket(", "__TOKEN__", "X-Token", "/api/halt", "/api/flatten"):
        if forbidden in html:
            sys.exit(f"build failed: {forbidden!r} is still present in the hosted page")
    if "/api/snapshot" not in html:
        sys.exit("build failed: the polling code was not inserted")
    DST.parent.mkdir(parents=True, exist_ok=True)
    DST.write_text(html, encoding="utf-8")
    print(f"wrote {DST.relative_to(ROOT)} ({len(html):,} bytes)")


if __name__ == "__main__":
    main()
