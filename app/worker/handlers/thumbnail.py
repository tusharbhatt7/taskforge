import asyncio
import io

import httpx
from PIL import Image

from app.worker.handlers import JobContext, handler
from app.worker.handlers.http_fetch import _assert_public_host

MAX_SOURCE_BYTES = 10 * 1024 * 1024


@handler("thumbnail")
async def thumbnail(payload: dict, ctx: JobContext) -> dict:
    """CPU-ish demo workload: download an image and resize it. In a real deployment the
    output would go to object storage; here we report dimensions and byte sizes."""
    width = min(int(payload.get("width", 128)), 1024)
    height = min(int(payload.get("height", 128)), 1024)

    if url := payload.get("image_url"):
        _assert_public_host(url)
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            response = await client.get(url)
            response.raise_for_status()
        if len(response.content) > MAX_SOURCE_BYTES:
            raise ValueError("source image exceeds 10MB limit")
        source_bytes = response.content
    else:
        # No URL given: synthesize a gradient so the job is self-contained.
        img = Image.new("RGB", (512, 512))
        img.putdata([(x % 256, y % 256, (x + y) % 256) for y in range(512) for x in range(512)])
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        source_bytes = buf.getvalue()

    def _resize() -> tuple[int, int, int]:
        img = Image.open(io.BytesIO(source_bytes))
        img.thumbnail((width, height))
        out = io.BytesIO()
        img.convert("RGB").save(out, format="JPEG", quality=85)
        return img.width, img.height, out.getbuffer().nbytes

    # Pillow is synchronous; run it off the event loop so it can't stall the worker.
    final_w, final_h, out_size = await asyncio.to_thread(_resize)
    return {
        "source_bytes": len(source_bytes),
        "thumbnail_bytes": out_size,
        "width": final_w,
        "height": final_h,
    }
