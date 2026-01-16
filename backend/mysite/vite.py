import json
from pathlib import Path
from django.conf import settings

def get_vite_assets(entry="index.html"):
    manifest_path = Path(settings.BASE_DIR) / "static" / "frontend" / ".vite" / "manifest.json"
    data = json.loads(manifest_path.read_text(encoding="utf-8"))

    chunk = data[entry]
    js = f"frontend/{chunk['file']}"
    css = None
    if chunk.get("css"):
        css = f"frontend/{chunk['css'][0]}"
    return js, css
