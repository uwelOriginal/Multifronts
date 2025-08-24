from pathlib import Path

def resolve_data_dir():
    candidates = [
        Path(__file__).parent.parent / "data",
        Path(__file__).parent.parent / "inventory_mvp" / "data",
        Path.cwd() / "data",
        Path("/mnt/data/inventory_mvp/data"),
    ]
    for p in candidates:
        if p.exists():
            return p
    # por defecto el primero (se creará si hace falta)
    return candidates[0]
