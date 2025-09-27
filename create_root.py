# create_tree.py
from pathlib import Path

def create_tree(root="."):
    root = Path(root)

    # Directorios
    dirs = [
        "data/raw/abastecimientos",
        "data/raw/actividades",
        "data/raw/insumos",
        "data/raw/rep_maquinaria",
        "data/processed",
        "config",
        "src/etl_sap/pipelines",
        "scripts",
        "tests",
    ]
    for d in dirs:
        (root / d).mkdir(parents=True, exist_ok=True)

    # Archivos
    files = [
        "pyproject.toml",
        "README.md",
        ".env.example",
        "config/settings.yaml",
        "src/etl_sap/__init__.py",
        "src/etl_sap/loaders.py",
        "src/etl_sap/transforms.py",
        "src/etl_sap/pipelines/abastecimientos.py",
        "src/etl_sap/pipelines/actividades.py",
        "src/etl_sap/pipelines/insumos.py",
        "src/etl_sap/pipelines/rep_maquinaria.py",
        "scripts/run_abastecimientos.py",
        "scripts/run_all.py",
        "tests/test_transforms.py",
        "tests/test_pipelines.py",
    ]
    for f in files:
        p = root / f
        p.parent.mkdir(parents=True, exist_ok=True)
        p.touch(exist_ok=True)

if __name__ == "__main__":
    create_tree(".")
