"""WXDispatch product identity with legacy runtime compatibility."""

from pathlib import Path
import tomllib

from app import __version__
from app.config import APP_DIRNAME, GITHUB_REPO
from app.main import create_app


ROOT = Path(__file__).resolve().parents[1]


def test_product_identity_is_wxdispatch_2():
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())

    assert __version__ == "2.1.0"
    assert project["project"]["name"] == "wxdispatch"
    assert project["project"]["version"] == "2.1.0"
    assert project["project"]["urls"]["Repository"] == \
        "https://github.com/fizzlepoof/WXDispatch"
    assert GITHUB_REPO == "fizzlepoof/WXDispatch"
    app = create_app()
    assert app.title == "WXDispatch"
    assert app.version == "2.1.0"


def test_web_ui_uses_new_product_name():
    base = (ROOT / "app/web/templates/base.html").read_text()
    settings = (ROOT / "app/web/templates/settings.html").read_text()
    routes = (ROOT / "app/web/routes.py").read_text()

    assert "WXDispatch" in base
    assert "WXDispatch version" in settings
    assert "<title>mesh-wx" not in base
    assert '"mesh-wx/1.0' not in routes
    assert '"WXDispatch/%s' in routes
    assert not (ROOT / "docs/dashboard.png").exists()
    assert not (ROOT / "docs/settings-1.png").exists()
    assert not (ROOT / "docs/settings-2.png").exists()


def test_release_artifact_uses_new_product_name():
    workflow = (ROOT / ".github/workflows/release.yml").read_text()
    spec = (ROOT / "packaging/meshwx.spec").read_text()
    dockerfile = (ROOT / "Dockerfile").read_text()

    assert "WXDispatch-windows-${{ github.ref_name }}.zip" in workflow
    assert "ghcr.io/${{ github.repository }}" in workflow
    assert "ghcr.io/fizzlepoof/meshwx" in workflow
    assert "ghcr.io/fizzlepoof/wxdispatch:latest" in (ROOT / "README.md").read_text()
    assert 'name="WXDispatch"' in spec
    assert '(os.path.join(ROOT, "LICENSE"), ".")' in spec
    assert "COPY LICENSE /app/LICENSE" in dockerfile


def test_legacy_runtime_identifiers_remain_for_upgrades():
    installer = (ROOT / "install.sh").read_text()
    service = (ROOT / "packaging/mesh-wx.service").read_text()

    assert APP_DIRNAME == "MeshWX"
    assert '${MESHWX_DIR:-/opt/MeshWX}' in installer
    assert "mesh-wx.service" in (ROOT / "packaging/install-linux.sh").read_text()
    assert "MESH_WX_DB" in service
    assert (ROOT / "docker-compose.yml").read_text().count("mesh-wx") >= 2


def test_original_mit_license_and_attribution_are_preserved():
    license_text = (ROOT / "LICENSE").read_text()
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
    readme = (ROOT / "README.md").read_text()

    assert license_text.startswith("MIT License\n")
    assert "Copyright (c) 2026 BrokenSignal" in license_text
    assert "included in all copies or substantial portions" in " ".join(license_text.split())
    assert project["license"]["text"] == "MIT"
    assert project["authors"] == [{"name": "BrokenSignal"}]
    assert project["maintainers"] == [{"name": "fizzlepoof"}]
    assert "MeshWX by BrokenSignal" in readme