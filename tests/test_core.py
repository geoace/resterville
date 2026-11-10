"""Unit tests for RESTerville. Run from the repo root: API_KEY=test python -m pytest tests"""
import os
from unittest import mock

import pytest

os.environ.setdefault('API_KEY', 'test-key-0123456789abcdef')

from lib import security  # noqa: E402
from lib.esri_to_geojson import esri_to_geojson  # noqa: E402
from lib import agol_to_pg  # noqa: E402
import app as app_module  # noqa: E402

KEY = os.environ['API_KEY']

# Esri winding: outer rings clockwise, holes counter-clockwise.
OUTER = [[0, 0], [0, 10], [10, 10], [10, 0], [0, 0]]
HOLE = [[2, 2], [4, 2], [4, 4], [2, 4], [2, 2]]
OUTER2 = [[20, 0], [20, 5], [25, 5], [25, 0], [20, 0]]


def _geom(g):
    return esri_to_geojson({"features": [{"attributes": {}, "geometry": g}]})["features"][0]["geometry"]


# --- esri_to_geojson -------------------------------------------------------

def test_polygon_with_hole_stays_one_polygon():
    g = _geom({"rings": [OUTER, HOLE]})
    assert g["type"] == "Polygon"
    assert g["coordinates"] == [OUTER, HOLE]


def test_two_shells_one_hole_is_multipolygon():
    g = _geom({"rings": [OUTER, OUTER2, HOLE]})
    assert g["type"] == "MultiPolygon"
    assert g["coordinates"] == [[OUTER, HOLE], [OUTER2]]


def test_null_and_empty_geometry_become_null():
    assert _geom(None) is None
    assert _geom({}) is None
    assert _geom({"rings": []}) is None
    assert _geom({"x": None, "y": None}) is None


def test_point_and_lines():
    assert _geom({"x": 1, "y": 2}) == {"type": "Point", "coordinates": [1, 2]}
    assert _geom({"paths": [[[0, 0], [1, 1]]]})["type"] == "LineString"
    assert _geom({"paths": [[[0, 0], [1, 1]], [[2, 2], [3, 3]]]})["type"] == "MultiLineString"


# --- security --------------------------------------------------------------

@pytest.mark.parametrize("bad", ["", "Public", "a;drop", "1abc", "a b", "x" * 64, None])
def test_identifier_rejects(bad):
    with pytest.raises(security.ValidationError):
        security.identifier(bad, "table")


def test_identifier_accepts():
    assert security.identifier("utl_stormline", "table") == "utl_stormline"


@pytest.mark.parametrize("bad", ["coz postgres", "a=b", "x' host=evil", ""])
def test_service_rejects(bad):
    with pytest.raises(security.ValidationError):
        security.service_name(bad)


def test_layer_url():
    assert security.layer_url("https://services9.arcgis.com/x/FeatureServer/2/?f=json") == \
        "https://services9.arcgis.com/x/FeatureServer/2"
    with pytest.raises(security.ValidationError):
        security.layer_url("http://services9.arcgis.com/x/FeatureServer/2")
    with pytest.raises(security.ValidationError):
        security.layer_url("file:///etc/passwd")


def test_token_allowed(monkeypatch):
    monkeypatch.setenv("ARCGIS_PORTAL_URL", "https://gis.example.org/portal")
    monkeypatch.setenv("ARCGIS_TOKEN_HOSTS", "wa.example.org")
    assert security.token_allowed("https://services9.arcgis.com/a/FeatureServer/0")
    assert security.token_allowed("https://gis.example.org/server/rest/services/x")
    assert security.token_allowed("https://wa.example.org/x")
    assert not security.token_allowed("https://evil.example.com/arcgis.com/")
    assert not security.token_allowed("https://arcgis.com.evil.net/x")
    assert not security.token_allowed("http://services9.arcgis.com/a")


# --- app -------------------------------------------------------------------

@pytest.fixture
def client():
    app_module.app.config['TESTING'] = True
    return app_module.app.test_client()


def test_api_key_required(client):
    assert client.get('/').status_code == 403
    assert client.get('/?api_key=wrong').status_code == 403
    assert client.get(f'/?api_key={KEY}').status_code == 200
    assert client.get('/', headers={'X-API-Key': KEY}).status_code == 200


def test_api_key_not_logged(client, caplog):
    caplog.set_level('DEBUG')
    client.get(f'/?api_key={KEY}')
    assert KEY not in caplog.text


def test_agol2pg_rejects_bad_table(client):
    r = client.get('/agol2pg', headers={'X-API-Key': KEY}, query_string={
        'service': 'coz_postgres', 'url': 'https://services9.arcgis.com/x/FeatureServer/0',
        'table': 'x; DROP TABLE y'})
    assert r.status_code == 400
    assert b'Invalid table' in r.data


METADATA = {"geometryType": "esriGeometryPolygon", "objectIdField": "OBJECTID",
            "globalIdField": "GlobalID", "fields": [{"name": "OBJECTID"}, {"name": "GlobalID"}],
            "extent": {"spatialReference": {"wkid": 102100, "latestWkid": 3857}}}


def _run_agol2pg(client, **params):
    base = {'service': 'coz_postgres', 'url': 'https://services9.arcgis.com/x/FeatureServer/0',
            'table': 'utl_stormline', 'oid': 'id'}
    base.update(params)
    return client.get('/agol2pg', headers={'X-API-Key': KEY}, query_string=base)


def test_save_attachments_false_skips_attachments(client):
    with mock.patch.object(agol_to_pg, '_fetch_metadata', return_value=METADATA), \
         mock.patch.object(agol_to_pg, 'connect'), \
         mock.patch.object(agol_to_pg, 'download_features', return_value=iter(['\nfeatures ok'])), \
         mock.patch.object(agol_to_pg, 'download_attachments') as dl_att:
        r = _run_agol2pg(client, save_attachments='FALSE')
        body = r.get_data(as_text=True)
    dl_att.assert_not_called()
    assert 'Skipping attachments' in body
    assert body.rstrip().endswith('__EXIT_CODE__=0')


def test_failure_reports_exit_code_1(client):
    def boom(**_):
        raise RuntimeError("ogr2ogr exited with status 1: boom")
        yield  # pragma: no cover

    with mock.patch.object(agol_to_pg, '_fetch_metadata', return_value=METADATA), \
         mock.patch.object(agol_to_pg, 'connect'), \
         mock.patch.object(agol_to_pg, 'download_features', side_effect=boom):
        body = _run_agol2pg(client).get_data(as_text=True)
    assert 'boom' in body
    assert body.rstrip().endswith('__EXIT_CODE__=1')


def test_oid_collision_is_400(client):
    meta = dict(METADATA, fields=[{"name": "ID"}])
    with mock.patch.object(agol_to_pg, '_fetch_metadata', return_value=meta):
        r = _run_agol2pg(client)
    assert r.status_code == 400


# --- agol_to_pg internals --------------------------------------------------

class _Resp:
    def __init__(self, payload, status=200):
        self._payload, self.status_code, self.text = payload, status, str(payload)

    def json(self):
        return self._payload


def test_token_retry_returns_retried_response(monkeypatch):
    monkeypatch.setattr(agol_to_pg, '_get_token_cached', lambda: 'TOKEN')
    with mock.patch.object(agol_to_pg.requests, 'get', return_value=_Resp({"error": {"code": 499}})), \
         mock.patch.object(agol_to_pg.requests, 'post', return_value=_Resp({"features": []})) as post:
        data = agol_to_pg.arcgis_request('https://services9.arcgis.com/x/FeatureServer/0/query', {})
    assert data == {"features": []}
    assert post.call_args.kwargs['data']['token'] == 'TOKEN'


def test_token_not_sent_to_foreign_host(monkeypatch):
    monkeypatch.delenv('ARCGIS_PORTAL_URL', raising=False)
    monkeypatch.setattr(agol_to_pg, '_get_token_cached', lambda: 'TOKEN')
    with mock.patch.object(agol_to_pg.requests, 'get', return_value=_Resp({"error": {"code": 499}})), \
         mock.patch.object(agol_to_pg.requests, 'post') as post:
        with pytest.raises(agol_to_pg.ArcGISRequestError):
            agol_to_pg.arcgis_request('https://evil.example.com/FeatureServer/0', {})
    post.assert_not_called()


def _fake_conn(table_exists=False, oid_column=True):
    cur = mock.MagicMock()
    results = []

    def execute(query, params=None):
        q = str(query)
        if 'to_regclass' in q:
            results.append(('x',) if table_exists else (None,))
        elif 'information_schema.columns' in q:
            results.append((1,) if oid_column else None)

    cur.execute.side_effect = execute
    cur.fetchone.side_effect = lambda: results.pop(0)
    conn = mock.MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    return conn


def test_paging_continues_past_server_page_cap(monkeypatch):
    """A service capped at 2 records per page must still load all 5 with batch=1000."""
    feats = [{"attributes": {"OBJECTID": i}, "geometry": {"rings": [OUTER]}} for i in range(5)]

    def fake_fetch(url, start, count, order_by):
        page = feats[start:start + 2]
        return {"features": page, "exceededTransferLimit": start + 2 < len(feats)}

    calls = []
    monkeypatch.setattr(agol_to_pg, '_fetch_data', fake_fetch)
    monkeypatch.setattr(agol_to_pg, '_run_ogr2ogr', lambda *a, **k: calls.append(k))
    out = ''.join(agol_to_pg.download_features(
        _fake_conn(), 'utl_stormline', 'public', 'coz_postgres', 'https://x', 'id', 1000, METADATA))
    assert 'Total features imported: 5' in out
    assert [c['append_mode'] for c in calls] == [False, True, True]
    assert all(c['geom_nlt'] == 'MULTIPOLYGON' for c in calls)


def test_existing_table_without_oid_column_fails_clearly(monkeypatch):
    monkeypatch.setattr(agol_to_pg, '_fetch_data', lambda *a: {"features": []})
    with pytest.raises(RuntimeError, match="has no 'id' column"):
        list(agol_to_pg.download_features(
            _fake_conn(table_exists=True, oid_column=False),
            'utl_stormline', 'public', 'coz_postgres', 'https://x', 'id', 1000, METADATA))


def test_ogr2ogr_error_includes_stderr(monkeypatch, tmp_path):
    f = tmp_path / 'x.geojson'
    f.write_text('{}')
    failed = mock.Mock(returncode=1, stderr='ERROR 1: geometry type mismatch', stdout='')
    with mock.patch.object(agol_to_pg.subprocess, 'run', return_value=failed) as run:
        with pytest.raises(RuntimeError, match='geometry type mismatch'):
            agol_to_pg._run_ogr2ogr(str(f), 'coz_postgres', 'public', 't', 'id', 'MULTIPOLYGON', 'geom')
    cmd = run.call_args.args[0]
    assert not any('sslmode=disable' in part for part in cmd)
