#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Atlas IPTV Scraper (formerly Premfun Scraper)
Integrado ao FenixFlix App com catálogo em SQLite (sem RAM para o índice de títulos).
"""

import os
import sys
import time
import re
import asyncio
import unicodedata
import traceback
from typing import Dict, List, Any, Optional
import hashlib
import httpx
import aiosqlite
# Aumenta o timeout padrão do SQLite para evitar "database is locked"
_orig_connect = aiosqlite.connect
def _custom_connect(*args, **kwargs):
    kwargs.setdefault("timeout", 30.0)
    return _orig_connect(*args, **kwargs)
aiosqlite.connect = _custom_connect
try:
    import orjson as _json
except ImportError:          # fallback se orjson não estiver instalado
    import json as _json
from dotenv import load_dotenv

_RE_BRACKETS = re.compile(r'\[.*?\]|\(.*?\)')
_RE_QUALITY  = re.compile(r'\b(4k|hd|fullhd|uhd|hdr|hybrid|dublado|legendado|leg|dub|dual|audio|cam|ts)\b')
_RE_YEAR     = re.compile(r'\b(19|20)\d{2}\b')
_RE_NON_ALNUM = re.compile(r'[^a-z0-9\s]')
_RE_SPACES   = re.compile(r'\s+')

load_dotenv()

DEFAULT_SERVER = os.getenv("ATLAS_SERVER")
DEFAULT_USER   = os.getenv("ATLAS_USER")
DEFAULT_PASS   = os.getenv("ATLAS_PASS")

# User-Agent crucial para evitar bloqueios (o servidor IPTV bloqueia Mozilla)
USER_AGENT = "VLC/3.0.18 LibVLC/3.0.18"

# Caminho do banco SQLite
CACHE_DIR  = "cache"
DB_PATH    = os.path.join(CACHE_DIR, "atlas_catalog.db")
CACHE_TTL  = 3600  # 1 hora
STREAM_CACHE_TTL = 21600  # 6 horas — TTL para streams já resolvidos

# Lock para impedir dois rebuilds simultâneos
_rebuild_lock = asyncio.Lock()

# Cache em RAM apenas para info de episódios de séries específicas (pequeno e volátil)
cache_series_info: Dict[Any, Dict] = {}

# Cache LRU simples para IDs de séries já encontrados (evita rebuscar no SQLite)
_known_series_ids: Dict[str, Any] = {}
_KNOWN_MAX = 500


class AtlasClient:
    """Classe cliente para interagir de forma assíncrona com o servidor Xtream Codes Atlas."""

    def __init__(self, base_url: str = DEFAULT_SERVER, username: str = DEFAULT_USER, password: str = DEFAULT_PASS):
        self.base_url  = base_url.rstrip("/")
        self.username  = username
        self.password  = password

        self.headers = {
            "User-Agent": USER_AGENT,
            "Accept":     "*/*",
            "Connection": "keep-alive"
        }
        # Client dedicado com User-Agent VLC — obrigatório para o servidor IPTV não bloquear
        self.client = httpx.AsyncClient(
            headers=self.headers,
            verify=False,
            timeout=httpx.Timeout(90.0, read=60.0)
        )

        self.user_info:  Dict[str, Any] = {}
        self.server_info: Dict[str, Any] = {}
        self.autenticado = False

    async def close(self):
        """Fecha a sessão do cliente HTTP."""
        await self.client.aclose()

    async def autenticar(self) -> bool:
        """Autentica no servidor e obtém informações da conta."""
        url    = f"{self.base_url}/player_api.php"
        params = {"username": self.username, "password": self.password}
        try:
            resp = await self.client.get(url, params=params)
            if resp.status_code != 200:
                print(f"[Atlas Erro] Falha na requisição HTTP: Status {resp.status_code}")
                return False

            data = resp.json()
            if "user_info" in data:
                self.user_info   = data["user_info"]
                self.server_info = data.get("server_info", {})

                if self.user_info.get("auth") == 1 and self.user_info.get("status") == "Active":
                    self.autenticado = True
                    return True
                else:
                    print("[Atlas Erro] Conta inativa ou dados incorretos.")
                    return False
            else:
                print("[Atlas Erro] Resposta do servidor não contém informações do usuário.")
                return False
        except Exception as e:
            print(f"[Atlas Erro] Falha ao conectar ao servidor: {e}")
            return False

    async def get_vod_categories(self) -> List[Dict[str, Any]]:
        """Obtém as categorias de filmes (VOD)."""
        params = {"username": self.username, "password": self.password, "action": "get_vod_categories"}
        try:
            resp = await self.client.get(f"{self.base_url}/player_api.php", params=params)
            return resp.json() if resp.status_code == 200 else []
        except Exception as e:
            print(f"[Atlas Erro] Falha ao carregar categorias de filmes: {e}")
            return []

    async def get_series_categories(self) -> List[Dict[str, Any]]:
        """Obtém as categorias de séries."""
        params = {"username": self.username, "password": self.password, "action": "get_series_categories"}
        try:
            resp = await self.client.get(f"{self.base_url}/player_api.php", params=params)
            return resp.json() if resp.status_code == 200 else []
        except Exception as e:
            print(f"[Atlas Erro] Falha ao carregar categorias de séries: {e}")
            return []

    async def get_vod_streams(self, category_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Obtém a lista de filmes, opcionalmente filtrada por categoria."""
        params = {"username": self.username, "password": self.password, "action": "get_vod_streams"}
        if category_id:
            params["category_id"] = category_id
        try:
            resp = await self.client.get(f"{self.base_url}/player_api.php", params=params)
            return _json.loads(resp.content) if resp.status_code == 200 else []
        except Exception as e:
            print(f"[Atlas Erro] Falha ao carregar filmes: {e}")
            return []

    async def get_series(self, category_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Obtém a lista de séries, opcionalmente filtrada por categoria."""
        params = {"username": self.username, "password": self.password, "action": "get_series"}
        if category_id:
            params["category_id"] = category_id
        try:
            resp = await self.client.get(f"{self.base_url}/player_api.php", params=params)
            return _json.loads(resp.content) if resp.status_code == 200 else []
        except Exception as e:
            print(f"[Atlas Erro] Falha ao carregar séries: {e}")
            return []

    async def get_series_info(self, series_id: int) -> Dict[str, Any]:
        """Obtém informações detalhadas de uma série, incluindo episódios por temporada."""
        params = {
            "username":  self.username,
            "password":  self.password,
            "action":    "get_series_info",
            "series_id": str(series_id)
        }
        try:
            resp = await self.client.get(f"{self.base_url}/player_api.php", params=params)
            return resp.json() if resp.status_code == 200 else {}
        except Exception as e:
            print(f"[Atlas Erro] Falha ao carregar informações da série {series_id}: {e}")
            return {}

    def get_movie_stream_url(self, stream_id: int, container_extension: str = "mp4") -> str:
        """Gera a URL direta para assistir/baixar um filme."""
        import urllib.parse
        ext  = container_extension if container_extension else "mp4"
        user = urllib.parse.quote(self.username)
        pwd  = urllib.parse.quote(self.password)
        return f"{self.base_url}/movie/{user}/{pwd}/{stream_id}.{ext}"

    def get_episode_stream_url(self, episode_id: int, container_extension: str = "mp4") -> str:
        """Gera a URL direta para assistir/baixar um episódio de série."""
        import urllib.parse
        ext  = container_extension if container_extension else "mp4"
        user = urllib.parse.quote(self.username)
        pwd  = urllib.parse.quote(self.password)
        return f"{self.base_url}/series/{user}/{pwd}/{episode_id}.{ext}"


# ---------------------------------------------------------------------------
# Cache de categorias — pequeno (centenas de itens), fica na RAM
# ---------------------------------------------------------------------------
cache_vod_categories    = {"data": None, "time": 0}
cache_series_categories = {"data": None, "time": 0}

# ---------------------------------------------------------------------------
# Client Atlas global (singleton)
# ---------------------------------------------------------------------------
_atlas_client: Optional[AtlasClient] = None

def get_atlas_client() -> AtlasClient:
    global _atlas_client
    if _atlas_client is None:
        _atlas_client = AtlasClient(DEFAULT_SERVER, DEFAULT_USER, DEFAULT_PASS)
    return _atlas_client


# ---------------------------------------------------------------------------
# SQLite — inicialização e rebuild do catálogo
# ---------------------------------------------------------------------------

async def _init_db(db: aiosqlite.Connection):
    """Cria as tabelas e índices se ainda não existirem."""
    await db.execute("PRAGMA journal_mode=WAL;")
    await db.execute("PRAGMA synchronous=NORMAL;")
    await db.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            key   TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS movies (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            clean_title   TEXT NOT NULL,
            stream_id     INTEGER,
            ext           TEXT,
            original_name TEXT,
            category_id   TEXT,
            year          INTEGER
        )
    """)
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_movies_clean ON movies(clean_title)
    """)
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_movies_year ON movies(year)
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS series (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            clean_title   TEXT NOT NULL,
            series_id     INTEGER,
            category_id   TEXT,
            original_name TEXT,
            year          INTEGER
        )
    """)
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_series_clean ON series(clean_title)
    """)
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_series_year ON series(year)
    """)

    # --- FTS5 TABLES AND TRIGGERS ---
    # Movies FTS5
    await db.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS movies_fts USING fts5(
            clean_title,
            pk_id UNINDEXED
        )
    """)
    await db.execute("""
        CREATE TRIGGER IF NOT EXISTS movies_ai AFTER INSERT ON movies BEGIN
            INSERT INTO movies_fts(rowid, clean_title, pk_id) VALUES (new.id, new.clean_title, new.id);
        END;
    """)
    await db.execute("""
        CREATE TRIGGER IF NOT EXISTS movies_ad AFTER DELETE ON movies BEGIN
            DELETE FROM movies_fts WHERE rowid = old.id;
        END;
    """)

    # Series FTS5
    await db.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS series_fts USING fts5(
            clean_title,
            pk_id UNINDEXED
        )
    """)
    await db.execute("""
        CREATE TRIGGER IF NOT EXISTS series_ai AFTER INSERT ON series BEGIN
            INSERT INTO series_fts(rowid, clean_title, pk_id) VALUES (new.id, new.clean_title, new.id);
        END;
    """)
    await db.execute("""
        CREATE TRIGGER IF NOT EXISTS series_ad AFTER DELETE ON series BEGIN
            DELETE FROM series_fts WHERE rowid = old.id;
        END;
    """)

    # Cache de streams já resolvidos (evita re-processar buscas repetidas)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS stream_cache (
            cache_key   TEXT PRIMARY KEY,
            result_json TEXT NOT NULL,
            created_at  REAL NOT NULL
        )
    """)

    await db.commit()


async def _get_catalog_age(db: aiosqlite.Connection, key: str) -> float:
    """Retorna o timestamp da última atualização do catálogo (0 se nunca foi feito)."""
    async with db.execute("SELECT value FROM meta WHERE key = ?", (key,)) as cur:
        row = await cur.fetchone()
    if row:
        try:
            return float(row[0])
        except Exception:
            pass
    return 0.0


async def _rebuild_movies(client: AtlasClient, db: aiosqlite.Connection):
    """Baixa o catálogo completo de filmes (categoria por categoria) e regrava no SQLite."""
    print("[Atlas SQLite] Iniciando rebuild de filmes categoria por categoria...")
    try:
        categories = await client.get_vod_categories()
        await db.execute("DELETE FROM movies")
        
        inserted_count = 0
        batch = []
        
        if not categories:
            print("[Atlas SQLite] Nenhuma categoria encontrada, tentando carregar tudo de uma vez...")
            data_list = await client.get_vod_streams()
            while data_list:
                m = data_list.pop()
                name = m.get("name")
                if name:
                    year_val = _extract_year(name)
                    if not year_val:
                        year_val = m.get("year")
                        if year_val:
                            try:
                                year_val = int(str(year_val)[:4])
                            except Exception:
                                year_val = None
                    if not year_val:
                        rd = m.get("release_date", "")
                        year_val = int(rd[:4]) if rd and len(rd) >= 4 else None
                    batch.append((
                        clean_title(name),
                        m.get("stream_id"),
                        m.get("container_extension", "mp4"),
                        name,
                        str(m.get("category_id", "")),
                        year_val
                    ))
                    if len(batch) >= 1000:
                        await db.executemany(
                            "INSERT INTO movies(clean_title, stream_id, ext, original_name, category_id, year) VALUES (?,?,?,?,?,?)",
                            batch
                        )
                        inserted_count += len(batch)
                        batch.clear()
            if batch:
                await db.executemany(
                    "INSERT INTO movies(clean_title, stream_id, ext, original_name, category_id, year) VALUES (?,?,?,?,?,?)",
                    batch
                )
                inserted_count += len(batch)
                batch.clear()
        else:
            print(f"[Atlas SQLite] Processando {len(categories)} categorias de filmes...")
            for cat in categories:
                cat_id = cat.get("category_id")
                if not cat_id:
                    continue
                
                data_list = await client.get_vod_streams(category_id=cat_id)
                if not data_list:
                    continue
                
                while data_list:
                    m = data_list.pop()
                    name = m.get("name")
                    if name:
                        year_val = _extract_year(name)
                        if not year_val:
                            year_val = m.get("year")
                            if year_val:
                                try:
                                    year_val = int(str(year_val)[:4])
                                except Exception:
                                    year_val = None
                        if not year_val:
                            rd = m.get("release_date", "")
                            year_val = int(rd[:4]) if rd and len(rd) >= 4 else None
                        
                        batch.append((
                            clean_title(name),
                            m.get("stream_id"),
                            m.get("container_extension", "mp4"),
                            name,
                            str(m.get("category_id", "")),
                            year_val
                        ))
                        
                        if len(batch) >= 1000:
                            await db.executemany(
                                "INSERT INTO movies(clean_title, stream_id, ext, original_name, category_id, year) VALUES (?,?,?,?,?,?)",
                                batch
                            )
                            inserted_count += len(batch)
                            batch.clear()
                import gc; gc.collect()
            
            if batch:
                await db.executemany(
                    "INSERT INTO movies(clean_title, stream_id, ext, original_name, category_id, year) VALUES (?,?,?,?,?,?)",
                    batch
                )
                inserted_count += len(batch)
                batch.clear()

        await db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES ('movies_updated', ?)", (str(time.time()),))
        await db.commit()
        print(f"[Atlas SQLite] Rebuild concluído. {inserted_count} filmes salvos no SQLite.")
    except Exception as e:
        print(f"[Atlas SQLite] Erro ao rebuild de filmes: {e}")


async def _rebuild_series(client: AtlasClient, db: aiosqlite.Connection):
    """Baixa o catálogo completo de séries (categoria por categoria) e regrava no SQLite."""
    print("[Atlas SQLite] Iniciando rebuild de séries categoria por categoria...")
    try:
        categories = await client.get_series_categories()
        await db.execute("DELETE FROM series")
        
        inserted_count = 0
        batch = []
        
        if not categories:
            print("[Atlas SQLite] Nenhuma categoria de série encontrada, tentando carregar tudo de uma vez...")
            data_list = await client.get_series()
            while data_list:
                s = data_list.pop()
                name = s.get("name")
                if name:
                    year_val = _extract_year(name)
                    if not year_val:
                        year_campo = s.get("year")
                        if year_campo and str(year_campo).strip() not in ("", "0", "None"):
                            try:
                                year_val = int(str(year_campo)[:4])
                            except Exception:
                                pass
                    batch.append((
                        clean_title(name),
                        s.get("series_id"),
                        str(s.get("category_id", "")),
                        name,
                        year_val
                    ))
                    if len(batch) >= 1000:
                        await db.executemany(
                            "INSERT INTO series(clean_title, series_id, category_id, original_name, year) VALUES (?,?,?,?,?)",
                            batch
                        )
                        inserted_count += len(batch)
                        batch.clear()
            if batch:
                await db.executemany(
                    "INSERT INTO series(clean_title, series_id, category_id, original_name, year) VALUES (?,?,?,?,?)",
                    batch
                )
                inserted_count += len(batch)
                batch.clear()
        else:
            print(f"[Atlas SQLite] Processando {len(categories)} categorias de séries...")
            for cat in categories:
                cat_id = cat.get("category_id")
                if not cat_id:
                    continue
                
                data_list = await client.get_series(category_id=cat_id)
                if not data_list:
                    continue
                
                while data_list:
                    s = data_list.pop()
                    name = s.get("name")
                    if name:
                        year_val = _extract_year(name)
                        if not year_val:
                            year_campo = s.get("year")
                            if year_campo and str(year_campo).strip() not in ("", "0", "None"):
                                try:
                                    year_val = int(str(year_campo)[:4])
                                except Exception:
                                    pass
                        batch.append((
                            clean_title(name),
                            s.get("series_id"),
                            str(s.get("category_id", "")),
                            name,
                            year_val
                        ))
                        if len(batch) >= 1000:
                            await db.executemany(
                                "INSERT INTO series(clean_title, series_id, category_id, original_name, year) VALUES (?,?,?,?,?)",
                                batch
                            )
                            inserted_count += len(batch)
                            batch.clear()
                import gc; gc.collect()
            
            if batch:
                await db.executemany(
                    "INSERT INTO series(clean_title, series_id, category_id, original_name, year) VALUES (?,?,?,?,?)",
                    batch
                )
                inserted_count += len(batch)
                batch.clear()

        await db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES ('series_updated', ?)", (str(time.time()),))
        await db.commit()
        print(f"[Atlas SQLite] Rebuild concluído. {inserted_count} séries salvas no SQLite.")
    except Exception as e:
        print(f"[Atlas SQLite] Erro ao rebuild de séries: {e}")


async def _background_rebuild(client: AtlasClient, content_type: str):
    """
    Executa o rebuild do catálogo em background.
    Chamado via asyncio.create_task() para não bloquear a requisição atual.
    """
    meta_key = "movies_updated" if content_type == "movie" else "series_updated"
    try:
        async with _rebuild_lock:
            # Double-check após adquirir o lock
            async with aiosqlite.connect(DB_PATH) as db:
                await _init_db(db)
                age = await _get_catalog_age(db, meta_key)
            if (time.time() - age) > CACHE_TTL:
                if not client.autenticado:
                    await client.autenticar()
                async with aiosqlite.connect(DB_PATH) as db:
                    await _init_db(db)
                    if content_type == "movie":
                        await _rebuild_movies(client, db)
                    else:
                        await _rebuild_series(client, db)
    except Exception as e:
        print(f"[Atlas SQLite] Erro no rebuild background de '{content_type}': {e}")


async def _ensure_catalog(client: AtlasClient, content_type: str):
    """
    Garante que o catálogo SQLite está atualizado.
    Se o TTL expirou, dispara rebuild em background (não bloqueia a busca atual).
    Se o catálogo nunca existiu, faz rebuild síncrono (primeira execução).
    """
    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR)

    meta_key = "movies_updated" if content_type == "movie" else "series_updated"

    async with aiosqlite.connect(DB_PATH) as db:
        await _init_db(db)
        age = await _get_catalog_age(db, meta_key)

    now = time.time()
    needs_rebuild = (now - age) > CACHE_TTL

    if needs_rebuild:
        if _rebuild_lock.locked():
            # Outro rebuild em andamento — usa o que tem no disco (pode estar desatualizado, tudo bem)
            print(f"[Atlas SQLite] Rebuild de '{content_type}' já em andamento, usando cache atual.")
            return

        if age == 0.0:
            # Primeira execução — catálogo vazio, rebuild síncrono obrigatório
            print(f"[Atlas SQLite] Catálogo '{content_type}' vazio, rebuild síncrono...")
            await _background_rebuild(client, content_type)
        else:
            # Catálogo existe mas expirou — rebuild em background, retorna imediatamente
            print(f"[Atlas SQLite] TTL expirado para '{content_type}', rebuild em background...")
            asyncio.create_task(_background_rebuild(client, content_type))


# ---------------------------------------------------------------------------
# Busca no SQLite
# ---------------------------------------------------------------------------

async def _search_movies_db(clean_titles: List[str], searched_year: Optional[int]) -> List[tuple]:
    """
    Busca filmes no SQLite por título limpo.
    Retorna lista de (stream_id, ext, original_name, category_id).
    Usa filtro de ano direto no SQL quando disponível.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        # 1. Match exato + filtro de ano no SQL
        for t in clean_titles:
            if searched_year:
                sql = "SELECT stream_id, ext, original_name, category_id FROM movies WHERE clean_title = ? AND (year IS NULL OR ABS(year - ?) <= 1)"
                params = (t, searched_year)
            else:
                sql = "SELECT stream_id, ext, original_name, category_id FROM movies WHERE clean_title = ?"
                params = (t,)
            async with db.execute(sql, params) as cur:
                rows = await cur.fetchall()
            if rows:
                print(f"[Atlas SQLite] MATCH EXATO filme: '{t}' → {len(rows)} resultado(s) [SQL+ano]")
                return list(rows)

        # 2. Match parcial (FTS5) + filtro de ano no SQL
        for t in clean_titles:
            # Prepara a query do FTS5 envolvendo cada palavra em aspas duplas
            fts_query = " AND ".join(f'"{w}"' for w in t.split() if w.strip())
            if not fts_query:
                continue

            if searched_year:
                sql = (
                    "SELECT m.stream_id, m.ext, m.original_name, m.category_id "
                    "FROM movies m "
                    "JOIN movies_fts f ON m.id = f.pk_id "
                    "WHERE movies_fts MATCH ? "
                    "AND (m.year IS NULL OR ABS(m.year - ?) <= 1)"
                )
                params = (fts_query, searched_year)
            else:
                sql = (
                    "SELECT m.stream_id, m.ext, m.original_name, m.category_id "
                    "FROM movies m "
                    "JOIN movies_fts f ON m.id = f.pk_id "
                    "WHERE movies_fts MATCH ?"
                )
                params = (fts_query,)
            async with db.execute(sql, params) as cur:
                rows = await cur.fetchall()
            if rows:
                filtered = _filter_partial(rows, t, searched_year, name_col=2)
                if filtered:
                    print(f"[Atlas SQLite] MATCH PARCIAL filme: '{t}' → {len(filtered)} resultado(s) [FTS5]")
                    return filtered

    return []


async def _search_series_db(clean_titles: List[str], searched_year: Optional[int]) -> List[int]:
    """
    Busca séries no SQLite por título limpo.
    Retorna lista de series_id.
    Usa filtro de ano direto no SQL quando disponível (ano presente em ~20% das séries).
    """
    async with aiosqlite.connect(DB_PATH) as db:
        # 1. Match exato + filtro de ano no SQL
        for t in clean_titles:
            if searched_year:
                sql = "SELECT series_id, original_name FROM series WHERE clean_title = ? AND (year IS NULL OR ABS(year - ?) <= 1)"
                params = (t, searched_year)
            else:
                sql = "SELECT series_id, original_name FROM series WHERE clean_title = ?"
                params = (t,)
            async with db.execute(sql, params) as cur:
                rows = await cur.fetchall()
            if rows:
                ids = [r[0] for r in rows]
                print(f"[Atlas SQLite] MATCH EXATO série: '{t}' → {len(ids)} resultado(s) [SQL+ano]")
                return ids

        # 2. Match parcial (FTS5) + filtro de ano no SQL
        for t in clean_titles:
            fts_query = " AND ".join(f'"{w}"' for w in t.split() if w.strip())
            if not fts_query:
                continue

            if searched_year:
                sql = (
                    "SELECT s.series_id, s.original_name "
                    "FROM series s "
                    "JOIN series_fts f ON s.id = f.pk_id "
                    "WHERE series_fts MATCH ? "
                    "AND (s.year IS NULL OR ABS(s.year - ?) <= 1)"
                )
                params = (fts_query, searched_year)
            else:
                sql = (
                    "SELECT s.series_id, s.original_name "
                    "FROM series s "
                    "JOIN series_fts f ON s.id = f.pk_id "
                    "WHERE series_fts MATCH ?"
                )
                params = (fts_query,)
            async with db.execute(sql, params) as cur:
                rows = await cur.fetchall()
            if rows:
                filtered = _filter_partial_series(rows, t, searched_year)
                if filtered:
                    print(f"[Atlas SQLite] MATCH PARCIAL série: '{t}' → {len(filtered)} resultado(s) [FTS5]")
                    return filtered

    return []


# ---------------------------------------------------------------------------
# Helpers de filtragem por ano e proporção de palavras
# ---------------------------------------------------------------------------

def _extract_year(text: str) -> Optional[int]:
    match = re.search(r'\b(19|20)\d{2}\b', text)
    return int(match.group(0)) if match else None


def _filter_by_year(rows, searched_year: Optional[int], name_col: int) -> list:
    if not searched_year:
        return list(rows)
    result = []
    for row in rows:
        stream_year = _extract_year(row[name_col])
        if stream_year and abs(stream_year - searched_year) > 1:
            continue
        result.append(row)
    return result


def _filter_partial(rows, search_t: str, searched_year: Optional[int], name_col: int) -> list:
    result = []
    words_search = set(search_t.split())
    for row in rows:
        orig = row[name_col] or ""
        ct   = clean_title(orig)
        stream_year = _extract_year(orig)
        if searched_year and stream_year and abs(stream_year - searched_year) > 1:
            continue
        words_dict  = set(ct.split())
        common      = words_search & words_dict
        ratio = len(common) / max(len(words_search), len(words_dict), 1)
        if ratio < 0.6:
            continue
        result.append(row)
    return result


def _filter_by_year_series(rows, searched_year: Optional[int]) -> list:
    """rows = [(series_id, original_name), ...]"""
    if not searched_year:
        return [r[0] for r in rows]
    result = []
    for row in rows:
        stream_year = _extract_year(row[1])
        if stream_year and abs(stream_year - searched_year) > 1:
            continue
        result.append(row[0])
    return result


def _filter_partial_series(rows, search_t: str, searched_year: Optional[int]) -> list:
    words_search = set(search_t.split())
    result = []
    for row in rows:
        orig = row[1] or ""
        ct   = clean_title(orig)
        stream_year = _extract_year(orig)
        if searched_year and stream_year and abs(stream_year - searched_year) > 1:
            continue
        words_dict = set(ct.split())
        common     = words_search & words_dict
        ratio = len(common) / max(len(words_search), len(words_dict), 1)
        if ratio < 0.6:
            continue
        result.append(row[0])
    return result


# ---------------------------------------------------------------------------
# Funções auxiliares (mantidas como antes)
# ---------------------------------------------------------------------------

async def get_category_name(client: AtlasClient, category_id: str, is_series: bool) -> str:
    now = time.time()
    try:
        if is_series:
            if not cache_series_categories["data"] or (now - cache_series_categories["time"]) > 43200:
                cats = await client.get_series_categories()
                cache_series_categories["data"] = {str(c.get("category_id")): str(c.get("category_name", "")) for c in cats}
                cache_series_categories["time"] = now
            return cache_series_categories["data"].get(str(category_id), "")
        else:
            if not cache_vod_categories["data"] or (now - cache_vod_categories["time"]) > 43200:
                cats = await client.get_vod_categories()
                cache_vod_categories["data"] = {str(c.get("category_id")): str(c.get("category_name", "")) for c in cats}
                cache_vod_categories["time"] = now
            return cache_vod_categories["data"].get(str(category_id), "")
    except Exception as e:
        print(f"[Atlas Debug] Erro ao obter nome da categoria: {e}")
        return ""


def detect_audio_info(name: str, category_name: str = "") -> str:
    combined = (name + " " + category_name).lower()
    if "dual" in combined:
        return "Dual Áudio"
    elif "nacional" in combined or "nac" in combined:
        return "Nacional"
    elif re.search(r'\[l\]|\(l\)|\bleg\b|legendad|sub\b|\bl\b', combined):
        return "Legendado"
    elif "dublado" in combined or "dub" in combined or "dubladas" in combined or "dublados" in combined:
        return "Dublado"
    return "Dublado"


def detect_quality(name: str) -> str:
    name_lower = name.lower()
    if any(x in name_lower for x in ["4k", "2160", "uhd"]):
        return "4K"
    elif any(x in name_lower for x in ["1080", "fhd", "fullhd", "full hd"]):
        return "1080p"
    elif any(x in name_lower for x in ["720", "hd"]):
        return "720p"
    elif any(x in name_lower for x in ["480", "sd", "ld"]):
        return "SD"
    elif "cinema" in name_lower or "telecine" in name_lower or re.search(r'\b(cam|ts|tc|hdtc|hdcam|camrip)\b', name_lower):
        return "CAM"
    return "1080p"


def format_stream_title(title_name: str, content_type: str, season=None, episode=None, audio_info: str = "Dublado") -> str:
    lines = [title_name]
    if content_type == "series" and season is not None and episode is not None:
        try:
            lines.append(f"T{int(season):02d} EP{int(episode):02d}")
        except Exception:
            lines.append(f"T{season} EP{episode}")
    lines.append(audio_info)
    return "\n".join(lines)


def clean_title(title) -> str:
    cleaned = str(title).lower().strip()
    cleaned = _RE_BRACKETS.sub(' ', cleaned)
    cleaned = unicodedata.normalize('NFKD', cleaned).encode('ASCII', 'ignore').decode('utf-8')
    cleaned = _RE_QUALITY.sub(' ', cleaned)
    cleaned = _RE_YEAR.sub(' ', cleaned)
    cleaned = _RE_NON_ALNUM.sub(' ', cleaned)
    cleaned = _RE_SPACES.sub(' ', cleaned).strip()
    return cleaned


# ---------------------------------------------------------------------------
# Cache de streams já resolvidos (SQLite)
# ---------------------------------------------------------------------------

def _make_stream_cache_key(titles: list, content_type: str, season, episode, year) -> str:
    """Gera uma chave determinística para o cache de streams."""
    raw = f"{','.join(sorted(str(t) for t in titles))}|{content_type}|{season}|{episode}|{year}"
    return hashlib.sha256(raw.encode()).hexdigest()


async def _get_cached_streams(cache_key: str) -> Optional[list]:
    """Busca streams já resolvidos no cache SQLite. Retorna None se ausente/expirado."""
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await _init_db(db)
            async with db.execute(
                "SELECT result_json, created_at FROM stream_cache WHERE cache_key = ?",
                (cache_key,)
            ) as cur:
                row = await cur.fetchone()
            if row:
                created_at = float(row[1])
                if (time.time() - created_at) < STREAM_CACHE_TTL:
                    data = _json.loads(row[0])
                    print(f"[Atlas Cache] ⚡ Stream cache HIT (key={cache_key[:12]}...)")
                    return data
                else:
                    # Expirado — limpa entrada antiga
                    await db.execute("DELETE FROM stream_cache WHERE cache_key = ?", (cache_key,))
                    await db.commit()
    except Exception as e:
        print(f"[Atlas Cache] Erro ao ler stream_cache: {e}")
    return None


async def _save_cached_streams(cache_key: str, streams: list):
    """Salva streams resolvidos no cache SQLite."""
    try:
        # Serializa com orjson (mais rápido) ou json
        if hasattr(_json, 'dumps') and hasattr(_json, 'loads'):
            json_bytes = _json.dumps(streams)
            if isinstance(json_bytes, bytes):
                json_str = json_bytes.decode('utf-8')
            else:
                json_str = json_bytes
        else:
            import json
            json_str = json.dumps(streams)
        async with aiosqlite.connect(DB_PATH) as db:
            await _init_db(db)
            await db.execute(
                "INSERT OR REPLACE INTO stream_cache(cache_key, result_json, created_at) VALUES (?, ?, ?)",
                (cache_key, json_str, time.time())
            )
            await db.commit()
    except Exception as e:
        print(f"[Atlas Cache] Erro ao salvar stream_cache: {e}")


# ---------------------------------------------------------------------------
# Ponto de entrada principal: search_serve
# ---------------------------------------------------------------------------

async def search_serve(titles_to_search, content_type, season=None, episode=None,
                       client=None, cached_series_id=None, year=None):
    streams = []
    if not titles_to_search:
        return streams

    c = get_atlas_client()
    try:
        if not c.autenticado:
            await c.autenticar()
    except Exception as e:
        print(f"[Atlas Debug] Falha na autenticação inicial: {e}")
        return streams

    # ── Cache de streams já resolvidos ──────────────────────────────────────
    stream_cache_key = _make_stream_cache_key(
        titles_to_search, content_type, season, episode, year
    )
    cached = await _get_cached_streams(stream_cache_key)
    if cached is not None:
        return cached

    matched_items = []
    clean_search_titles = []

    searched_year = int(year) if (year and str(year).isdigit()) else None

    # ── 1. BUSCA DIRETA via cache raiz (série já conhecida) ────────────────
    if content_type == "series" and cached_series_id and cached_series_id != "N":
        if "," in str(cached_series_id):
            matched_items = [s_id.strip() for s_id in str(cached_series_id).split(",")]
        else:
            matched_items = [cached_series_id]
        print(f"\n[Atlas Debug] ⚡ BUSCA DIRETA (CACHE RAIZ) → Série IDs: {matched_items} | S{season}E{episode}")

    else:
        clean_search_titles = [clean_title(t) for t in titles_to_search]
        print(f"\n{'='*50}")
        print(f"[Atlas Debug] NOVA BUSCA | Tipo: {content_type} | Títulos: {clean_search_titles} | Ano: {year}")
        if content_type == "series":
            print(f"[Atlas Debug] Procurando → S{season}E{episode}")
        print('='*50)

    try:
        # ── FILMES ────────────────────────────────────────────────────────
        if content_type == "movie":
            # Garante catálogo atualizado (rebuild em background se TTL expirou)
            await _ensure_catalog(c, "movie")

            matched_items = await _search_movies_db(clean_search_titles, searched_year)

            if matched_items:
                for m_id, ext, original_name, category_id in matched_items:
                    cat_name   = await get_category_name(c, category_id, is_series=False)
                    audio_info = detect_audio_info(original_name, cat_name)
                    qualidade  = detect_quality(original_name)
                    title_name = titles_to_search[0] if titles_to_search else "Filme"
                    title_str  = format_stream_title(title_name, "movie", audio_info=audio_info)
                    direct_url = c.get_movie_stream_url(m_id, ext)
                    streams.append({
                        "name":  f"FenixFlix\n{qualidade}",
                        "title": f"{title_str}\nAtlas",
                        "url":   direct_url,
                        "behaviorHints": {"notWebReady": False, "bingeGroup": "fenixflix-atlas"}
                    })
            else:
                print("[Atlas Debug] Nenhum filme encontrado.")

        # ── SÉRIES ────────────────────────────────────────────────────────
        elif content_type == "series" and season is not None and episode is not None:

            # Se não temos IDs ainda, tenta LRU em RAM depois SQLite
            if not matched_items:
                for t in clean_search_titles:
                    if t in _known_series_ids:
                        v = _known_series_ids[t]
                        matched_items = v if isinstance(v, list) else [v]
                        print(f"[Atlas Debug] ⚡ LRU RAM hit para '{t}'")
                        break

            if not matched_items:
                await _ensure_catalog(c, "series")
                matched_items = await _search_series_db(clean_search_titles, searched_year)

                if matched_items:
                    # Salva no LRU RAM com limite de tamanho
                    for t in clean_search_titles:
                        if len(_known_series_ids) >= _KNOWN_MAX:
                            # Remove o mais antigo (primeira chave inserida)
                            oldest = next(iter(_known_series_ids))
                            del _known_series_ids[oldest]
                        _known_series_ids[t] = matched_items

            # ── Processamento dos episódios ────────────────────────────────
            if matched_items:
                streams.append({"_atlas_series_id": ",".join(str(s) for s in matched_items)})

                for s_id in matched_items:
                    now    = time.time()
                    i_data = None

                    if s_id in cache_series_info and (now - cache_series_info[s_id]["time"]) < CACHE_TTL:
                        print(f"[Atlas Debug] ⚡ Cache RAM de episódios para série ID {s_id}")
                        i_data = cache_series_info[s_id]["data"]
                    else:
                        for tentativa in range(3):
                            try:
                                i_data = await c.get_series_info(s_id)
                                if i_data:
                                    cache_series_info[s_id] = {"data": i_data, "time": now}
                                    break
                            except Exception as ex:
                                print(f"[Atlas Debug] ⚠️ Tentativa {tentativa+1}/3: {ex}")
                                if tentativa < 2:
                                    await asyncio.sleep(2)

                    if not i_data:
                        continue

                    episodes_dict = i_data.get("episodes", {})
                    season_str    = str(season)

                    if season_str in episodes_dict:
                        eps   = episodes_dict[season_str]
                        ep_id = None
                        ep_ext   = "mp4"
                        ep_title = ""
                        for ep in eps:
                            if str(ep.get("episode_num")) == str(episode):
                                ep_id    = ep.get("id")
                                ep_ext   = ep.get("container_extension", "mp4")
                                ep_title = ep.get("title", "")
                                break

                        if ep_id:
                            info_dict = i_data.get("info", {})
                            orig_name = info_dict.get("name", "")
                            cat_id    = info_dict.get("category_id", "")
                            cat_name  = await get_category_name(c, cat_id, is_series=True)

                            audio_info = detect_audio_info(orig_name + " " + ep_title, cat_name)
                            qualidade  = detect_quality(orig_name + " " + ep_title + " " + cat_name)
                            title_name = titles_to_search[0] if titles_to_search else "Série"
                            title_str  = format_stream_title(title_name, "series", season, episode, audio_info=audio_info)
                            direct_url = c.get_episode_stream_url(ep_id, ep_ext)
                            streams.append({
                                "name":  f"FenixFlix\n{qualidade}",
                                "title": f"{title_str}\nAtlas",
                                "url":   direct_url,
                                "behaviorHints": {"notWebReady": False, "bingeGroup": "fenixflix-atlas"},
                                "_atlas_series_id": s_id
                            })
                        else:
                            print(f"[Atlas Debug] ❌ Temporada {season} existe mas não tem o episódio {episode}.")
                    else:
                        print(f"[Atlas Debug] ❌ Série ID {s_id} não tem a temporada {season_str}.")

            else:
                print("[Atlas Debug] Série não encontrada. Retornando flag 'N'.")
                streams.append({"_atlas_series_id": "N"})

    except Exception as e:
        print(f"[Atlas Debug] Erro global: {e}")
        traceback.print_exc()

    # ── Salva resultado no cache de streams (se houver resultados) ─────────
    if streams:
        asyncio.create_task(_save_cached_streams(stream_cache_key, streams))

    return streams


# Bloqueio de Execução Standalone
if __name__ == "__main__":
    print("Este script não pode ser executado diretamente. Ele funciona apenas integrado ao FenixFlix App.")
    sys.exit(1)
