import uvloop
import asyncio
# Troca o motor do asyncio antes de qualquer outra coisa
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
import httpx
import os
import time
import orjson
import uvicorn
import aiofiles
import re
from contextlib import asynccontextmanager
from dotenv import load_dotenv

from datetime import datetime, date
import serve
import mywallpaper
import on
import net

load_dotenv()

VERSION = "1.0.5"
CACHE_DIR = "cache"
CATALOG_CACHE_TIME = 6 * 60 * 60
POPULAR_CACHE_TIME = 24 * 60 * 60
SCRAPER_STATUS_FILE = os.path.join(CACHE_DIR, "scrapers_status.json")
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
SERVE_ = os.getenv("serve")

if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

_http_client: httpx.AsyncClient = None
_serve_client: httpx.AsyncClient = None
tmdb_semaphore = asyncio.Semaphore(7)

# --- SISTEMA DE CACHE GLOBAL COM LOCK E GRAVAÇÃO EM BACKGROUND ---
GLOBAL_SCRAPER_CACHE = None
CACHE_LOCK = asyncio.Lock()
_CACHE_DIRTY = False

def load_scraper_cache():
    global GLOBAL_SCRAPER_CACHE

    if GLOBAL_SCRAPER_CACHE is not None:
        return GLOBAL_SCRAPER_CACHE

    if os.path.exists(SCRAPER_STATUS_FILE):
        try:
            with open(SCRAPER_STATUS_FILE, "rb") as f:
                GLOBAL_SCRAPER_CACHE = orjson.loads(f.read())
                return GLOBAL_SCRAPER_CACHE
        except Exception as e:
            print(f"[CACHE ERROR] Falha ao ler scrapers_status.json: {e}")

    GLOBAL_SCRAPER_CACHE = {}
    return GLOBAL_SCRAPER_CACHE

async def save_scraper_cache(cache_data):
    """Apenas atualiza a RAM e sinaliza que precisa ser salvo pelo processo em background."""
    global GLOBAL_SCRAPER_CACHE, _CACHE_DIRTY
    GLOBAL_SCRAPER_CACHE = cache_data
    _CACHE_DIRTY = True

async def background_cache_writer():
    """Roda no background e salva a cada 60 segundos se houver mudanças."""
    global _CACHE_DIRTY, GLOBAL_SCRAPER_CACHE
    while True:
        await asyncio.sleep(10)
        if _CACHE_DIRTY and GLOBAL_SCRAPER_CACHE is not None:
            async with CACHE_LOCK:
                try:
                    async with aiofiles.open(SCRAPER_STATUS_FILE, mode="wb") as f:
                        await f.write(orjson.dumps(GLOBAL_SCRAPER_CACHE, option=orjson.OPT_INDENT_2))
                    _CACHE_DIRTY = False
                    print("[CACHE] Arquivo JSON sincronizado com sucesso no disco em background.")
                except Exception as e:
                    print(f"[CACHE FATAL] Erro na gravação de background: {e}")

async def _resolve_popular_item(tmdb_id: str, content_type: str):
    tmdb_type = "movie" if content_type == "movie" else "tv"
    url_pt = f"https://api.themoviedb.org/3/{tmdb_type}/{tmdb_id}?api_key={TMDB_API_KEY}&language=pt-BR&append_to_response=external_ids"
    url_en = f"https://api.themoviedb.org/3/{tmdb_type}/{tmdb_id}?api_key={TMDB_API_KEY}&language=en-US&append_to_response=external_ids"

    try:
        async with tmdb_semaphore:
            res_pt, res_en = await asyncio.gather(
                _http_client.get(url_pt, timeout=5.0),
                _http_client.get(url_en, timeout=5.0),
                return_exceptions=True,
            )

        imdb_id = None
        titles  = []

        for res in (res_pt, res_en):
            if isinstance(res, Exception) or res.status_code != 200:
                continue
            data = res.json()
            if not imdb_id:
                ext = data.get("external_ids") or {}
                imdb_id = ext.get("imdb_id") or None
            name = data.get("title") or data.get("name")
            if name and name not in titles:
                titles.append(name)

        if imdb_id:
            return imdb_id, tmdb_id, titles
    except Exception as e:
        print(f"[SCRAPER-CACHE] Erro ao resolver tmdb_id {tmdb_id}: {e}")

    return None

async def sync_scraper_cache_from_items(items: list, content_type: str):
    scraper_cache = load_scraper_cache()

    known_tmdb_ids = {v.get("tmdb_id") for v in scraper_cache.values() if v.get("tmdb_id")}

    novos = []
    for item in items:
        item_id = item.get("id", "")
        if not item_id.startswith("tmdb:"):
            continue
        tmdb_id = item_id.split(":")[1]
        if tmdb_id not in known_tmdb_ids:
            novos.append(tmdb_id)

    if not novos:
        return

    print(f"[SCRAPER-CACHE] {len(novos)} itens novos em '{content_type}' — resolvendo imdb_id+titles...")

    results = await asyncio.gather(
        *[_resolve_popular_item(tmdb_id, content_type) for tmdb_id in novos],
        return_exceptions=True,
    )

    count = 0
    for result in results:
        if isinstance(result, Exception) or result is None:
            continue
        imdb_id, tmdb_id_r, titles = result
        if imdb_id and imdb_id not in scraper_cache:
            scraper_cache[imdb_id] = {
                "tmdb_id":  tmdb_id_r,
                "titles":   titles,
                "type":     content_type,
                "scrapers": {},
            }
            count += 1

    if count:
        await save_scraper_cache(scraper_cache)

async def prepopulate_scraper_cache_from_popular():
    for content_type in ("movie", "series"):
        cache_file = os.path.join(CACHE_DIR, f"tmdb_popular_{content_type}.json")
        if not os.path.exists(cache_file):
            continue
        try:
            with open(cache_file, "rb") as f:
                items = orjson.loads(f.read())
            await sync_scraper_cache_from_items(items, content_type)
        except Exception as e:
            print(f"[SCRAPER-CACHE] Erro ao ler cache popular '{content_type}': {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _http_client, _serve_client
    _http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(15.0, connect=5.0),
        follow_redirects=True,
        limits=httpx.Limits(max_connections=200, max_keepalive_connections=30, keepalive_expiry=30),
        verify=False,
    )
    _serve_client = httpx.AsyncClient(
        timeout=httpx.Timeout(10.0, connect=5.0),
        follow_redirects=True,
        limits=httpx.Limits(max_connections=40, max_keepalive_connections=10, keepalive_expiry=30),
        verify=False,
    )

    # Inicia a task de gravação em background
    task_writer = asyncio.create_task(background_cache_writer())

    await prepopulate_scraper_cache_from_popular()

    yield

    # Encerra processos graciosamente
    await _http_client.aclose()
    await _serve_client.aclose()
    task_writer.cancel()

    # Garante a última gravação ao desligar a API
    if _CACHE_DIRTY and GLOBAL_SCRAPER_CACHE is not None:
        with open(SCRAPER_STATUS_FILE, "wb") as f:
            f.write(orjson.dumps(GLOBAL_SCRAPER_CACHE, option=orjson.OPT_INDENT_2))

app = FastAPI(lifespan=lifespan)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

async def obter_dados_base_tmdb(imdb_id: str, content_type: str, client: httpx.AsyncClient = None):
    tmdb_id_final = None
    real_imdb_id  = None
    titulos = []
    tmdb_type = "movie" if content_type == "movie" else "tv"

    async def _do_requests(c):
        nonlocal tmdb_id_final, real_imdb_id
        if imdb_id.startswith("tmdb:"):
            tmdb_id_final = imdb_id.split(":")[1]
            url_pt = f"https://api.themoviedb.org/3/{tmdb_type}/{tmdb_id_final}?api_key={TMDB_API_KEY}&language=pt-BR&append_to_response=external_ids"
            url_en = f"https://api.themoviedb.org/3/{tmdb_type}/{tmdb_id_final}?api_key={TMDB_API_KEY}&language=en-US&append_to_response=external_ids"
            reqs = await asyncio.gather(c.get(url_pt), c.get(url_en), return_exceptions=True)
            for r in reqs:
                if not isinstance(r, Exception) and r.status_code == 200:
                    data = r.json()
                    if not real_imdb_id:
                        real_imdb_id = (data.get("external_ids") or {}).get("imdb_id") or None
                    name = data.get("title") or data.get("name")
                    if name and name not in titulos:
                        titulos.append(name)
        else:
            real_imdb_id = imdb_id
            url_pt = f"https://api.themoviedb.org/3/find/{imdb_id}?api_key={TMDB_API_KEY}&external_source=imdb_id&language=pt-BR"
            url_en = f"https://api.themoviedb.org/3/find/{imdb_id}?api_key={TMDB_API_KEY}&external_source=imdb_id&language=en-US"
            reqs = await asyncio.gather(c.get(url_pt), c.get(url_en), return_exceptions=True)
            for r in reqs:
                if not isinstance(r, Exception) and r.status_code == 200:
                    data = r.json()
                    results = data.get(f"{tmdb_type}_results", [])
                    if results:
                        if not tmdb_id_final:
                            tmdb_id_final = str(results[0].get("id"))
                        name = results[0].get("title") or results[0].get("name")
                        if name and name not in titulos:
                            titulos.append(name)

    try:
        await _do_requests(client or _http_client)
    except Exception as e:
        print(f"[TMDB] Cliente global falhou ({e}), a criar cliente temporario...")
        try:
            async with httpx.AsyncClient(timeout=10.0, verify=False) as tmp:
                await _do_requests(tmp)
        except Exception as e2:
            pass

    return tmdb_id_final, real_imdb_id, titulos

async def fetch_tmdb_meta_ptbr(item_id: str, content_type: str):
    async with tmdb_semaphore:
        tmdb_id_final = None
        true_type = content_type
        tmdb_type = "movie" if content_type == "movie" else "tv"

        if item_id.startswith("tmdb:"):
            tmdb_id_final = item_id.split(":")[1]
        else:
            url_find = f"https://api.themoviedb.org/3/find/{item_id}?api_key={TMDB_API_KEY}&external_source=imdb_id&language=pt-BR"
            try:
                resp = await _http_client.get(url_find, timeout=5.0)
                if resp.status_code == 200:
                    data = resp.json()
                    results = data.get(f"{tmdb_type}_results", [])
                    if results:
                        tmdb_id_final = str(results[0].get("id"))
                    else:
                        other_type = "tv" if tmdb_type == "movie" else "movie"
                        other_results = data.get(f"{other_type}_results", [])
                        if other_results:
                            tmdb_id_final = str(other_results[0].get("id"))
                            tmdb_type = other_type
                            true_type = "series" if other_type == "tv" else "movie"
            except:
                pass

        if tmdb_id_final:
            url_details = f"https://api.themoviedb.org/3/{tmdb_type}/{tmdb_id_final}?api_key={TMDB_API_KEY}&language=pt-BR"
            try:
                resp = await _http_client.get(url_details, timeout=5.0)
                if resp.status_code == 200:
                    data = resp.json()
                    title = data.get("title") or data.get("name") or f"Conteúdo {item_id}"
                    poster_path = data.get("poster_path")
                    poster = f"https://image.tmdb.org/t/p/w500{poster_path}" if poster_path else None
                    description = data.get("overview") or ""

                    return {
                        "id": item_id,
                        "type": true_type,
                        "name": title,
                        "poster": poster,
                        "description": description,
                        "_tmdb_id": tmdb_id_final,
                    }
            except:
                pass

        return {"id": item_id, "type": true_type, "name": f"Conteúdo {item_id}"}

async def build_recent_catalog():
    url = f"{SERVE_}/api/all"
    try:
        resp = await _http_client.get(url, timeout=30.0)
        all_data = resp.json() if resp.status_code == 200 else []
    except:
        return {"movie": [], "series": []}

    items = all_data if isinstance(all_data, list) else list(all_data.values())
    movie_ids, series_ids = [], []
    for data in items:
        if len(movie_ids) >= 27 and len(series_ids) >= 27:
            break
        imdb_id = data.get("id")
        if not imdb_id:
            continue
        content_type = data.get("type", "movie")
        streams = data.get("streams", {})

        if content_type == "series" and isinstance(streams, dict) and len(series_ids) < 27:
            series_ids.append(imdb_id)
        elif (content_type == "movie" or isinstance(streams, list)) and len(movie_ids) < 27:
            movie_ids.append(imdb_id)

    movie_tasks  = [fetch_tmdb_meta_ptbr(i, "movie")  for i in movie_ids]
    series_tasks = [fetch_tmdb_meta_ptbr(i, "series") for i in series_ids]
    movies_raw  = await asyncio.gather(*movie_tasks,  return_exceptions=True)
    series_raw  = await asyncio.gather(*series_tasks, return_exceptions=True)

    def clean(item):
        if isinstance(item, dict):
            return {k: v for k, v in item.items() if k != "_tmdb_id"}
        return item

    movies = [clean(m) for m in movies_raw if isinstance(m, dict) and m.get("type") == "movie"][:25]
    series = [clean(s) for s in series_raw if isinstance(s, dict) and s.get("type") == "series"][:25]
    return {"movie": movies, "series": series}

async def get_recent_catalog_cached(content_type):
    cache_file = os.path.join(CACHE_DIR, "server_recent_catalog.json")

    if os.path.exists(cache_file) and (time.time() - os.path.getmtime(cache_file)) < CATALOG_CACHE_TIME:
        try:
            with open(cache_file, "rb") as f:
                dados_salvos = orjson.loads(f.read())
                itens_salvos = dados_salvos.get(content_type, [])
                if itens_salvos:
                    return itens_salvos
        except:
            pass

    catalogs = await build_recent_catalog()
    if catalogs.get("movie") or catalogs.get("series"):
        try:
            with open(cache_file, "wb") as f:
                f.write(orjson.dumps(catalogs, option=orjson.OPT_INDENT_2))

            if os.path.exists(SCRAPER_STATUS_FILE):
                os.remove(SCRAPER_STATUS_FILE)
        except:
            pass
    return catalogs.get(content_type, [])

def filtrar_populares_por_data(metas):
    filtrados = []
    for m in metas:
        if m.get("type") == "movie":
            rel_date_str = m.get("release_date")
            if rel_date_str:
                try:
                    rel_date = datetime.strptime(rel_date_str, "%Y-%m-%d").date()
                    if rel_date >= date.today():
                        continue
                except Exception:
                    pass
        filtrados.append(m)
    return filtrados

async def get_popular_catalog_cached(content_type):
    cache_file = os.path.join(CACHE_DIR, f"tmdb_popular_{content_type}.json")

    if os.path.exists(cache_file) and (time.time() - os.path.getmtime(cache_file)) < POPULAR_CACHE_TIME:
        try:
            with open(cache_file, "rb") as f:
                dados_salvos = orjson.loads(f.read())
                if dados_salvos:
                    return filtrar_populares_por_data(dados_salvos)
        except:
            pass

    tmdb_type = "movie" if content_type == "movie" else "tv"

    url_page1 = f"https://api.themoviedb.org/3/trending/{tmdb_type}/week?api_key={TMDB_API_KEY}&language=pt-BR&region=BR&page=1"
    url_page2 = f"https://api.themoviedb.org/3/trending/{tmdb_type}/week?api_key={TMDB_API_KEY}&language=pt-BR&region=BR&page=2"

    try:
        reqs = await asyncio.gather(
            _http_client.get(url_page1, timeout=10.0),
            _http_client.get(url_page2, timeout=10.0),
            return_exceptions=True
        )

        metas = []
        for resp in reqs:
            if not isinstance(resp, Exception) and resp.status_code == 200:
                data = resp.json()
                for item in data.get("results", []):
                    title = item.get("title") or item.get("name")
                    poster_path = item.get("poster_path")
                    if poster_path:
                        metas.append({
                            "id": f"tmdb:{item.get('id')}",
                            "type": content_type,
                            "name": title,
                            "poster": f"https://image.tmdb.org/t/p/w500{poster_path}",
                            "description": item.get("overview", ""),
                            "release_date": item.get("release_date") or item.get("first_air_date")
                        })

        vistos = set()
        metas_unicas = []
        for m in metas:
            if m["id"] not in vistos:
                vistos.add(m["id"])
                metas_unicas.append(m)

        if metas_unicas:
            try:
                with open(cache_file, "wb") as f:
                    f.write(orjson.dumps(metas_unicas, option=orjson.OPT_INDENT_2))
            except:
                pass
            asyncio.create_task(sync_scraper_cache_from_items(metas_unicas, content_type))

        return filtrar_populares_por_data(metas_unicas)
    except:
        return []

async def get_populares_fenix_cached(content_type: str):
    cache_file = os.path.join(CACHE_DIR, f"populares_fenix_{content_type}.json")

    if os.path.exists(cache_file) and (time.time() - os.path.getmtime(cache_file)) < CATALOG_CACHE_TIME:
        try:
            with open(cache_file, "rb") as f:
                dados_salvos = orjson.loads(f.read())
                if dados_salvos:
                    return dados_salvos
        except:
            pass

    url = f"{SERVE_}/api/vistos" if SERVE_ else "https://fenixflix-2ymu.onrender.com/api/vistos"
    try:
        resp = await _http_client.get(url, timeout=15.0)
        vistos = resp.json() if resp.status_code == 200 else []
    except Exception as e:
        print(f"[POPULARES FENIX] Erro ao buscar vistos: {e}")
        if os.path.exists(cache_file):
            try:
                with open(cache_file, "rb") as f:
                    return orjson.loads(f.read())
            except:
                pass
        return []

    if not vistos:
        return []

    # Ordenar por visualizações de forma decrescente
    vistos = sorted(vistos, key=lambda x: x.get("v", 0), reverse=True)
    scraper_cache = load_scraper_cache()
    
    ids_to_resolve = []
    for item in vistos:
        item_id = item.get("id")
        if not item_id:
            continue
        
        if not (item_id.startswith("tt") or item_id.startswith("tmdb:")):
            continue
            
        cached_entry = scraper_cache.get(item_id)
        if cached_entry:
            cached_type = cached_entry.get("type")
            if cached_type and cached_type != content_type:
                continue
        
        ids_to_resolve.append(item_id)
        # Buscar até 60 candidatos para termos margem de encontrar 20 do tipo solicitado
        if len(ids_to_resolve) >= 60:
            break

    if not ids_to_resolve:
        return []

    tasks = [fetch_tmdb_meta_ptbr(item_id, content_type) for item_id in ids_to_resolve]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    resolved_metas = []
    cache_mudou = False
    
    for r in results:
        if isinstance(r, dict):
            if r.get("type") == content_type:
                meta_item = {k: v for k, v in r.items() if k != "_tmdb_id"}
                resolved_metas.append(meta_item)
            
            # Aproveitar para preencher no cache se não estiver lá
            item_id = r.get("id")
            if item_id and item_id not in scraper_cache:
                tmdb_id_r = r.get("_tmdb_id")
                if tmdb_id_r:
                    scraper_cache[item_id] = {
                        "tmdb_id": tmdb_id_r,
                        "titles": [r.get("name")],
                        "type": r.get("type"),
                        "scrapers": {}
                    }
                    cache_mudou = True

    if cache_mudou:
        await save_scraper_cache(scraper_cache)

    resolved_metas = resolved_metas[:20]

    if resolved_metas:
        try:
            with open(cache_file, "wb") as f:
                f.write(orjson.dumps(resolved_metas, option=orjson.OPT_INDENT_2))
        except Exception as e:
            print(f"[POPULARES FENIX] Erro ao gravar cache: {e}")

    return resolved_metas

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    manifest_data = {"name": "FENIXFLIX", "description": "Addon de Filmes e Séries", "types": ["movie", "series"]}
    return templates.TemplateResponse(request=request, name="index.html", context={"manifest": manifest_data, "version": VERSION})

@app.get("/manifest.json")
async def manifest_endpoint():
    return JSONResponse(content={
        "id": "com.fenixflix", "version": VERSION, "name": "FENIXFLIX",
        "description": "Addon de Filmes e Séries",
        "logo": "https://i.imgur.com/9SKgxfU.png",
        "background": "https://dl.strem.io/addon-background.jpg",
        "resources": ["stream", "catalog"],
        "types": ["movie", "series"],
        "catalogs": [
            {"type": "movie",  "id": "popular",           "name": "Populares"},
            {"type": "movie",  "id": "populares_fenix",   "name": "Populares (Fenix)"},
            {"type": "movie",  "id": "recentes_servidor", "name": "Recém Adicionado (Fenix)"},
            {"type": "series", "id": "popular",           "name": "Populares"},
            {"type": "series", "id": "populares_fenix",   "name": "Populares (Fenix)"},
            {"type": "series", "id": "recentes_servidor", "name": "Recém Adicionado (Fenix)"}
        ],
        "idPrefixes": ["tt", "tmdb"]
    })

@app.get("/catalog/{type}/{id}.json")
@app.get("/catalog/{type}/{id}/{extra}.json")
async def catalog_endpoint(type: str, id: str, extra: str = None, skip: int = 0):
    if (extra and "skip=" in extra) or skip > 0:
        return JSONResponse(content={"metas": []})

    if id == "recentes_servidor":
        metas = await get_recent_catalog_cached(type)
        return JSONResponse(content={"metas": metas})
    elif id == "popular":
        metas = await get_popular_catalog_cached(type)
        return JSONResponse(content={"metas": metas})
    elif id == "populares_fenix":
        metas = await get_populares_fenix_cached(type)
        return JSONResponse(content={"metas": metas})

    return JSONResponse(content={"metas": []})

async def enviar_pedido_background(url: str):
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) FenixFlixAddon/1.0"}
        async with httpx.AsyncClient(timeout=10.0, verify=False) as client:
            response = await client.get(url, headers=headers)
            print(f"[AUTO-PEDIDO] Pedido enviado para {url} - Status: {response.status_code}")
    except Exception as e:
        print(f"[AUTO-PEDIDO] Erro ao enviar pedido automático: {e}")

async def atualizar_cache_e_pedido(
    base_id, tmdb_id, titles, type, novos_flags, outras_tarefas, 
    pending_names, mywallpaper_teve_resultados, season, episode,
    imdb_id_for_request, len_todos_streams
):
    try:
        cache_status = load_scraper_cache()
        cache_mudou = False

        if base_id not in cache_status:
            cache_status[base_id] = {"tmdb_id": tmdb_id, "titles": titles, "type": type}
            cache_mudou = True

        episodes_backup = cache_status[base_id].pop("episodes", None)
        scrapers_backup = cache_status[base_id].pop("scrapers", None)

        slug_novo = novos_flags.pop("_doramogo_slug_novo", None)
        if slug_novo:
            cache_status[base_id]["doramogo_slug"] = slug_novo
            cache_mudou = True

        if "mywallpaper" in outras_tarefas and mywallpaper_teve_resultados is not None:
            mw_atual = cache_status[base_id].get("mywallpaper_global")
            is_mw_pending = "mywallpaper" in pending_names
            if not mywallpaper_teve_resultados and not is_mw_pending:
                if mw_atual != "N":
                    cache_status[base_id]["mywallpaper_global"] = "N"
                    cache_mudou = True
            elif not is_mw_pending:
                if mw_atual != "S":
                    cache_status[base_id]["mywallpaper_global"] = "S"
                    cache_mudou = True

        if tmdb_id and type == "series":
            if episodes_backup is None:
                episodes_backup = {}
            flags_para_salvar = {k: v for k, v in novos_flags.items() if k != "doramogo_slug"}

            ep_key = f"{season}:{episode}"
            if ep_key not in episodes_backup:
                episodes_backup[ep_key] = flags_para_salvar
                cache_mudou = True
            else:
                if episodes_backup[ep_key] != flags_para_salvar:
                    episodes_backup[ep_key].update(flags_para_salvar)
                    cache_mudou = True

            cache_status[base_id]["episodes"] = episodes_backup

        elif tmdb_id:
            if scrapers_backup is None:
                scrapers_backup = {}
            if scrapers_backup != novos_flags:
                scrapers_backup.update(novos_flags)
                cache_mudou = True
            cache_status[base_id]["scrapers"] = scrapers_backup

        if "streams_data" in cache_status[base_id]:
            del cache_status[base_id]["streams_data"]
            cache_mudou = True

        if cache_mudou:
            await save_scraper_cache(cache_status)

        # Envia o pedido de forma automática/em background
        if imdb_id_for_request and len_todos_streams == 0:
            episode_suffix = ""
            if type == "series" and season is not None and episode is not None:
                episode_suffix = f"&episode=T{season:02d}EP{episode:02d}"
            
            base_server = SERVE_ or "https://fenixflix-2ymu.onrender.com"
            base_server = base_server.rstrip('/')
            pedidos_url = f"{base_server}/api/pedidos?id={imdb_id_for_request}&type={type}{episode_suffix}"
            
            await enviar_pedido_background(pedidos_url)
    except Exception as e:
        print(f"[BACKGROUND TASK ERROR] Erro na atualização do cache ou pedido: {e}")


@app.get("/stream/{type}/{id}.json")
@limiter.limit("30/minute")
async def stream(type: str, id: str, request: Request, background_tasks: BackgroundTasks):
    season, episode = None, None
    if id.startswith("tmdb:"):
        parts = id.split(':')
        clean_id = f"tmdb:{parts[1]}"
        if type == 'series' and len(parts) >= 4:
            season, episode = int(parts[2]), int(parts[3])
    else:
        parts = id.split(':')
        clean_id = parts[0]
        if type == 'series' and len(parts) >= 3:
            season, episode = int(parts[1]), int(parts[2])

    cache_status = load_scraper_cache()
    entry = cache_status.get(clean_id)
    base_id = clean_id

    if entry is None and clean_id.startswith("tmdb:"):
        tmdb_id_raw = clean_id.split(":")[1]
        for key, val in cache_status.items():
            if val.get("tmdb_id") == tmdb_id_raw:
                entry = val
                base_id = key
                break

    if entry and entry.get("tmdb_id"):
        tmdb_id = entry.get("tmdb_id")
        titles  = entry.get("titles", [])

        if type == "series":
            scraper_flags = entry.get("episodes", {}).get(f"{season}:{episode}", {})
            if isinstance(scraper_flags, dict) and "flags" in scraper_flags:
                scraper_flags = scraper_flags["flags"]
        else:
            scraper_flags = entry.get("scrapers", {})
    else:
        tmdb_id, real_imdb_id, titles = await obter_dados_base_tmdb(clean_id, type, client=_http_client)
        scraper_flags = {}
        if real_imdb_id and real_imdb_id.startswith("tt"):
            base_id = real_imdb_id
        else:
            base_id = clean_id

    import inspect

    # Resolve serve.search_serve signature dynamically
    kwargs_serve = {}
    if "titles" in inspect.signature(serve.search_serve).parameters:
        kwargs_serve["titles"] = titles

    tarefa_serve = asyncio.create_task(
        serve.search_serve(clean_id, type, season, episode, client=_serve_client, **kwargs_serve)
    )

    outras_tarefas = {}
    novos_flags = scraper_flags.copy()

    if tmdb_id:
        on_flag = scraper_flags.get("on")

        azullog_falhou_total = isinstance(on_flag, dict) and on_flag.get("D") == "N" and on_flag.get("L") == "N"
        on_ja_completo = isinstance(on_flag, dict) and "D" in on_flag and "L" in on_flag

        if on_flag == "N" or azullog_falhou_total:
            novos_flags["on"] = on_flag
        else:
            on_cache = {}
            if isinstance(on_flag, dict):
                for k, v in on_flag.items():
                    if isinstance(v, str) and not v.startswith("http") and v != "N" and v != "S":
                        on_cache[k] = f"https://www.mediafire.com/file_premium/{v}/file"
                    else:
                        on_cache[k] = v

            kwargs_on = {}
            if "titles" in inspect.signature(on.search_serve).parameters:
                kwargs_on["titles"] = titles
            outras_tarefas["on"] = asyncio.create_task(
                on.search_serve(tmdb_id, type, season, episode, client=_http_client, cached_links=on_cache, **kwargs_on)
            )

        mw_flag_salva = cache_status.get(base_id, {}).get("mywallpaper_global")

        if mw_flag_salva == "N":
            novos_flags["mywallpaper"] = "N"
        elif scraper_flags.get("mywallpaper") != "N":
            outras_tarefas["mywallpaper"] = asyncio.create_task(
                mywallpaper.search_serve(tmdb_id, titles, type, season, episode, client=_http_client)
            )

        net_flag = scraper_flags.get("net")
        if net_flag == "N":
            novos_flags["net"] = "N"
        else:
            kwargs_net = {}
            if "titles" in inspect.signature(net.search_serve).parameters:
                kwargs_net["titles"] = titles
            outras_tarefas["net"] = asyncio.create_task(
                net.search_serve(tmdb_id, type, season, episode, client=_http_client, **kwargs_net)
            )

    tarefas_ativas = {"serve": tarefa_serve}
    tarefas_ativas.update(outras_tarefas)

    # Espera até 12 segundos para não sofrer timeout silencioso no Stremio
    done, pending = await asyncio.wait(
        tarefas_ativas.values(),
        timeout=32.0
    )

    for p in pending:
        p.cancel()

    todos_streams = []
    mywallpaper_teve_resultados = False

    for nome, tarefa in tarefas_ativas.items():
        if tarefa in pending:
            continue

        try:
            res = tarefa.result()
        except Exception as e:
            res = None

        if not res:
            if nome != "serve":
                novos_flags[nome] = "N"
            continue

        if nome == "mywallpaper" and isinstance(res, list) and len(res) > 0:
            mywallpaper_teve_resultados = True

        elif nome == "on":
            on_dict = {}
            for s in res:
                if isinstance(s, dict) and s.get("_cache_key"):
                    url_completa = s.get("_mediafire_url", "N")

                    if url_completa == "N":
                        on_dict[s["_cache_key"]] = "N"
                    else:
                        match = re.search(r'mediafire\.com/(?:file_premium|file)/([a-zA-Z0-9]+)', url_completa)
                        on_dict[s["_cache_key"]] = match.group(1) if match else url_completa

            novos_flags[nome] = on_dict if on_dict else "S"

        elif nome != "serve":
            novos_flags[nome] = "S"

        for s_info in res:
            if isinstance(s_info, dict) and s_info.get("url"):
                s_info.pop("_slug_found", None)
                s_info.pop("_mediafire_url", None)
                s_info.pop("_label", None)
                s_info.pop("_cache_key", None)

                if "behaviorHints" not in s_info:
                    s_info["behaviorHints"] = {"notWebReady": False, "bingeGroup": "fenixflix"}
                todos_streams.append(s_info)

    # Identify pending names
    pending_names = []
    for nome, tarefa in tarefas_ativas.items():
        if tarefa in pending:
            pending_names.append(nome)

    imdb_id_for_request = None
    if base_id and base_id.startswith("tt"):
        imdb_id_for_request = base_id
    elif clean_id and clean_id.startswith("tt"):
        imdb_id_for_request = clean_id
    else:
        imdb_id_for_request = clean_id

    background_tasks.add_task(
        atualizar_cache_e_pedido,
        base_id=base_id,
        tmdb_id=tmdb_id,
        titles=titles,
        type=type,
        novos_flags=novos_flags,
        outras_tarefas=list(outras_tarefas.keys()),
        pending_names=pending_names,
        mywallpaper_teve_resultados=mywallpaper_teve_resultados,
        season=season,
        episode=episode,
        imdb_id_for_request=imdb_id_for_request,
        len_todos_streams=len(todos_streams)
    )

    return JSONResponse(content={"streams": todos_streams})

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
