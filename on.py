import httpx
from bs4 import BeautifulSoup
import re
from urllib.parse import unquote
import asyncio
import time

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"

cache_final_links = {}

_RE_IFRAME_PLAYER = re.compile(
    r'<iframe\s+[^>]*src=["\']([^"\']*player_2\.php[^"\']*)["\']', 
    re.IGNORECASE
)


def parse_iframe(html_text):
    """Lê o HTML do site 1take e encontra o iframe do player."""
    match = _RE_IFRAME_PLAYER.search(html_text)
    if match:
        url = match.group(1)
        if url.startswith("//"):
            return "https:" + url
        return url
    return None


def parse_mediafire_btn(html_text):
    """Lê o HTML do Mediafire e encontra o botão final de download (MP4)."""
    soup = BeautifulSoup(html_text, 'lxml')
    btn = soup.select_one("a#downloadButton")
    if btn:
        return btn.get("href")
    return None

# --------------------------------------------------------

async def extrair_link(label, cache_key, target_url, client, cached_mediafire_url=None, nome=None, tep=None, season=None):
    now = time.time()
    
    # Limpeza de expirados e controle de tamanho da RAM (máx 1000 itens)
    if len(cache_final_links) > 200:
        expired_keys = [k for k, v in cache_final_links.items() if (now - v["time"]) >= 7200]
        for ek in expired_keys:
            cache_final_links.pop(ek, None)
            
    if len(cache_final_links) > 1000:
        sorted_keys = sorted(cache_final_links.keys(), key=lambda k: cache_final_links[k]["time"])
        for k_to_del in sorted_keys[:200]:
            cache_final_links.pop(k_to_del, None)

    ram_key = f"{cache_key}:{target_url}"
    if ram_key in cache_final_links:
        cached_entry = cache_final_links[ram_key]
        if (now - cached_entry["time"]) < 7200: # 2 horas
            print(f"[Azullog Debug] ⚡ Usando link MP4 direto em RAM para {label} ({cache_key})!")
            partes = []
            if nome:
                partes.append(nome)
            if tep:
                partes.append(tep)
            partes.append(f"{label}\nON")
            desc = "\n".join(partes)

            name_str = "ON"
            if season is not None and str(season).strip():
                name_str = f"ON - Temporada {season}"

            return {
                "name": "FenixFlix",
                "description": desc,
                "url": cached_entry["url"],
                "behaviorHints": {"notWebReady": False, "bingeGroup": f"fenixflix-azullog-{label.lower()}"},
                "_mediafire_url": cached_entry["mediafire_url"],
                "_cache_key": cache_key,
                "_label": label
            }

    mediafire_url = cached_mediafire_url

    # 1. Se o cache disser "N", pula tudo!
    if mediafire_url == "N":
        print(f"[Azullog Debug] ⏭️ Cache negativo ('N') encontrado para {label} ({cache_key}). Pulando busca pesada!")
        return {"_cache_key": cache_key, "_label": label, "_mediafire_url": "N"}

    # 2. Se tem um link real do Mediafire no cache, usa ele
    elif mediafire_url:
        print(f"[Azullog Debug] ⚡ Usando cache do scrapers_status.json pro Mediafire {label} ({cache_key}): {mediafire_url}")

    # 3. Se não tem nada no cache, faz o scraping
    else:
        try:
            print(f"[Azullog Debug] 🔍 Testando link direto: {target_url}")
            res = await client.get(target_url, headers={"User-Agent": USER_AGENT, "accept": "*/*"})

            if res.status_code == 200:
                # MÁGICA AQUI: O parse pesado do BeautifulSoup vai para uma Thread separada!
                # O FastAPI fica livre para processar outros usuários enquanto isso acontece.
                player_url = await asyncio.to_thread(parse_iframe, res.text)

                if player_url:
                    print(f"[Azullog Debug] Resolvendo player ({label}): {player_url}")
                    res2 = await client.get(player_url, headers={"referer": target_url, "User-Agent": USER_AGENT})

                    api_url_match = re.search(r"const apiUrl = `([^`]+)`", res2.text)
                    if api_url_match:
                        api_url = api_url_match.group(1)
                        mediafire_enc_match = re.search(r"[?&]url=([^&]+)", api_url)

                        if mediafire_enc_match:
                            mediafire_url = unquote(mediafire_enc_match.group(1))
                            print(f"[Azullog Debug] Mediafire URL encontrada ({label})!")
        except Exception as e:
            print(f"[Azullog Debug] Erro durante a extração de {label}: {e}")

    # 4. Aborta se não conseguiu a URL base
    if not mediafire_url:
        print(f"[Azullog Debug] ❌ Falha ao obter link base para {label}. Marcando cache como 'N'.")
        return {"_cache_key": cache_key, "_label": label, "_mediafire_url": "N"}

    # 5. Tendo o link base, entra no Mediafire e pega o botão MP4 fresco
    try:
        mf_res = await client.get(mediafire_url, headers={"User-Agent": USER_AGENT})

        # MÁGICA AQUI: O parse do Mediafire também vai para uma Thread separada!
        final_mp4 = await asyncio.to_thread(parse_mediafire_btn, mf_res.text)

        if final_mp4:
            cache_final_links[ram_key] = {
                "url": final_mp4,
                "mediafire_url": mediafire_url,
                "time": now
            }
            partes = []
            if nome:
                partes.append(nome)
            if tep:
                partes.append(tep)
            partes.append(f"{label}\nON")
            desc = "\n".join(partes)

            name_str = "ON"
            if season is not None and str(season).strip():
                name_str = f"ON - Temporada {season}"

            return {
                "name": "FenixFlix",
                "description": desc,
                "url": final_mp4,
                "behaviorHints": {"notWebReady": False, "bingeGroup": f"fenixflix-azullog-{label.lower()}"},
                "_mediafire_url": mediafire_url,
                "_cache_key": cache_key,
                "_label": label
            }
        else:
            print(f"[Azullog Debug] ❌ Botão MP4 não encontrado no Mediafire ({label}). Marcando como 'N'.")
            return {"_cache_key": cache_key, "_label": label, "_mediafire_url": "N"}

    except Exception as e:
        print(f"[Azullog Debug] ❌ Erro ao extrair o botão MP4 para {label}: {e}. Marcando cache como 'N'.")
        return {"_cache_key": cache_key, "_label": label, "_mediafire_url": "N"}

    return None

async def search_serve(tmdb_id: str, content_type: str, season=None, episode=None, client: httpx.AsyncClient = None, cached_links=None, titles: list = None):
    streams = []
    urls_to_check = []

    if cached_links is None:
        cached_links = {}

    print(f"\n[Azullog Debug] Iniciando busca para TMDB ID: {tmdb_id} | Tipo: {content_type} | S{season}E{episode}")

    nome = ""
    if titles:
        if isinstance(titles, list) or isinstance(titles, tuple):
            nome = titles[0] if len(titles) > 0 else ""
        else:
            nome = str(titles)

    tep = ""
    if content_type == "series":
        try:
            s_pad = f"{int(season):02d}"
            e_pad = f"{int(episode):02d}"
            tep = f"T{s_pad} EP{e_pad}"
        except Exception:
            tep = f"T{season} EP{episode}"

    if content_type == "movie":
        urls_to_check.append(("Dublado", "D", f"https://1take.top/e/tmdb{tmdb_id}dub"))
        urls_to_check.append(("Legendado", "L", f"https://1take.top/e/tmdb{tmdb_id}leg"))
    else:
        urls_to_check.append(("Dublado", "D", f"https://1take.top/e/tvtmdb{tmdb_id}t{season}e{episode}dub"))
        urls_to_check.append(("Legendado", "L", f"https://1take.top/e/tvtmdb{tmdb_id}t{season}e{episode}leg"))

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=15.0, follow_redirects=True)
        close_client = True

    try:
        tarefas = [extrair_link(label, cache_key, target_url, client, cached_links.get(cache_key), nome=nome, tep=tep, season=season if content_type == "series" else None) for label, cache_key, target_url in urls_to_check]
        resultados = await asyncio.gather(*tarefas)

        dublado_ok = False
        legendado_ok = False

        for res in resultados:
            if res:
                streams.append(res)
                if res.get("url"):
                    if res.get("_cache_key") == "D":
                        dublado_ok = True
                    elif res.get("_cache_key") == "L":
                        legendado_ok = True

        if dublado_ok and not legendado_ok:
            print(f"[Azullog Debug] É dublado, mas não tem legendado disponível pro TMDB {tmdb_id}.")
        elif legendado_ok and not dublado_ok:
            print(f"[Azullog Debug] Tem legendado, mas não achamos o dublado pro TMDB {tmdb_id}.")
        elif dublado_ok and legendado_ok:
            print(f"[Azullog Debug] Tudo certo, achei as versões dublado e legendado pro TMDB {tmdb_id}.")
        else:
            print(f"[Azullog Debug] Infelizmente nenhum link foi encontrado pro TMDB {tmdb_id}.")

    finally:
        if close_client:
            await client.aclose()

    return streams

if __name__ == "__main__":
    async def rodar_teste():
        cache_simulado = {"D": "https://www.mediafire.com/file_premium/simulacao_filme", "L": "N"}
        streams_filme = await search_serve("550", "movie", cached_links=cache_simulado, titles=["Clube da Luta"])
        print("\nFilme Resultado Final:", streams_filme)

    asyncio.run(rodar_teste())
