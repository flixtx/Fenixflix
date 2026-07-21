import re
import asyncio
import base64
import urllib.parse
import json
import time
import os
import unicodedata
from bs4 import BeautifulSoup
import httpx
from dotenv import load_dotenv

load_dotenv()

# Headers padrão
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def get_headers_for_url(url, parent_url=None):
    """Retorna os cabeçalhos HTTP corretos para a URL dada."""
    headers = HEADERS.copy()
    if "painelflix.novefx.biz" in url:
        headers["Referer"] = "https://techflixnews.com/"
    elif "painelpsn.novefx.biz" in url:
        headers["Referer"] = "https://novefx.biz/"
    elif parent_url:
        headers["Referer"] = parent_url
    return headers

def js_unpack(source):
    """Descompacta JavaScript obnubilado (P.A.C.K.E.R)."""
    source = source.strip()
    full_pattern = r"eval\s*\(\s*function\s*\(\s*p\s*,\s*a\s*,\s*c\s*,\s*k\s*,\s*e\s*,\s*d\s*\).+?\}\s*\(\s*['\"](.+?)['\"]\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*['\"](.+?)['\"]\s*\.split\s*\(\s*['\"]\|['\"]\s*\)"
    match = re.search(full_pattern, source, re.DOTALL)
    if not match:
        return None
    try:
        payload = match.group(1)
        radix = int(match.group(2))
        count = int(match.group(3))
        keywords = match.group(4).split('|')

        def base_encode(n):
            chars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
            if n < len(chars):
                return chars[n]
            return base_encode(n // len(chars)) + chars[n % len(chars)]

        symtab = {}
        for i in range(count):
            key = base_encode(i)
            val = keywords[i] if i < len(keywords) and keywords[i] else key
            symtab[key] = val

        def replace_token(m):
            return symtab.get(m.group(0), m.group(0))

        result = re.sub(r'\b\w+\b', replace_token, payload)
        return result.replace('\\', '')
    except Exception as e:
        print(f"[NFlix] Erro no unpack JS: {e}")
        return None

async def search_noveflix_async(client, site_type, query):
    """Pesquisa filmes ou séries no Noveflix de forma assíncrona."""
    domain = "noveflixgo.com" if site_type == "tv" else "noveflixfilmes.com"
    search_url = f"https://{domain}/?s={urllib.parse.quote(query)}"

    print(f"[NFlix] Buscando '{query}' em {domain}...")
    try:
        r = await client.get(search_url, headers=HEADERS, timeout=15.0)
        if r.status_code != 200:
            print(f"[NFlix] Erro na busca: HTTP {r.status_code}")
            return []

        soup = BeautifulSoup(r.text, 'html.parser')
        results = []

        articles = soup.find_all('article')
        for art in articles:
            a_tag = art.find('a')
            h3_tag = art.find('h3')
            img_tag = art.find('img')

            if a_tag and (h3_tag or img_tag):
                title = h3_tag.text.strip() if h3_tag else img_tag.get('alt', '').strip()
                link = a_tag.get('href')
                if link and title:
                    results.append({"title": title, "url": link})

        return results
    except Exception as e:
        print(f"[NFlix] Erro na busca: {e}")
        return []

async def get_dooplay_player_embed_async(client, domain, post_id, nume, media_type, referer):
    """Faz o POST para obter a URL do iframe/embed do DooPlay player."""
    ajax_url = f"https://{domain}/wp-admin/admin-ajax.php"
    post_headers = HEADERS.copy()
    post_headers.update({
        "Referer": referer,
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/x-www-form-urlencoded"
    })

    post_data = {
        "action": "doo_player_ajax",
        "post": post_id,
        "nume": nume,
        "type": media_type
    }

    try:
        r = await client.post(ajax_url, data=post_data, headers=post_headers, timeout=10.0)
        if r.status_code == 200:
            return r.json().get("embed_url")
    except Exception as e:
        print(f"[NFlix] Erro ao obter player AJAX do DooPlay: {e}")
    return None

async def resolve_embed_url_async(client, embed_url, max_depth=5):
    """Analisa a URL do embed do player e resolve os encurtadores."""
    if not embed_url or max_depth <= 0:
        return embed_url

    print(f"[NFlix] -> Analisando Embed: {embed_url}")

    # Caso especial: cozinhandocomigo.com
    if "cozinhandocomigo.com" in embed_url and "v.php?video=" in embed_url:
        try:
            parsed = urllib.parse.urlparse(embed_url)
            params = dict(urllib.parse.parse_qsl(parsed.query))
            video_param = params.get("video")
            if video_param and video_param.startswith("http"):
                return await resolve_embed_url_async(client, video_param, max_depth - 1)
        except Exception as e:
            print(f"[NFlix] Erro ao extrair parâmetro video: {e}")

    if "cozinhandocomigo.com" in embed_url:
        try:
            r = await client.get(embed_url, headers=HEADERS, timeout=10.0)
            match = re.search(r'safelink_redirect=([a-zA-Z0-9+/=]+)', r.text)
            if match:
                payload_base64 = match.group(1)
                payload_base64 += "=" * (4 - len(payload_base64) % 4) if len(payload_base64) % 4 else ""
                decoded_json = base64.b64decode(payload_base64).decode('utf-8')
                data = json.loads(decoded_json)
                target_url = urllib.parse.unquote(data.get("safelink", ""))
                if target_url:
                    return await resolve_embed_url_async(client, target_url, max_depth - 1)
        except Exception as e:
            print(f"[NFlix] Erro ao resolver Cozinhando Comigo: {e}")

    # Techflix News / Painelflix
    elif "techflixnews.com" in embed_url or "v.php?video=" in embed_url:
        try:
            if "v.php?video=" in embed_url:
                try:
                    r = await client.get(embed_url, headers=HEADERS, timeout=10.0)
                    soup = BeautifulSoup(r.text, 'html.parser')
                    a_btn = soup.find('a', class_='button')
                    if a_btn and a_btn.get('href'):
                        return await resolve_embed_url_async(client, a_btn.get('href'), max_depth - 1)
                    else:
                        parsed = urllib.parse.urlparse(embed_url)
                        params = dict(urllib.parse.parse_qsl(parsed.query))
                        video_param = params.get("video")
                        if video_param and video_param.startswith("http"):
                            return await resolve_embed_url_async(client, video_param, max_depth - 1)
                except Exception as e:
                    print(f"[NFlix] Erro ao acessar v.php: {e}")
                    parsed = urllib.parse.urlparse(embed_url)
                    params = dict(urllib.parse.parse_qsl(parsed.query))
                    video_param = params.get("video")
                    if video_param and video_param.startswith("http"):
                        return await resolve_embed_url_async(client, video_param, max_depth - 1)

            if "techflixnews.com" in embed_url:
                parsed = urllib.parse.urlparse(embed_url)
                b64_query = parsed.query
                if b64_query:
                    b64_query += "=" * (4 - len(b64_query) % 4) if len(b64_query) % 4 else ""
                    decoded_url = base64.b64decode(b64_query).decode('utf-8')
                    decoded_url = decoded_url.replace("painelflibx", "painelflix")
                    return await resolve_embed_url_async(client, decoded_url, max_depth - 1)
        except Exception as e:
            print(f"[NFlix] Erro ao resolver Techflix News: {e}")

    return embed_url

async def extract_direct_stream_from_player_async(client, player_url, referer_url):
    """Acessa a URL final do CDN/player e extrai a URL direta do vídeo."""
    headers = get_headers_for_url(player_url, referer_url)
    try:
        r = await client.get(player_url, headers=headers, timeout=15.0)
        if r.status_code == 200:
            # Regex robusta para capturar o eval do packer
            match_eval = re.search(
                r'(eval\s*\(\s*function\s*\(\s*p\s*,\s*a\s*,\s*c\s*,\s*k\s*,\s*e\s*,\s*d\s*\).+?\.split\s*\(\s*[\'\"].+?[\'\"]\s*\)[^)]*?\)\s*\))',
                r.text, re.DOTALL
            )
            if match_eval:
                unpacked = js_unpack(match_eval.group(1))
                if unpacked:
                    match_file = re.search(r'[\'"]?file[\'"]?\s*:\s*[\'"](https?://[^\'"]+)[\'"]', unpacked)
                    if match_file:
                        return match_file.group(1)

                    match_any_url = re.search(r'(https?://[^"\'\<\>]+?\.(?:mp4|m3u8)[^"\'\<\>]*)', unpacked)
                    if match_any_url:
                        return match_any_url.group(1)
    except Exception as e:
        print(f"[NFlix] Erro ao extrair link direto de {player_url}: {e}")
    return None

def clean_title(title):
    """Normaliza e limpa o título para correspondência robusta."""
    cleaned = str(title).lower().strip()
    cleaned = re.sub(r'\[.*?\]|\(.*?\)', ' ', cleaned)
    cleaned = unicodedata.normalize('NFKD', cleaned).encode('ASCII', 'ignore').decode('utf-8')
    cleaned = re.sub(r'\b(4k|hd|fullhd|uhd|hdr|hybrid|dublado|legendado|leg|dub|dual|audio|cam|ts)\b', ' ', cleaned)
    cleaned = re.sub(r'\b(19|20)\d{2}\b', ' ', cleaned)
    cleaned = re.sub(r'[^a-z0-9\s]', ' ', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned

def parse_season_episode(label):
    """Extrai temporada e episódio a partir de uma label textual."""
    label_lower = label.lower()
    match = re.search(r'(?:temp(?:orada)?|t|s)\s*(\d+)\s*(?:ep(?:isódio)?|e)\s*(\d+)', label_lower)
    if match:
        return int(match.group(1)), int(match.group(2))
    
    match = re.search(r'(\d+)ª?\s*temp(?:orada)?\s*[-–—]?\s*(\d+)', label_lower)
    if match:
        return int(match.group(1)), int(match.group(2))
        
    match = re.search(r'(\d+)\s*[x\-–—]\s*(\d+)', label_lower)
    if match:
        return int(match.group(1)), int(match.group(2))

    return None, None

def parse_season_from_title(title):
    """Tenta extrair o número da temporada a partir do título da opção de player."""
    title_lower = title.lower()
    
    match = re.search(r'(\d+)\s*ª?\s*temp(?:orada)?|temp(?:orada)?\s*(\d+)', title_lower)
    if match:
        return int(match.group(1) or match.group(2))
        
    match = re.search(r'\bt(?:emp)?\.?\s*(\d+)\b', title_lower)
    if match:
        return int(match.group(1))
        
    return None

def find_best_match(results, query_titles):
    """Encontra o melhor resultado comparando títulos limpos."""
    cleaned_queries = [clean_title(q) for q in query_titles if q]
    for res in results:
        res_cleaned = clean_title(res["title"])
        for q_cleaned in cleaned_queries:
            if res_cleaned == q_cleaned:
                return res
    for res in results:
        res_cleaned = clean_title(res["title"])
        for q_cleaned in cleaned_queries:
            if q_cleaned in res_cleaned or res_cleaned in q_cleaned:
                return res
    return None

# Cache do NFlix
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE_DIR, "cache")
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)
CACHE_FILE = os.path.join(CACHE_DIR, "nflix.json")

def load_cache():
    """Carrega o cache JSON local para o NFlix."""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"[NFlix Cache] Erro ao ler o arquivo cache: {e}")
    return {}

def save_cache(cache_data):
    """Grava os dados no cache JSON local do NFlix."""
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache_data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"[NFlix Cache] Erro crítico ao salvar o cache: {e}")

# FUNÇÃO PRINCIPAL - ASSÍNCRONA PARA INTEGRAÇÃO COM APP.PY
async def search_serve(tmdb_id, content_type, season=None, episode=None, client=None, cached_links=None, titles=None):
    """
    Função principal de busca e resolução de streams para integração no app.py.
    """
    cache = load_cache()
    cache_key = str(tmdb_id).strip().lower()
    
    print(f"[NFlix] Recebida busca para ID: '{cache_key}' (Títulos: {titles})")
    
    # Se não temos títulos, não podemos buscar
    if not titles:
        print("[NFlix] Sem títulos para buscar")
        return []
    
    search_titles = titles if isinstance(titles, list) else [titles]
    
    # Verifica cache
    if cache_key in cache:
        media = cache[cache_key]
        if not media.get("exists", True):
            print(f"[NFlix] ID '{cache_key}' marcado como inexistente no cache.")
            return []
        
        print(f"[NFlix] ID '{cache_key}' encontrado no cache.")
        
        # Caso de Filme
        if media.get("type") == "movie" and content_type == "movie":
            streams = []
            updated = False
            for player in media.get("players", []):
                mp4 = player.get("mp4_url")
                if not mp4 and client:
                    print(f"[NFlix] Resolvendo link direto para player...")
                    mp4 = await extract_direct_stream_from_player_async(
                        client, 
                        player.get("resolved_url", ""), 
                        player.get("resolved_url", "")
                    )
                    if mp4:
                        player["mp4_url"] = mp4
                        updated = True
                
                if mp4:
                    streams.append({
                        "name": "FenixFlix\nNFlix",
                        "title": f"{search_titles[0]}\nNFlix",
                        "url": mp4,
                        "behaviorHints": {"notWebReady": False, "bingeGroup": "fenixflix-nflix"}
                    })
            if updated:
                save_cache(cache)
            return streams
            
        # Caso de Série
        elif media.get("type") == "series" and content_type == "series":
            s_str = str(season) if season else "1"
            e_str = str(episode) if episode else "1"
            
            seasons = media.get("seasons", {})
            if s_str in seasons and e_str in seasons[s_str]:
                episode_players = seasons[s_str][e_str]
                streams = []
                updated = False
                for player in episode_players:
                    mp4 = player.get("mp4_url")
                    if not mp4 and client:
                        print(f"[NFlix] Resolvendo link direto para ep S{s_str}E{e_str}...")
                        mp4 = await extract_direct_stream_from_player_async(
                            client,
                            player.get("embed_url", ""),
                            player.get("resolved_url", "")
                        )
                        if mp4:
                            player["mp4_url"] = mp4
                            updated = True
                            
                    if mp4:
                        streams.append({
                            "name": "FenixFlix\nNFlix",
                            "title": f"{search_titles[0]}\nNFlix",
                            "url": mp4,
                            "behaviorHints": {"notWebReady": False, "bingeGroup": "fenixflix-nflix"}
                        })
                if updated:
                    save_cache(cache)
                return streams
    
    # Se não está no cache, faz a busca (se tiver cliente)
    if not client:
        return []
    
    site_type = "tv" if content_type == "series" else "filmes"
    selected_result = None
    
    # Busca por cada título
    for title in search_titles:
        if not title:
            continue
        
        results = await search_noveflix_async(client, site_type, title)
        if results:
            selected_result = find_best_match(results, search_titles)
            if selected_result:
                break
    
    if not selected_result:
        print(f"[NFlix] Nenhum resultado encontrado para {search_titles}.")
        cache[cache_key] = {"exists": False}
        save_cache(cache)
        return []
    
    selected_title = selected_result["title"]
    selected_url = selected_result["url"]
    domain = urllib.parse.urlparse(selected_url).netloc
    
    print(f"[NFlix] Selecionado: '{selected_title}' -> {selected_url}")
    
    try:
        r = await client.get(selected_url, headers=HEADERS, timeout=15.0)
        soup = BeautifulSoup(r.text, 'html.parser')
        
        player_options = soup.find('ul', id='playeroptionsul')
        if not player_options:
            print("[NFlix] Nenhum player do DooPlay encontrado.")
            cache[cache_key] = {"exists": False}
            save_cache(cache)
            return []
        
        options_li = player_options.find_all('li', class_='dooplay_player_option')
        
        # Resolve para Filme
        if content_type == "movie":
            players_list = []
            for li in options_li:
                nume = li.get('data-nume')
                post_id = li.get('data-post')
                media_type = li.get('data-type')
                opt_title = li.find('span', class_='title')
                
                if nume == "trailer":
                    continue
                    
                embed_url = await get_dooplay_player_embed_async(
                    client, domain, post_id, nume, media_type, selected_url
                )
                if embed_url:
                    resolved_url = await resolve_embed_url_async(client, embed_url)
                    if resolved_url:
                        players_list.append({
                            "resolved_url": resolved_url,
                            "mp4_url": None
                        })
            
            media_entry = {
                "exists": True,
                "title": selected_title,
                "url": selected_url,
                "type": "movie",
                "players": players_list
            }
            cache[cache_key] = media_entry
            save_cache(cache)
            
            # Resolve os players
            streams = []
            updated = False
            for player in media_entry["players"]:
                mp4 = await extract_direct_stream_from_player_async(
                    client, player["resolved_url"], player["resolved_url"]
                )
                if mp4:
                    player["mp4_url"] = mp4
                    updated = True
                    streams.append({
                        "name": "FenixFlix\nNFlix",
                        "title": f"{search_titles[0]}\nNFlix",
                        "url": mp4,
                        "behaviorHints": {"notWebReady": False, "bingeGroup": "fenixflix-nflix"}
                    })
            if updated:
                save_cache(cache)
            return streams
            
        # Resolve para Série
        elif content_type == "series":
            seasons_dict = {}
            for li in options_li:
                nume = li.get('data-nume')
                post_id = li.get('data-post')
                media_type = li.get('data-type')
                opt_title = li.find('span', class_='title')
                opt_title_str = opt_title.text.strip() if opt_title else f"Player {nume}"
                
                if nume == "trailer":
                    continue
                    
                # Filtra pela temporada
                if season is not None:
                    player_season = parse_season_from_title(opt_title_str)
                    if player_season is not None and player_season != int(season):
                        continue

                embed_url = await get_dooplay_player_embed_async(
                    client, domain, post_id, nume, media_type, selected_url
                )
                if embed_url:
                    resolved_url = await resolve_embed_url_async(client, embed_url)
                    if resolved_url:
                        res_headers = get_headers_for_url(resolved_url, selected_url)
                        try:
                            r_res = await client.get(resolved_url, headers=res_headers, timeout=15.0)
                            if r_res.status_code == 200:
                                soup_res = BeautifulSoup(r_res.text, 'html.parser')
                                select_cap = soup_res.find('select', id='capitulos')
                                if select_cap:
                                    player_season = parse_season_from_title(opt_title_str)
                                    options = select_cap.find_all('option')
                                    for opt in options:
                                        val = opt.get('value')
                                        label = opt.text.strip()
                                        if val and ("novefx" in val or "painelpsn" in val):
                                            s_num, e_num = parse_season_episode(label)
                                            if s_num is None or e_num is None:
                                                match_ep = re.search(r'(?:ep(?:isódio)?|cap(?:ítulo)?|e)\s*(\d+)', label.lower())
                                                if not match_ep:
                                                    match_ep = re.search(r'\b(\d+)\b', label.lower())
                                                if match_ep:
                                                    e_num = int(match_ep.group(1))
                                                    s_num = player_season if player_season is not None else 1

                                            if s_num is not None and e_num is not None:
                                                s_str = str(s_num)
                                                e_str = str(e_num)
                                                
                                                if s_str not in seasons_dict:
                                                    seasons_dict[s_str] = {}
                                                if e_str not in seasons_dict[s_str]:
                                                    seasons_dict[s_str][e_str] = []
                                                    
                                                seasons_dict[s_str][e_str].append({
                                                    "embed_url": val,
                                                    "resolved_url": resolved_url,
                                                    "mp4_url": None
                                                })
                        except Exception as e:
                            print(f"[NFlix] Erro ao ler capitulos: {e}")
            
            # Salva cache
            media_entry = {
                "exists": True,
                "title": selected_title,
                "url": selected_url,
                "type": "series",
                "available_seasons": sorted(list(seasons_dict.keys()), key=int),
                "seasons": seasons_dict
            }
            cache[cache_key] = media_entry
            save_cache(cache)
            
            # Resolve o episódio específico
            s_str = str(season) if season else "1"
            e_str = str(episode) if episode else "1"
            
            streams = []
            if s_str in seasons_dict and e_str in seasons_dict[s_str]:
                episode_players = seasons_dict[s_str][e_str]
                updated = False
                for player in episode_players:
                    mp4 = await extract_direct_stream_from_player_async(
                        client, player["embed_url"], player["resolved_url"]
                    )
                    if mp4:
                        player["mp4_url"] = mp4
                        updated = True
                        streams.append({
                            "name": "FenixFlix\nNFlix",
                            "title": f"{search_titles[0]}\nNFlix",
                            "url": mp4,
                            "behaviorHints": {"notWebReady": False, "bingeGroup": "fenixflix-nflix"}
                        })
                if updated:
                    save_cache(cache)
            return streams
            
    except Exception as e:
        print(f"[NFlix] Erro durante o fluxo: {e}")
    
    return []
