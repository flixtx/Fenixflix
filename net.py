import re
import json
import httpx
import asyncio

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

async def search_serve(tmdb_id: str, content_type: str, season=None, episode=None, client: httpx.AsyncClient = None, titles: list = None):
    if not tmdb_id:
        return []
    
    if content_type == "movie":
        url = f"https://embed-api.clickhost.xyz/embed/filme/{tmdb_id}"
    elif content_type == "series":
        if season is None or episode is None:
            return []
        url = f"https://embed-api.clickhost.xyz/embed/serie/{tmdb_id}/{season}/{episode}"
    else:
        return []
    
    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=15.0, follow_redirects=True, verify=False)
        close_client = True
        
    streams = []
    try:
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        }
        print(f"[NET Scraper] Realizando requisição para: {url}")
        response = await client.get(url, headers=headers, timeout=10.0)
        
        if response.status_code != 200:
            print(f"[NET Scraper] Código de status HTTP inválido: {response.status_code}")
            return []
            
        html_content = response.text
        
        match = re.search(r'const\s+servers\s*=\s*(\[.*?\])\s*;', html_content, re.DOTALL)
        if match:
            servers_json_string = match.group(1)
            try:
                servers = json.loads(servers_json_string)
            except Exception as json_err:
                print(f"[NET Scraper] Erro ao decodificar JSON dos servidores: {json_err}")
                return []
                
            title_name = ""
            if titles:
                if isinstance(titles, list) or isinstance(titles, tuple):
                    title_name = titles[0] if len(titles) > 0 else ""
                else:
                    title_name = str(titles)
            
            ep_info = ""
            if content_type == "series":
                try:
                    ep_info = f"T{int(season):02d} EP{int(episode):02d} "
                except Exception:
                    ep_info = f"T{season} EP{episode} "
                
            for server in servers:
                server_url = server.get("url")
                server_label = server.get("label", "Desconhecido")
                
                label_lower = server_label.lower() if server_label else ""
                url_lower = server_url.lower() if server_url else ""
                
                blocked_terms = ["dnxs70", "xwpluss", "xwplus", "xwpus"]
                if any(term in label_lower for term in blocked_terms) or any(term in url_lower for term in blocked_terms):
                    continue
                
                if not server_url:
                    continue
                    
                if server_url.startswith("/embed/"):
                    final_url = f"https://embed-api.clickhost.xyz{server_url}"
                elif server_url.startswith("/"):
                    final_url = f"https://embed-api.clickhost.xyz/embed{server_url}"
                else:
                    final_url = f"https://embed-api.clickhost.xyz/embed/{server_url}"
                
                quality = ""
                url_lower = final_url.lower()
                label_lower = server_label.lower()
                if "1080" in url_lower or "1080" in label_lower:
                    quality = "1080p"
                elif "720" in url_lower or "720" in label_lower:
                    quality = "720p"
                elif "4k" in url_lower or "2160" in label_lower:
                    quality = "4K"
                
                display_label = server_label
                if "mts-server" in label_lower:
                    display_label = "server"
                elif "principal p2p" in label_lower:
                    display_label = "principal"
                
                if content_type == "movie":
                    description = f"{title_name}\nNet({display_label})"
                else:
                    description = f"{title_name}\n{ep_info}\nNet({display_label})"
                
                streams.append({
                    "name": f"FenixFlix\n{quality}",
                    "description": description,
                    "url": final_url,
                    "behaviorHints": {
                        "notWebReady": False,
                        "bingeGroup": "fenixflix-net"
                    }
                })
        else:
            print("[NET Scraper] Variável 'servers' não encontrada no código HTML.")
            
    except Exception as e:
        print(f"[NET Scraper] Erro durante o scraping: {e}")
    finally:
        if close_client:
            await client.aclose()
            
    return streams

if __name__ == "__main__":
    async def rodar_teste():
        print("=== Testando NET Scraper ===")
        # Test movie
        streams_movie = await search_serve("603", "movie", titles=["Matrix"])
        print("\nFilme Resultado Final:", json.dumps(streams_movie, indent=2))
        
        # Test series
        streams_series = await search_serve("1399", "series", season=1, episode=1, titles=["Game of Thrones"])
        print("\nSérie Resultado Final:", json.dumps(streams_series, indent=2))

    asyncio.run(rodar_teste())
