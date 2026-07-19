import httpx
import re

async def resolve_redeflix(url: str, client: httpx.AsyncClient = None) -> str:
    """
    Extrai o link direto (mp4/m3u8) da página HTML da RedeFlix.
    """
    should_close = False
    if client is None:
        client = httpx.AsyncClient(http2=True, verify=False)
        should_close = True

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Referer": "https://redeflixapi.store/"
        }

        resp = await client.get(url, headers=headers, timeout=15.0)
        if resp.status_code == 200:
            # Tenta encontrar a variável defaultUrl = "link"
            match = re.search(r'const\s+defaultUrl\s*=\s*["\'](.*?)["\']', resp.text)
            if match:
                return match.group(1)
            
            # Se não achar defaultUrl, procura por mp4/m3u8 genérico no código
            match_generic = re.search(r'(https?://[^"\']*\.(?:mp4|m3u8)[^"\']*)', resp.text)
            if match_generic:
                return match_generic.group(1)
                
        return None
    except Exception as e:
        print(f"[RedeFlix Resolver] Erro ao extrair link de {url}: {e}")
        return None
    finally:
        if should_close:
            await client.aclose()
