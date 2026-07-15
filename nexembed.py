import httpx
import re
import urllib.parse
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def unpack_packer(packed_code: str) -> str:
    """
    Decodifica código empacotado com Dean Edwards Packer (eval(function(p,a,c,k,e,d)...))
    """
    pattern = r"}\s*\(\s*'(.*)'\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*'(.*)'\.split\('\|'\)"
    match = re.search(pattern, packed_code, re.DOTALL)
    if not match:
        pattern = r'}\s*\(\s*"(.*)"\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*"(.*)"\.split\("\|"\)'
        match = re.search(pattern, packed_code, re.DOTALL)
        
    if not match:
        return None
        
    p, a, c, k = match.groups()
    a = int(a)
    c = int(c)
    k = k.split('|')
    
    def baseN(num, base):
        digits = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        if num < base:
            return digits[num]
        return baseN(num // base, base) + digits[num % base]

    d = {}
    for i in range(c):
        if i < len(k) and k[i]:
            d[baseN(i, a)] = k[i]
        else:
            d[baseN(i, a)] = baseN(i, a)
            
    def replace_word(m):
        word = m.group(0)
        return d.get(word, word)
        
    p_cleaned = p.replace("\\'", "'").replace('\\"', '"')
    return re.sub(r'\b\w+\b', replace_word, p_cleaned)

async def resolve_nexembed(imdb_id: str = None, tmdb_id: str = None, content_type: str = "movie", season: int = None, episode: int = None, client: httpx.AsyncClient = None, player_url: str = None) -> tuple[str, str] | None:
    """
    Resolve o link HLS (.m3u8) direto de um filme ou episódio hospedado na NexEmbed.
    """
    # Gerencia o client HTTP
    should_close = False
    if client is None:
        client = httpx.AsyncClient(http2=True, verify=False)
        should_close = True

    try:
        if not player_url:
            # 1. Determina a URL inicial do embed baseado nas IDs disponíveis (IMDb preferencial, TMDB como fallback)
            show_id = imdb_id or tmdb_id
            if not show_id:
                print("[NexEmbed Resolver] Nenhuma ID (IMDb/TMDB) fornecida.")
                return None

            if content_type == "movie":
                embed_url = f"https://nexembed.xyz/embed/{show_id}"
            else:
                # Série / Show
                if not season or not episode:
                    print("[NexEmbed Resolver] Temporada e Episódio são obrigatórios para séries.")
                    return None
                embed_url = f"https://nexembed.xyz/embed/{show_id}/{season}/{episode}"

            # Passo 1: Carrega a página de embed da NexEmbed
            print(f"[NexEmbed Resolver] Carregando: {embed_url}")
            resp = await client.get(embed_url, headers=HEADERS, follow_redirects=True, timeout=15.0)
            if resp.status_code != 200:
                print(f"[NexEmbed Resolver] Erro HTTP {resp.status_code} na página de embed.")
                return None
                
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Procura pelo botão com o data-src do player
            btn = soup.find('button', class_='player-btn')
            if not btn:
                btn = soup.find(attrs={"data-src": True})
                
            if not btn:
                print("[NexEmbed Resolver] Botão do player com 'data-src' não foi encontrado.")
                # Alguns layouts podem carregar direto no iframe principal se for embed direto
                # Vamos tentar procurar por iframe com src
                iframe = soup.find('iframe')
                if iframe and iframe.get('src'):
                    player_url = iframe.get('src')
                    if not player_url.startswith('http'):
                        player_url = urllib.parse.urljoin(embed_url, player_url)
                else:
                    return None
            else:
                player_url = btn.get('data-src')

        print(f"[NexEmbed Resolver] URL do player real encontrada: {player_url}")
        
        # Passo 2: Carrega a página do player (normalmente comprarebom.xyz)
        parsed_player = urllib.parse.urlparse(player_url)
        player_domain = parsed_player.netloc
        
        player_headers = {
            **HEADERS,
            "Referer": "https://nexembed.xyz/"
        }
        
        resp2 = await client.get(player_url, headers=player_headers, follow_redirects=True, timeout=15.0)
        if resp2.status_code != 200:
            print(f"[NexEmbed Resolver] Erro HTTP {resp2.status_code} ao carregar player principal.")
            return None
            
        # Passo 3: Encontra o script empacotado que inicializa o FirePlayer
        soup2 = BeautifulSoup(resp2.text, 'html.parser')
        scripts = soup2.find_all('script')
        unpacked_js = None
        for scr in scripts:
            content = scr.get_text()
            if 'eval(function(p,a,c,k,e,d)' in content:
                unpacked_js = unpack_packer(content)
                if unpacked_js:
                    break
                    
        if not unpacked_js:
            print("[NexEmbed Resolver] Código de inicialização do player (FirePlayer) não foi encontrado.")
            return None
            
        # Passo 4: Extrai o ID do vídeo (parâmetro hash/data do FirePlayer)
        id_match = re.search(r'FirePlayer\(\s*"([0-9a-fA-F]+)"', unpacked_js)
        if not id_match:
            print("[NexEmbed Resolver] Hash do FirePlayer não encontrado no script desempacotado.")
            return None
            
        video_id = id_match.group(1)
        print(f"[NexEmbed Resolver] Video ID extraído: {video_id}")
        
        # Passo 5: Faz a requisição AJAX getVideo para recuperar as URLs de streaming diretas
        api_url = f"https://{player_domain}/player/index.php?data={video_id}&do=getVideo"
        api_headers = {
            **HEADERS,
            "Referer": player_url,
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        api_data = {
            "hash": video_id,
            "r": "https://nexembed.xyz/"
        }
        
        resp3 = await client.post(api_url, headers=api_headers, data=api_data, timeout=15.0)
        if resp3.status_code != 200:
            print(f"[NexEmbed Resolver] Erro HTTP {resp3.status_code} na API getVideo.")
            return None
            
        res_json = resp3.json()
        secured_link = res_json.get("securedLink")
        
        if secured_link:
            print(f"[NexEmbed Resolver] Sucesso ao obter streaming link: {secured_link}")
            return player_url, secured_link
            
        return None
        
    except Exception as e:
        print(f"[NexEmbed Resolver] Falha inesperada ao processar embed: {e}")
        return None
    finally:
        if should_close:
            await client.aclose()
