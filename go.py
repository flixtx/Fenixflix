import asyncio
import httpx
from bs4 import BeautifulSoup
from urllib.parse import quote, urljoin, urlparse
import re
import json
import base64
import hashlib
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

# Importar manualmente a biblioteca Crypto.Util.Padding se não estiver disponível
try:
    from Crypto.Util.Padding import unpad
except ImportError:
    # Fallback para uma implementação simples de unpad se a biblioteca não estiver presente
    def unpad(data, block_size):
        padding_len = data[-1]
        if padding_len < 1 or padding_len > block_size:
            raise ValueError("Padding is incorrect.")
        return data[:-padding_len]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
}

def evp_kdf(password, salt, key_size, iv_size):
    d = d_i = b''
    while len(d) < key_size + iv_size:
        d_i = hashlib.md5(d_i + password + salt).digest()
        d += d_i
    return d[:key_size], d[key_size:key_size+iv_size]

def descriptografar_link(encrypted_str, password):
    try:
        obj = json.loads(encrypted_str)
        salt = bytes.fromhex(obj['s'])
        ct = base64.b64decode(obj['ct'])
        iv = bytes.fromhex(obj['iv'])
        
        # A senha deve ser bytes para hashlib.md5
        key, _ = evp_kdf(password.encode('utf-8'), salt, 32, 16)
        cipher = AES.new(key, AES.MODE_CBC, iv)
        
        # Certificar-se de que unpad usa AES.block_size
        decrypted = unpad(cipher.decrypt(ct), AES.block_size)
        return decrypted.decode('utf-8').replace('"', '')
    except Exception as e:
        print(f"[Go Debug] Erro ao descriptografar: {e}")
        return None

def decodificar_ck(ck_raw):
    try:
        # Tenta extrair hexadecimais
        hexvals = re.findall(r'\\x([0-9a-fA-F]{2})', ck_raw)
        if hexvals:
            b64_str_encoded = bytes.fromhex(''.join(hexvals))
            try:
                # Tenta decodificar como UTF-8 primeiro
                b64_str = b64_str_encoded.decode('utf-8')
            except UnicodeDecodeError:
                # Se falhar, usa a string bruta
                b64_str = b64_str_encoded.decode('latin-1') # Ou outra codificação adequada
            
            # Tenta decodificar base64 sem padding extra primeiro
            try:
                return base64.b64decode(b64_str).decode('utf-8')
            except Exception:
                # Se falhar, tenta com padding
                return base64.b64decode(b64_str + '==').decode('utf-8')
        return ck_raw
    except Exception as e:
        print(f"[Go Debug] Erro ao decodificar ck: {e}")
        return ck_raw

async def resolver_master_txt(client, master_url, referer):
    headers = {'Referer': referer}
    try:
        res = await client.get(master_url, headers=headers)
        conteudo = res.text.strip()
        base = master_url.rsplit('/master.txt', 1)[0]

        # Regex mais específica para URLs base64, que geralmente terminam com = ou ==
        b64s = re.findall(r'https?://[a-zA-Z0-9./\-]+(?:[a-zA-Z0-9+/]{4})*(?:[a-zA-Z0-9+/]{2}==|[a-zA-Z0-9+/]{3}=)?', conteudo)
        for b64_url in reversed(b64s):
            try:
                # Tenta decodificar a parte base64 da URL se houver
                # Isso é uma heurística, pode precisar de ajuste dependendo do formato exato
                if 'base64,' in b64_url:
                    path_b64 = b64_url.split('base64,')[1]
                    caminho = base64.b64decode(path_b64 + '==').decode('utf-8').strip()
                else:
                    caminho = b64_url # Se não for base64, assume que é a URL completa

                if caminho.endswith('.m3u8') or '/hls/' in caminho:
                    return caminho if caminho.startswith('http') else urljoin(base + '/', caminho)
            except Exception as e:
                print(f"[Go Debug] Erro ao decodificar b64_url em master.txt: {e}")
                continue

        for linha in conteudo.splitlines():
            linha = linha.strip()
            if linha.endswith('.m3u8') or '/hls/' in linha:
                return linha if linha.startswith('http') else urljoin(base + '/', linha)
    except Exception as e:
        print(f"[Go Debug] Erro ao resolver master.txt: {e}")
    return None

async def extrair_embedplayer1(client, url_video):
    video_id = url_video.split('/')[-1].split('?')[0]
    api_url = f'https://embedplayer1.xyz/player/index.php?data={video_id}&do=getVideo'

    headers_get  = {'Referer': 'https://gofilmeshd.top/'}
    headers_post = {**headers_get, 'X-Requested-With': 'XMLHttpRequest'}

    try:
        await client.get(url_video, headers=headers_get)
        response = await client.post(api_url, headers=headers_post, data={'hash': video_id, 'r': 'https://gofilmeshd.top/'})
        dados = response.json()

        secured = dados.get('securedLink', '')
        if secured and ('.m3u8' in secured or '/hls/' in secured):
            return secured

        video_source = dados.get('videoSource', '')
        if video_source:
            if 'master.txt' in video_source:
                url_real = await resolver_master_txt(client, video_source, 'https://embedplayer1.xyz/')
                if url_real:
                    return url_real
            if video_source.endswith('.m3u8') or '/hls/' in video_source:
                return video_source

        ck_raw   = dados.get('ck', '')
        chave_ck = decodificar_ck(ck_raw) if ck_raw else ''
        if 'videoSources' in dados:
            for source in dados['videoSources']:
                raw_file = source.get('file', '')
                if '{"ct":' in raw_file:
                    dec = descriptografar_link(raw_file, chave_ck)
                    if dec:
                        # Usar urljoin para garantir URLs absolutas corretas
                        if dec.startswith('http'): return dec
                        if '/m3/' in dec or '/hls/' in dec: return urljoin('https://embedplayer1.xyz', dec)
                elif raw_file.startswith('http'): return raw_file
                elif '/m3/' in raw_file or '/hls/' in raw_file: return urljoin('https://embedplayer1.xyz', raw_file)

        return None
    except Exception as e:
        print(f"[Go Debug] Erro no embedplayer1: {e}")
        return None

async def resolve_stream_async(client, player_url):
    try:
        r = await client.get(player_url)
        html = r.text

        # Tenta encontrar videoSrc ou master.txt na página principal
        m_match = re.search(r'const\s+videoSrc\s*=\s*["\']([^"\']*?(?:m3u8|master\.txt)[^"\']*)["\']', html) # Mais específico para m3u8 ou master.txt
        if not m_match:
            m_match = re.search(r'(https?://[^"\s]+/master\.txt[^"\s]*)', html)

        if m_match:
            m_url    = m_match.group(1).split('#')[0]
            # Determinar o referer com base na URL do master.txt
            parsed_m_url = urlparse(m_url)
            referer_base = f"{parsed_m_url.scheme}://{parsed_m_url.netloc}/"
            
            url_real = await resolver_master_txt(client, m_url, referer_base)
            return url_real or m_url, {'Referer': referer_base, 'Origin': referer_base.rstrip('/')}

        mf_match = re.search(r'(https?://[^\s"\\]+mediafire\.com[^\s"\\]*)', html)
        if mf_match:
            print(f"[Go Debug] Mediafire encontrado na página principal.")
            return mf_match.group(1), {'Referer': player_url}

        soup   = BeautifulSoup(html, 'lxml')
        iframe = soup.find('iframe')
        src    = iframe.get('src') if iframe else None

        if src:
            if src.startswith('//'):
                src = 'https:' + src
            
            # Normaliza o src para garantir que é uma URL absoluta
            src_absolute = urljoin(player_url, src)

            if 'embedplayer1.xyz' in src_absolute:
                link = await extrair_embedplayer1(client, src_absolute)
                if link:
                    return link, {'Referer': 'https://embedplayer1.xyz/', 'Origin': 'https://embedplayer1.xyz'}

            elif '112234152.xyz' in src_absolute:
                vid        = src_absolute.split('?data=')[-1].split('&')[0]
                master_url = f'https://112234152.xyz/hls/{vid}/master.txt'
                referer    = 'https://112234152.xyz/'
                url_real   = await resolver_master_txt(client, master_url, referer)
                return url_real or f'https://112234152.xyz/hls/{vid}/index.m3u8', {'Referer': referer}

            elif 'mediafire.com' in src_absolute:
                return src_absolute, {'Referer': player_url}

            else:
                # Se o iframe não for um dos conhecidos, tenta buscar o conteúdo do iframe
                res_iframe = await client.get(src_absolute, headers={'Referer': player_url})

                m_match = re.search(r'const\s+videoSrc\s*=\s*["\']([^"\']*?(?:m3u8|master\.txt)[^"\']*)["\']', res_iframe.text) # Mais específico para m3u8 ou master.txt
                if not m_match:
                    m_match = re.search(r'(https?://[^"\s]+/master\.txt[^"\s]*)', res_iframe.text)

                if m_match:
                    stream_url = m_match.group(1)
                    origin     = f"https://{urlparse(src_absolute).netloc}"
                    return stream_url, {'Origin': origin, 'Referer': origin + '/'}

                mf_match = re.search(r'(https?://[^\s"\\]+mediafire\.com[^\s"\\]*)', res_iframe.text)
                if mf_match:
                    return mf_match.group(1), {'Referer': src_absolute}

    except Exception as e:
        print(f"[Go Debug] Erro no resolve_stream_async: {e}")

    return '', {}

def gerar_slugs(title):
    limpo = re.sub(r'[^\w\s-]', '', title)
    limpo = re.sub(r'\s+', ' ', limpo).strip()
    slug_principal = re.sub(r'-+', '-', limpo.replace(' ', '-')).lower()

    slugs = [slug_principal]

    sem_artigos = re.sub(r'\b(o|a|os|as|the|de|da|do|dos|das|em|por|para)\b', '', limpo, flags=re.IGNORECASE)
    sem_artigos = re.sub(r'\s+', ' ', sem_artigos).strip()
    slug_sem_artigos = re.sub(r'-+', '-', sem_artigos.replace(' ', '-')).lower()
    if slug_sem_artigos and slug_sem_artigos != slug_principal:
        slugs.append(slug_sem_artigos)

    palavras = limpo.split()
    if len(palavras) > 3:
        slug_curto = '-'.join(palavras[:3]).lower()
        if slug_curto and slug_curto not in slugs:
            slugs.append(slug_curto)

    return slugs

def extrair_opcoes(html, content_type, season, episode):
    soup = BeautifulSoup(html, 'lxml')
    opts = []
    base_url = 'https://gofilmeshd.top'

    if content_type == 'series' and season and episode:
        # A nova estrutura usa botões para temporadas e divs para episódios
        season_buttons = soup.select('button')
        target_panel = None

        for btn in season_buttons:
            btn_text = btn.get_text(strip=True)
            match = re.search(r'(\d+)º?\s*[Tt]emporada', btn_text, re.IGNORECASE)
            if match and int(match.group(1)) == int(season):
                # O painel de episódios é o div irmão seguinte ao botão da temporada
                target_panel = btn.find_next_sibling('div')
                break
        
        if target_panel:
            # Episódios estão dentro de <a> tags diretamente no div do painel
            links = target_panel.select('a[href]')
            ep_idx = int(episode) - 1
            if 0 <= ep_idx < len(links):
                opts.append({
                    'name': f"GoFilmes - S{season}E{episode}",
                    'url': urljoin(base_url, links[ep_idx].get('href'))
                })
    else:
        # Para filmes, a lógica original de 'div.link a[href]' parece estar correta
        links = soup.select('div.link a[href]')
        for l in links:
            opts.append({
                'name': f"GoFilmes - {l.get_text(strip=True)}",
                'url': urljoin(base_url, l.get('href'))
            })
    return opts

async def tentar_slug(client, slug, content_type, season, episode):
    base_url = 'https://gofilmeshd.top'
    path = 'series' if content_type == 'series' else ''
    url = f"{base_url}/{path}/{quote(slug)}" if path else f"{base_url}/{quote(slug)}"
    print(f"[Go Debug] Testando URL: {url}")
    
    try:
        res = await client.get(url)
        if res.status_code == 200:
            print(f"[Go Debug] Sucesso! Página encontrada para '{slug}'")
            return extrair_opcoes(res.text, content_type, season, episode)
    except httpx.HTTPStatusError as e:
        print(f"[Go Debug] Erro HTTP ao buscar slug '{slug}': {e.response.status_code}")
    except Exception as e:
        print(f"[Go Debug] Erro ao buscar slug '{slug}': {e}")
    return None

async def search_gofilmes(client, titles, content_type, season=None, episode=None):
    slugs_para_tentar = []
    for title in titles:
        slugs_para_tentar.extend(gerar_slugs(title))

    vistos = set()
    # Garante slugs únicos e mantém a ordem de geração
    slugs_unicos = []
    for s in slugs_para_tentar:
        if s not in vistos:
            slugs_unicos.append(s)
            vistos.add(s)

    print(f"[Go Debug] Total de variações de nomes a testar em paralelo: {len(slugs_unicos)}")

    sem = asyncio.Semaphore(6)

    async def tentar_com_semaforo(slug):
        async with sem:
            return await tentar_slug(client, slug, content_type, season, episode)

    tarefas = [asyncio.create_task(tentar_com_semaforo(slug)) for slug in slugs_unicos]

    for resultado_futuro in asyncio.as_completed(tarefas):
        try:
            opcoes = await resultado_futuro
            if opcoes:
                print("[Go Debug] Sucesso! Encontramos o certo. Cancelando o resto para acelerar.")
                for t in tarefas:
                    if not t.done():
                        t.cancel()
                return opcoes
        except asyncio.CancelledError:
            # Ignorar tarefas canceladas
            pass
        except Exception as e:
            print(f"[Go Debug] Erro durante a resolução de slug: {e}")
            
    return []

async def search_serve(titles, content_type, season=None, episode=None):
    results = []
    print(f"\n[Go Debug] Iniciando busca para títulos: {titles} | Tipo: {content_type}")

    async with httpx.AsyncClient(
        headers=HEADERS,
        timeout=10.0,
        follow_redirects=True
    ) as client:
        
        options = await search_gofilmes(client, titles, content_type, season, episode)

        if not options:
            print("[Go Debug] Nenhuma opção de link encontrada no site principal.")
            return results

        print(f"[Go Debug] Encontradas {len(options)} opções de players. Resolvendo...")

        # Limitar o número de streams a resolver para evitar sobrecarga e focar nos mais relevantes
        # Apenas as duas primeiras opções são resolvidas, como no código original
        tasks = [resolve_stream_async(client, opt['url']) for opt in options[:2]]
        resolved_streams = await asyncio.gather(*tasks, return_exceptions=True)

        for res in resolved_streams:
            if isinstance(res, Exception):
                print(f"[Go Debug] Exceção durante a resolução de stream: {res}")
                continue
            
            # Certifica-se de que res é uma tupla (url, headers)
            if isinstance(res, tuple) and len(res) == 2:
                url, head = res
            else:
                # Se res não for uma tupla esperada, pode ser um caso de erro não capturado
                print(f"[Go Debug] Resultado inesperado da resolução de stream: {res}")
                continue

            if url and ('master.txt' in url or '.m3u8' in url or 'mediafire' in url or '.mp4' in url):
                print(f"[Go Debug] Stream extraído com sucesso: {url[:60]}...")
                entry = {
                    'name': 'FenixFlix',
                    'title': 'Leg/Dub\nGO',
                    'url': url,
                }
                if head:
                    entry['behaviorHints'] = {'proxyHeaders': {'request': head}}
                results.append(entry)

    return results

# Exemplo de uso (para testes)
async def main():
    # Teste para série
    titles = ["Silo"]
    content_type = "series"
    season = "1"
    episode = "1"

    # Teste para filme
    # titles = ["Devoradores de Estrelas"]
    # content_type = "movie"
    # season = None
    # episode = None

    streams = await search_serve(titles, content_type, season, episode)
    if streams:
        print("Streams encontrados:")
        for stream in streams:
            print(json.dumps(stream, indent=2))
    else:
        print("Nenhum stream encontrado.")

if __name__ == '__main__':
    asyncio.run(main())
