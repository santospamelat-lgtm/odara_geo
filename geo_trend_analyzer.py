import pandas as pd
from pytrends.request import TrendReq
from datetime import datetime, timedelta
import json
import logging
from typing import Dict, List
import time

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GeoBeautyTrendAnalyzer:
    """
    Classe para analisar dados geolocationalizados do Google Trends
    por bairros de São Paulo focado no setor de beleza
    """
    
    # Palavras-chave do setor de beleza
    BEAUTY_KEYWORDS = {
        'Procedimentos invasivos': ['botox', 'preenchimento labial', 'plástica facial', 'lifting'],
        'Estética geral': ['estética', 'rejuvenescimento facial', 'peeling químico', 'microagulhamento'],
        'Cabelos': ['implante capilar', 'transplante capilar', 'tratamento queda cabelo'],
        'Pele': ['dermatologia', 'acne treatment', 'limpeza de pele profunda', 'hidroxiácidos'],
        'Depilação': ['depilação a laser', 'depilação definitiva', 'eletrólise'],
        'Tatuagem e arte': ['tatuagem', 'remoção tatuagem', 'micropigmentação sobrancelha'],
        'Manicure/Pedicure': ['unhas de gel', 'alongamento de unhas', 'esmaltação em gel'],
        'Cosméticos': ['produtos skincare', 'cuidados com pele', 'cosméticos naturais'],
    }
    
    # Bairros de São Paulo
    SP_NEIGHBORHOODS = {
        'Jabaquara': 'São Paulo - Jabaquara',
        'Pinheiros': 'São Paulo - Pinheiros',
        'Vila Mariana': 'São Paulo - Vila Mariana',
        'Santo Amaro': 'São Paulo - Santo Amaro',
        'Tatuapé': 'São Paulo - Tatuapé',
        'Mooca': 'São Paulo - Mooca',
        'Bom Retiro': 'São Paulo - Bom Retiro',
        'Consolação': 'São Paulo - Consolação',
        'Higienópolis': 'São Paulo - Higienópolis',
        'Liberdade': 'São Paulo - Liberdade',
        'Vila Prudente': 'São Paulo - Vila Prudente',
        'Campo Limpo': 'São Paulo - Campo Limpo',
        'Itaim Bibi': 'São Paulo - Itaim Bibi',
        'Brooklin': 'São Paulo - Brooklin',
        'Saúde': 'São Paulo - Saúde',
        'Santana': 'São Paulo - Santana',
        'Vila Madalena': 'São Paulo - Vila Madalena',
        'Lapa': 'São Paulo - Lapa',
        'Vila Olímpia': 'São Paulo - Vila Olímpia',
        'Perdizes': 'São Paulo - Perdizes',
    }
    
    def __init__(self):
        self.pytrends = TrendReq(hl='pt-BR', tz=360)
        self.results = []
        
    def search_beauty_trends(
        self, 
        keyword: str, 
        timeframe: str = 'today 3m'
    ) -> Dict:
        """
        Busca dados de trends de beleza
        
        Args:
            keyword: Palavra-chave de beleza para buscar
            timeframe: Período de busca
            
        Returns:
            Dicionário com dados dos trends
        """
        try:
            self.pytrends.build_payload(
                [keyword],
                timeframe=timeframe,
                geo='BR'
            )
            
            # Obter dados de interesse ao longo do tempo
            interest_over_time = self.pytrends.interest_over_time()
            
            # Obter dados por localização
            interest_by_region = self.pytrends.interest_by_region()
            
            # Pequena pausa para não sobrecarregar a API
            time.sleep(1)
            
            return {
                'keyword': keyword,
                'interest_over_time': interest_over_time,
                'interest_by_region': interest_by_region,
                'timestamp': datetime.now().isoformat()
            }
        
        except Exception as e:
            logger.error(f"Erro ao buscar trends para '{keyword}': {str(e)}")
            return None
    
    def analyze_neighborhood_beauty_search(
        self, 
        keyword: str,
        search_volume: int = 100
    ) -> Dict:
        """
        Analisa dados de busca de beleza para um bairro específico
        
        Args:
            keyword: Palavra-chave de beleza
            search_volume: Volume de buscas
            
        Returns:
            Dados analisados
        """
        logger.info(f"Analisando '{keyword}' em São Paulo")
        
        trend_data = self.search_beauty_trends(keyword)
        
        if trend_data is None:
            return None
        
        # Calcular estatísticas
        interest_series = trend_data['interest_over_time'][keyword]
        
        result = {
            'keyword': keyword,
            'search_volume': search_volume,
            'interest_score': float(interest_series.mean()),
            'max_interest': int(interest_series.max()),
            'trend_direction': self._calculate_trend_direction(interest_series),
            'popularity': self._classify_popularity(interest_series.mean()),
            'data': trend_data
        }
        
        self.results.append(result)
        return result
    
    def _calculate_trend_direction(self, series) -> str:
        """Calcula a direção da tendência"""
        if len(series) < 2:
            return 'dados insuficientes'
        
        first_half_mean = series.iloc[:len(series)//2].mean()
        second_half_mean = series.iloc[len(series)//2:].mean()
        
        if second_half_mean > first_half_mean * 1.15:
            return 'em alta 📈'
        elif second_half_mean < first_half_mean * 0.85:
            return 'em queda 📉'
        else:
            return 'estável ➡️'
    
    def _classify_popularity(self, score: float) -> str:
        """Classifica o nível de popularidade""" 
        if score >= 75:
            return 'Muito popular ⭐⭐⭐⭐⭐'
        elif score >= 50:
            return 'Popular ⭐⭐⭐⭐'
        elif score >= 25:
            return 'Moderado ⭐⭐⭐'
        else:
            return 'Baixo ⭐'
    
    def search_multiple_beauty_keywords(
        self, 
        keywords: List[str] = None
    ) -> List[Dict]:
        """
        Busca múltiplas palavras-chave de beleza
        
        Args:
            keywords: Lista de palavras-chave (se None, usa as padrões)
            
        Returns:
            Lista de resultados
        """
        if keywords is None:
            # Usar palavras-chave padrão
            keywords = ['botox', 'estética', 'preenchimento labial', 'implante capilar', 
                       'rejuvenescimento facial', 'micropigmentação', 'peeling', 'dermatologia']
        
        all_results = []
        
        for idx, keyword in enumerate(keywords, 1):
            logger.info(f"[{idx}/{len(keywords)}] Buscando '{keyword}'...")
            result = self.analyze_neighborhood_beauty_search(keyword)
            
            if result:
                all_results.append(result)
                logger.info(f"✓ {keyword}: {result['trend_direction']}")
        
        return all_results
    
    def get_top_beauty_trends(self, top_n: int = 10) -> List[Dict]:
        """
        Retorna as top N palavras-chave de beleza mais buscadas
        
        Args:
            top_n: Número de top resultados
            
        Returns:
            Lista ordenada dos top trends
        """
        if not self.results:
            logger.warning("Nenhum resultado disponível")
            return []
        
        sorted_results = sorted(
            self.results, 
            key=lambda x: x['interest_score'], 
            reverse=True
        )
        
        return sorted_results[:top_n]
    
    def export_to_csv(self, filename: str = 'beauty_trends_analysis.csv') -> str:
        """Exporta resultados para CSV"""
        if not self.results:
            logger.warning("Nenhum resultado para exportar")
            return None
        
        export_data = []
        for result in self.results:
            export_data.append({
                'Palavra-chave': result['keyword'],
                'Volume de Busca': result['search_volume'],
                'Score de Interesse': round(result['interest_score'], 2),
                'Interesse Máximo': result['max_interest'],
                'Tendência': result['trend_direction'],
                'Popularidade': result['popularity'],
                'Data': result['data']['timestamp']
            })
        
        df = pd.DataFrame(export_data)
        df = df.sort_values('Score de Interesse', ascending=False)
        df.to_csv(filename, index=False, encoding='utf-8')
        
        logger.info(f"✓ Dados exportados para {filename}")
        return filename
    
    def export_to_json(self, filename: str = 'beauty_trends_analysis.json') -> str:
        """Exporta resultados para JSON"""
        if not self.results:
            logger.warning("Nenhum resultado para exportar")
            return None
        
        json_results = []
        for result in self.results:
            json_result = {
                'keyword': result['keyword'],
                'search_volume': result['search_volume'],
                'interest_score': float(result['interest_score']),
                'max_interest': result['max_interest'],
                'trend_direction': result['trend_direction'],
                'popularity': result['popularity'],
                'timestamp': result['data']['timestamp']
            }
            json_results.append(json_result)
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(json_results, f, ensure_ascii=False, indent=2)
        
        logger.info(f"✓ Dados exportados para {filename}")
        return filename
    
    def print_report(self):
        """Imprime relatório formatado dos resultados"""
        if not self.results:
            print("Nenhum resultado disponível")
            return
        
        print("\n" + "="*70)
        print("RELATÓRIO DE TENDÊNCIAS DE BELEZA - SÃO PAULO")
        print("="*70 + "\n")
        
        sorted_results = sorted(
            self.results,
            key=lambda x: x['interest_score'],
            reverse=True
        )
        
        for idx, result in enumerate(sorted_results, 1):
            print(f"{idx}. {result['keyword'].upper()}")
            print(f"   Score de Interesse: {result['interest_score']:.2f}")
            print(f"   Interesse Máximo: {result['max_interest']}/100")
            print(f"   Tendência: {result['trend_direction']}")
            print(f"   Popularidade: {result['popularity']}")
            print() 


# Exemplo de uso
if __name__ == "__main__":
    # Inicializar analisador
    analyzer = GeoBeautyTrendAnalyzer()
    
    print("🔍 Iniciando análise de trends de beleza em São Paulo...\n")
    
    # Buscar palavras-chave de beleza
    results = analyzer.search_multiple_beauty_keywords()
    
    # Imprimir relatório
    analyzer.print_report()
    
    # Obter top 5
    print("\n🏆 TOP 5 TENDÊNCIAS DE BELEZA:")
    print("-" * 70)
    top_trends = analyzer.get_top_beauty_trends(5)
    for idx, trend in enumerate(top_trends, 1):
        print(f"{idx}. {trend['keyword']}: {trend['interest_score']:.2f} pontos")
    
    # Exportar resultados
    print("\n📊 Exportando resultados...")
    analyzer.export_to_csv('analise_beleza_sp.csv')
    analyzer.export_to_json('analise_beleza_sp.json')
    
    print("\n✅ Análise concluída!")