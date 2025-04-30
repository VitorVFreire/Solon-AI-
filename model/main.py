from src import *
from utils import *
import os
import logging

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Caminhos dos prompts
SYSTEM_PROMPT_PATH = 'prompts/analysis/system_analysis.md'
HUMAN_PROMPT_PATH = 'prompts/analysis/human_analysis.md'

# Verificar existência dos arquivos de prompt
if not os.path.exists(SYSTEM_PROMPT_PATH):
    raise FileNotFoundError(f"Arquivo de prompt de sistema não encontrado: {SYSTEM_PROMPT_PATH}")
if not os.path.exists(HUMAN_PROMPT_PATH):
    raise FileNotFoundError(f"Arquivo de prompt humano não encontrado: {HUMAN_PROMPT_PATH}")

def main():
    try:
        logger.info("Iniciando processamento de análise de notícias")
        
        # Configuração do cliente LLM
        logger.info("Configurando cliente LLM")
        config = setup_client('openAI')
        llm_client = AIClient(config)
    
        # Definindo pasta de saída
        output_dir = 'output/companies'
        os.makedirs(output_dir, exist_ok=True)
        
        # Inicialização do processador de notícias
        company_name = 'Tesla'
        logger.info(f"Inicializando processador de notícias para {company_name}")
        processor = NewsProcessor( 
            llm_client=llm_client, 
            system_prompt_file=SYSTEM_PROMPT_PATH, 
            human_prompt_file=HUMAN_PROMPT_PATH,
            company=company_name,
            output_dir=output_dir
        )
        
        # Dados de entrada para análise
        news_data = {
            "data_limite": '20/10/2022',
            "perfil": 'Moderado',
            "news": """
            O empresário Elon Musk anunciou nesta terça-feira que reduzirá o tempo que dedica ao Departamento de Eficiência Governamental (DOGE) do governo dos Estados Unidos para "um ou dois dias por semana" a partir de maio, para dar mais atenção à Tesla.

            Musk anunciou a decisão durante uma teleconferência com analistas do setor automotivo e depois que sua montadora de carros elétricos informou que seu lucro líquido caiu 71% no primeiro trimestre do ano, para US$ 409 milhões.

            O empresário acrescentou que, embora o trabalho que o DOGE esteja fazendo "seja muito importante" e que tenha conseguido "grande progresso na resposta ao desperdício e à fraude" no governo federal dos EUA, a maior parte do trabalho para estabelecer o departamento está concluída.

"A partir do próximo mês, o tempo que dedico a ele diminuirá significativamente", frisou.

"Acredito que continuarei trabalhando um ou dois dias por semana, desde que o presidente (Donald Trump) queira que eu o faça e desde que eu seja útil. Mas, a partir do próximo mês, vou dedicar muito mais tempo à Tesla", explicou.

Musk também relacionou as críticas e boicotes contra a Tesla a pessoas que lucram com o "desperdício" de dinheiro público e afirmou que "elas são muito organizadas e pagas" para participar dos protestos.

"Obviamente, elas não vão admitir que o motivo pelo qual estão protestando é que estão sendo pagas", disse.
            """
        }
        
        # Processamento das notícias
        logger.info(f"Iniciando processamento de notícias para {company_name}")
        result = processor.process_news(news_data)
        
        # Exibir resultado
        logger.info(f"Análise concluída para {company_name}")
        output_file = os.path.join(output_dir, f"{clean_filename(company_name)}.json")
        
        if "formatted_result" in result:
            print(f"\nResultado da análise para {company_name}:")
            print(result["formatted_result"])
            print(f"\nO resultado completo foi salvo em: {output_file}")
        else:
            logger.warning("Resultado não formatado corretamente.")
            print("Resultado não formatado corretamente. Verifique o arquivo de saída.")
            
    except FileNotFoundError as e:
        logger.error(f"Arquivo não encontrado: {str(e)}")
        print(f"Erro: {str(e)}")
    except Exception as e:
        logger.error(f"Erro na execução: {str(e)}", exc_info=True)
        print(f"Erro na execução: {str(e)}")

if __name__ == "__main__":
    main()