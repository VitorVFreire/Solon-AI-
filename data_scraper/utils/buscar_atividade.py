import requests

URL_BASE = 'https://open.cnpja.com/office/'

def buscar_atividade(cnpj) -> (list, str):
    try:
        resposta = requests.get(f'{URL_BASE}{cnpj}')
        resposta.raise_for_status()
        dados = resposta.json()
        atividades = []
        atividades.append(dados['mainActivity']['text'])
        for atividade in dados['sideActivities']:
            if atividade['text'] is not None:
                atividades.append(atividade['text'])
        return atividades, dados['company']['equity']
    except requests.exceptions.HTTPError as http_err:
        print(f"Erro HTTP para o CNPJ {cnpj}: {http_err}")
        return [], ''
    except requests.exceptions.RequestException as err:
        print(f"Erro na requisição para o CNPJ {cnpj}: {err}")
        return [], ''
    except ValueError as json_err:
        print(f"Erro ao processar JSON para o CNPJ {cnpj}: {json_err}")
        return [], ''