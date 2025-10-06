import streamlit as st
import pandas as pd
import altair as alt
from urllib.error import URLError
import requests
from datetime import date, datetime

st.set_page_config(layout="wide")

st.markdown(
    """
    <style>
        body {
            background-color: #0e1117;
            color: white;
            font-family: "Arial", sans-serif;
        }
        [data-testid="stAppViewContainer"] *,
        [data-testid="stHeader"] *,
        [data-testid="stSidebar"] * {
            color: white;
        }
        .stApp {
            background-color: #0e1117;
        }
        h1, h2, h3, h4 {
            text-align: center;
        }
        div[data-baseweb="select"] > div,
        div[data-baseweb="radio"] > div {
            background-color: #262730;
        }
    </style>
    """,
    unsafe_allow_html=True
)

st.markdown("# Solon AI - Análise de Empresas")
st.write("Esta demo interativa permite analisar o score de diferentes empresas de investimento ao longo do tempo.")

@st.cache_data
def get_companies():
    COMPANIES_URL = 'http://localhost:8000/companies'
    companies = requests.get(COMPANIES_URL).json()
    df = pd.DataFrame(companies['data'])
    return df

@st.cache_data
def get_datas(company, perfil, tipo):
    COMPANIES_URL = f'http://localhost:8000/company/{company}?perfil={perfil}&tipo={tipo}'
    try:
        response = requests.get(COMPANIES_URL)
        response.raise_for_status()
        dados_companies = response.json()
    except requests.RequestException:
        return pd.DataFrame(columns=["periodo", "score", "company"])
    
    if not dados_companies or "data" not in dados_companies or not dados_companies["data"]:
        return pd.DataFrame(columns=["periodo", "score", "company"])
    
    df = pd.DataFrame(dados_companies["data"])
    df["company"] = company
    df['periodo'] = pd.to_datetime(df['periodo'], format="%m/%d/%Y", errors='coerce')
    return df

@st.cache_data
def get_datas_b3(symbol: str, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    SYMBOL_URL = f'http://localhost:8000/b3/{symbol}/{start_date_str}/{end_date_str}'
    
    try:
        response = requests.get(SYMBOL_URL, timeout=10)
        response.raise_for_status()
        dados_companies = response.json()
    except requests.RequestException as e:
        st.error(f"Erro ao buscar dados da B3: {e}")
        return pd.DataFrame(columns=["periodo", "close", "symbol"])

    df = pd.DataFrame(dados_companies.get("data", []))
    if df.empty:
        return pd.DataFrame(columns=["periodo", "close", "symbol"])

    df["symbol"] = symbol
    df['periodo'] = pd.to_datetime(df['periodo'], format="%m/%d/%Y", errors='coerce')
    return df

@st.cache_data
def get_datas_news(company, perfil):
    URL = f'http://localhost:8000/news/company/{company}&{perfil}'
    try:
        response = requests.get(URL)
        response.raise_for_status()
        dados_company = response.json()
    except Exception:
        return pd.DataFrame()
    
    if not dados_company or "data" not in dados_company or not dados_company["data"]:
        return pd.DataFrame()
    
    df = pd.DataFrame(dados_company["data"])
    if 'Data' in df.columns:
        df['Data'] = pd.to_datetime(df['Data'], format="%m/%d/%Y", errors='coerce')
    return df

try:
    df_companies = get_companies()
    
    company = st.selectbox(
        'Escolha a Empresa', list(df_companies.set_index('companies').index)
    )
    
    symbol = None
    if company:
        symbol_list = df_companies.loc[df_companies['companies'] == company, 'symbol'].iloc[0]
        if symbol_list:
            symbol = symbol_list[0]
        else:
            st.error(f"Nenhum símbolo (ticker) encontrado para a empresa {company}.")
            st.stop()

    col1, col2 = st.columns(2, gap="large")
    with col1:
        perfis = st.multiselect(
            "Selecione o seu perfil de investidor:",
            ('Conservador', 'Moderado', 'Agressivo')
        )
    with col2:
        tipo = st.radio(
            "Selecione a granularidade:",
            ("Diario", "Mensal"),
            index=0,
            horizontal=True
        )
        
    if not company:
        st.error('Por favor, selecione pelo menos uma empresa!')
    elif not perfis:
        st.warning('Por favor, selecione um perfil de investidor para continuar.')
    else:
        data_full = pd.concat([get_datas(company, perfil, tipo) for perfil in perfis], ignore_index=True)
        news_full = pd.concat([get_datas_news(company, perfil) for perfil in perfis], ignore_index=True)

        if data_full.empty:
            st.warning("Não foram encontrados dados de score para a empresa e perfis selecionados.")
            st.stop()

        min_date = data_full["periodo"].min().date()
        max_date = data_full["periodo"].max().date()
        st.markdown("---")

        start_date, end_date = st.slider(
            "Selecione o intervalo de datas para análise:",
            min_value=min_date,
            max_value=max_date,
            value=(min_date, max_date),
            format="DD/MM/YYYY"
        )
        st.markdown("---")

        start_ts = pd.to_datetime(start_date)
        end_ts = pd.to_datetime(end_date)
        
        data_full_b3 = get_datas_b3(symbol, start_ts, end_ts)
        data_filtered = data_full[(data_full['periodo'] >= start_ts) & (data_full['periodo'] <= end_ts)]
        
        if data_filtered.empty and data_full_b3.empty:
            st.warning("Não há dados disponíveis para o período selecionado.")
        else:
            score_chart = None
            cotacao_chart = None
            x_format = "%d/%m/%Y" if tipo == "Diario" else "%b/%Y"

            if not data_filtered.empty:
                chart_data = data_filtered
                perfil_colors = {"Conservador": "#1f77b4", "Moderado": "#ff7f0e", "Agressivo": "#2ca02c"}

                score_chart = alt.Chart(chart_data).mark_line(point=True, strokeWidth=2).encode(
                    x=alt.X("periodo:T", axis=alt.Axis(format=x_format, title="Período", grid=True, gridColor="#444")),
                    y=alt.Y("score:Q", title="Score", scale=alt.Scale(domain=[0, 5])),
                    color=alt.Color("perfil:N", scale=alt.Scale(domain=list(perfil_colors.keys()), range=list(perfil_colors.values())), legend=alt.Legend(title="Perfil", orient="bottom")),
                    tooltip=[
                        alt.Tooltip("periodo:T", title="Data", format=x_format),
                        alt.Tooltip("score:Q", title="Score", format=".2f"),
                        alt.Tooltip("perfil:N", title="Perfil")
                    ]
                )

            if not data_full_b3.empty:
                chart_data2 = data_full_b3
                min_cotacao = chart_data2['close'].min()
                max_cotacao = chart_data2['close'].max()
                padding = (max_cotacao - min_cotacao) * 0.1
                y_domain = [min_cotacao - padding, max_cotacao + padding]

                cotacao_chart = alt.Chart(chart_data2).mark_line(point=True, strokeWidth=2, color="#d62728").encode(
                    x=alt.X("periodo:T"),
                    y=alt.Y("close:Q", title="Cotação (R$)", axis=alt.Axis(titleColor="#d62728"), scale=alt.Scale(domain=y_domain)),
                    tooltip=[
                        alt.Tooltip("periodo:T", title="Data", format=x_format),
                        alt.Tooltip("close:Q", title="Cotação", format=".2f")
                    ]
                )

            if score_chart and cotacao_chart:
                combined_chart = alt.layer(score_chart, cotacao_chart).resolve_scale(y='independent')
            else:
                combined_chart = score_chart if score_chart is not None else cotacao_chart

            st.altair_chart(
                combined_chart.properties(
                    title=alt.TitleParams(
                        text=f"Score × Cotação ({symbol}) - {tipo.capitalize()}",
                        anchor='middle', color='white', fontSize=20
                    ),
                    height=500, background="#0e1117"
                ).configure_axis(
                    labelColor='white', titleColor='white'
                ).configure_legend(
                    labelColor='white', titleColor='white'
                ),
                use_container_width=True
            )

        st.markdown("### Notícias Relevantes no Período")
        st.dataframe(
            news_full, 
            column_config={
                "URL": st.column_config.LinkColumn(
                    "Link da Notícia",
                    display_text="Acessar ▸"
                ),
                "Data": st.column_config.DatetimeColumn(
                    "Data",
                    format="DD/MM/YYYY"
                )
            },
            use_container_width=True,
            hide_index=True
        )
            
except URLError as e:
    st.error(f"Erro de conexão: {e.reason}")
except requests.exceptions.ConnectionError:
    st.error("Não foi possível conectar à API. Verifique se o servidor local está rodando em http://localhost:8000.")
except Exception as e:
    st.error(f"Ocorreu um erro inesperado: {e}")