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

        /* Força a cor do texto para branco em todo o app */
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
        
        /* Ajusta o fundo dos widgets para combinar com o tema escuro */
        div[data-baseweb="select"] > div,
        div[data-baseweb="radio"] > div {
            background-color: #262730;
        }
    </style>
    """,
    unsafe_allow_html=True
)

st.markdown("# Solon AI - Análise de Setores")
st.write("Esta demo interativa permite analisar o score de diferentes setores de investimento ao longo do tempo.")

@st.cache_data
def get_setores():
    SETORES_URL = 'http://localhost:8000/setores'
    setores = requests.get(SETORES_URL).json()
    df = pd.DataFrame(setores)
    return df.set_index('setores')

@st.cache_data
def get_datas(setor, perfil, tipo):
    SETORES_URL = f'http://localhost:8000/{setor}?perfil={perfil}&tipo={tipo}'
    dados_setores = requests.get(SETORES_URL).json()
    if not dados_setores or "data" not in dados_setores or not dados_setores["data"]:
        return pd.DataFrame()
    df = pd.DataFrame(dados_setores["data"])
    df["setor"] = setor
    df["periodo"] = pd.to_datetime(df["periodo"])
    return df

@st.cache_data
def get_datas_news(setor, perfil):
    URL = f'http://localhost:8000/news/setor/{setor}&{perfil}'
    response = requests.get(URL)
    response.raise_for_status()
    dados_setor = response.json()
    if not dados_setor or "data" not in dados_setor or not dados_setor["data"]:
        return pd.DataFrame()
    df = pd.DataFrame(dados_setor["data"])
    if 'Data' in df.columns:
        df['Data'] = pd.to_datetime(df['Data'])
    return df

try:
    df_setores = get_setores()
    
    setores_selecionados = st.multiselect(
        'Escolha os Setores', list(df_setores.index), ['Agricultura']
    )

    col1, col2 = st.columns(2, gap="large")

    with col1:
        perfil = st.radio(
            "Selecione o seu perfil de investidor:",
            ('Conservador', 'Moderado', 'Agressivo'),
            index=0,
            horizontal=True
        )

    with col2:
        tipo = st.radio(
            "Selecione a granularidade:",
            ("Diario", "Mensal"),
            index=0,
            horizontal=True
        )
        
    if not setores_selecionados:
        st.error('Por favor, selecione pelo menos um setor!')
    elif not perfil:
        st.warning('Por favor, selecione um perfil de investidor para continuar.')
    else:
        data_full = pd.concat([get_datas(setor, perfil, tipo) for setor in setores_selecionados], ignore_index=True)
        news_full = pd.concat([get_datas_news(setor, perfil) for setor in setores_selecionados], ignore_index=True)

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

        data_filtered = data_full[(data_full['periodo'] >= start_ts) & (data_full['periodo'] <= end_ts)]
        
        if 'Data' in news_full.columns:
            news_filtered = news_full[(news_full['Data'] >= start_ts) & (news_full['Data'] <= end_ts)]
        else:
            news_filtered = news_full

        if data_filtered.empty:
            st.warning("Não há dados disponíveis para o período selecionado.")
        else:
            if tipo == "Mensal":
                x_format = "%b/%Y"
                chart_data = (
                    data_filtered.groupby('setor')
                    .resample('M', on='periodo')['score']
                    .mean()
                    .reset_index()
                )
            else:
                x_format = "%d/%m/%Y"
                chart_data = data_filtered

            line_chart = (
                alt.Chart(chart_data)
                .mark_line(point=True, strokeWidth=3)
                .encode(
                    x=alt.X(
                        "periodo:T",
                        axis=alt.Axis(format=x_format, title="Período", grid=True, gridColor="#444444"),
                    ),
                    y=alt.Y(
                        "score:Q",
                        title="Score",
                        axis=alt.Axis(grid=True, gridColor="#444444"),
                        scale=alt.Scale(domain=[0, 5])
                    ),
                    color=alt.Color(
                        "setor:N",
                        scale=alt.Scale(scheme='tableau10'),
                        legend=alt.Legend(title="Setores", orient="bottom")
                    ),
                    tooltip=[
                        alt.Tooltip("periodo:T", title="Data", format=x_format),
                        alt.Tooltip("score:Q", title="Score", format=".2f"),
                        alt.Tooltip("setor:N", title="Setor")
                    ]
                )
            )
            
            chart = (
                line_chart
                .interactive()
                .properties(
                    title=alt.TitleParams(
                        text=f'Score {tipo.capitalize()} por Setor',
                        anchor='middle',
                        color='white',
                        fontSize=20
                    ),
                    height=500,
                    background="#0e1117",
                )
            ).configure_axis(
                labelColor='white',
                titleColor='white'
            ).configure_legend(
                labelColor='white',
                titleColor='white'
            )

            st.altair_chart(chart, use_container_width=True)
            
            st.markdown("### Notícias Relevantes no Período")
            st.dataframe(
                news_filtered, 
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
except requests.exceptions.ConnectionError as e:
    st.error(f"Não foi possível conectar à API. Verifique se o servidor local está rodando em http://localhost:8000. Erro: {e}")
except Exception as e:
    st.error(f"Ocorreu um erro inesperado: {e}")